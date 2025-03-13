# /tests/test_backup_engine.py
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock, ANY, call

from dbsavr.backup_engine import BackupEngine
from dbsavr.config import DatabaseConfig

@pytest.fixture
def mysql_config():
    """Create a sample MySQL configuration for testing."""
    return DatabaseConfig(
        type="mysql",
        host="localhost",
        port=3306,
        username="test_user",
        password="test_password",
        database="test_database",
        options={"extra_args": ["--skip-triggers"]}
    )

@pytest.fixture
def postgresql_config():
    """Create a sample PostgreSQL configuration for testing."""
    return DatabaseConfig(
        type="postgresql",
        host="localhost",
        port=5432,
        username="test_user",
        password="test_password",
        database="test_database",
        options={"extra_args": ["--exclude-table=temp_logs"]}
    )

@pytest.fixture
def mongodb_config():
    """Create a sample MongoDB configuration for testing."""
    return DatabaseConfig(
        type="mongodb",
        host="localhost",
        port=27017,
        username="test_user",
        password="test_password",
        database="test_database"
    )

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
def test_backup_mysql(mock_tempfile, mock_popen, mysql_config):
    """Test MySQL backup functionality with detailed verification of commands."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    
    # Mock mysqldump process
    mysqldump_mock = MagicMock()
    mysqldump_mock.returncode = 0
    mysqldump_mock.communicate.return_value = (b'', b'')
    mysqldump_mock.stdout = MagicMock()
    
    # Mock gzip process
    gzip_mock = MagicMock()
    gzip_mock.returncode = 0
    gzip_mock.communicate.return_value = (b'', b'')
    
    # Configure mock_popen to return different mocks based on command
    mock_popen.side_effect = [mysqldump_mock, gzip_mock]
    
    # Call the method
    with patch('builtins.open', MagicMock()):
        path, filename = BackupEngine._backup_mysql(mysql_config, "20250101_120000")
    
    # Assertions
    assert path.startswith("/tmp/")
    assert path.endswith(".sql.gz")
    assert "test_database_20250101_120000" in filename
    
    # Verify mysqldump command in detail
    assert len(mock_popen.call_args_list) >= 2
    # First call should be mysqldump
    mysqldump_call = mock_popen.call_args_list[0]
    
    # Verify all expected arguments are present
    mysqldump_args = mysqldump_call[0][0]
    assert "mysqldump" in mysqldump_args[0]
    assert f"--host={mysql_config.host}" in mysqldump_args
    assert f"--port={mysql_config.port}" in mysqldump_args
    assert f"--user={mysql_config.username}" in mysqldump_args
    assert "--single-transaction" in mysqldump_args
    assert "--routines" in mysqldump_args
    assert "--triggers" in mysqldump_args
    assert "--events" in mysqldump_args
    assert "--skip-triggers" in mysqldump_args
    assert mysql_config.database in mysqldump_args
    
    # Verify password is NOT in command arguments (security)
    for arg in mysqldump_args:
        assert mysql_config.password not in arg
    
    # Verify environment has password set correctly
    env = mysqldump_call[1].get('env', {})
    assert 'MYSQL_PWD' in env
    assert env['MYSQL_PWD'] == mysql_config.password
    
    # Verify gzip command
    gzip_call = mock_popen.call_args_list[1]
    assert gzip_call[0][0][0] == 'gzip'

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
@patch('dbsavr.backup_engine.os.environ.copy')
def test_backup_postgresql(mock_environ, mock_tempfile, mock_popen, postgresql_config):
    """Test PostgreSQL backup functionality."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    mock_environ.return_value = {"PATH": "/usr/bin"}
    
    # Mock pg_dump process
    pg_dump_mock = MagicMock()
    pg_dump_mock.returncode = 0
    pg_dump_mock.communicate.return_value = (b'', b'')
    pg_dump_mock.stdout = MagicMock()
    
    # Mock gzip process
    gzip_mock = MagicMock()
    gzip_mock.returncode = 0
    gzip_mock.communicate.return_value = (b'', b'')
    
    # Configure mock_popen to return different mocks based on command
    mock_popen.side_effect = [pg_dump_mock, gzip_mock]
    
    # Call the method
    with patch('builtins.open', MagicMock()):
        path, filename = BackupEngine._backup_postgresql(postgresql_config, "20250101_120000")
    
    # Assertions
    assert path.startswith("/tmp/")
    assert path.endswith(".sql.gz")
    assert "test_database_20250101_120000" in filename
    
    # Check if pg_dump was called with correct args
    calls = mock_popen.call_args_list
    assert len(calls) >= 1
    pg_dump_args = calls[0][0][0]
    assert "pg_dump" in pg_dump_args[0]
    assert "--host=localhost" in pg_dump_args
    assert "--username=test_user" in pg_dump_args
    assert "--exclude-table=temp_logs" in pg_dump_args
    
    # Check if environment had password set
    env = calls[0][1]['env']
    assert "PGPASSWORD" in env
    assert env["PGPASSWORD"] == "test_password"
    
    # Check if gzip was called
    assert "gzip" in calls[1][0][0][0]

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.subprocess.run')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
@patch('dbsavr.backup_engine.os.makedirs')
@patch('dbsavr.backup_engine.shutil.rmtree')
def test_backup_mongodb(mock_rmtree, mock_makedirs, mock_tempfile, mock_run, mock_popen, mongodb_config):
    """Test MongoDB backup functionality."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    
    # Mock mongodump process
    mongodump_mock = MagicMock()
    mongodump_mock.returncode = 0
    mongodump_mock.communicate.return_value = (b'', b'')
    
    # Mock tar process
    tar_mock = MagicMock()
    tar_mock.returncode = 0
    tar_mock.communicate.return_value = (b'', b'')
    
    # Configure mock_popen to return different mocks
    mock_popen.side_effect = [mongodump_mock, tar_mock]
    
    # Call the method
    path, filename = BackupEngine._backup_mongodb(mongodb_config, "20250101_120000")
    
    # Assertions
    assert path.startswith("/tmp/")
    assert path.endswith(".tar.gz")
    assert "test_database_20250101_120000" in filename
    
    # Check if mongodump was called with correct args
    calls = mock_popen.call_args_list
    assert len(calls) >= 1
    mongodump_args = calls[0][0][0]
    assert "mongodump" in mongodump_args[0]
    assert f"--host={mongodb_config.host}" in mongodump_args
    assert f"--username={mongodb_config.username}" in mongodump_args
    assert f"--db={mongodb_config.database}" in mongodump_args
    
    # Check if tar was called
    tar_args = calls[1][0][0]
    assert "tar" in tar_args[0]
    assert "-czf" in tar_args
    
    # Verify temporary directory cleanup was called
    mock_rmtree.assert_called_once_with(ANY, ignore_errors=True)

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
@patch('dbsavr.backup_engine.os.unlink')
def test_backup_mysql_failure_cleanup(mock_unlink, mock_tempfile, mock_popen, mysql_config):
    """Test file cleanup when MySQL backup fails."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    expected_path = "/tmp/test_database_20250101_120000.sql.gz"
    
    # Create a mock file path checker
    def path_exists_mock(path):
        return path == expected_path
    
    # Mock mysqldump process that fails
    mysqldump_mock = MagicMock()
    mysqldump_mock.returncode = 1  # Failure
    mysqldump_mock.communicate.return_value = (b'', b'Error: command failed')
    mysqldump_mock.stdout = MagicMock()
    
    # Configure mock_popen to return failed process
    mock_popen.return_value = mysqldump_mock
    
    # Call the method and expect exception
    with patch('builtins.open', MagicMock()):
        with patch('os.path.exists', side_effect=path_exists_mock):
            with pytest.raises(Exception) as excinfo:
                BackupEngine._backup_mysql(mysql_config, "20250101_120000")
    
    # Verify temporary file was cleaned up
    mock_unlink.assert_called_once_with(expected_path)
    assert "mysqldump failed" in str(excinfo.value)

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
@patch('dbsavr.backup_engine.os.environ.copy')
@patch('dbsavr.backup_engine.os.unlink')
def test_backup_postgresql_failure_cleanup(mock_unlink, mock_environ, mock_tempfile, mock_popen, postgresql_config):
    """Test file cleanup when PostgreSQL backup fails."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    mock_environ.return_value = {"PATH": "/usr/bin"}
    expected_path = "/tmp/test_database_20250101_120000.sql.gz"
    
    # Create a mock file path checker
    def path_exists_mock(path):
        return path == expected_path
    
    # Mock pg_dump process that fails
    pg_dump_mock = MagicMock()
    pg_dump_mock.returncode = 1  # Failure
    pg_dump_mock.communicate.return_value = (b'', b'Error: command failed')
    pg_dump_mock.stdout = MagicMock()
    
    # Configure mock_popen to return failed process
    mock_popen.return_value = pg_dump_mock
    
    # Call the method and expect exception
    with patch('builtins.open', MagicMock()):
        with patch('os.path.exists', side_effect=path_exists_mock):
            with pytest.raises(Exception) as excinfo:
                BackupEngine._backup_postgresql(postgresql_config, "20250101_120000")
    
    # Verify temporary file was cleaned up
    mock_unlink.assert_called_once_with(expected_path)
    assert "pg_dump failed" in str(excinfo.value)

@patch('dbsavr.backup_engine.subprocess.Popen')
@patch('dbsavr.backup_engine.tempfile.gettempdir')
@patch('dbsavr.backup_engine.os.makedirs')
@patch('dbsavr.backup_engine.shutil.rmtree')
@patch('dbsavr.backup_engine.os.unlink')
def test_backup_mongodb_failure_cleanup(mock_unlink, mock_rmtree, mock_makedirs, mock_tempfile, mock_popen, mongodb_config):
    """Test directory and file cleanup when MongoDB backup fails."""
    # Setup
    mock_tempfile.return_value = "/tmp"
    backup_dir = "/tmp/mongodb_backup_20250101_120000"
    archive_path = "/tmp/test_database_20250101_120000.tar.gz"
    
    # Create mock path checkers
    def dir_exists_mock(path):
        return path == backup_dir
    
    def file_exists_mock(path):
        return path == archive_path
    
    # Mock mongodump process that fails
    mongodump_mock = MagicMock()
    mongodump_mock.returncode = 1  # Failure
    mongodump_mock.communicate.return_value = (b'', b'Error: command failed')
    
    # Configure mock_popen to return failed process
    mock_popen.return_value = mongodump_mock
    
    # Call the method and expect exception
    with patch('os.path.exists', side_effect=[dir_exists_mock, file_exists_mock]):
        with pytest.raises(Exception) as excinfo:
            BackupEngine._backup_mongodb(mongodb_config, "20250101_120000")
    
    # Verify temporary directory was cleaned up
    mock_rmtree.assert_called_once_with(backup_dir, ignore_errors=True)
    
    # Verify temporary archive was cleaned up if created
    mock_unlink.assert_called_once_with(archive_path)
    assert "mongodump failed" in str(excinfo.value)

def test_unsupported_database_type():
    """Test that an unsupported database type raises a ValueError."""
    config = DatabaseConfig(
        type="unsupported",
        host="localhost",
        port=1234,
        username="test_user",
        password="test_password",
        database="test_database"
    )
    
    with pytest.raises(ValueError, match="Unsupported database type"):
        BackupEngine.backup_database(config)