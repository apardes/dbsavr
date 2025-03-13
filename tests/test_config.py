# /tests/test_config.py
import os
import tempfile
import pytest
import yaml

from dbsavr.config import load_config, Config, DatabaseConfig, S3Config, BackupSchedule

@pytest.fixture
def sample_config_file():
    """Create a sample config file for testing."""
    config_data = {
        "databases": {
            "test_db": {
                "type": "postgresql",
                "host": "localhost",
                "port": 5432,
                "username": "test_user",
                "password": "test_password",
                "database": "test_database"
            }
        },
        "s3": {
            "bucket_name": "test-bucket",
            "prefix": "test-prefix",
            "region": "us-east-1"
        },
        "schedules": [
            {
                "database_name": "test_db",
                "cron_expression": "0 0 * * *",
                "retention_days": 7
            }
        ],
        "log_level": "INFO",
        "notifications_email": "test@example.com"
    }
    
    # Create temporary file
    fd, path = tempfile.mkstemp(suffix=".yaml")
    with os.fdopen(fd, 'w') as f:
        yaml.dump(config_data, f)
    
    yield path
    
    # Clean up
    os.unlink(path)

def test_load_config(sample_config_file):
    """Test that loading a config file works correctly."""
    config = load_config(sample_config_file)
    
    assert isinstance(config, Config)
    assert len(config.databases) == 1
    assert "test_db" in config.databases
    
    db_config = config.databases["test_db"]
    assert isinstance(db_config, DatabaseConfig)
    assert db_config.type == "postgresql"
    assert db_config.host == "localhost"
    assert db_config.port == 5432
    assert db_config.username == "test_user"
    assert db_config.password == "test_password"
    assert db_config.database == "test_database"
    
    assert isinstance(config.s3, S3Config)
    assert config.s3.bucket_name == "test-bucket"
    assert config.s3.prefix == "test-prefix"
    assert config.s3.region == "us-east-1"
    
    assert len(config.schedules) == 1
    schedule = config.schedules[0]
    assert isinstance(schedule, BackupSchedule)
    assert schedule.database_name == "test_db"
    assert schedule.cron_expression == "0 0 * * *"
    assert schedule.retention_days == 7
    
    assert config.log_level == "INFO"
    assert config.notifications_email == "test@example.com"

def test_load_nonexistent_config():
    """Test that loading a nonexistent config file raises an exception."""
    with pytest.raises(FileNotFoundError):
        load_config("nonexistent_file.yaml")