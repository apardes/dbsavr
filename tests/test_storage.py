# /tests/test_storage.py
import os
import pytest
from unittest.mock import patch, MagicMock, call
from datetime import datetime, timedelta

from dbsavr.storage import S3Storage
from dbsavr.config import S3Config

@pytest.fixture
def s3_config():
    """Create a sample S3 configuration for testing."""
    return S3Config(
        bucket_name="test-bucket",
        prefix="backups",
        region="us-east-1",
        access_key="test-access-key",
        secret_key="test-secret-key"
    )

@pytest.fixture
def s3_config_iam():
    """Create a sample S3 configuration using IAM roles for testing."""
    return S3Config(
        bucket_name="test-bucket",
        prefix="backups",
        region="us-east-1"
    )

@patch('dbsavr.storage.boto3.client')
def test_create_s3_client_with_keys(mock_boto3_client, s3_config):
    """Test creating S3 client with access keys."""
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Check if boto3.client was called correctly
    mock_boto3_client.assert_called_once_with(
        's3',
        region_name='us-east-1',
        aws_access_key_id='test-access-key',
        aws_secret_access_key='test-secret-key'
    )

@patch('dbsavr.storage.boto3.client')
def test_create_s3_client_with_iam(mock_boto3_client, s3_config_iam):
    """Test creating S3 client with IAM role credentials."""
    # Create S3Storage instance
    storage = S3Storage(s3_config_iam)
    
    # Check if boto3.client was called correctly
    mock_boto3_client.assert_called_once_with(
        's3',
        region_name='us-east-1'
    )

@patch('dbsavr.storage.boto3.client')
def test_upload_backup_without_schedule_prefix(mock_boto3_client, s3_config):
    """Test uploading a backup file to S3 without a schedule prefix."""
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call upload_backup method without custom prefix
    file_path = "/tmp/test_backup.sql.gz"
    db_name = "test_db"
    filename = "test_backup.sql.gz"
    
    s3_key = storage.upload_backup(file_path, db_name, filename)
    
    # Assertions
    assert s3_key == "backups/test_db/test_backup.sql.gz"
    mock_s3.upload_file.assert_called_once_with(
        file_path,
        "test-bucket",
        "backups/test_db/test_backup.sql.gz"
    )

@patch('dbsavr.storage.boto3.client')
def test_upload_backup_with_schedule_prefix(mock_boto3_client, s3_config):
    """Test uploading a backup file to S3 with a schedule prefix."""
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call upload_backup method with custom prefix
    file_path = "/tmp/test_backup.sql.gz"
    db_name = "test_db"
    filename = "test_backup.sql.gz"
    schedule_prefix = "daily"
    
    s3_key = storage.upload_backup(file_path, db_name, filename, custom_prefix=schedule_prefix)
    
    # Assertions
    assert s3_key == "backups/test_db/daily/test_backup.sql.gz"
    mock_s3.upload_file.assert_called_once_with(
        file_path,
        "test-bucket",
        "backups/test_db/daily/test_backup.sql.gz"
    )

@patch('dbsavr.storage.boto3.client')
def test_upload_backup_with_custom_bucket(mock_boto3_client, s3_config):
    """Test uploading a backup file to S3 with a custom bucket."""
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call upload_backup method with custom bucket
    file_path = "/tmp/test_backup.sql.gz"
    db_name = "test_db"
    filename = "test_backup.sql.gz"
    custom_bucket = "custom-bucket"
    
    s3_key = storage.upload_backup(file_path, db_name, filename, custom_bucket=custom_bucket)
    
    # Assertions
    assert s3_key == "backups/test_db/test_backup.sql.gz"
    mock_s3.upload_file.assert_called_once_with(
        file_path,
        "custom-bucket",
        "backups/test_db/test_backup.sql.gz"
    )

@patch('dbsavr.storage.boto3.client')
def test_cleanup_old_backups_without_schedule_prefix(mock_boto3_client, s3_config):
    """Test cleaning up old backups without schedule prefix."""
    # Setup current time for testing
    now = datetime.utcnow()
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock paginator
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    # Setup mock response for list_objects_v2
    old_backup = {
        'Key': 'backups/test_db/old_backup.sql.gz',
        'LastModified': now - timedelta(days=40)
    }
    recent_backup = {
        'Key': 'backups/test_db/recent_backup.sql.gz',
        'LastModified': now - timedelta(days=5)
    }
    
    mock_paginator.paginate.return_value = [{
        'Contents': [old_backup, recent_backup]
    }]
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call cleanup_old_backups method with 30 day retention
    deleted_keys = storage.cleanup_old_backups("test_db", 30)
    
    # Assertions
    assert len(deleted_keys) == 1
    assert deleted_keys[0] == 'backups/test_db/old_backup.sql.gz'
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key='backups/test_db/old_backup.sql.gz'
    )
    
    # Verify that the paginator was called with the correct prefix
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test-bucket", 
        Prefix="backups/test_db"
    )

@patch('dbsavr.storage.boto3.client')
def test_cleanup_old_backups_with_schedule_prefix(mock_boto3_client, s3_config):
    """Test cleaning up old backups with schedule prefix."""
    # Setup current time for testing
    now = datetime.utcnow()
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock paginator
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    # Setup mock response for list_objects_v2
    old_backup = {
        'Key': 'backups/test_db/daily/old_backup.sql.gz',
        'LastModified': now - timedelta(days=40)
    }
    recent_backup = {
        'Key': 'backups/test_db/daily/recent_backup.sql.gz',
        'LastModified': now - timedelta(days=5)
    }
    
    mock_paginator.paginate.return_value = [{
        'Contents': [old_backup, recent_backup]
    }]
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call cleanup_old_backups method with 30 day retention and schedule prefix
    deleted_keys = storage.cleanup_old_backups("test_db", 30, custom_prefix="daily")
    
    # Assertions
    assert len(deleted_keys) == 1
    assert deleted_keys[0] == 'backups/test_db/daily/old_backup.sql.gz'
    mock_s3.delete_object.assert_called_once_with(
        Bucket="test-bucket",
        Key='backups/test_db/daily/old_backup.sql.gz'
    )
    
    # Verify that the paginator was called with the correct prefix
    mock_paginator.paginate.assert_called_once_with(
        Bucket="test-bucket", 
        Prefix="backups/test_db/daily"
    )

@patch('dbsavr.storage.boto3.client')
def test_cleanup_old_backups_path_structure(mock_boto3_client, s3_config):
    """Test that cleanup_old_backups only deletes files in the specific path structure."""
    # Setup current time for testing
    now = datetime.utcnow()
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock paginator
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    # Setup mock response with files from multiple paths
    old_backup_testdb = {
        'Key': 'backups/test_db/old_backup.sql.gz',
        'LastModified': now - timedelta(days=40)
    }
    old_backup_testdb_daily = {
        'Key': 'backups/test_db/daily/old_backup.sql.gz',
        'LastModified': now - timedelta(days=40)
    }
    old_backup_otherdb = {
        'Key': 'backups/other_db/old_backup.sql.gz',
        'LastModified': now - timedelta(days=40)
    }
    
    # Set up paginator to return different responses based on prefix
    def mock_paginate(Bucket, Prefix):
        if Prefix == 'backups/test_db':
            return [{
                'Contents': [old_backup_testdb]
            }]
        elif Prefix == 'backups/test_db/daily':
            return [{
                'Contents': [old_backup_testdb_daily]
            }]
        else:
            return [{
                'Contents': [old_backup_otherdb]
            }]
    
    mock_paginator.paginate.side_effect = mock_paginate
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call cleanup_old_backups method for test_db without schedule prefix
    deleted_keys = storage.cleanup_old_backups("test_db", 30)
    
    # Assertions
    assert len(deleted_keys) == 1
    assert deleted_keys[0] == 'backups/test_db/old_backup.sql.gz'
    
    # Verify that the paginator was called with the correct prefix
    mock_paginator.paginate.assert_called_with(
        Bucket="test-bucket", 
        Prefix="backups/test_db"
    )
    
    # Reset mock and call with schedule prefix
    mock_s3.delete_object.reset_mock()
    mock_paginator.paginate.reset_mock()
    
    deleted_keys = storage.cleanup_old_backups("test_db", 30, custom_prefix="daily")
    
    # Assertions
    assert len(deleted_keys) == 1
    assert deleted_keys[0] == 'backups/test_db/daily/old_backup.sql.gz'
    
    # Verify that the paginator was called with the correct prefix
    mock_paginator.paginate.assert_called_with(
        Bucket="test-bucket", 
        Prefix="backups/test_db/daily"
    )

@patch('dbsavr.storage.boto3.client')
def test_upload_backup_error(mock_boto3_client, s3_config):
    """Test error handling during backup upload."""
    from botocore.exceptions import ClientError
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Set up the mock to raise a ClientError when upload_file is called
    mock_s3.upload_file.side_effect = ClientError(
        {'Error': {'Code': 'AccessDenied', 'Message': 'Access Denied'}},
        'upload_file'
    )
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call upload_backup method
    file_path = "/tmp/test_backup.sql.gz"
    db_name = "test_db"
    filename = "test_backup.sql.gz"
    
    # Expect the ClientError to be propagated
    with pytest.raises(ClientError):
        storage.upload_backup(file_path, db_name, filename)

@patch('dbsavr.storage.boto3.client')
def test_multiple_pages_cleanup(mock_boto3_client, s3_config):
    """Test cleanup with multiple pages of results."""
    # Setup current time for testing
    now = datetime.utcnow()
    
    # Mock S3 client
    mock_s3 = MagicMock()
    mock_boto3_client.return_value = mock_s3
    
    # Mock paginator
    mock_paginator = MagicMock()
    mock_s3.get_paginator.return_value = mock_paginator
    
    # Create a large set of old backups across multiple pages
    old_backups_page1 = [{
        'Key': f'backups/test_db/old_backup_1_{i}.sql.gz',
        'LastModified': now - timedelta(days=40)
    } for i in range(5)]
    
    old_backups_page2 = [{
        'Key': f'backups/test_db/old_backup_2_{i}.sql.gz',
        'LastModified': now - timedelta(days=40)
    } for i in range(5)]
    
    # Setup mock response for list_objects_v2 with multiple pages
    mock_paginator.paginate.return_value = [
        {'Contents': old_backups_page1},
        {'Contents': old_backups_page2}
    ]
    
    # Create S3Storage instance
    storage = S3Storage(s3_config)
    
    # Call cleanup_old_backups method with 30 day retention
    deleted_keys = storage.cleanup_old_backups("test_db", 30)
    
    # Assertions
    assert len(deleted_keys) == 10  # 5 from page 1 + 5 from page 2
    
    # Verify all deletes were called
    assert mock_s3.delete_object.call_count == 10
    
    # Check that all expected keys were deleted
    expected_deletes = []
    for page in [old_backups_page1, old_backups_page2]:
        for obj in page:
            expected_deletes.append(call(
                Bucket="test-bucket",
                Key=obj['Key']
            ))
    
    mock_s3.delete_object.assert_has_calls(expected_deletes, any_order=True)