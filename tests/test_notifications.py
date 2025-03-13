# /tests/test_notifications.py
import pytest
from unittest.mock import patch, MagicMock, call

from dbsavr.notifications import EmailNotifier

@pytest.fixture
def email_notifier():
    """Create an EmailNotifier instance for testing."""
    return EmailNotifier("test@example.com")

@patch('dbsavr.notifications.smtplib.SMTP')
@patch('dbsavr.notifications.os.environ.get')
def test_email_notifier_init(mock_environ_get, mock_smtp, email_notifier):
    """Test EmailNotifier initialization with environment variables."""
    # Setup environment variable mocks with correct function signature
    mock_environ_get.side_effect = lambda key, default=None: {
        'SMTP_SERVER': 'smtp.example.com',
        'SMTP_PORT': '587',
        'SMTP_USERNAME': 'user',
        'SMTP_PASSWORD': 'pass',
        'SMTP_SENDER': 'sender@example.com'
    }.get(key, default)
    
    # Create a new notifier with mocked environment
    notifier = EmailNotifier("test@example.com")
    
    # Verify environment variables were read correctly
    assert notifier.smtp_server == "smtp.example.com"
    assert notifier.smtp_port == 587
    assert notifier.smtp_username == "user"
    assert notifier.smtp_password == "pass"
    assert notifier.sender == "sender@example.com"
    assert notifier.recipient == "test@example.com"

@patch('dbsavr.notifications.smtplib.SMTP')
def test_send_success_notification(mock_smtp, email_notifier):
    """Test sending a success notification email."""
    # Setup mock SMTP server
    server_mock = MagicMock()
    mock_smtp.return_value.__enter__.return_value = server_mock
    
    # Call the method
    email_notifier.send_success_notification(
        db_name="test_db",
        backup_size=1048576,  # 1 MB
        s3_key="backups/test_db/backup.sql.gz",
        duration=15.75,
        deleted_backups=2
    )
    
    # Verify SMTP was called correctly
    mock_smtp.assert_called_once_with(email_notifier.smtp_server, email_notifier.smtp_port)
    
    # Verify email was sent
    assert server_mock.send_message.called
    
    # Get the message that was sent
    msg = server_mock.send_message.call_args[0][0]
    
    # Verify email headers
    assert msg['From'] == email_notifier.sender
    assert msg['To'] == email_notifier.recipient
    assert "Backup Successful: test_db" in msg['Subject']
    
    # Verify email content
    payload = msg.get_payload(0).get_payload()
    assert "Database: test_db" in payload
    assert "Backup Size: 1.00 MB" in payload
    assert "S3 Location: backups/test_db/backup.sql.gz" in payload
    assert "Duration: 15.75 seconds" in payload
    assert "Old Backups Removed: 2" in payload

@patch('dbsavr.notifications.smtplib.SMTP')
def test_send_failure_notification(mock_smtp, email_notifier):
    """Test sending a failure notification email."""
    # Setup mock SMTP server
    server_mock = MagicMock()
    mock_smtp.return_value.__enter__.return_value = server_mock
    
    # Call the method
    email_notifier.send_failure_notification(
        db_name="test_db",
        error="Connection refused"
    )
    
    # Verify SMTP was called correctly
    mock_smtp.assert_called_once_with(email_notifier.smtp_server, email_notifier.smtp_port)
    
    # Verify email was sent
    assert server_mock.send_message.called
    
    # Get the message that was sent
    msg = server_mock.send_message.call_args[0][0]
    
    # Verify email headers
    assert msg['From'] == email_notifier.sender
    assert msg['To'] == email_notifier.recipient
    assert "Backup Failed: test_db" in msg['Subject']
    
    # Verify email content
    payload = msg.get_payload(0).get_payload()
    assert "Database: test_db" in payload
    assert "Error: Connection refused" in payload

@patch('dbsavr.notifications.smtplib.SMTP')
def test_send_email_with_authentication(mock_smtp, email_notifier):
    """Test sending email with SMTP authentication."""
    # Configure notifier to use authentication
    email_notifier.smtp_username = "user"
    email_notifier.smtp_password = "pass"
    email_notifier.use_tls = True  # Make sure TLS is enabled for this test
    
    # Setup mock SMTP server
    server_mock = MagicMock()
    mock_smtp.return_value.__enter__.return_value = server_mock
    
    # Call the method
    email_notifier.send_success_notification(
        db_name="test_db",
        backup_size=1048576,
        s3_key="backups/test_db/backup.sql.gz",
        duration=10.5,
        deleted_backups=0
    )
    
    # Verify SMTP authentication was used
    server_mock.starttls.assert_called_once()
    server_mock.login.assert_called_once_with("user", "pass")
    assert server_mock.send_message.called

@patch('dbsavr.notifications.smtplib.SMTP')
@patch('dbsavr.notifications.logger.error')
def test_send_email_error_handling(mock_logging_error, mock_smtp, email_notifier):
    """Test error handling when sending email fails."""
    # Make SMTP raise an exception
    mock_smtp.return_value.__enter__.side_effect = Exception("Connection refused")
    
    # Call the method (should not raise exception)
    email_notifier.send_success_notification(
        db_name="test_db",
        backup_size=1048576,
        s3_key="backups/test_db/backup.sql.gz",
        duration=10.5,
        deleted_backups=0
    )
    
    # Verify error was logged - use a more flexible assertion
    assert mock_logging_error.called
    
    # Use a more flexible check for the error message
    # Check that the error message contains the expected text
    called_args = mock_logging_error.call_args[0]
    assert "Failed to send email notification" in called_args[0]
    assert "Connection refused" in str(called_args)