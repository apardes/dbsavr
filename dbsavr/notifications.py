# /dbsavr/notifications.py
import logging
import smtplib
import ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List, Optional, Dict, Any
import os

logger = logging.getLogger(__name__)

class EmailNotifier:
    def __init__(self, recipient_email: str):
        """
        Initialize the email notifier with configuration from environment variables
        
        Args:
            recipient_email: Email address to send notifications to
        """
        self.recipient = recipient_email
        
        # Get SMTP settings from environment variables
        self.smtp_server = os.environ.get('SMTP_SERVER', 'localhost')
        self.smtp_port = int(os.environ.get('SMTP_PORT', 25))
        self.smtp_username = os.environ.get('SMTP_USERNAME')
        self.smtp_password = os.environ.get('SMTP_PASSWORD')
        self.sender = os.environ.get('SMTP_SENDER', 'db-backup@example.com')
        
        # SSL/TLS configuration
        self.use_tls = os.environ.get('SMTP_USE_TLS', 'false').lower() in ('true', '1', 'yes')
        self.use_ssl = os.environ.get('SMTP_USE_SSL', 'false').lower() in ('true', '1', 'yes')
        
        # Validate configuration
        if self.use_ssl and self.use_tls:
            logger.warning("Both SMTP_USE_SSL and SMTP_USE_TLS are enabled. Using TLS.")
            self.use_ssl = False
        
        # Set proper default port if TLS/SSL is enabled but port is default
        if self.use_ssl and self.smtp_port == 25:
            self.smtp_port = 465
        elif self.use_tls and self.smtp_port == 25:
            self.smtp_port = 587
    
    def send_success_notification(self, db_name: str, backup_size: int, s3_key: str, 
                                  duration: float, deleted_backups: int) -> None:
        """
        Send a notification about successful backup
        
        Args:
            db_name: Database name
            backup_size: Size of the backup in bytes
            s3_key: S3 key where the backup is stored
            duration: Duration of the backup in seconds
            deleted_backups: Number of old backups deleted
        """
        subject = f"Backup Successful: {db_name}"
        
        # Create a human-readable size
        size_mb = backup_size / (1024 * 1024)
        
        body = f"""
        Database Backup Completed Successfully
        
        Database: {db_name}
        Backup Size: {size_mb:.2f} MB
        S3 Location: {s3_key}
        Duration: {duration:.2f} seconds
        Old Backups Removed: {deleted_backups}
        
        This is an automated message from the database backup utility.
        """
        
        self._send_email(subject, body)
    
    def send_failure_notification(self, db_name: str, error: str) -> None:
        """
        Send a notification about failed backup
        
        Args:
            db_name: Database name
            error: Error message
        """
        subject = f"Backup Failed: {db_name}"
        
        body = f"""
        Database Backup Failed
        
        Database: {db_name}
        Error: {error}
        
        Please check the logs for more details.
        This is an automated message from the database backup utility.
        """
        
        self._send_email(subject, body)
    
    def _send_email(self, subject: str, body: str) -> None:
        """
        Send an email notification with proper SSL/TLS handling
        
        Args:
            subject: Email subject
            body: Email body text
        """
        try:
            msg = MIMEMultipart()
            msg['From'] = self.sender
            msg['To'] = self.recipient
            msg['Subject'] = subject
            
            msg.attach(MIMEText(body, 'plain'))
            
            # Different connection methods based on SSL/TLS settings
            if self.use_ssl:
                context = ssl.create_default_context()
                with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port, context=context) as server:
                    self._authenticate_and_send(server, msg)
            else:
                with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                    if self.use_tls:
                        server.starttls()
                    self._authenticate_and_send(server, msg)
            
            logger.info(f"Sent email notification: {subject}")
        except Exception as e:
            # Fix the logging format to match test expectations
            logger.error(f"Failed to send email notification: {str(e)}")
    
    def _authenticate_and_send(self, server: Any, msg: MIMEMultipart) -> None:
        """
        Authenticate with the SMTP server if credentials are provided and send the message
        
        Args:
            server: SMTP server connection
            msg: Email message to send
        """
        if self.smtp_username and self.smtp_password:
            server.login(self.smtp_username, self.smtp_password)
        
        server.send_message(msg)