# /dbsavr/backup_service.py
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from .config import Config, DatabaseConfig
from .backup_engine import BackupEngine
from .storage import S3Storage
from .notifications import EmailNotifier

logger = logging.getLogger(__name__)

class BackupService:
    """Service for managing database backups without Celery dependency"""
    
    def __init__(self, config: Config):
        """
        Initialize the backup service
        
        Args:
            config: Application configuration
        """
        self.config = config
    
    def perform_backup(self, db_name: str) -> Dict[str, Any]:
        """
        Execute a complete backup workflow for a database
        
        Args:
            db_name: Name of the database to back up
            
        Returns:
            Dict with backup results (status, s3_key, duration, deleted_backups)
            
        Raises:
            ValueError: If database configuration is not found
            Exception: If backup process fails
        """
        start_time = datetime.now()
        logger.info(f"Starting backup for database: {db_name}")
        
        try:
            # Get database configuration
            if db_name not in self.config.databases:
                raise ValueError(f"Database configuration not found for: {db_name}")
            
            db_config = self.config.databases[db_name]
            
            # Get schedule information for this database
            schedule_info = self._get_schedule_info(db_name)
            retention_days = schedule_info.get('retention_days', 30)
            schedule_prefix = schedule_info.get('prefix')
            
            # Create the backup
            backup_path, filename = BackupEngine.backup_database(db_config)
            
            # Get backup details
            backup_details = BackupEngine.get_backup_details(backup_path)
            backup_size = backup_details['size']
            
            # Get database-specific bucket if configured
            custom_bucket = db_config.bucket_name
            
            # Upload to S3
            s3_storage = S3Storage(self.config.s3)
            s3_key = s3_storage.upload_backup(
                backup_path, 
                db_name, 
                filename, 
                custom_bucket=custom_bucket,
                custom_prefix=schedule_prefix
            )
            
            # Clean up the local backup file
            os.remove(backup_path)
            
            # Clean up old backups based on retention policy
            deleted = self._cleanup_old_backups(
                s3_storage,
                db_name,
                retention_days,
                custom_bucket=custom_bucket,
                custom_prefix=schedule_prefix
            )
            
            # Calculate duration
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            # Send notification if configured
            self._send_success_notification(
                db_name=db_name,
                backup_size=backup_size,
                s3_key=s3_key,
                duration=duration,
                deleted_backups=len(deleted)
            )
            
            logger.info(f"Backup completed successfully for {db_name} in {duration:.2f} seconds")
            return {
                'status': 'success',
                'database': db_name,
                's3_key': s3_key,
                'size_bytes': backup_size,
                'size_mb': round(backup_size / (1024 * 1024), 2),
                'duration': duration,
                'deleted_backups': len(deleted),
                'deleted_keys': deleted,
                'timestamp': datetime.now().isoformat()
            }
        
        except Exception as e:
            logger.error(f"Backup failed for {db_name}: {str(e)}", exc_info=True)
            
            # Send failure notification if configured
            self._send_failure_notification(
                db_name=db_name,
                error=str(e)
            )
            
            # Re-raise the exception for the caller to handle
            raise
    
    def get_database_info(self, db_name: str) -> Dict[str, Any]:
        """
        Get information about a database configuration
        
        Args:
            db_name: Name of the database
            
        Returns:
            Dict with database information
            
        Raises:
            ValueError: If database configuration is not found
        """
        if db_name not in self.config.databases:
            raise ValueError(f"Database configuration not found for: {db_name}")
        
        db_config = self.config.databases[db_name]
        schedule_info = self._get_schedule_info(db_name)
        
        return {
            'name': db_name,
            'type': db_config.type,
            'host': db_config.host,
            'port': db_config.port,
            'database': db_config.database,
            'custom_bucket': db_config.bucket_name,
            'schedule': schedule_info
        }
    
    def list_available_databases(self) -> List[str]:
        """
        Get list of all configured database names
        
        Returns:
            List of database names
        """
        return list(self.config.databases.keys())
    
    def cleanup_old_backups(self, db_name: str, days: Optional[int] = None) -> List[str]:
        """
        Clean up old backups for a specific database
        
        Args:
            db_name: Name of the database
            days: Optional override for retention days
            
        Returns:
            List of deleted S3 keys
            
        Raises:
            ValueError: If database configuration is not found
        """
        # Get database configuration
        if db_name not in self.config.databases:
            raise ValueError(f"Database configuration not found for: {db_name}")
        
        db_config = self.config.databases[db_name]
        
        # Get schedule information
        schedule_info = self._get_schedule_info(db_name)
        retention_days = days if days is not None else schedule_info.get('retention_days', 30)
        schedule_prefix = schedule_info.get('prefix')
        
        # Get custom bucket if configured
        custom_bucket = db_config.bucket_name
        
        # Initialize S3 storage
        s3_storage = S3Storage(self.config.s3)
        
        # Clean up old backups
        return self._cleanup_old_backups(
            s3_storage,
            db_name,
            retention_days,
            custom_bucket=custom_bucket,
            custom_prefix=schedule_prefix
        )
    
    def _get_schedule_info(self, db_name: str) -> Dict[str, Any]:
        """
        Get schedule information for a database
        
        Args:
            db_name: Name of the database
            
        Returns:
            Dict with schedule information
        """
        schedule = next((s for s in self.config.schedules if s.database_name == db_name), None)
        
        if not schedule:
            logger.warning(f"No schedule found for {db_name}, using default retention of 30 days")
            return {
                'retention_days': 30,
                'prefix': None,
                'cron_expression': None
            }
        
        return {
            'retention_days': schedule.retention_days,
            'prefix': schedule.prefix,
            'cron_expression': schedule.cron_expression
        }
    
    def _cleanup_old_backups(
        self,
        s3_storage: S3Storage,
        db_name: str,
        retention_days: int,
        custom_bucket: Optional[str] = None,
        custom_prefix: Optional[str] = None
    ) -> List[str]:
        """
        Clean up old backups using S3Storage
        
        Args:
            s3_storage: Initialized S3Storage instance
            db_name: Name of the database
            retention_days: Number of days to keep backups
            custom_bucket: Optional custom bucket name
            custom_prefix: Optional custom prefix
            
        Returns:
            List of deleted S3 keys
        """
        logger.info(f"Cleaning up old backups for {db_name} (retention: {retention_days} days)")
        
        deleted = s3_storage.cleanup_old_backups(
            db_name,
            retention_days,
            custom_bucket=custom_bucket,
            custom_prefix=custom_prefix
        )
        
        logger.info(f"Deleted {len(deleted)} old backups for {db_name}")
        return deleted
    
    def _send_success_notification(
        self,
        db_name: str,
        backup_size: int,
        s3_key: str,
        duration: float,
        deleted_backups: int
    ) -> None:
        """
        Send success notification if configured
        
        Args:
            db_name: Database name
            backup_size: Size of backup in bytes
            s3_key: S3 key where backup is stored
            duration: Duration of backup in seconds
            deleted_backups: Number of old backups deleted
        """
        if not self.config.notifications_email:
            return
        
        try:
            notifier = EmailNotifier(self.config.notifications_email)
            notifier.send_success_notification(
                db_name=db_name,
                backup_size=backup_size,
                s3_key=s3_key,
                duration=duration,
                deleted_backups=deleted_backups
            )
        except Exception as e:
            logger.error(f"Failed to send success notification: {str(e)}")
    
    def _send_failure_notification(self, db_name: str, error: str) -> None:
        """
        Send failure notification if configured
        
        Args:
            db_name: Database name
            error: Error message
        """
        if not self.config.notifications_email:
            return
        
        try:
            notifier = EmailNotifier(self.config.notifications_email)
            notifier.send_failure_notification(
                db_name=db_name,
                error=error
            )
        except Exception as e:
            logger.error(f"Failed to send failure notification: {str(e)}")