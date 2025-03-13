# /dbsavr/tasks.py
import os
import logging
from datetime import datetime
from celery import Celery
import tempfile

from .config import Config, load_config
from .backup_service import BackupService

# Initialize Celery app
app = Celery('db_backup_tasks')

# Only load Celery config if available, making it optional
try:
    app.config_from_object('celeryconfig')
except ImportError:
    # When running without Celery, this is expected
    pass

# Global variables for lazy loading
_config = None
_backup_service = None

def get_config():
    """Lazy load configuration"""
    global _config
    if _config is None:
        config_path = os.environ.get('DB_BACKUP_CONFIG', 'config.yaml')
        _config = load_config(config_path)
    return _config

def get_backup_service():
    """Lazy load backup service"""
    global _backup_service
    if _backup_service is None:
        _backup_service = BackupService(get_config())
    return _backup_service

@app.task(bind=True, name='backup_database')
def backup_database(self, db_name: str):
    """
    Celery task to backup a database and upload to S3
    
    This is now a thin wrapper around BackupService.perform_backup()
    
    Args:
        db_name: Name of the database to backup
        
    Returns:
        Dict with status and details of the backup operation
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Celery task started for database backup: {db_name}")
    
    try:
        # Use BackupService to perform the backup
        backup_service = get_backup_service()
        result = backup_service.perform_backup(db_name)
        logger.info(f"Backup task completed successfully for {db_name}")
        return result
    except Exception as e:
        logger.error(f"Backup task failed for {db_name}: {str(e)}", exc_info=True)
        raise