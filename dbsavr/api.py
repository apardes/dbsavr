# /dbsavr/api.py
"""
High-level API functions for dbsavr.

These functions provide a simple interface to the core functionality
for use in external applications or scripts.
"""

from .config import load_config
from .backup_service import BackupService
from .scheduler_service import SchedulerService

def create_backup(config_path: str, db_name: str):
    """
    Create a backup for a database
    
    Args:
        config_path: Path to the configuration file
        db_name: Name of the database to back up
        
    Returns:
        Dictionary with backup results
    """
    config = load_config(config_path)
    service = BackupService(config)
    return service.perform_backup(db_name)

def start_scheduler(config_path: str, daemon: bool = False):
    """
    Start the backup scheduler
    
    Args:
        config_path: Path to the configuration file
        daemon: Whether to run the scheduler as a daemon
        
    Returns:
        Scheduler service instance
    """
    config = load_config(config_path)
    service = SchedulerService(config)
    service.start_scheduler(daemon=daemon)
    return service

def list_databases(config_path: str):
    """
    List all available databases
    
    Args:
        config_path: Path to the configuration file
        
    Returns:
        List of database names
    """
    config = load_config(config_path)
    service = BackupService(config)
    return service.list_available_databases()

def cleanup_backups(config_path: str, db_name: str, days: int = None):
    """
    Clean up old backups
    
    Args:
        config_path: Path to the configuration file
        db_name: Name of the database
        days: Optional override for retention days
        
    Returns:
        List of deleted S3 keys
    """
    config = load_config(config_path)
    service = BackupService(config)
    return service.cleanup_old_backups(db_name, days)

def generate_celery_config(config_path: str, broker_url: str, result_backend: str = None):
    """
    Generate Celery configuration
    
    Args:
        config_path: Path to the configuration file
        broker_url: URL for the Celery broker
        result_backend: Optional URL for the Celery result backend
        
    Returns:
        Celery configuration as a string
    """
    config = load_config(config_path)
    service = SchedulerService(config)
    return service.generate_celery_config(broker_url, result_backend)