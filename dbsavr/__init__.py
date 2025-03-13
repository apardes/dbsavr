# /dbsavr/__init__.py
# dbsavr: An open source tool for easily creating database backups and storing them in S3-compatible object storage

from .version import __version__

from .config import Config, DatabaseConfig, S3Config, BackupSchedule, load_config
from .backup_engine import BackupEngine
from .storage import S3Storage
from .notifications import EmailNotifier
from .backup_service import BackupService
from .scheduler_service import SchedulerService
from .api import (
    create_backup,
    start_scheduler,
    list_databases,
    cleanup_backups,
    generate_celery_config,
)

__all__ = [
    "__version__",
    "Config",
    "DatabaseConfig",
    "S3Config",
    "BackupSchedule",
    "load_config",
    "BackupEngine",
    "S3Storage",
    "EmailNotifier",
    "BackupService",
    "SchedulerService",
    # API functions
    "create_backup",
    "start_scheduler",
    "list_databases",
    "cleanup_backups",
    "generate_celery_config",
]