# /dbsavr/config.py
import os
import yaml
from dataclasses import dataclass
from typing import List, Dict, Any, Optional

@dataclass
class DatabaseConfig:
    type: str  # mysql, postgresql, mongodb, etc.
    host: str
    port: int
    username: str
    password: str
    database: str
    options: Optional[Dict[str, Any]] = None
    bucket_name: Optional[str] = None  # Optional bucket_name override

@dataclass
class S3Config:
    bucket_name: str
    prefix: str
    region: str
    access_key: Optional[str] = None
    secret_key: Optional[str] = None
    # If None, will use IAM role or AWS credentials from environment

@dataclass
class BackupSchedule:
    database_name: str
    cron_expression: str  # "0 3 * * *" for daily at 3 AM
    retention_days: int = 30
    prefix: Optional[str] = None  # Optional prefix override

@dataclass
class Config:
    databases: Dict[str, DatabaseConfig]
    s3: S3Config
    schedules: List[BackupSchedule]
    log_level: str = "INFO"
    notifications_email: Optional[str] = None

def load_config(config_path: str) -> Config:
    """Load configuration from YAML file"""
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")
    
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    
    # Parse database configs
    databases = {}
    for name, db_config in config_data.get('databases', {}).items():
        databases[name] = DatabaseConfig(
            type=db_config['type'],
            host=db_config['host'],
            port=db_config['port'],
            username=db_config['username'],
            password=db_config['password'],
            database=db_config['database'],
            options=db_config.get('options'),
            bucket_name=db_config.get('bucket_name')  # Parse optional bucket_name
        )
    
    # Parse S3 config
    s3_config = config_data.get('s3', {})
    s3 = S3Config(
        bucket_name=s3_config['bucket_name'],
        prefix=s3_config['prefix'],
        region=s3_config['region'],
        access_key=s3_config.get('access_key'),
        secret_key=s3_config.get('secret_key')
    )
    
    # Parse schedule configs
    schedules = []
    for schedule in config_data.get('schedules', []):
        schedules.append(BackupSchedule(
            database_name=schedule['database_name'],
            cron_expression=schedule['cron_expression'],
            retention_days=schedule.get('retention_days', 30),
            prefix=schedule.get('prefix')  # Parse optional prefix
        ))
    
    return Config(
        databases=databases,
        s3=s3,
        schedules=schedules,
        log_level=config_data.get('log_level', 'INFO'),
        notifications_email=config_data.get('notifications_email')
    )