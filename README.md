# dbsavr

Database backup utility with S3 storage and scheduling support.

## Features

- PostgreSQL, MySQL/MariaDB, and MongoDB support
- S3-compatible storage with retention policies
- Scheduled backups via Celery
- Email notifications
- Automatic compression

## Installation

```bash
pip install dbsavr
```

From source:
```bash
git clone https://github.com/yourusername/dbsavr.git
cd dbsavr
pip install -e .
```

## Configuration

Create a `config.yaml` file:

```yaml
# Database configurations
databases:
  myapp_db:                      # Database identifier (used in commands)
    type: postgresql             # Database type: postgresql, mysql, mariadb, mongodb
    host: db.example.com
    port: 5432
    username: backup_user
    password: secure_password
    database: myapp_production
    bucket_name: myapp-backups   # Optional: Override default S3 bucket
    options:                     # Optional: Database-specific options
      extra_args:                # Additional command-line arguments
        - "--exclude-table=logs"
      # For MongoDB only:
      # auth_db: admin           # Authentication database

# S3 storage settings
s3:
  bucket_name: my-backups        # Default bucket for all backups
  prefix: backups                # S3 key prefix (like a folder)
  region: us-west-2
  access_key: AWS_ACCESS_KEY     # Optional: Uses IAM role if not provided
  secret_key: AWS_SECRET_KEY     # Optional: Uses IAM role if not provided

# Backup schedules (cron format)
schedules:
  - database_name: myapp_db
    cron_expression: "0 2 * * *" # Daily at 2 AM
    retention_days: 30           # Keep backups for 30 days
    prefix: daily                # Optional: Subfolder for this schedule

# Other settings
log_level: INFO                  # DEBUG, INFO, WARNING, ERROR, CRITICAL
notifications_email: ops@example.com  # Optional: Email for notifications
```

### Email Configuration (Environment Variables)

```bash
export SMTP_SERVER=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USERNAME=alerts@example.com
export SMTP_PASSWORD=app-password
export SMTP_SENDER=dbsavr@example.com
export SMTP_USE_TLS=true
```

## Commands

### List configured databases
```bash
dbsavr list-databases
```

### List backup schedules
```bash
dbsavr list-schedules
```

### Run a manual backup
```bash
dbsavr backup myapp_db
```

### Clean up old backups
```bash
# Use retention from config
dbsavr cleanup myapp_db

# Override retention days
dbsavr cleanup myapp_db --days 7
```

### Set up scheduled backups

Generate Celery configuration:
```bash
dbsavr setup-celery-schedule
```

Start the scheduler:
```bash
# Foreground (see logs)
dbsavr run-scheduler

# Background (daemon mode)
dbsavr run-scheduler --detach

# With custom settings
dbsavr run-scheduler --workers 4 --loglevel debug
```

Stop the scheduler:
```bash
dbsavr stop-scheduler --pid-file dbsavr-worker.pid
```

### One-step scheduler setup
```bash
# Generate config and start scheduler
dbsavr setup-celery-schedule --run-after
```

## S3 Storage Structure

```
bucket/
  prefix/
    database_name/
      [schedule_prefix/]
        database_name_YYYYMMDD_HHMMSS.sql.gz   # PostgreSQL/MySQL
        database_name_YYYYMMDD_HHMMSS.tar.gz   # MongoDB
```

## Prerequisites

- Python 3.7+
- Database tools:
  - PostgreSQL: `pg_dump`
  - MySQL/MariaDB: `mysqldump`
  - MongoDB: `mongodump`
- Redis (for scheduled backups with Celery)

## License

Perpetual Business Source License (PBSL) - See LICENSE file