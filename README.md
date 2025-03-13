# dbsavr

An open source tool for easily creating database backups and storing them in S3-compatible object storage.

![Version](https://img.shields.io/badge/version-0.1.0-blue)
![Python](https://img.shields.io/badge/python-3.7%2B-blue)
![License](https://img.shields.io/badge/license-PBSL-blue)

## Features

- Support for PostgreSQL, MySQL/MariaDB, and MongoDB databases
- Automatic compression of backups
- S3-compatible storage backend with configurable retention policies
- Scheduled backups via integrated scheduler (with or without Celery)
- Email notifications for backup success/failure
- Simple command-line interface

## Table of Contents

- [Installation](#installation)
- [Configuration](#configuration)
  - [Database Settings](#database-settings)
  - [S3 Settings](#s3-settings)
  - [Backup Schedules](#backup-schedules)
  - [Notification Settings](#notification-settings)
  - [Environment Variables](#environment-variables)
- [CLI Commands](#cli-commands)
  - [Basic Commands](#basic-commands)
  - [Scheduler Commands](#scheduler-commands)
  - [Maintenance Commands](#maintenance-commands)
- [Setting up Scheduled Backups](#setting-up-scheduled-backups)
  - [One-step Setup](#one-step-setup)
  - [Manual Setup](#manual-setup)
  - [Advanced Scheduler Options](#advanced-scheduler-options)
- [Backup Storage and Retention](#backup-storage-and-retention)
- [Email Notifications](#email-notifications)
- [Advanced Usage](#advanced-usage)
  - [Using Custom S3 Buckets](#using-custom-s3-buckets)
  - [Custom Backup Prefixes](#custom-backup-prefixes)
  - [Database-Specific Options](#database-specific-options)
- [Security Best Practices](#security-best-practices)
- [Development](#development)
  - [Running Tests](#running-tests)
  - [Contributing](#contributing)
- [License](#license)

## Installation

You can install dbsavr using pip:

```bash
pip install dbsavr
```

Or directly from source:

```bash
git clone https://github.com/yourusername/dbsavr.git
cd dbsavr
pip install -e .
```

For development, install with additional dependencies:

```bash
pip install -e '.[dev]'
```

### Prerequisites

- Python 3.7 or higher
- Database command-line tools for the databases you want to back up:
  - PostgreSQL: `pg_dump`
  - MySQL/MariaDB: `mysqldump`
  - MongoDB: `mongodump`
- Access to an S3-compatible storage service

## Configuration

dbsavr uses a YAML configuration file to define databases, S3 storage settings, and backup schedules.

### Database Settings

Configure one or more databases to back up:

```yaml
databases:
  myapp_db:  # This is the database identifier you'll refer to in commands
    type: postgresql  # Options: postgresql, mysql, mariadb, mongodb
    host: db.example.com
    port: 5432
    username: backup_user
    password: secure_password
    database: myapp_db
    options:  # Optional database-specific settings
      extra_args:
        - "--exclude-table=temp_logs"
    bucket_name: myapp-specific-backups  # Optional override of default S3 bucket
  
  analytics_db:
    type: mysql
    host: analytics.example.com
    port: 3306
    username: backup_user
    password: secure_password
    database: analytics_db
```

### S3 Settings

Configure the S3-compatible storage for backups:

```yaml
s3:
  bucket_name: my-database-backups  # Default bucket for all backups
  prefix: backups  # Prefix for all backup files (acts like a folder)
  region: us-west-2
  # Use IAM role or environment credentials by default
  # Or explicitly provide keys:
  # access_key: AWS_ACCESS_KEY
  # secret_key: AWS_SECRET_KEY
```

### Backup Schedules

Define when to run backups and how long to keep them:

```yaml
schedules:
  - database_name: myapp_db
    cron_expression: "0 2 * * *"  # Daily at 2 AM (cron syntax)
    retention_days: 30  # Keep backups for 30 days
    prefix: daily-backups  # Optional custom prefix for this schedule
  
  - database_name: myapp_db
    cron_expression: "0 0 * * 0"  # Weekly on Sunday at midnight
    retention_days: 90  # Keep weekly backups longer (90 days)
    prefix: weekly-backups  # Different prefix for weekly backups
  
  - database_name: analytics_db
    cron_expression: "0 3 * * 0"  # Weekly on Sunday at 3 AM
    retention_days: 90
```

### Notification Settings

Configure logging and email notifications:

```yaml
log_level: INFO  # Options: DEBUG, INFO, WARNING, ERROR, CRITICAL
notifications_email: alerts@example.com  # Email address for notifications
```

### Environment Variables

The following environment variables can be used to configure email notifications:

- `SMTP_SERVER`: SMTP server for sending notifications (default: localhost)
- `SMTP_PORT`: SMTP port (default: 25)
- `SMTP_USERNAME`: SMTP username (optional)
- `SMTP_PASSWORD`: SMTP password (optional)
- `SMTP_SENDER`: Sender email address (default: db-backup@example.com)
- `SMTP_USE_TLS`: Use TLS for SMTP connection (default: false)
- `SMTP_USE_SSL`: Use SSL for SMTP connection (default: false)
- `DB_BACKUP_CONFIG`: Path to the configuration file (alternative to --config option)

## CLI Commands

dbsavr provides a comprehensive command-line interface for managing backups.

### Basic Commands

**Get help and version information:**

```bash
# Show general help
dbsavr --help

# Show help for a specific command
dbsavr backup --help

# Show version
dbsavr --version
```

**List configured databases:**

```bash
dbsavr --config config.yaml list-databases
```

Example output:
```
Configured databases:

myapp_db:
  Type: postgresql
  Host: db.example.com:5432
  Database: myapp_db

analytics_db:
  Type: mysql
  Host: analytics.example.com:3306
  Database: analytics_db
```

**List backup schedules:**

```bash
dbsavr --config config.yaml list-schedules
```

Example output:
```
Configured backup schedules:

Database: myapp_db
  Schedule: 0 2 * * *
  Retention: 30 days
  Next Run: 2025-03-13 02:00:00 (in 720.5 minutes)

Database: myapp_db
  Schedule: 0 0 * * 0
  Retention: 90 days
  Next Run: 2025-03-16 00:00:00 (in 4320.0 minutes)

Database: analytics_db
  Schedule: 0 3 * * 0
  Retention: 90 days
  Next Run: 2025-03-16 03:00:00 (in 4500.0 minutes)
```

**Run a one-time backup:**

```bash
dbsavr --config config.yaml backup myapp_db
```

Example output:
```
Starting backup for myapp_db...
Backup completed: success
S3 Key: backups/myapp_db/myapp_db_20250312_120000.sql.gz
Backup Size: 1.00 MB
Duration: 15.35 seconds
Deleted old backups: 0
```

### Scheduler Commands

**Generate Celery scheduler configuration:**

```bash
dbsavr --config config.yaml setup-celery-schedule
```

This will create a `celeryconfig.py` file with all the necessary settings derived from your backup schedules.

**Start the scheduler:**

```bash
dbsavr --config config.yaml run-scheduler
```

Example output:
```
Starting Celery worker with 1 processes...
Starting Celery beat scheduler...
Scheduler is running. Press Ctrl+C to stop.
```

**One-step setup and run:**

```bash
dbsavr --config config.yaml setup-celery-schedule --run-after
```

**Stop the scheduler:**

```bash
dbsavr stop-scheduler --pid-file dbsavr-worker.pid
```

### Maintenance Commands

**Clean up old backups manually:**

```bash
# Use retention days from configuration
dbsavr --config config.yaml cleanup myapp_db

# Override retention period
dbsavr --config config.yaml cleanup myapp_db --days 7
```

Example output:
```
Cleaning up backups for myapp_db older than 7 days...
Deleted 3 old backups

Deleted backups:
  - backups/myapp_db/myapp_db_20250305_120000.sql.gz
  - backups/myapp_db/myapp_db_20250306_120000.sql.gz
  - backups/myapp_db/myapp_db_20250307_120000.sql.gz
```

## Setting up Scheduled Backups

### One-step Setup

The simplest way to set up and run scheduled backups:

```bash
dbsavr --config config.yaml setup-celery-schedule --run-after
```

This will generate the necessary Celery configuration and immediately start the scheduler.

### Manual Setup

For more control, you can set up and run separately:

1. Generate Celery configuration:

```bash
dbsavr --config config.yaml setup-celery-schedule
```

2. Start the scheduler:

```bash
dbsavr --config config.yaml run-scheduler
```

### Advanced Scheduler Options

You can customize the scheduler operation with various options:

- Use multiple worker processes for better performance:
  ```bash
  dbsavr --config config.yaml run-scheduler --workers 4
  ```

- Run the scheduler in the background (detached mode):
  ```bash
  dbsavr --config config.yaml run-scheduler --detach --pid-file /var/run/dbsavr.pid
  ```

- Control the logging verbosity:
  ```bash
  dbsavr --config config.yaml run-scheduler --loglevel debug
  ```

- Specify a log directory:
  ```bash
  dbsavr --config config.yaml run-scheduler --log-dir /var/log/dbsavr
  ```

## Backup Storage and Retention

Backups are stored in your S3 bucket using the following structure:

```
{bucket_name}/
  {prefix}/
    {database_name}/
      {database_name}_{timestamp}.sql.gz  # PostgreSQL/MySQL
      {database_name}_{timestamp}.tar.gz  # MongoDB
```

When using custom prefixes for different schedules:

```
{bucket_name}/
  {prefix}/
    {database_name}/
      {schedule_prefix}/
        {database_name}_{timestamp}.sql.gz
```

Example:
```
my-database-backups/
  backups/
    myapp_db/
      daily-backups/
        myapp_db_20250312_020000.sql.gz
      weekly-backups/
        myapp_db_20250309_000000.sql.gz
    analytics_db/
      analytics_db_20250309_030000.sql.gz
```

Old backups are automatically cleaned up based on the `retention_days` setting for each schedule.

## Email Notifications

dbsavr can send email notifications for backup success or failure. To use this feature:

1. Set the `notifications_email` in your configuration file
2. Configure SMTP settings using environment variables

Example email setup:

```bash
# Set environment variables for SMTP
export SMTP_SERVER=smtp.gmail.com
export SMTP_PORT=587
export SMTP_USERNAME=your-email@gmail.com
export SMTP_PASSWORD=your-app-password
export SMTP_SENDER=db-backup@yourcompany.com
export SMTP_USE_TLS=true

# Run a backup with notifications
dbsavr --config config.yaml backup myapp_db
```

Success emails include details like:
- Database name
- Backup size
- S3 location
- Duration of the backup
- Number of old backups removed

Failure emails include:
- Database name
- Error message
- Suggestion to check logs for more details

## Advanced Usage

### Using Custom S3 Buckets

You can specify different S3 buckets for specific databases:

```yaml
databases:
  myapp_db:
    # ... other database config ...
    bucket_name: myapp-specific-backups
```

### Custom Backup Prefixes

You can use different prefixes for different backup schedules:

```yaml
schedules:
  - database_name: myapp_db
    cron_expression: "0 2 * * *"
    retention_days: 30
    prefix: daily-backups
  
  - database_name: myapp_db
    cron_expression: "0 0 * * 0" 
    retention_days: 90
    prefix: weekly-backups
```

This allows you to have different retention policies for daily vs. weekly backups of the same database.

### Database-Specific Options

Each database type supports specific options:

**PostgreSQL:**
```yaml
options:
  extra_args:
    - "--exclude-table=temp_logs"
    - "--no-owner"
```

**MySQL/MariaDB:**
```yaml
options:
  extra_args:
    - "--skip-triggers"
    - "--skip-events"
```

**MongoDB:**
```yaml
options:
  auth_db: admin  # Authentication database
  extra_args:
    - "--excludeCollection=temp_data"
```

## Security Best Practices

- **IAM Roles**: When running on AWS EC2 or ECS, use IAM roles instead of hardcoded credentials
- **Least Privilege**: Create specific S3 IAM policies that only allow access to the backup bucket
- **Secure Passwords**: Use environment variables or secure secret management for database passwords
- **TLS for SMTP**: Always use TLS when sending email notifications

## Development

### Running Tests

dbsavr uses pytest for testing. To run the test suite:

```bash
# Install development dependencies
pip install -e '.[dev]'

# Run all tests
pytest

# Run tests with coverage
pytest --cov=dbsavr

# Run a specific test file
pytest tests/test_backup_engine.py

# Run a specific test
pytest tests/test_backup_engine.py::test_backup_mysql
```

### Contributing

Contributions are welcome! Here's how to get started:

1. Fork the repository
2. Create a feature branch: `git checkout -b feature-name`
3. Make your changes
4. Run the tests: `pytest`
5. Format the code: `black dbsavr tests`
6. Submit a pull request

## License

dbsavr is licensed under the Perpetual Business Source License (PBSL), a license that permits free usage for most purposes but with specific commercial restrictions.

### What You Can Do:
- Use dbsavr commercially to back up your own databases
- Modify and redistribute the code
- Include dbsavr in larger applications or services
- Create and distribute derivative works

### What You Cannot Do:
- Offer dbsavr as a hosted Database Backup Service (a commercial service where database backup is a primary feature)

### License Clarification

The Perpetual Business Source License allows businesses and individuals to use the software freely for their own database backup needs while preventing competitors from offering dbsavr itself as a hosted service.

If you're interested in offering dbsavr as a service or have questions about licensing, please contact [Your Contact Information].

For the complete license text, see the [LICENSE](LICENSE) file in the repository.