# /dbsavr/backup_engine.py
import os
import tempfile
import subprocess
import logging
import shutil
from datetime import datetime
from typing import Tuple, Optional, Dict, Any, List

from .config import DatabaseConfig

logger = logging.getLogger(__name__)

class BackupEngine:
    @staticmethod
    def backup_database(db_config: DatabaseConfig) -> Tuple[str, str]:
        """
        Backup the database and return the path to the backup file and filename
        
        Args:
            db_config: Configuration for the database to backup
            
        Returns:
            Tuple containing (backup_path, filename)
            
        Raises:
            ValueError: If the database type is unsupported
            Exception: If the backup process fails
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        db_type = db_config.type.lower()
        
        if db_type == "mysql" or db_type == "mariadb":
            return BackupEngine._backup_mysql(db_config, timestamp)
        elif db_type == "postgresql":
            return BackupEngine._backup_postgresql(db_config, timestamp)
        elif db_type == "mongodb":
            return BackupEngine._backup_mongodb(db_config, timestamp)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
    
    @staticmethod
    def get_backup_details(backup_path: str) -> Dict[str, Any]:
        """
        Get details about a backup file
        
        Args:
            backup_path: Path to the backup file
            
        Returns:
            Dict containing details like size, creation time
        """
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Backup file not found: {backup_path}")
            
        stat_info = os.stat(backup_path)
        return {
            'size': stat_info.st_size,
            'created_at': datetime.fromtimestamp(stat_info.st_ctime),
            'extension': os.path.splitext(backup_path)[1],
            'full_path': os.path.abspath(backup_path)
        }
    
    @staticmethod
    def _backup_mysql(db_config: DatabaseConfig, timestamp: str) -> Tuple[str, str]:
        """
        Create a MySQL/MariaDB backup
        
        Uses environment variables for authentication to avoid exposing credentials in process list.
        Ensures proper cleanup of temporary files in case of errors.
        
        Args:
            db_config: MySQL/MariaDB configuration
            timestamp: Timestamp string for the backup filename
            
        Returns:
            Tuple containing (backup_path, filename)
        """
        temp_dir = tempfile.gettempdir()
        filename = f"{db_config.database}_{timestamp}.sql.gz"
        backup_path = os.path.join(temp_dir, filename)
        temp_files = []
        
        try:
            # Build mysqldump command
            cmd = [
                "mysqldump",
                f"--host={db_config.host}",
                f"--port={db_config.port}",
                f"--user={db_config.username}",
                "--single-transaction",  # Consistent backup without locking tables
                "--routines",  # Include stored procedures
                "--triggers",  # Include triggers
                "--events",    # Include events
                db_config.database
            ]
            
            # Add any additional options
            if db_config.options and 'extra_args' in db_config.options:
                cmd.extend(db_config.options['extra_args'])
            
            # Set up environment with password for security
            env = os.environ.copy()
            env['MYSQL_PWD'] = db_config.password
            
            # Pipe to gzip for compression
            with open(backup_path, 'wb') as f:
                mysqldump_process = subprocess.Popen(
                    cmd, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    env=env
                )
                gzip_process = subprocess.Popen(
                    ['gzip'], 
                    stdin=mysqldump_process.stdout, 
                    stdout=f, 
                    stderr=subprocess.PIPE
                )
                
                # Allow mysqldump to receive a SIGPIPE if gzip exits
                if mysqldump_process.stdout:
                    mysqldump_process.stdout.close()
                
                # Wait for gzip to complete
                gzip_stdout, gzip_stderr = gzip_process.communicate()
                gzip_returncode = gzip_process.returncode
                
                # Wait for mysqldump to complete
                mysqldump_stdout, mysqldump_stderr = mysqldump_process.communicate()
                mysqldump_returncode = mysqldump_process.returncode
                
                if mysqldump_returncode != 0:
                    error_msg = f"mysqldump failed: {mysqldump_stderr.decode('utf-8')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                if gzip_returncode != 0:
                    error_msg = f"gzip failed: {gzip_stderr.decode('utf-8')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            
            logger.info(f"Created MySQL backup: {backup_path}")
            return backup_path, filename
            
        except Exception as e:
            # Clean up any partial or failed backup file
            if os.path.exists(backup_path):
                os.unlink(backup_path)
            logger.error(f"MySQL backup failed: {str(e)}")
            raise
    
    @staticmethod
    def _backup_postgresql(db_config: DatabaseConfig, timestamp: str) -> Tuple[str, str]:
        """
        Create a PostgreSQL backup
        
        Uses environment variables for authentication and ensures proper cleanup on errors.
        
        Args:
            db_config: PostgreSQL configuration
            timestamp: Timestamp string for the backup filename
            
        Returns:
            Tuple containing (backup_path, filename)
        """
        temp_dir = tempfile.gettempdir()
        filename = f"{db_config.database}_{timestamp}.sql.gz"
        backup_path = os.path.join(temp_dir, filename)
        
        try:
            # Set environment variables for authentication
            env = os.environ.copy()
            
            # Always set PGPASSWORD as a string, even if it's empty
            env['PGPASSWORD'] = str(db_config.password) if db_config.password is not None else ""
            
            # Build pg_dump command
            cmd = [
                "pg_dump",
                f"--host={db_config.host}",
                f"--port={db_config.port}",
                f"--username={db_config.username}",
                "--format=plain",  # Plain SQL output
                "--no-owner",      # Skip ownership commands
                "--no-acl",        # Skip access privilege commands
                db_config.database
            ]
            
            # Add any additional options
            if db_config.options and 'extra_args' in db_config.options:
                cmd.extend(db_config.options['extra_args'])
            
            # Pipe to gzip for compression
            with open(backup_path, 'wb') as f:
                pg_dump_process = subprocess.Popen(
                    cmd, 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE, 
                    env=env
                )
                gzip_process = subprocess.Popen(
                    ['gzip'], 
                    stdin=pg_dump_process.stdout, 
                    stdout=f, 
                    stderr=subprocess.PIPE
                )
                
                # Allow pg_dump to receive a SIGPIPE if gzip exits
                if pg_dump_process.stdout:
                    pg_dump_process.stdout.close()
                
                # Wait for gzip to complete
                gzip_stdout, gzip_stderr = gzip_process.communicate()
                gzip_returncode = gzip_process.returncode
                
                # Wait for pg_dump to complete
                pg_dump_stdout, pg_dump_stderr = pg_dump_process.communicate()
                pg_dump_returncode = pg_dump_process.returncode
                
                if pg_dump_returncode != 0:
                    error_msg = f"pg_dump failed: {pg_dump_stderr.decode('utf-8')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
                
                if gzip_returncode != 0:
                    error_msg = f"gzip failed: {gzip_stderr.decode('utf-8')}"
                    logger.error(error_msg)
                    raise Exception(error_msg)
            
            logger.info(f"Created PostgreSQL backup: {backup_path}")
            return backup_path, filename
            
        except Exception as e:
            # Clean up any partial or failed backup file
            if os.path.exists(backup_path):
                os.unlink(backup_path)
            logger.error(f"PostgreSQL backup failed: {str(e)}")
            raise
        
    @staticmethod
    def _backup_mongodb(db_config: DatabaseConfig, timestamp: str) -> Tuple[str, str]:
        """
        Create a MongoDB backup
        
        Adds support for authentication database and read preference options to ensure
        consistent backups without locking. Properly cleans up temporary files on error.
        
        Args:
            db_config: MongoDB configuration
            timestamp: Timestamp string for the backup filename
            
        Returns:
            Tuple containing (backup_path, filename)
        """
        temp_dir = tempfile.gettempdir()
        backup_dir = os.path.join(temp_dir, f"mongodb_backup_{timestamp}")
        
        try:
            os.makedirs(backup_dir, exist_ok=True)
            
            # Build mongodump command
            cmd = [
                "mongodump",
                f"--host={db_config.host}",
                f"--port={db_config.port}",
                f"--username={db_config.username}",
                f"--password={db_config.password}",
                f"--db={db_config.database}",
                f"--out={backup_dir}"
            ]
            
            # Add authentication database if provided in options
            auth_db = None
            if db_config.options and 'auth_db' in db_config.options:
                auth_db = db_config.options['auth_db']
                cmd.append(f"--authenticationDatabase={auth_db}")
            
            # Add read preference for non-blocking backups (similar to MySQL's single-transaction)
            cmd.append("--readPreference=secondary")
            
            # Add any additional options
            if db_config.options and 'extra_args' in db_config.options:
                cmd.extend(db_config.options['extra_args'])
            
            # Execute mongodump
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            stdout, stderr = process.communicate()
            
            if process.returncode != 0:
                error_msg = f"mongodump failed: {stderr.decode('utf-8')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Create a tar.gz archive of the backup
            filename = f"{db_config.database}_{timestamp}.tar.gz"
            archive_path = os.path.join(temp_dir, filename)
            
            tar_cmd = [
                "tar",
                "-czf",
                archive_path,
                "-C",
                temp_dir,
                f"mongodb_backup_{timestamp}"
            ]
            
            tar_process = subprocess.Popen(tar_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            tar_stdout, tar_stderr = tar_process.communicate()
            
            if tar_process.returncode != 0:
                error_msg = f"tar failed: {tar_stderr.decode('utf-8')}"
                logger.error(error_msg)
                raise Exception(error_msg)
            
            # Clean up temporary directory
            shutil.rmtree(backup_dir, ignore_errors=True)
            
            logger.info(f"Created MongoDB backup: {archive_path}")
            return archive_path, filename
            
        except Exception as e:
            # Clean up all temporary files and directories
            if os.path.exists(backup_dir):
                shutil.rmtree(backup_dir, ignore_errors=True)
                
            # Remove archive if it was partially created
            archive_path = os.path.join(temp_dir, f"{db_config.database}_{timestamp}.tar.gz")
            if os.path.exists(archive_path):
                os.unlink(archive_path)
                
            logger.error(f"MongoDB backup failed: {str(e)}")
            raise