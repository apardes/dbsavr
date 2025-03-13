# /dbsavr/storage.py
import os
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import boto3
from botocore.exceptions import ClientError

from .config import S3Config

logger = logging.getLogger(__name__)

class S3Storage:
    def __init__(self, config: S3Config):
        self.config = config
        self.s3_client = self._create_s3_client()
    
    def _create_s3_client(self):
        """Create and return an S3 client"""
        session_kwargs = {
            'region_name': self.config.region
        }
        
        if self.config.access_key and self.config.secret_key:
            session_kwargs['aws_access_key_id'] = self.config.access_key
            session_kwargs['aws_secret_access_key'] = self.config.secret_key
        
        return boto3.client('s3', **session_kwargs)
    
    def upload_backup(self, file_path: str, db_name: str, filename: str, 
                     custom_bucket: Optional[str] = None, custom_prefix: Optional[str] = None) -> str:
        """
        Upload backup file to S3 and return the S3 object key
        
        Args:
            file_path: Local path to the backup file
            db_name: Name of the database (used in the S3 key path)
            filename: Filename of the backup
            custom_bucket: Optional override for the bucket name
            custom_prefix: Optional override for the schedule-specific prefix
            
        Returns:
            S3 object key of the uploaded backup
            
        Raises:
            ClientError: If the upload to S3 fails
        """
        # Use custom bucket if provided, otherwise use the default
        bucket_name = custom_bucket or self.config.bucket_name
        
        # Base prefix is always from the config
        prefix = self.config.prefix
        
        # Create S3 key in format: prefix/db_name/[schedule_prefix/]filename
        if custom_prefix:
            s3_key = os.path.join(prefix, db_name, custom_prefix, filename)
        else:
            s3_key = os.path.join(prefix, db_name, filename)
        
        try:
            logger.info(f"Uploading {file_path} to s3://{bucket_name}/{s3_key}")
            self.s3_client.upload_file(file_path, bucket_name, s3_key)
            logger.info(f"Successfully uploaded backup to S3: {s3_key}")
            return s3_key
        except ClientError as e:
            logger.error(f"Failed to upload backup to S3: {str(e)}")
            raise
    
    def cleanup_old_backups(self, db_name: str, retention_days: int, 
                           custom_bucket: Optional[str] = None, custom_prefix: Optional[str] = None) -> List[str]:
        """
        Delete backups older than retention_days for a specific database
        
        This method only affects objects under the specific path formed by:
        {prefix}/{db_name}/[{schedule_prefix}/]
        
        Other backup files outside this path (for other databases or in different
        prefixes) will not be affected by this operation.
        
        Args:
            db_name: Name of the database to clean up backups for
            retention_days: Number of days to keep backups (backups older than this will be deleted)
            custom_bucket: Optional override for the bucket name
            custom_prefix: Optional override for the schedule-specific prefix
            
        Returns:
            List of S3 keys that were deleted
            
        Raises:
            ClientError: If listing or deleting objects from S3 fails
        """
        # Use custom bucket if provided, otherwise use the default
        bucket_name = custom_bucket or self.config.bucket_name
        
        # Base prefix is always from the config
        base_prefix = self.config.prefix
        
        # Construct the full prefix with optional schedule prefix
        if custom_prefix:
            prefix = os.path.join(base_prefix, db_name, custom_prefix)
        else:
            prefix = os.path.join(base_prefix, db_name)
        
        cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
        
        deleted_keys = []
        
        try:
            # List all objects with the given prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
            
            for page in pages:
                if 'Contents' not in page:
                    continue
                
                for obj in page['Contents']:
                    # Check if object is older than retention period
                    if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                        s3_key = obj['Key']
                        logger.info(f"Deleting old backup: s3://{bucket_name}/{s3_key}")
                        self.s3_client.delete_object(Bucket=bucket_name, Key=s3_key)
                        deleted_keys.append(s3_key)
            
            logger.info(f"Deleted {len(deleted_keys)} old backups for {db_name}")
            return deleted_keys
        except ClientError as e:
            logger.error(f"Failed to cleanup old backups: {str(e)}")
            raise