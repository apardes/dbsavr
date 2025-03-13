# /dbsavr/scheduler_service.py
import os
import logging
import threading
import time
import signal
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Callable, Any, Set
from celery.schedules import crontab

from .config import Config, BackupSchedule
from .backup_service import BackupService

logger = logging.getLogger(__name__)

class SchedulerService:
    """Service for managing backup schedules without Celery dependency"""
    
    def __init__(self, config: Config, backup_service: Optional[BackupService] = None):
        """
        Initialize the scheduler service
        
        Args:
            config: Application configuration
            backup_service: Optional BackupService instance (will create one if not provided)
        """
        self.config = config
        self.backup_service = backup_service or BackupService(config)
        self._scheduler_thread = None
        self._stop_event = threading.Event()
        self._active_backups: Set[str] = set()  # Track active backup jobs by database name
    
    def start_scheduler(self, daemon: bool = False) -> None:
        """
        Start a simple scheduler thread that doesn't use Celery
        
        Args:
            daemon: Whether to run the thread as a daemon
        """
        if self._scheduler_thread and self._scheduler_thread.is_alive():
            logger.warning("Scheduler is already running")
            return
        
        # Reset stop event in case it was set previously
        self._stop_event.clear()
        
        # Create and start the scheduler thread
        self._scheduler_thread = threading.Thread(
            target=self._scheduler_loop,
            name="DBSavrScheduler"
        )
        self._scheduler_thread.daemon = daemon
        self._scheduler_thread.start()
        
        logger.info("Scheduler started")
    
    def stop_scheduler(self, wait: bool = True, timeout: Optional[float] = None) -> bool:
        """
        Stop the scheduler thread
        
        Args:
            wait: Whether to wait for the thread to stop
            timeout: Optional timeout in seconds to wait
            
        Returns:
            True if the scheduler was stopped successfully, False otherwise
        """
        if not self._scheduler_thread or not self._scheduler_thread.is_alive():
            logger.warning("Scheduler is not running")
            return True
        
        # Signal the thread to stop
        logger.info("Stopping scheduler...")
        self._stop_event.set()
        
        if wait:
            logger.info("Waiting for scheduler to stop...")
            self._scheduler_thread.join(timeout=timeout)
            
            if self._scheduler_thread.is_alive():
                logger.warning("Scheduler did not stop within the timeout period")
                return False
        
        logger.info("Scheduler stopped")
        return True
    
    def is_running(self) -> bool:
        """
        Check if the scheduler is running
        
        Returns:
            True if the scheduler is running, False otherwise
        """
        return self._scheduler_thread is not None and self._scheduler_thread.is_alive()
    
    def get_next_run_times(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the next scheduled run time for each database
        
        Returns:
            Dict mapping database names to next run information
        """
        now = datetime.now()
        result = {}
        
        for schedule in self.config.schedules:
            db_name = schedule.database_name
            cron_expression = schedule.cron_expression
            
            try:
                # Parse cron expression
                cron_parts = cron_expression.split()
                if len(cron_parts) != 5:
                    raise ValueError(f"Invalid cron expression: {cron_expression}")
                    
                minute, hour, day_of_month, month_of_year, day_of_week = cron_parts
                
                # Use Celery's crontab to calculate next run time
                # First create a crontab object
                cron_schedule = crontab(
                    minute=minute,
                    hour=hour,
                    day_of_month=day_of_month,
                    month_of_year=month_of_year,
                    day_of_week=day_of_week
                )
                
                # Calculate next run time
                schedule_entry = cron_schedule
                next_run = schedule_entry.maybe_make_aware(schedule_entry.now())
                delta = schedule_entry.remaining_estimate(next_run)
                next_run = next_run + delta
                
                result[db_name] = {
                    'next_run': next_run,
                    'next_run_in': delta.total_seconds(),
                    'cron_expression': cron_expression,
                    'retention_days': schedule.retention_days,
                    'is_active': db_name in self._active_backups
                }
            except Exception as e:
                logger.error(f"Failed to parse cron expression for {db_name}: {str(e)}")
                result[db_name] = {
                    'error': str(e),
                    'cron_expression': cron_expression
                }
        
        return result
    
    def generate_celery_config(self, broker_url: str, result_backend: Optional[str] = None) -> str:
        """
        Generate Celery configuration based on schedules
        
        Args:
            broker_url: URL for the Celery broker
            result_backend: Optional URL for the Celery result backend (defaults to broker_url)
            
        Returns:
            Celery configuration as a string
        """
        if not result_backend:
            result_backend = broker_url
        
        # Start with basic configuration
        config = f"""'''
Celery configuration for dbsavr scheduled backups.
Generated by dbsavr scheduler_service on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
'''

from celery.schedules import crontab

broker_url = '{broker_url}'
result_backend = '{result_backend}'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'UTC'
enable_utc = True

beat_scheduler = 'celery.beat.PersistentScheduler'
beat_schedule = {{
"""
        
        # Add schedule entries
        for idx, schedule in enumerate(self.config.schedules):
            db_name = schedule.database_name
            cron_parts = schedule.cron_expression.split()
            
            if len(cron_parts) != 5:
                logger.warning(f"Invalid cron expression for {db_name}: {schedule.cron_expression}")
                continue
            
            minute, hour, day_of_month, month_of_year, day_of_week = cron_parts
            
            config += f"""    'backup-{db_name}-{idx}': {{
        'task': 'backup_database',
        'schedule': crontab(
            minute='{minute}',
            hour='{hour}',
            day_of_month='{day_of_month}',
            month_of_year='{month_of_year}',
            day_of_week='{day_of_week}'
        ),
        'args': ['{db_name}']
    }},
"""
        
        # Close the configuration
        config += "}\n"
        return config
    
    def _scheduler_loop(self) -> None:
        """
        Main scheduler loop that checks schedules and runs backups
        """
        logger.info("Scheduler loop started")
        
        # Keep track of the next time each backup should run
        next_runs: Dict[str, datetime] = {}
        
        # Initialize next_runs with current schedules
        self._update_next_runs(next_runs)
        
        # Main loop
        while not self._stop_event.is_set():
            now = datetime.now()
            
            # Check each schedule
            for db_name, next_run in list(next_runs.items()):
                if now >= next_run and db_name not in self._active_backups:
                    # Time to run this backup
                    logger.info(f"Scheduling backup for {db_name}")
                    self._run_backup(db_name)
                    
                    # Update next run time for this database
                    self._update_next_run(next_runs, db_name)
            
            # Sleep for a short time before checking again
            # Using Event.wait() instead of time.sleep() allows for quicker shutdown
            self._stop_event.wait(10)  # Check every 10 seconds
        
        logger.info("Scheduler loop stopped")
    
    def _update_next_runs(self, next_runs: Dict[str, datetime]) -> None:
        """
        Update the next run times for all schedules
        
        Args:
            next_runs: Dict to update with next run times
        """
        now = datetime.now()
        
        for schedule in self.config.schedules:
            db_name = schedule.database_name
            self._update_next_run(next_runs, db_name, now)
    
    def _update_next_run(
        self, 
        next_runs: Dict[str, datetime],
        db_name: str,
        base_time: Optional[datetime] = None
    ) -> None:
        """
        Update the next run time for a specific database
        
        Args:
            next_runs: Dict to update with next run time
            db_name: Name of the database
            base_time: Optional base time to use (defaults to now)
        """
        now = base_time or datetime.now()
        schedule = next((s for s in self.config.schedules if s.database_name == db_name), None)
        
        if not schedule:
            logger.warning(f"No schedule found for {db_name}")
            return
        
        try:
            # Parse cron expression
            cron_parts = schedule.cron_expression.split()
            if len(cron_parts) != 5:
                raise ValueError(f"Invalid cron expression: {schedule.cron_expression}")
                
            minute, hour, day_of_month, month_of_year, day_of_week = cron_parts
            
            # Use Celery's crontab to calculate next run time
            cron_schedule = crontab(
                minute=minute,
                hour=hour,
                day_of_month=day_of_month,
                month_of_year=month_of_year,
                day_of_week=day_of_week
            )
            
            # Calculate next run time
            schedule_entry = cron_schedule
            curr_time = schedule_entry.maybe_make_aware(now)
            delta = schedule_entry.remaining_estimate(curr_time)
            next_run = curr_time + delta
            
            next_runs[db_name] = next_run
            
            logger.info(f"Next run for {db_name}: {next_run}")
        except Exception as e:
            logger.error(f"Failed to parse cron expression for {db_name}: {str(e)}")
    
    def _run_backup(self, db_name: str) -> None:
        """
        Run a backup for a database in a separate thread
        
        Args:
            db_name: Name of the database to back up
        """
        self._active_backups.add(db_name)
        
        def backup_thread():
            try:
                logger.info(f"Starting backup for {db_name}")
                self.backup_service.perform_backup(db_name)
                logger.info(f"Backup completed for {db_name}")
            except Exception as e:
                logger.error(f"Backup failed for {db_name}: {str(e)}", exc_info=True)
            finally:
                self._active_backups.remove(db_name)
        
        thread = threading.Thread(
            target=backup_thread,
            name=f"Backup-{db_name}-{int(time.time())}"
        )
        thread.daemon = True
        thread.start()