# /dbsavr/cli.py
import os
import sys
import logging
import click
import multiprocessing
import signal
import time
import subprocess
import tempfile
from datetime import datetime
from celery import Celery
from celery.bin.worker import worker as celery_worker
from celery.bin.beat import beat as celery_beat
from celery.schedules import crontab
import celery.exceptions

from .config import load_config
from .version import __version__
from .backup_service import BackupService
from .scheduler_service import SchedulerService

@click.group()
@click.option('--config', '-c', default='config.yaml', help='Path to config file')
@click.version_option(version=__version__, prog_name="DBSavr")
@click.pass_context
def cli(ctx, config):
    """DBSavr: Database Backup Utility

    Create and manage database backups with S3 storage integration.
    """
    # Set environment variable for config path
    os.environ['DB_BACKUP_CONFIG'] = config
    
    try:
        # Load config
        ctx.obj = load_config(config)
        
        # Configure logging
        logging.basicConfig(
            level=getattr(logging, ctx.obj.log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    except FileNotFoundError:
        click.echo(f"Error: Config file not found: {config}", err=True)
        if config == 'config.yaml':
            click.echo("\nTip: Create a config file from the example:")
            click.echo("     cp examples/config.yaml.example config.yaml")
            click.echo("     Then edit the file with your database and S3 settings.")
        sys.exit(1)
    except Exception as e:
        click.echo(f"Error loading configuration: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.argument('database_name')
@click.option('--timeout', '-t', default=3600, help='Timeout in seconds for backup operation (default: 1 hour)')
@click.pass_obj
def backup(config, database_name, timeout):
    """Run a backup for a specific database"""
    backup_service = BackupService(config)
    
    if database_name not in backup_service.list_available_databases():
        click.echo(f"Error: Database '{database_name}' not found in configuration.", err=True)
        click.echo(f"Available databases: {', '.join(backup_service.list_available_databases())}")
        sys.exit(1)
    
    # Get all schedules for this database
    schedules = [s for i, s in enumerate(config.schedules) if s.database_name == database_name]
    
    if not schedules:
        # No schedules defined, just run a default backup
        click.echo(f"Starting backup for {database_name}...")
        try:
            result = backup_service.perform_backup(database_name)
            click.echo(f"Backup completed: {result['status']}")
            click.echo(f"S3 Key: {result['s3_key']}")
            click.echo(f"Backup Size: {result['size_mb']:.2f} MB")
            click.echo(f"Duration: {result['duration']:.2f} seconds")
            click.echo(f"Deleted old backups: {result['deleted_backups']}")
        except Exception as e:
            click.echo(f"Backup failed: {str(e)}", err=True)
            sys.exit(1)
    else:
        # Run all schedules
        click.echo(f"Starting backup for {database_name} ({len(schedules)} schedules)...")
        
        for idx, schedule in enumerate(config.schedules):
            if schedule.database_name != database_name:
                continue
                
            prefix = schedule.prefix or 'default'
            click.echo(f"\nRunning {prefix} backup...")
            
            try:
                result = backup_service.perform_backup(database_name, schedule_index=idx)
                click.echo(f"✓ Backup completed: {result['status']}")
                click.echo(f"  S3 Key: {result['s3_key']}")
                click.echo(f"  Size: {result['size_mb']:.2f} MB")
                click.echo(f"  Duration: {result['duration']:.2f} seconds")
                click.echo(f"  Deleted old backups: {result['deleted_backups']}")
            except Exception as e:
                click.echo(f"✗ Backup failed for {prefix}: {str(e)}", err=True)
                # Continue with other schedules even if one fails

@cli.command()
@click.pass_obj
def list_databases(config):
    """List all configured databases"""
    backup_service = BackupService(config)
    db_names = backup_service.list_available_databases()
    
    click.echo("Configured databases:")
    
    for db_name in db_names:
        db_info = backup_service.get_database_info(db_name)
        click.echo(f"\n{db_name}:")
        click.echo(f"  Type: {db_info['type']}")
        click.echo(f"  Host: {db_info['host']}:{db_info['port']}")
        click.echo(f"  Database: {db_info['database']}")

@cli.command()
@click.pass_obj
def list_schedules(config):
    """List all configured backup schedules"""
    backup_service = BackupService(config)
    scheduler_service = SchedulerService(config, backup_service)
    
    if not config.schedules:
        click.echo("No backup schedules configured.")
        return
        
    click.echo("Configured backup schedules:")
    
    # Get information about next scheduled runs
    next_runs = scheduler_service.get_next_run_times()
    
    for schedule in config.schedules:
        db_name = schedule.database_name
        click.echo(f"\nDatabase: {db_name}")
        click.echo(f"  Schedule: {schedule.cron_expression}")
        click.echo(f"  Retention: {schedule.retention_days} days")
        
        # Show next run time if available
        if db_name in next_runs and 'next_run' in next_runs[db_name]:
            next_run = next_runs[db_name]['next_run']
            next_run_in = next_runs[db_name]['next_run_in']
            click.echo(f"  Next Run: {next_run.strftime('%Y-%m-%d %H:%M:%S')} (in {next_run_in/60:.1f} minutes)")

@cli.command()
@click.option('--output', '-o', default='celeryconfig.py', help='Output file path')
@click.option('--force', '-f', is_flag=True, help='Overwrite existing file')
@click.option('--broker-url', help='Celery broker URL (e.g., redis://localhost:6379/0)')
@click.option('--result-backend', help='Celery result backend URL')
@click.option('--run-after', is_flag=True, help='Run the scheduler after configuration')
@click.pass_context
def setup_celery_schedule(ctx, output, force, broker_url, result_backend, run_after):
    """Generate Celery schedule configuration"""
    if os.path.exists(output) and not force:
        click.echo(f"Error: Output file exists: {output}", err=True)
        click.echo("Use --force to overwrite")
        sys.exit(1)
        
    click.echo("Generating Celery schedule configuration...")
    
    # Use provided broker URL or prompt user
    if not broker_url:
        broker_url = click.prompt(
            "Enter Celery broker URL", 
            default="redis://localhost:6379/0",
            show_default=True
        )
    
    # Use provided result backend or same as broker if not specified
    if not result_backend:
        result_backend = click.prompt(
            "Enter Celery result backend URL", 
            default=broker_url,
            show_default=True
        )
    
    # Use the SchedulerService to generate the configuration
    scheduler_service = SchedulerService(ctx.obj)
    config_content = scheduler_service.generate_celery_config(broker_url, result_backend)
    
    # Write the configuration to file
    with open(output, 'w') as f:
        f.write(config_content)
    
    click.echo(f"Celery configuration written to {output}")
    
    if run_after:
        # Run the scheduler immediately using the run_scheduler command
        click.echo("\nStarting scheduler as requested...")
        ctx.invoke(run_scheduler)
    else:
        click.echo("\nTo start the scheduler, run:")
        click.echo("  dbsavr run-scheduler")

@cli.command()
@click.argument('database_name')
@click.option('--days', '-d', default=None, type=int, help='Override retention days')
@click.pass_obj
def cleanup(config, database_name, days):
    """Clean up old backups based on retention policy"""
    backup_service = BackupService(config)
    
    if database_name not in backup_service.list_available_databases():
        click.echo(f"Error: Database '{database_name}' not found in configuration.", err=True)
        click.echo(f"Available databases: {', '.join(backup_service.list_available_databases())}")
        sys.exit(1)

    # Get schedule information
    db_info = backup_service.get_database_info(database_name)
    retention_days = days if days is not None else db_info['schedule'].get('retention_days', 30)
        
    click.echo(f"Cleaning up backups for {database_name} older than {retention_days} days...")
    
    try:
        # Clean up old backups using the BackupService
        deleted = backup_service.cleanup_old_backups(database_name, days)
        
        click.echo(f"Deleted {len(deleted)} old backups")
        
        if deleted:
            click.echo("\nDeleted backups:")
            for key in deleted:
                click.echo(f"  - {key}")
    except Exception as e:
        click.echo(f"Cleanup failed: {str(e)}", err=True)
        sys.exit(1)

@cli.command()
@click.option('--workers', '-w', default=1, help='Number of worker processes')
@click.option('--loglevel', '-l', default='info', help='Logging level (debug, info, warning, error, critical)')
@click.option('--detach', '-d', is_flag=True, help='Run in background (detached mode)')
@click.option('--pid-file', help='Base name for PID files (defaults to dbsavr)')
@click.option('--log-dir', default='logs', help='Directory for log files when running in detached mode')
@click.pass_obj
def run_scheduler(config, workers, loglevel, detach, pid_file, log_dir):
    """
    Start both Celery worker and beat scheduler in a single command.
    """
    # Ensure celeryconfig.py exists
    if not os.path.exists('celeryconfig.py'):
        click.echo("Error: celeryconfig.py not found. Run the following command first:", err=True)
        click.echo("  dbsavr setup-celery-schedule", err=True)
        sys.exit(1)

    # Create log directory if it doesn't exist
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    # Set up PID files
    pid_base = pid_file or 'dbsavr'
    pid_file_worker = f"{pid_base}-worker.pid"
    pid_file_beat = f"{pid_base}-beat.pid"
    
    # Set up log files in the log directory
    worker_log = os.path.join(log_dir, 'celery-worker.log')
    beat_log = os.path.join(log_dir, 'celery-beat.log')
    
    # Set up common Celery arguments
    worker_cmd = [
        'celery', 
        '-A', 'dbsavr.tasks', 
        'worker',
        '--concurrency', str(workers),
        '--loglevel', loglevel
    ]
    
    beat_cmd = [
        'celery', 
        '-A', 'dbsavr.tasks', 
        'beat',
        '--loglevel', loglevel
    ]
    
    # For detached mode
    if detach:
        # Add detach-specific arguments
        worker_cmd.extend(['--detach', '--pidfile', pid_file_worker, '--logfile', worker_log])
        beat_cmd.extend(['--detach', '--pidfile', pid_file_beat, '--logfile', beat_log])
        
        click.echo(f"Starting Celery worker in detached mode (log: {worker_log})...")
        try:
            # Run the worker process
            worker_process = subprocess.run(
                worker_cmd, 
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Give the worker a moment to initialize and write PID file
            click.echo("Waiting for worker to initialize...")
            
            # Poll for PID file with timeout
            worker_pid = None
            for _ in range(10):  # Try for up to 5 seconds (10 * 0.5)
                if os.path.exists(pid_file_worker):
                    try:
                        with open(pid_file_worker, 'r') as f:
                            worker_pid = f.read().strip()
                        if worker_pid:
                            break
                    except:
                        pass
                time.sleep(0.5)
            
            if worker_pid:
                click.echo(f"Worker started with PID: {worker_pid}")
            else:
                # Even if PID file isn't detected, the process might still be running
                click.echo(f"Worker started but PID file not found. Check logs: {worker_log}")
                # Try to find process by command pattern
                try:
                    ps_cmd = ["ps", "aux"]
                    grep_cmd = ["grep", "-E", "celery.*worker.*dbsavr.tasks"]
                    pipe1 = subprocess.Popen(ps_cmd, stdout=subprocess.PIPE)
                    pipe2 = subprocess.Popen(grep_cmd, stdin=pipe1.stdout, stdout=subprocess.PIPE, text=True)
                    pipe1.stdout.close()
                    worker_ps = pipe2.communicate()[0]
                    if worker_ps and "grep" not in worker_ps:
                        click.echo("Worker process is running based on process list")
                except:
                    pass
            
            click.echo(f"Starting Celery beat in detached mode (log: {beat_log})...")
            
            # Run the beat process
            beat_process = subprocess.run(
                beat_cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Poll for PID file with timeout
            beat_pid = None
            click.echo("Waiting for beat scheduler to initialize...")
            for _ in range(10):  # Try for up to 5 seconds
                if os.path.exists(pid_file_beat):
                    try:
                        with open(pid_file_beat, 'r') as f:
                            beat_pid = f.read().strip()
                        if beat_pid:
                            break
                    except:
                        pass
                time.sleep(0.5)
            
            if beat_pid:
                click.echo(f"Beat scheduler started with PID: {beat_pid}")
            else:
                # Even if PID file isn't detected, process might still be running
                click.echo(f"Beat scheduler started but PID file not found. Check logs: {beat_log}")
                # Try to find process by command pattern
                try:
                    ps_cmd = ["ps", "aux"]
                    grep_cmd = ["grep", "-E", "celery.*beat.*dbsavr.tasks"]
                    pipe1 = subprocess.Popen(ps_cmd, stdout=subprocess.PIPE)
                    pipe2 = subprocess.Popen(grep_cmd, stdin=pipe1.stdout, stdout=subprocess.PIPE, text=True)
                    pipe1.stdout.close()
                    beat_ps = pipe2.communicate()[0]
                    if beat_ps and "grep" not in beat_ps:
                        click.echo("Beat process is running based on process list")
                except:
                    pass
            
            # Create a combined PID file if requested
            if pid_file and pid_file != 'dbsavr':
                try:
                    with open(pid_file, 'w') as f:
                        if os.path.exists(pid_file_worker):
                            with open(pid_file_worker, 'r') as wf:
                                worker_pid = wf.read().strip()
                                f.write(f"Worker PID: {worker_pid}\n")
                        
                        if os.path.exists(pid_file_beat):
                            with open(pid_file_beat, 'r') as bf:
                                beat_pid = bf.read().strip()
                                f.write(f"Beat PID: {beat_pid}\n")
                except Exception as e:
                    click.echo(f"Warning: Failed to create combined PID file: {str(e)}", err=True)
            
            click.echo("\nScheduler started in background mode.")
            click.echo(f"Check worker logs: {worker_log}")
            click.echo(f"Check beat logs: {beat_log}")
            click.echo("\nTo stop the scheduler:")
            click.echo(f"  dbsavr stop_scheduler --pid-file {pid_file_worker}")
            
        except subprocess.CalledProcessError as e:
            click.echo(f"Error starting detached processes: {str(e)}", err=True)
            if e.stderr:
                click.echo(f"Error output: {e.stderr}", err=True)
            sys.exit(1)
        except Exception as e:
            click.echo(f"Error setting up detached processes: {str(e)}", err=True)
            sys.exit(1)
            
    else:
        # For non-detached mode, run interactively
        # Create output files
        worker_output = open(worker_log, 'w')
        beat_output = open(beat_log, 'w')
        
        try:
            click.echo(f"Starting Celery worker with {workers} processes...")
            worker_process = subprocess.Popen(
                worker_cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # Line buffered
            )
            
            click.echo("Starting Celery beat scheduler...")
            beat_process = subprocess.Popen(
                beat_cmd, 
                stdout=subprocess.PIPE, 
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1  # Line buffered
            )
            
            click.echo("Scheduler is running. Press Ctrl+C to stop.")
            
            # Setup signal handling for non-detached mode
            def signal_handler(sig, frame):
                click.echo("\nShutting down scheduler...")
                worker_process.terminate()
                beat_process.terminate()
                try:
                    worker_process.wait(timeout=5)
                    beat_process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    click.echo("Processes did not terminate gracefully, forcing exit...")
                    worker_process.kill()
                    beat_process.kill()
                
                # Close file handlers
                worker_output.close()
                beat_output.close()
                sys.exit(0)
            
            signal.signal(signal.SIGINT, signal_handler)
            signal.signal(signal.SIGTERM, signal_handler)
            
            # Function to handle process output
            def print_process_output(process, prefix, output_file):
                for line in iter(process.stdout.readline, ''):
                    if not line:
                        break
                    # Write to console and log file
                    click.echo(line, nl=False)
                    output_file.write(line)
                    output_file.flush()
            
            # Monitor processes and print output
            import threading
            
            # Create threads to handle output
            worker_thread = threading.Thread(
                target=print_process_output, 
                args=(worker_process, "worker", worker_output)
            )
            beat_thread = threading.Thread(
                target=print_process_output, 
                args=(beat_process, "beat", beat_output)
            )
            
            # Start threads
            worker_thread.daemon = True
            beat_thread.daemon = True
            worker_thread.start()
            beat_thread.start()
            
            # Wait for processes to complete or be interrupted
            while True:
                worker_status = worker_process.poll()
                beat_status = beat_process.poll()
                
                if worker_status is not None or beat_status is not None:
                    # At least one process died
                    click.echo("\nOne of the scheduler processes has terminated unexpectedly.", err=True)
                    
                    if worker_status is not None:
                        click.echo(f"Worker process exited with code {worker_status}", err=True)
                    if beat_status is not None:
                        click.echo(f"Beat process exited with code {beat_status}", err=True)
                    
                    # Terminate any remaining processes
                    if worker_status is None:
                        worker_process.terminate()
                    if beat_status is None:
                        beat_process.terminate()
                    
                    # Close file handlers
                    worker_output.close()
                    beat_output.close()
                    
                    sys.exit(1)
                
                time.sleep(1)
            
        except Exception as e:
            click.echo(f"Error running scheduler: {str(e)}", err=True)
            
            # Close file handlers
            worker_output.close()
            beat_output.close()
            
            # Terminate any processes that might be running
            try:
                worker_process.terminate()
                beat_process.terminate()
            except:
                pass
            
            sys.exit(1)

@cli.command()
@click.option('--pid-file', help='File containing the process ID of the running scheduler')
@click.option('--graceful', '-g', is_flag=True, help='Attempt graceful shutdown by waiting for running tasks to complete')
@click.option('--timeout', '-t', default=60, type=int, help='Timeout in seconds for graceful shutdown (default: 60)')
@click.option('--force', '-f', is_flag=True, help='Force kill the scheduler if graceful shutdown fails or times out')
def stop_scheduler(pid_file, graceful, timeout, force):
    """
    Stop the running scheduler process.
    
    This command will stop the Celery worker and beat scheduler processes.
    By default, it sends a SIGTERM to the process group, which allows
    for a clean shutdown.
    
    If --pid-file is not provided, it will attempt to find running
    scheduler processes based on command patterns.
    """
    
    if pid_file:
        # Try to read PID from file
        try:
            with open(pid_file, 'r') as f:
                pid = int(f.read().strip())
                
            click.echo(f"Found scheduler process with PID {pid}")
            
            # Send SIGTERM for clean shutdown by default
            if graceful:
                click.echo(f"Sending graceful shutdown signal to PID {pid}...")
                os.kill(pid, signal.SIGTERM)
                
                # Wait for the process to exit
                start_time = time.time()
                while time.time() - start_time < timeout:
                    try:
                        # Check if process exists by sending signal 0
                        os.kill(pid, 0)
                        click.echo(".", nl=False)
                        time.sleep(1)
                    except OSError:
                        # Process has exited
                        click.echo("\nScheduler has been stopped gracefully.")
                        # Clean up PID file
                        try:
                            os.unlink(pid_file)
                        except OSError:
                            pass
                        return 0
                
                # If we get here, graceful shutdown timed out
                click.echo("\nGraceful shutdown timed out.")
                if force:
                    click.echo(f"Sending SIGKILL to PID {pid}...")
                    try:
                        os.kill(pid, signal.SIGKILL)
                        click.echo("Scheduler has been forcefully stopped.")
                        # Clean up PID file
                        try:
                            os.unlink(pid_file)
                        except OSError:
                            pass
                        return 0
                    except OSError as e:
                        click.echo(f"Error stopping scheduler: {str(e)}", err=True)
                        return 1
                else:
                    click.echo("Use --force to kill the process.")
                    return 1
            else:
                # Simple termination
                try:
                    os.kill(pid, signal.SIGTERM)
                    click.echo("Sent termination signal to scheduler.")
                    
                    # Clean up PID file
                    try:
                        os.unlink(pid_file)
                    except OSError:
                        pass
                    return 0
                except OSError as e:
                    click.echo(f"Error stopping scheduler: {str(e)}", err=True)
                    return 1
        except (IOError, ValueError) as e:
            click.echo(f"Error reading PID file: {str(e)}", err=True)
            return 1
    else:
        # Find scheduler processes by pattern matching
        click.echo("No PID file provided. Attempting to find scheduler processes...")
        
        # Look for Celery processes and our simple scheduler
        try:
            # Try using pgrep first, as it's more efficient
            try:
                pgrep_cmd = ["pgrep", "-f", "(celery.*dbsavr|DBSavrScheduler)"]
                result = subprocess.run(pgrep_cmd, capture_output=True, text=True)
                pids = result.stdout.strip().split("\n")
                pids = [pid.strip() for pid in pids if pid.strip()]
            except (FileNotFoundError, subprocess.SubprocessError):
                # Fall back to ps | grep if pgrep is not available
                ps_cmd = ["ps", "-ef"]
                grep_cmd = ["grep", "-E", "(celery.*dbsavr|DBSavrScheduler)"]
                pipe1 = subprocess.Popen(ps_cmd, stdout=subprocess.PIPE)
                pipe2 = subprocess.Popen(grep_cmd, stdin=pipe1.stdout, stdout=subprocess.PIPE, text=True)
                pipe1.stdout.close()
                grep_result = pipe2.communicate()[0]
                
                # Filter out the grep process itself and extract PIDs
                process_lines = [line for line in grep_result.strip().split("\n") 
                                if "grep" not in line and line.strip()]
                pids = []
                for line in process_lines:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        pids.append(parts[1])
            
            if not pids:
                click.echo("No running dbsavr scheduler processes found.", err=True)
                return 1
            
            click.echo(f"Found {len(pids)} scheduler processes: {', '.join(pids)}")
            
            # Send signal to each process
            for pid in pids:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    click.echo(f"Sent termination signal to PID {pid}")
                except (OSError, ValueError) as e:
                    click.echo(f"Error stopping process {pid}: {str(e)}", err=True)
            
            if graceful:
                # Wait for processes to exit
                click.echo(f"Waiting up to {timeout} seconds for processes to exit...")
                start_time = time.time()
                remaining_pids = pids.copy()
                
                while remaining_pids and time.time() - start_time < timeout:
                    for pid in remaining_pids.copy():
                        try:
                            # Check if process exists by sending signal 0
                            os.kill(int(pid), 0)
                        except (OSError, ValueError):
                            # Process has exited
                            remaining_pids.remove(pid)
                            click.echo(f"Process {pid} has exited.")
                    
                    if remaining_pids:
                        click.echo(".", nl=False)
                        time.sleep(1)
                
                if remaining_pids:
                    click.echo("\nSome processes did not exit in time.")
                    if force:
                        click.echo(f"Forcefully killing remaining processes: {', '.join(remaining_pids)}")
                        for pid in remaining_pids:
                            try:
                                os.kill(int(pid), signal.SIGKILL)
                                click.echo(f"Sent SIGKILL to PID {pid}")
                            except (OSError, ValueError) as e:
                                click.echo(f"Error killing process {pid}: {str(e)}", err=True)
                    else:
                        click.echo("Use --force to kill remaining processes.")
                        return 1
                else:
                    click.echo("\nAll scheduler processes have been stopped gracefully.")
            
            return 0
        except Exception as e:
            click.echo(f"Error finding or stopping scheduler processes: {str(e)}", err=True)
            return 1

def start_worker(app, worker_args):
    """Start the Celery worker process"""
    # Convert loglevel from string to int if necessary
    if 'loglevel' in worker_args and isinstance(worker_args['loglevel'], str):
        # Map common loglevel strings to their numeric values
        loglevel_map = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
        }
        worker_args['loglevel'] = loglevel_map.get(
            worker_args['loglevel'].lower(), 
            logging.INFO
        )
    
    # Use the worker command from celery.bin
    from celery.bin.worker import worker as worker_command
    
    # Create worker command instance
    worker = worker_command(app=app)
    
    # Convert args dict to list of command line args
    worker_cmd_args = ['worker']
    if 'loglevel' in worker_args:
        worker_cmd_args.extend(['--loglevel', str(worker_args['loglevel'])])
    if 'concurrency' in worker_args:
        worker_cmd_args.extend(['--concurrency', str(worker_args['concurrency'])])
    
    # Execute the worker command
    worker.execute_from_commandline(worker_cmd_args)

def start_beat(app, beat_args):
    """Start the Celery beat scheduler process"""
    # Convert loglevel from string to int if necessary
    if 'loglevel' in beat_args and isinstance(beat_args['loglevel'], str):
        # Map common loglevel strings to their numeric values
        loglevel_map = {
            'debug': logging.DEBUG,
            'info': logging.INFO,
            'warning': logging.WARNING,
            'error': logging.ERROR,
            'critical': logging.CRITICAL
        }
        beat_args['loglevel'] = loglevel_map.get(
            beat_args['loglevel'].lower(), 
            logging.INFO
        )
    
    # Import the beat command from celery.bin
    from celery.bin.beat import beat as beat_command
    
    # Create beat command instance
    beat = beat_command(app=app)
    
    # Convert args dict to list of command line args
    beat_cmd_args = ['beat']
    if 'loglevel' in beat_args:
        beat_cmd_args.extend(['--loglevel', str(beat_args['loglevel'])])
    
    # Execute the beat command
    beat.execute_from_commandline(beat_cmd_args)