"""
Background Job Scheduler for WizData ETL Pipeline
Implements cron-like scheduling for data ingestion and processing
"""

import asyncio
import schedule
import threading
import time
import logging
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Callable, Any, Optional
from dataclasses import dataclass
from enum import Enum
import redis
from redis.exceptions import RedisError
import json

logger = logging.getLogger(__name__)

class JobStatus(Enum):
    """Job execution status"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    SCHEDULED = "scheduled"

class JobPriority(Enum):
    """Job priority levels"""
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4

@dataclass
class JobResult:
    """Job execution result"""
    job_id: str
    status: JobStatus
    start_time: datetime
    end_time: Optional[datetime] = None
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    execution_time_seconds: Optional[float] = None

class JobDefinition:
    """Defines a scheduled job"""
    
    def __init__(
        self,
        job_id: str,
        name: str,
        function: Callable,
        schedule_expression: str,
        priority: JobPriority = JobPriority.MEDIUM,
        timeout_seconds: int = 3600,
        retry_count: int = 3,
        retry_delay_seconds: int = 60,
        tags: List[str] = None,
        dependencies: List[str] = None,
        enabled: bool = True
    ):
        self.job_id = job_id
        self.name = name
        self.function = function
        self.schedule_expression = schedule_expression
        self.priority = priority
        self.timeout_seconds = timeout_seconds
        self.retry_count = retry_count
        self.retry_delay_seconds = retry_delay_seconds
        self.tags = tags or []
        self.dependencies = dependencies or []
        self.enabled = enabled
        self.created_at = datetime.now()
        self.last_run = None
        self.next_run = None
        self.run_count = 0
        self.failure_count = 0

class BackgroundJobScheduler:
    """Advanced job scheduler with Redis backend for distributed job management"""
    
    def __init__(self, redis_client: redis.Redis):
        self.redis = redis_client
        self.jobs: Dict[str, JobDefinition] = {}
        self.running_jobs: Dict[str, threading.Thread] = {}
        self.job_queue_key = "wizdata:job_queue"
        self.job_results_key = "wizdata:job_results"
        self.job_locks_key = "wizdata:job_locks"
        self.scheduler_running = False
        self.scheduler_thread = None
        
    def register_job(self, job_def: JobDefinition) -> bool:
        """Register a new job with the scheduler"""
        try:
            self.jobs[job_def.job_id] = job_def
            
            # Store job definition in Redis for persistence
            job_data = {
                'job_id': job_def.job_id,
                'name': job_def.name,
                'schedule_expression': job_def.schedule_expression,
                'priority': job_def.priority.value,
                'timeout_seconds': job_def.timeout_seconds,
                'retry_count': job_def.retry_count,
                'retry_delay_seconds': job_def.retry_delay_seconds,
                'tags': ','.join(job_def.tags),
                'dependencies': ','.join(job_def.dependencies),
                'enabled': job_def.enabled,
                'created_at': job_def.created_at.isoformat(),
                'run_count': job_def.run_count,
                'failure_count': job_def.failure_count
            }
            
            self.redis.hset(f"wizdata:job_def:{job_def.job_id}", mapping=job_data)
            
            # Schedule the job
            self._schedule_job(job_def)
            
            logger.info(f"Registered job: {job_def.name} ({job_def.job_id})")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register job {job_def.job_id}: {str(e)}")
            return False
    
    def _schedule_job(self, job_def: JobDefinition):
        """Schedule a job using the schedule library"""
        if not job_def.enabled:
            return
        
        schedule_expr = job_def.schedule_expression
        
        try:
            # Parse schedule expression and create schedule
            if schedule_expr.startswith('every '):
                # Handle "every X minutes/hours/days" format
                parts = schedule_expr.split()
                if len(parts) >= 3:
                    interval = int(parts[1])
                    unit = parts[2].lower()
                    
                    if unit.startswith('minute'):
                        schedule.every(interval).minutes.do(self._execute_job, job_def.job_id)
                    elif unit.startswith('hour'):
                        schedule.every(interval).hours.do(self._execute_job, job_def.job_id)
                    elif unit.startswith('day'):
                        schedule.every(interval).days.do(self._execute_job, job_def.job_id)
                    elif unit.startswith('week'):
                        schedule.every(interval).weeks.do(self._execute_job, job_def.job_id)
            
            elif schedule_expr.startswith('daily at '):
                # Handle "daily at HH:MM" format
                time_str = schedule_expr.replace('daily at ', '')
                schedule.every().day.at(time_str).do(self._execute_job, job_def.job_id)
            
            elif schedule_expr.startswith('weekly on '):
                # Handle "weekly on monday at HH:MM" format
                parts = schedule_expr.split()
                if len(parts) >= 5:
                    day = parts[2].lower()
                    time_str = parts[4]
                    getattr(schedule.every(), day).at(time_str).do(self._execute_job, job_def.job_id)
            
            else:
                logger.warning(f"Unsupported schedule expression: {schedule_expr}")
                
        except Exception as e:
            logger.error(f"Failed to schedule job {job_def.job_id}: {str(e)}")
    
    def _execute_job(self, job_id: str):
        """Execute a job in a separate thread"""
        if job_id not in self.jobs:
            logger.error(f"Job {job_id} not found")
            return
        
        job_def = self.jobs[job_id]
        
        # Check if job is already running
        if job_id in self.running_jobs:
            logger.warning(f"Job {job_id} is already running, skipping")
            return
        
        # Check dependencies
        if not self._check_dependencies(job_def):
            logger.warning(f"Dependencies not met for job {job_id}, postponing")
            return
        
        # Acquire distributed lock
        lock_acquired = self._acquire_job_lock(job_id)
        if not lock_acquired:
            logger.info(f"Could not acquire lock for job {job_id}, another instance might be running")
            return
        
        # Start job execution in separate thread
        thread = threading.Thread(target=self._run_job_with_error_handling, args=(job_def,))
        self.running_jobs[job_id] = thread
        thread.start()
    
    def _run_job_with_error_handling(self, job_def: JobDefinition):
        """Run a job with comprehensive error handling and monitoring"""
        start_time = datetime.now()
        result = JobResult(
            job_id=job_def.job_id,
            status=JobStatus.RUNNING,
            start_time=start_time
        )
        
        try:
            # Update job status
            self._update_job_status(job_def.job_id, JobStatus.RUNNING, start_time)
            
            logger.info(f"Starting job: {job_def.name} ({job_def.job_id})")
            
            # Execute the job function with timeout
            if asyncio.iscoroutinefunction(job_def.function):
                # Async function
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    task = loop.create_task(job_def.function())
                    result.result_data = loop.run_until_complete(
                        asyncio.wait_for(task, timeout=job_def.timeout_seconds)
                    )
                finally:
                    loop.close()
            else:
                # Sync function - wrap in timeout
                import signal
                
                def timeout_handler(signum, frame):
                    raise TimeoutError(f"Job {job_def.job_id} timed out after {job_def.timeout_seconds} seconds")
                
                old_handler = signal.signal(signal.SIGALRM, timeout_handler)
                signal.alarm(job_def.timeout_seconds)
                
                try:
                    result.result_data = job_def.function()
                finally:
                    signal.alarm(0)
                    signal.signal(signal.SIGALRM, old_handler)
            
            # Job completed successfully
            end_time = datetime.now()
            result.status = JobStatus.COMPLETED
            result.end_time = end_time
            result.execution_time_seconds = (end_time - start_time).total_seconds()
            
            # Update job statistics
            job_def.last_run = start_time
            job_def.run_count += 1
            
            logger.info(f"Job completed: {job_def.name} ({job_def.job_id}) in {result.execution_time_seconds:.2f}s")
            
        except TimeoutError as e:
            end_time = datetime.now()
            result.status = JobStatus.FAILED
            result.end_time = end_time
            result.error_message = str(e)
            result.execution_time_seconds = (end_time - start_time).total_seconds()
            
            job_def.failure_count += 1
            logger.error(f"Job timed out: {job_def.name} ({job_def.job_id})")
            
        except Exception as e:
            end_time = datetime.now()
            result.status = JobStatus.FAILED
            result.end_time = end_time
            result.error_message = str(e)
            result.execution_time_seconds = (end_time - start_time).total_seconds()
            
            job_def.failure_count += 1
            logger.error(f"Job failed: {job_def.name} ({job_def.job_id}): {str(e)}")
            
        finally:
            # Clean up
            self._release_job_lock(job_def.job_id)
            if job_def.job_id in self.running_jobs:
                del self.running_jobs[job_def.job_id]
            
            # Store job result
            self._store_job_result(result)
            
            # Update job definition in Redis
            self._update_job_definition(job_def)
            
            # Handle retries for failed jobs
            if result.status == JobStatus.FAILED and job_def.retry_count > 0:
                self._schedule_retry(job_def, result)
    
    def _check_dependencies(self, job_def: JobDefinition) -> bool:
        """Check if job dependencies are satisfied"""
        if not job_def.dependencies:
            return True
        
        for dep_job_id in job_def.dependencies:
            # Get last result for dependency
            last_result = self._get_last_job_result(dep_job_id)
            if not last_result or last_result.status != JobStatus.COMPLETED:
                return False
        
        return True
    
    def _acquire_job_lock(self, job_id: str) -> bool:
        """Acquire distributed lock for job execution"""
        try:
            lock_key = f"{self.job_locks_key}:{job_id}"
            # Set lock with expiration (in case process dies)
            return self.redis.set(lock_key, "locked", nx=True, ex=3600)  # 1 hour expiration
        except RedisError:
            return False
    
    def _release_job_lock(self, job_id: str):
        """Release distributed lock for job execution"""
        try:
            lock_key = f"{self.job_locks_key}:{job_id}"
            self.redis.delete(lock_key)
        except RedisError:
            pass
    
    def _update_job_status(self, job_id: str, status: JobStatus, timestamp: datetime):
        """Update job status in Redis"""
        try:
            status_data = {
                'job_id': job_id,
                'status': status.value,
                'timestamp': timestamp.isoformat()
            }
            self.redis.hset(f"wizdata:job_status:{job_id}", mapping=status_data)
        except RedisError as e:
            logger.error(f"Failed to update job status: {str(e)}")
    
    def _store_job_result(self, result: JobResult):
        """Store job execution result in Redis"""
        try:
            result_data = {
                'job_id': result.job_id,
                'status': result.status.value,
                'start_time': result.start_time.isoformat(),
                'end_time': result.end_time.isoformat() if result.end_time else None,
                'execution_time_seconds': result.execution_time_seconds,
                'error_message': result.error_message,
                'result_data': json.dumps(result.result_data) if result.result_data else None
            }
            
            # Store with timestamp-based key for history
            result_key = f"{self.job_results_key}:{result.job_id}:{int(result.start_time.timestamp())}"
            self.redis.hset(result_key, mapping=result_data)
            
            # Set expiration for cleanup (keep results for 30 days)
            self.redis.expire(result_key, 30 * 24 * 3600)
            
            # Also store as latest result
            latest_key = f"{self.job_results_key}:latest:{result.job_id}"
            self.redis.hset(latest_key, mapping=result_data)
            
        except RedisError as e:
            logger.error(f"Failed to store job result: {str(e)}")
    
    def _update_job_definition(self, job_def: JobDefinition):
        """Update job definition in Redis"""
        try:
            job_data = {
                'last_run': job_def.last_run.isoformat() if job_def.last_run else None,
                'run_count': job_def.run_count,
                'failure_count': job_def.failure_count
            }
            self.redis.hset(f"wizdata:job_def:{job_def.job_id}", mapping=job_data)
        except RedisError as e:
            logger.error(f"Failed to update job definition: {str(e)}")
    
    def _get_last_job_result(self, job_id: str) -> Optional[JobResult]:
        """Get the last execution result for a job"""
        try:
            latest_key = f"{self.job_results_key}:latest:{job_id}"
            result_data = self.redis.hgetall(latest_key)
            
            if not result_data:
                return None
            
            # Decode bytes to strings
            result_data = {k.decode(): v.decode() for k, v in result_data.items()}
            
            return JobResult(
                job_id=result_data['job_id'],
                status=JobStatus(result_data['status']),
                start_time=datetime.fromisoformat(result_data['start_time']),
                end_time=datetime.fromisoformat(result_data['end_time']) if result_data.get('end_time') else None,
                execution_time_seconds=float(result_data['execution_time_seconds']) if result_data.get('execution_time_seconds') else None,
                error_message=result_data.get('error_message'),
                result_data=json.loads(result_data['result_data']) if result_data.get('result_data') else None
            )
            
        except (RedisError, ValueError, KeyError) as e:
            logger.error(f"Failed to get last job result: {str(e)}")
            return None
    
    def _schedule_retry(self, job_def: JobDefinition, failed_result: JobResult):
        """Schedule a retry for a failed job"""
        def retry_job():
            time.sleep(job_def.retry_delay_seconds)
            self._execute_job(job_def.job_id)
        
        retry_thread = threading.Thread(target=retry_job)
        retry_thread.start()
        
        logger.info(f"Scheduled retry for job {job_def.job_id} in {job_def.retry_delay_seconds} seconds")
    
    def start_scheduler(self):
        """Start the job scheduler"""
        if self.scheduler_running:
            logger.warning("Scheduler is already running")
            return
        
        self.scheduler_running = True
        
        def run_scheduler():
            logger.info("Starting WizData job scheduler")
            while self.scheduler_running:
                try:
                    schedule.run_pending()
                    time.sleep(1)
                except Exception as e:
                    logger.error(f"Scheduler error: {str(e)}")
                    time.sleep(5)
            
            logger.info("Job scheduler stopped")
        
        self.scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
        self.scheduler_thread.start()
    
    def stop_scheduler(self):
        """Stop the job scheduler"""
        self.scheduler_running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5)
        
        # Cancel all running jobs
        for job_id, thread in self.running_jobs.items():
            logger.info(f"Stopping running job: {job_id}")
            # Note: Thread termination is not graceful, jobs should handle interruption
        
        self.running_jobs.clear()
        schedule.clear()
        logger.info("Job scheduler stopped")
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a job"""
        try:
            status_key = f"wizdata:job_status:{job_id}"
            status_data = self.redis.hgetall(status_key)
            
            if not status_data:
                return None
            
            return {k.decode(): v.decode() for k, v in status_data.items()}
            
        except RedisError as e:
            logger.error(f"Failed to get job status: {str(e)}")
            return None
    
    def get_job_history(self, job_id: str, limit: int = 10) -> List[JobResult]:
        """Get execution history for a job"""
        try:
            pattern = f"{self.job_results_key}:{job_id}:*"
            keys = list(self.redis.scan_iter(match=pattern))
            
            # Sort by timestamp (newest first)
            keys.sort(reverse=True)
            keys = keys[:limit]
            
            results = []
            for key in keys:
                result_data = self.redis.hgetall(key)
                if result_data:
                    result_data = {k.decode(): v.decode() for k, v in result_data.items()}
                    results.append(JobResult(
                        job_id=result_data['job_id'],
                        status=JobStatus(result_data['status']),
                        start_time=datetime.fromisoformat(result_data['start_time']),
                        end_time=datetime.fromisoformat(result_data['end_time']) if result_data.get('end_time') else None,
                        execution_time_seconds=float(result_data['execution_time_seconds']) if result_data.get('execution_time_seconds') else None,
                        error_message=result_data.get('error_message'),
                        result_data=json.loads(result_data['result_data']) if result_data.get('result_data') else None
                    ))
            
            return results
            
        except (RedisError, ValueError, KeyError) as e:
            logger.error(f"Failed to get job history: {str(e)}")
            return []

# Global scheduler instance
job_scheduler = None

def init_job_scheduler(redis_client: redis.Redis) -> BackgroundJobScheduler:
    """Initialize the global job scheduler"""
    global job_scheduler
    job_scheduler = BackgroundJobScheduler(redis_client)
    return job_scheduler

def get_job_scheduler() -> Optional[BackgroundJobScheduler]:
    """Get the global job scheduler instance"""
    return job_scheduler
