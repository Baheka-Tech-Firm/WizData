"""
Scraper Orchestration Engine
Manages multiple scrapers, scheduling, monitoring, and quality assurance
"""

import asyncio
import time
import json
from typing import Dict, List, Any, Optional, Type
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, asdict
import logging

# Import scraper classes
try:
    from scrapers.sources.jse_scraper import JSEScraper
    from scrapers.sources.coingecko_scraper import CoinGeckoScraper
    from scrapers.base.scraper_base import BaseScraper
    SCRAPERS_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Scraper imports failed: {e}")
    SCRAPERS_AVAILABLE = False

logger = logging.getLogger(__name__)

@dataclass
class ScrapingJob:
    """Configuration for a scraping job"""
    scraper_name: str
    scraper_class: str
    config: Dict[str, Any]
    schedule: Dict[str, Any]  # {'interval': 300, 'enabled': True}
    last_run: Optional[datetime] = None
    next_run: Optional[datetime] = None
    success_count: int = 0
    error_count: int = 0
    enabled: bool = True

@dataclass
class ScrapingResult:
    """Result of a scraping operation"""
    job_name: str
    start_time: datetime
    end_time: datetime
    success: bool
    items_processed: int
    errors: List[str]
    duration_seconds: float
    quality_score: float

class ScraperOrchestrator:
    """
    Orchestrates multiple scrapers with scheduling, monitoring, and QA
    Implements the intelligent scheduling and prioritization system
    """
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.jobs = {}
        self.scrapers = {}
        self.running = False
        
        # Performance tracking
        self.total_runs = 0
        self.total_errors = 0
        self.start_time = time.time()
        
        # Initialize available scrapers
        self.scraper_classes = self._get_available_scrapers()
        
        logger.info(f"Scraper orchestrator initialized with scrapers: {list(self.scraper_classes.keys())}")
    
    def _get_available_scrapers(self) -> Dict[str, Type[BaseScraper]]:
        """Get available scraper classes"""
        scrapers = {}
        
        if SCRAPERS_AVAILABLE:
            scrapers.update({
                'jse': JSEScraper,
                'coingecko': CoinGeckoScraper,
            })
        
        # Import additional scrapers
        try:
            from scrapers.sources.simple_news_scraper import SimpleNewsScraper
            scrapers['financial_news'] = SimpleNewsScraper
        except ImportError:
            pass
        
        try:
            from scrapers.sources.simple_forex_scraper import SimpleForexScraper
            scrapers['forex'] = SimpleForexScraper
        except ImportError:
            pass
        
        try:
            from scrapers.sources.simple_economic_scraper import SimpleEconomicScraper
            scrapers['economic'] = SimpleEconomicScraper
        except ImportError:
            pass
        
        return scrapers
    
    def add_job(self, job_name: str, job_config: Dict[str, Any]):
        """Add a new scraping job"""
        try:
            job = ScrapingJob(
                scraper_name=job_name,
                scraper_class=job_config['scraper_class'],
                config=job_config.get('config', {}),
                schedule=job_config.get('schedule', {'interval': 300, 'enabled': True}),
                enabled=job_config.get('enabled', True)
            )
            
            # Calculate next run time
            if job.schedule.get('enabled', True):
                interval = job.schedule.get('interval', 300)
                job.next_run = datetime.now(timezone.utc) + timedelta(seconds=interval)
            
            self.jobs[job_name] = job
            
            logger.info(f"Scraping job added: {job_name} ({job.scraper_class}) - interval: {job.schedule.get('interval')}s")
            
        except Exception as e:
            logger.error(f"Failed to add scraping job {job_name}: {str(e)}")
            raise
    
    def remove_job(self, job_name: str):
        """Remove a scraping job"""
        if job_name in self.jobs:
            del self.jobs[job_name]
            logger.info(f"Scraping job removed: {job_name}")
        
        if job_name in self.scrapers:
            del self.scrapers[job_name]
    
    def enable_job(self, job_name: str):
        """Enable a scraping job"""
        if job_name in self.jobs:
            self.jobs[job_name].enabled = True
            # Recalculate next run
            interval = self.jobs[job_name].schedule.get('interval', 300)
            self.jobs[job_name].next_run = datetime.now(timezone.utc) + timedelta(seconds=interval)
            logger.info(f"Scraping job enabled: {job_name}")
    
    def disable_job(self, job_name: str):
        """Disable a scraping job"""
        if job_name in self.jobs:
            self.jobs[job_name].enabled = False
            self.jobs[job_name].next_run = None
            logger.info(f"Scraping job disabled: {job_name}")
    
    async def run_job_once(self, job_name: str, **kwargs) -> ScrapingResult:
        """Run a single scraping job immediately"""
        if job_name not in self.jobs:
            raise ValueError(f"Job '{job_name}' not found")
        
        job = self.jobs[job_name]
        
        logger.info(f"Running scraping job: {job_name}")
        
        start_time = datetime.now(timezone.utc)
        result = ScrapingResult(
            job_name=job_name,
            start_time=start_time,
            end_time=start_time,
            success=False,
            items_processed=0,
            errors=[],
            duration_seconds=0.0,
            quality_score=0.0
        )
        
        try:
            # Get or create scraper instance
            if job_name not in self.scrapers:
                scraper_class = self.scraper_classes.get(job.scraper_class)
                if not scraper_class:
                    raise ValueError(f"Scraper class '{job.scraper_class}' not available")
                
                self.scrapers[job_name] = scraper_class(job.config)
            
            scraper = self.scrapers[job_name]
            
            # Run the scraper
            scrape_result = await scraper.run_scrape(**kwargs)
            
            # Update result
            result.success = scrape_result.get('success', False)
            result.items_processed = scrape_result.get('items_processed', 0)
            result.errors = scrape_result.get('errors', [])
            
            # Update job statistics
            if result.success:
                job.success_count += 1
                self.total_runs += 1
            else:
                job.error_count += 1
                self.total_errors += 1
            
            job.last_run = start_time
            
            # Calculate next run time
            if job.schedule.get('enabled', True) and job.enabled:
                interval = job.schedule.get('interval', 300)
                job.next_run = start_time + timedelta(seconds=interval)
            
            logger.info(f"Scraping job completed: {job_name} - success: {result.success}, items: {result.items_processed}")
            
        except Exception as e:
            result.errors.append(str(e))
            job.error_count += 1
            self.total_errors += 1
            
            logger.error(f"Scraping job failed: {job_name} - {str(e)}")
        
        finally:
            result.end_time = datetime.now(timezone.utc)
            result.duration_seconds = (result.end_time - result.start_time).total_seconds()
            
            # Calculate quality score based on success and items processed
            if result.success and result.items_processed > 0:
                result.quality_score = min(1.0, result.items_processed / 10)  # Normalize to 0-1
            else:
                result.quality_score = 0.0
        
        return result
    
    async def run_scheduler(self):
        """Run the job scheduler (main orchestration loop)"""
        self.running = True
        logger.info("Scraper scheduler started")
        
        while self.running:
            try:
                current_time = datetime.now(timezone.utc)
                
                # Check which jobs are ready to run
                ready_jobs = []
                for job_name, job in self.jobs.items():
                    if (job.enabled and 
                        job.next_run and 
                        current_time >= job.next_run):
                        ready_jobs.append(job_name)
                
                # Run ready jobs concurrently (with some limits)
                if ready_jobs:
                    logger.info(
                        "Running scheduled jobs",
                        job_count=len(ready_jobs),
                        jobs=ready_jobs
                    )
                    
                    # Limit concurrent jobs to avoid overwhelming system
                    max_concurrent = self.config.get('max_concurrent_jobs', 3)
                    
                    for i in range(0, len(ready_jobs), max_concurrent):
                        batch = ready_jobs[i:i + max_concurrent]
                        
                        # Run batch concurrently
                        tasks = [self.run_job_once(job_name) for job_name in batch]
                        results = await asyncio.gather(*tasks, return_exceptions=True)
                        
                        # Log results
                        for job_name, result in zip(batch, results):
                            if isinstance(result, Exception):
                                logger.error(
                                    "Job execution failed",
                                    job_name=job_name,
                                    error=str(result)
                                )
                            else:
                                logger.debug(
                                    "Job completed",
                                    job_name=job_name,
                                    success=result.success,
                                    duration=result.duration_seconds
                                )
                
                # Sleep before next check
                await asyncio.sleep(self.config.get('scheduler_interval', 30))
                
            except Exception as e:
                logger.error(
                    "Error in scheduler loop",
                    error=str(e)
                )
                await asyncio.sleep(60)  # Wait longer on error
    
    def stop_scheduler(self):
        """Stop the job scheduler"""
        self.running = False
        logger.info("Scraper scheduler stopped")
    
    async def health_check_all(self) -> Dict[str, Any]:
        """Run health checks on all scrapers"""
        health_results = {}
        
        for job_name, job in self.jobs.items():
            try:
                if job_name in self.scrapers:
                    scraper = self.scrapers[job_name]
                    health = await scraper.health_check()
                    health_results[job_name] = health
                else:
                    health_results[job_name] = {
                        'status': 'not_initialized',
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    
            except Exception as e:
                health_results[job_name] = {
                    'status': 'error',
                    'error': str(e),
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
        
        return health_results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get orchestrator statistics"""
        runtime = time.time() - self.start_time
        
        job_stats = {}
        for job_name, job in self.jobs.items():
            job_stats[job_name] = {
                'enabled': job.enabled,
                'success_count': job.success_count,
                'error_count': job.error_count,
                'success_rate': job.success_count / max(1, job.success_count + job.error_count),
                'last_run': job.last_run.isoformat() if job.last_run else None,
                'next_run': job.next_run.isoformat() if job.next_run else None,
                'scraper_class': job.scraper_class
            }
        
        return {
            'orchestrator': {
                'running': self.running,
                'runtime_seconds': runtime,
                'total_jobs': len(self.jobs),
                'enabled_jobs': sum(1 for job in self.jobs.values() if job.enabled),
                'total_runs': self.total_runs,
                'total_errors': self.total_errors,
                'overall_success_rate': self.total_runs / max(1, self.total_runs + self.total_errors)
            },
            'jobs': job_stats,
            'available_scrapers': list(self.scraper_classes.keys())
        }
    
    def get_job_config(self, job_name: str) -> Dict[str, Any]:
        """Get configuration for a specific job"""
        if job_name not in self.jobs:
            return {}
        
        job = self.jobs[job_name]
        return asdict(job)
    
    def update_job_config(self, job_name: str, updates: Dict[str, Any]):
        """Update configuration for a specific job"""
        if job_name not in self.jobs:
            raise ValueError(f"Job '{job_name}' not found")
        
        job = self.jobs[job_name]
        
        # Update allowed fields
        if 'enabled' in updates:
            job.enabled = updates['enabled']
        
        if 'schedule' in updates:
            job.schedule.update(updates['schedule'])
            
            # Recalculate next run
            if job.enabled and job.schedule.get('enabled', True):
                interval = job.schedule.get('interval', 300)
                job.next_run = datetime.now(timezone.utc) + timedelta(seconds=interval)
            else:
                job.next_run = None
        
        if 'config' in updates:
            job.config.update(updates['config'])
            
            # Recreate scraper instance with new config
            if job_name in self.scrapers:
                del self.scrapers[job_name]
        
        logger.info(
            "Job configuration updated",
            job_name=job_name,
            updates=list(updates.keys())
        )