"""
Base scraper class implementing the core scraping infrastructure
Follows the pattern: Fetch → Parse → Normalize → Push to message queue
"""

import asyncio
import time
import json
import logging
import hashlib
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from fake_useragent import UserAgent
try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

try:
    from scrapers.base.proxy_manager import ProxyManager
    from scrapers.base.queue_manager import MessageQueueManager
    from middleware.monitoring import monitor_function
except ImportError as e:
    logger.warning(f"Import error in scraper_base: {e}")
    # Create fallback classes
    class ProxyManager:
        def __init__(self, config): pass
        async def get_session_config(self): return {'headers': {}}
        def get_stats(self): return {}
    
    class MessageQueueManager:
        def __init__(self, config): pass
        async def publish(self, topic, message, key=None): pass
        def get_stats(self): return {}
    
    def monitor_function(name):
        def decorator(func):
            return func
        return decorator

@dataclass
class ScrapedData:
    """Standardized data structure for all scraped content"""
    source: str
    data_type: str  # 'price', 'news', 'filing', 'esg', etc.
    symbol: Optional[str]
    timestamp: datetime
    raw_data: Dict[str, Any]
    metadata: Dict[str, Any]
    quality_score: float = 1.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        data = asdict(self)
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def get_cache_key(self) -> str:
        """Generate a unique cache key for this data"""
        key_data = f"{self.source}:{self.data_type}:{self.symbol}:{self.timestamp.isoformat()}"
        return hashlib.md5(key_data.encode()).hexdigest()

class BaseScraper(ABC):
    """
    Abstract base class for all scrapers
    Implements common functionality: proxy rotation, rate limiting, error handling
    """
    
    def __init__(self, source_name: str, config: Dict[str, Any]):
        self.source_name = source_name
        self.config = config
        self.logger = structlog.get_logger(f"scraper.{source_name}")
        
        # Initialize components
        self.proxy_manager = ProxyManager(config.get('proxy_config', {}))
        self.queue_manager = MessageQueueManager(config.get('queue_config', {}))
        self.user_agent = UserAgent()
        
        # Rate limiting
        self.request_delay = config.get('request_delay', 1.0)
        self.last_request_time = 0
        
        # Quality monitoring
        self.success_count = 0
        self.error_count = 0
        self.start_time = time.time()
        
        # Session management
        self.session_headers = self._get_default_headers()
    
    def _get_default_headers(self) -> Dict[str, str]:
        """Get default HTTP headers with rotation"""
        return {
            'User-Agent': self.user_agent.random,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
    
    async def _rate_limit(self):
        """Implement rate limiting between requests"""
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.request_delay:
            sleep_time = self.request_delay - time_since_last
            await asyncio.sleep(sleep_time)
        
        self.last_request_time = time.time()
    
    @abstractmethod
    async def fetch_data(self, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch raw data from the source
        Must be implemented by each scraper
        """
        pass
    
    @abstractmethod
    def parse_data(self, raw_data: Dict[str, Any]) -> List[ScrapedData]:
        """
        Parse raw data into standardized format
        Must be implemented by each scraper
        """
        pass
    
    def normalize_data(self, parsed_data: List[ScrapedData]) -> List[ScrapedData]:
        """
        Normalize data format and apply quality checks
        Can be overridden by specific scrapers
        """
        normalized = []
        
        for data in parsed_data:
            # Apply quality checks
            if self._validate_data(data):
                data.quality_score = self._calculate_quality_score(data)
                normalized.append(data)
            else:
                self.logger.warning(
                    "Data validation failed",
                    source=data.source,
                    data_type=data.data_type,
                    symbol=data.symbol
                )
        
        return normalized
    
    def _validate_data(self, data: ScrapedData) -> bool:
        """Basic data validation"""
        # Check required fields
        if not data.source or not data.data_type or not data.timestamp:
            return False
        
        # Check timestamp is recent (within last 24 hours for most data)
        time_diff = datetime.now(timezone.utc) - data.timestamp.replace(tzinfo=timezone.utc)
        if time_diff.total_seconds() > 86400:  # 24 hours
            return False
        
        # Check raw_data is not empty
        if not data.raw_data:
            return False
        
        return True
    
    def _calculate_quality_score(self, data: ScrapedData) -> float:
        """Calculate quality score based on completeness and freshness"""
        score = 1.0
        
        # Completeness check
        required_fields = data.raw_data.keys()
        if len(required_fields) < 3:  # Minimum fields expected
            score -= 0.2
        
        # Freshness check
        time_diff = datetime.now(timezone.utc) - data.timestamp.replace(tzinfo=timezone.utc)
        freshness_hours = time_diff.total_seconds() / 3600
        
        if freshness_hours > 1:
            score -= min(0.3, freshness_hours * 0.05)
        
        return max(0.1, score)
    
    async def push_to_queue(self, data_list: List[ScrapedData]):
        """Push normalized data to message queue"""
        for data in data_list:
            topic = f"raw.{self.source_name}.{data.data_type}"
            
            try:
                await self.queue_manager.publish(
                    topic=topic,
                    message=data.to_dict(),
                    key=data.get_cache_key()
                )
                
                self.logger.info(
                    "Data pushed to queue",
                    topic=topic,
                    symbol=data.symbol,
                    quality_score=data.quality_score
                )
                
            except Exception as e:
                self.logger.error(
                    "Failed to push data to queue",
                    topic=topic,
                    error=str(e),
                    symbol=data.symbol
                )
                raise
    
    @monitor_function("scraper_run")
    async def run_scrape(self, **kwargs) -> Dict[str, Any]:
        """
        Main scraping workflow: Fetch → Parse → Normalize → Push
        """
        start_time = time.time()
        results = {
            'source': self.source_name,
            'start_time': datetime.now(timezone.utc).isoformat(),
            'success': False,
            'items_processed': 0,
            'errors': []
        }
        
        try:
            # Step 1: Fetch raw data
            self.logger.info("Starting data fetch", source=self.source_name)
            raw_data_list = await self.fetch_data(**kwargs)
            
            if not raw_data_list:
                self.logger.warning("No data fetched", source=self.source_name)
                results['errors'].append("No data returned from source")
                return results
            
            # Step 2: Parse data
            self.logger.info(
                "Parsing data", 
                source=self.source_name, 
                raw_items=len(raw_data_list)
            )
            
            all_parsed_data = []
            for raw_data in raw_data_list:
                try:
                    parsed_data = self.parse_data(raw_data)
                    all_parsed_data.extend(parsed_data)
                except Exception as e:
                    self.logger.error(
                        "Parse error for item",
                        error=str(e),
                        raw_data_preview=str(raw_data)[:200]
                    )
                    results['errors'].append(f"Parse error: {str(e)}")
            
            # Step 3: Normalize data
            self.logger.info(
                "Normalizing data",
                source=self.source_name,
                parsed_items=len(all_parsed_data)
            )
            normalized_data = self.normalize_data(all_parsed_data)
            
            # Step 4: Push to queue
            if normalized_data:
                self.logger.info(
                    "Pushing to queue",
                    source=self.source_name,
                    normalized_items=len(normalized_data)
                )
                await self.push_to_queue(normalized_data)
                
                self.success_count += len(normalized_data)
                results['success'] = True
                results['items_processed'] = len(normalized_data)
            
            # Update metrics
            duration = time.time() - start_time
            results['duration_seconds'] = duration
            results['end_time'] = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(
                "Scrape completed successfully",
                source=self.source_name,
                items=len(normalized_data),
                duration=duration
            )
            
        except Exception as e:
            self.error_count += 1
            results['errors'].append(str(e))
            
            self.logger.error(
                "Scrape failed",
                source=self.source_name,
                error=str(e),
                exc_info=True
            )
            
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get scraper performance statistics"""
        runtime = time.time() - self.start_time
        total_requests = self.success_count + self.error_count
        
        return {
            'source': self.source_name,
            'runtime_seconds': runtime,
            'success_count': self.success_count,
            'error_count': self.error_count,
            'total_requests': total_requests,
            'success_rate': self.success_count / max(1, total_requests),
            'requests_per_minute': (total_requests / max(1, runtime)) * 60
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Check if scraper can connect to source"""
        try:
            # This should be overridden by specific scrapers
            return {
                'source': self.source_name,
                'status': 'healthy',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            return {
                'source': self.source_name,
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }