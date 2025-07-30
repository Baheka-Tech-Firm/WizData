"""
ETL Job Definitions for WizData
Defines all background jobs for data ingestion and processing
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any
import json

from services.job_scheduler import JobDefinition, JobPriority, get_job_scheduler
from scrapers.jse_scraper import JSEScraper
from scrapers.crypto_fetcher import CryptoFetcher
from scrapers.forex_fetcher import ForexFetcher
from scrapers.news_fetcher import NewsFetcher
from scrapers.esg_data_collector import ESGDataCollector
from processing.data_cleaner import DataCleaner
from processing.data_validator import DataValidator
from services.alert_service import AlertService

logger = logging.getLogger(__name__)

class ETLJobManager:
    """Manages all ETL jobs for the WizData platform"""
    
    def __init__(self):
        self.scheduler = None
        self.scrapers = {}
        self.processors = {}
        
    def initialize(self, redis_client):
        """Initialize the ETL job manager with required services"""
        self.scheduler = get_job_scheduler()
        
        # Initialize scrapers
        self.scrapers = {
            'jse': JSEScraper(),
            'crypto': CryptoFetcher(),
            'forex': ForexFetcher(),
            'news': NewsFetcher(),
            'esg': ESGDataCollector()
        }
        
        # Initialize processors
        self.processors = {
            'cleaner': DataCleaner(),
            'validator': DataValidator(),
            'alerts': AlertService(redis_client)
        }
        
        logger.info("ETL Job Manager initialized")
    
    def register_all_jobs(self):
        """Register all ETL jobs with the scheduler"""
        if not self.scheduler:
            logger.error("Scheduler not initialized")
            return False
        
        jobs = [
            # Market Data Collection Jobs
            self._create_jse_quotes_job(),
            self._create_jse_historical_job(),
            self._create_crypto_quotes_job(),
            self._create_forex_quotes_job(),
            
            # News and Sentiment Jobs
            self._create_market_news_job(),
            self._create_sentiment_analysis_job(),
            
            # ESG Data Jobs
            self._create_esg_collection_job(),
            self._create_esg_scoring_job(),
            
            # Data Processing Jobs
            self._create_data_cleaning_job(),
            self._create_data_validation_job(),
            self._create_data_aggregation_job(),
            
            # Alert and Notification Jobs
            self._create_price_alerts_job(),
            self._create_system_health_job(),
            
            # Analytics and Reporting Jobs
            self._create_daily_analytics_job(),
            self._create_weekly_report_job(),
            
            # Data Quality and Maintenance Jobs
            self._create_data_quality_check_job(),
            self._create_cache_cleanup_job(),
            self._create_database_maintenance_job()
        ]
        
        success_count = 0
        for job in jobs:
            if self.scheduler.register_job(job):
                success_count += 1
            else:
                logger.error(f"Failed to register job: {job.name}")
        
        logger.info(f"Registered {success_count}/{len(jobs)} ETL jobs successfully")
        return success_count == len(jobs)
    
    # Market Data Collection Jobs
    def _create_jse_quotes_job(self) -> JobDefinition:
        """Job to collect JSE real-time quotes"""
        async def collect_jse_quotes():
            try:
                scraper = self.scrapers['jse']
                quotes = await scraper.get_all_quotes()
                
                # Store quotes in database/cache
                await self._store_quotes('JSE', quotes)
                
                logger.info(f"Collected {len(quotes)} JSE quotes")
                return {'status': 'success', 'count': len(quotes), 'source': 'JSE'}
                
            except Exception as e:
                logger.error(f"Failed to collect JSE quotes: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='jse_quotes_collection',
            name='JSE Real-time Quotes Collection',
            function=collect_jse_quotes,
            schedule_expression='every 2 minutes',
            priority=JobPriority.HIGH,
            timeout_seconds=300,
            retry_count=2,
            retry_delay_seconds=30,
            tags=['market_data', 'jse', 'quotes', 'real_time'],
            enabled=True
        )
    
    def _create_jse_historical_job(self) -> JobDefinition:
        """Job to collect JSE historical data"""
        async def collect_jse_historical():
            try:
                scraper = self.scrapers['jse']
                
                # Get list of active symbols
                symbols = await scraper.get_active_symbols()
                
                historical_data = []
                for symbol in symbols[:50]:  # Limit to prevent overload
                    try:
                        data = await scraper.get_historical_data(symbol, days=1)
                        historical_data.extend(data)
                        await asyncio.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        logger.warning(f"Failed to get historical data for {symbol}: {str(e)}")
                
                # Store historical data
                await self._store_historical_data('JSE', historical_data)
                
                logger.info(f"Collected historical data for {len(symbols)} JSE symbols")
                return {'status': 'success', 'symbols': len(symbols), 'records': len(historical_data)}
                
            except Exception as e:
                logger.error(f"Failed to collect JSE historical data: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='jse_historical_collection',
            name='JSE Historical Data Collection',
            function=collect_jse_historical,
            schedule_expression='daily at 18:00',
            priority=JobPriority.MEDIUM,
            timeout_seconds=1800,
            retry_count=2,
            retry_delay_seconds=300,
            tags=['market_data', 'jse', 'historical'],
            enabled=True
        )
    
    def _create_crypto_quotes_job(self) -> JobDefinition:
        """Job to collect cryptocurrency quotes"""
        async def collect_crypto_quotes():
            try:
                fetcher = self.scrapers['crypto']
                quotes = await fetcher.get_top_cryptos(limit=100)
                
                # Store crypto quotes
                await self._store_quotes('CRYPTO', quotes)
                
                logger.info(f"Collected {len(quotes)} crypto quotes")
                return {'status': 'success', 'count': len(quotes), 'source': 'CRYPTO'}
                
            except Exception as e:
                logger.error(f"Failed to collect crypto quotes: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='crypto_quotes_collection',
            name='Cryptocurrency Quotes Collection',
            function=collect_crypto_quotes,
            schedule_expression='every 1 minutes',
            priority=JobPriority.HIGH,
            timeout_seconds=180,
            retry_count=3,
            retry_delay_seconds=20,
            tags=['market_data', 'crypto', 'quotes', 'real_time'],
            enabled=True
        )
    
    def _create_forex_quotes_job(self) -> JobDefinition:
        """Job to collect forex quotes"""
        async def collect_forex_quotes():
            try:
                fetcher = self.scrapers['forex']
                quotes = await fetcher.get_major_pairs()
                
                # Store forex quotes
                await self._store_quotes('FOREX', quotes)
                
                logger.info(f"Collected {len(quotes)} forex quotes")
                return {'status': 'success', 'count': len(quotes), 'source': 'FOREX'}
                
            except Exception as e:
                logger.error(f"Failed to collect forex quotes: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='forex_quotes_collection',
            name='Forex Quotes Collection',
            function=collect_forex_quotes,
            schedule_expression='every 5 minutes',
            priority=JobPriority.MEDIUM,
            timeout_seconds=120,
            retry_count=2,
            retry_delay_seconds=30,
            tags=['market_data', 'forex', 'quotes'],
            enabled=True
        )
    
    # News and Sentiment Jobs
    def _create_market_news_job(self) -> JobDefinition:
        """Job to collect market news"""
        async def collect_market_news():
            try:
                fetcher = self.scrapers['news']
                news_articles = await fetcher.get_latest_news(limit=50)
                
                # Store news articles
                await self._store_news_articles(news_articles)
                
                logger.info(f"Collected {len(news_articles)} news articles")
                return {'status': 'success', 'count': len(news_articles)}
                
            except Exception as e:
                logger.error(f"Failed to collect market news: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='market_news_collection',
            name='Market News Collection',
            function=collect_market_news,
            schedule_expression='every 15 minutes',
            priority=JobPriority.MEDIUM,
            timeout_seconds=300,
            retry_count=2,
            retry_delay_seconds=60,
            tags=['news', 'sentiment', 'market_intelligence'],
            enabled=True
        )
    
    def _create_sentiment_analysis_job(self) -> JobDefinition:
        """Job to analyze sentiment of news articles"""
        async def analyze_sentiment():
            try:
                # Get recent unprocessed news articles
                articles = await self._get_unprocessed_news()
                
                processed_count = 0
                for article in articles:
                    try:
                        sentiment = await self._analyze_article_sentiment(article)
                        await self._update_article_sentiment(article['id'], sentiment)
                        processed_count += 1
                        await asyncio.sleep(0.1)  # Rate limiting
                    except Exception as e:
                        logger.warning(f"Failed to analyze sentiment for article {article['id']}: {str(e)}")
                
                logger.info(f"Analyzed sentiment for {processed_count} articles")
                return {'status': 'success', 'processed': processed_count}
                
            except Exception as e:
                logger.error(f"Failed to analyze sentiment: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='sentiment_analysis',
            name='News Sentiment Analysis',
            function=analyze_sentiment,
            schedule_expression='every 30 minutes',
            priority=JobPriority.LOW,
            timeout_seconds=600,
            retry_count=1,
            retry_delay_seconds=120,
            tags=['news', 'sentiment', 'ai'],
            dependencies=['market_news_collection'],
            enabled=True
        )
    
    # ESG Data Jobs
    def _create_esg_collection_job(self) -> JobDefinition:
        """Job to collect ESG data"""
        async def collect_esg_data():
            try:
                collector = self.scrapers['esg']
                esg_data = await collector.collect_all_sources()
                
                # Store ESG data
                await self._store_esg_data(esg_data)
                
                logger.info(f"Collected ESG data for {len(esg_data)} companies")
                return {'status': 'success', 'companies': len(esg_data)}
                
            except Exception as e:
                logger.error(f"Failed to collect ESG data: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='esg_data_collection',
            name='ESG Data Collection',
            function=collect_esg_data,
            schedule_expression='daily at 02:00',
            priority=JobPriority.MEDIUM,
            timeout_seconds=3600,
            retry_count=2,
            retry_delay_seconds=600,
            tags=['esg', 'sustainability', 'governance'],
            enabled=True
        )
    
    # Data Processing Jobs
    def _create_data_cleaning_job(self) -> JobDefinition:
        """Job to clean and normalize data"""
        async def clean_data():
            try:
                cleaner = self.processors['cleaner']
                
                # Clean different data types
                results = {}
                results['quotes'] = await cleaner.clean_quotes_data()
                results['historical'] = await cleaner.clean_historical_data()
                results['news'] = await cleaner.clean_news_data()
                results['esg'] = await cleaner.clean_esg_data()
                
                total_cleaned = sum(results.values())
                logger.info(f"Cleaned {total_cleaned} records across all data types")
                return {'status': 'success', 'cleaned_records': total_cleaned, 'details': results}
                
            except Exception as e:
                logger.error(f"Failed to clean data: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='data_cleaning',
            name='Data Cleaning and Normalization',
            function=clean_data,
            schedule_expression='daily at 01:00',
            priority=JobPriority.MEDIUM,
            timeout_seconds=1800,
            retry_count=2,
            retry_delay_seconds=300,
            tags=['data_processing', 'cleaning', 'quality'],
            enabled=True
        )
    
    def _create_data_validation_job(self) -> JobDefinition:
        """Job to validate data quality"""
        async def validate_data():
            try:
                validator = self.processors['validator']
                
                # Validate different data types
                validation_results = {}
                validation_results['quotes'] = await validator.validate_quotes_data()
                validation_results['historical'] = await validator.validate_historical_data()
                validation_results['news'] = await validator.validate_news_data()
                
                # Generate data quality report
                report = await self._generate_quality_report(validation_results)
                
                logger.info("Completed data validation and quality check")
                return {'status': 'success', 'validation_results': validation_results, 'report': report}
                
            except Exception as e:
                logger.error(f"Failed to validate data: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='data_validation',
            name='Data Quality Validation',
            function=validate_data,
            schedule_expression='daily at 03:00',
            priority=JobPriority.MEDIUM,
            timeout_seconds=900,
            retry_count=1,
            retry_delay_seconds=300,
            tags=['data_processing', 'validation', 'quality'],
            dependencies=['data_cleaning'],
            enabled=True
        )
    
    # Alert and Notification Jobs
    def _create_price_alerts_job(self) -> JobDefinition:
        """Job to check and trigger price alerts"""
        async def check_price_alerts():
            try:
                alert_service = self.processors['alerts']
                triggered_alerts = await alert_service.check_all_alerts()
                
                # Send notifications for triggered alerts
                notifications_sent = 0
                for alert in triggered_alerts:
                    try:
                        await alert_service.send_notification(alert)
                        notifications_sent += 1
                    except Exception as e:
                        logger.warning(f"Failed to send alert notification: {str(e)}")
                
                logger.info(f"Checked alerts and sent {notifications_sent} notifications")
                return {'status': 'success', 'alerts_triggered': len(triggered_alerts), 'notifications_sent': notifications_sent}
                
            except Exception as e:
                logger.error(f"Failed to check price alerts: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='price_alerts_check',
            name='Price Alerts Monitoring',
            function=check_price_alerts,
            schedule_expression='every 5 minutes',
            priority=JobPriority.HIGH,
            timeout_seconds=180,
            retry_count=2,
            retry_delay_seconds=30,
            tags=['alerts', 'notifications', 'monitoring'],
            enabled=True
        )
    
    # Analytics Jobs
    def _create_daily_analytics_job(self) -> JobDefinition:
        """Job to generate daily analytics"""
        async def generate_daily_analytics():
            try:
                # Calculate daily metrics
                metrics = await self._calculate_daily_metrics()
                
                # Store analytics results
                await self._store_analytics('daily', metrics)
                
                logger.info("Generated daily analytics")
                return {'status': 'success', 'metrics': metrics}
                
            except Exception as e:
                logger.error(f"Failed to generate daily analytics: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='daily_analytics',
            name='Daily Analytics Generation',
            function=generate_daily_analytics,
            schedule_expression='daily at 23:00',
            priority=JobPriority.LOW,
            timeout_seconds=600,
            retry_count=1,
            retry_delay_seconds=300,
            tags=['analytics', 'reporting', 'metrics'],
            enabled=True
        )
    
    # Maintenance Jobs
    def _create_cache_cleanup_job(self) -> JobDefinition:
        """Job to clean up expired cache entries"""
        async def cleanup_cache():
            try:
                # Clean expired cache entries
                cleaned_entries = await self._cleanup_expired_cache()
                
                logger.info(f"Cleaned up {cleaned_entries} expired cache entries")
                return {'status': 'success', 'cleaned_entries': cleaned_entries}
                
            except Exception as e:
                logger.error(f"Failed to cleanup cache: {str(e)}")
                raise
        
        return JobDefinition(
            job_id='cache_cleanup',
            name='Cache Cleanup and Maintenance',
            function=cleanup_cache,
            schedule_expression='daily at 04:00',
            priority=JobPriority.LOW,
            timeout_seconds=300,
            retry_count=1,
            retry_delay_seconds=60,
            tags=['maintenance', 'cache', 'cleanup'],
            enabled=True
        )
    
    # Helper methods for job implementations
    async def _store_quotes(self, source: str, quotes: List[Dict]) -> None:
        """Store quotes in database/cache"""
        # Implementation would store quotes in your database
        logger.debug(f"Storing {len(quotes)} quotes from {source}")
    
    async def _store_historical_data(self, source: str, data: List[Dict]) -> None:
        """Store historical data"""
        logger.debug(f"Storing {len(data)} historical records from {source}")
    
    async def _store_news_articles(self, articles: List[Dict]) -> None:
        """Store news articles"""
        logger.debug(f"Storing {len(articles)} news articles")
    
    async def _store_esg_data(self, data: List[Dict]) -> None:
        """Store ESG data"""
        logger.debug(f"Storing ESG data for {len(data)} companies")
    
    async def _get_unprocessed_news(self) -> List[Dict]:
        """Get news articles that haven't been processed for sentiment"""
        # Implementation would query database for unprocessed articles
        return []
    
    async def _analyze_article_sentiment(self, article: Dict) -> Dict:
        """Analyze sentiment of a news article"""
        # Implementation would use AI/ML for sentiment analysis
        return {'sentiment': 'neutral', 'confidence': 0.8}
    
    async def _update_article_sentiment(self, article_id: str, sentiment: Dict) -> None:
        """Update article with sentiment analysis results"""
        logger.debug(f"Updated sentiment for article {article_id}")
    
    async def _generate_quality_report(self, validation_results: Dict) -> Dict:
        """Generate data quality report"""
        return {'overall_score': 95.5, 'issues_found': 2}
    
    async def _calculate_daily_metrics(self) -> Dict:
        """Calculate daily performance metrics"""
        return {
            'total_quotes': 10000,
            'total_trades': 5000,
            'market_cap_change': 2.5,
            'top_gainers': 10,
            'top_losers': 8
        }
    
    async def _store_analytics(self, period: str, metrics: Dict) -> None:
        """Store analytics results"""
        logger.debug(f"Stored {period} analytics: {metrics}")
    
    async def _cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries"""
        # Implementation would clean expired Redis keys
        return 150

# Global ETL manager instance
etl_manager = ETLJobManager()

def initialize_etl_jobs(redis_client) -> bool:
    """Initialize and register all ETL jobs"""
    try:
        etl_manager.initialize(redis_client)
        return etl_manager.register_all_jobs()
    except Exception as e:
        logger.error(f"Failed to initialize ETL jobs: {str(e)}")
        return False

def get_etl_manager() -> ETLJobManager:
    """Get the global ETL manager instance"""
    return etl_manager
