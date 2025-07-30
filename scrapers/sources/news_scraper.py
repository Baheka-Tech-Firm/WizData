"""
Financial News Scraper
Collects financial news from multiple sources
"""

import asyncio
import aiohttp
from typing import Dict, List, Any
from datetime import datetime, timezone
import hashlib

try:
    from scrapers.base.scraper_base import BaseScraper, ScrapedData
except ImportError:
    class BaseScraper:
        def __init__(self, config): pass
    class ScrapedData:
        def __init__(self, **kwargs): pass

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

class NewsAPIError(Exception):
    """Exception for News API related errors"""
    pass

class FinancialNewsScraper(BaseScraper):
    """
    Scraper for financial news from multiple sources
    Uses RSS feeds and public APIs for news aggregation
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("financial_news", config)
        self.session = None
        
        # News sources configuration
        self.news_sources = {
            'reuters_finance': {
                'url': 'https://www.reuters.com/business/finance/',
                'rss': 'https://www.reuters.com/business/finance/rss/',
                'type': 'rss'
            },
            'bloomberg_markets': {
                'url': 'https://www.bloomberg.com/markets',
                'type': 'web'
            },
            'cnbc_markets': {
                'url': 'https://www.cnbc.com/finance/',
                'type': 'web'
            },
            'ft_markets': {
                'url': 'https://www.ft.com/markets',
                'type': 'web'
            }
        }
        
        # Keywords for financial relevance filtering
        self.financial_keywords = [
            'stock', 'market', 'trading', 'investment', 'finance', 'economy',
            'earnings', 'revenue', 'profit', 'bank', 'crypto', 'bitcoin',
            'fed', 'interest rate', 'inflation', 'gdp', 'recession'
        ]
    
    async def setup_session(self):
        """Setup HTTP session with appropriate headers"""
        if not self.session:
            headers = await self.proxy_manager.get_session_config()
            headers['headers'].update({
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            
            timeout = aiohttp.ClientTimeout(total=30)
            self.session = aiohttp.ClientSession(
                headers=headers['headers'],
                timeout=timeout
            )
    
    async def cleanup_session(self):
        """Cleanup HTTP session"""
        if self.session:
            await self.session.close()
            self.session = None
    
    async def scrape_reuters_rss(self) -> List[Dict[str, Any]]:
        """Scrape Reuters business RSS feed"""
        try:
            url = 'https://www.reuters.com/business/finance/rss/'
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    content = await response.text()
                    
                    # Simple RSS parsing (would use feedparser in production)
                    import re
                    
                    # Extract items from RSS
                    items = []
                    item_pattern = r'<item>(.*?)</item>'
                    title_pattern = r'<title><!\[CDATA\[(.*?)\]\]></title>'
                    link_pattern = r'<link>(.*?)</link>'
                    desc_pattern = r'<description><!\[CDATA\[(.*?)\]\]></description>'
                    date_pattern = r'<pubDate>(.*?)</pubDate>'
                    
                    item_matches = re.findall(item_pattern, content, re.DOTALL)
                    
                    for item_content in item_matches[:10]:  # Limit to 10 items
                        title_match = re.search(title_pattern, item_content)
                        link_match = re.search(link_pattern, item_content)
                        desc_match = re.search(desc_pattern, item_content)
                        date_match = re.search(date_pattern, item_content)
                        
                        if title_match and link_match:
                            item = {
                                'title': title_match.group(1).strip(),
                                'link': link_match.group(1).strip(),
                                'description': desc_match.group(1).strip() if desc_match else '',
                                'pub_date': date_match.group(1).strip() if date_match else '',
                                'source': 'Reuters Finance',
                                'category': 'finance'
                            }
                            
                            # Check financial relevance
                            text_to_check = f"{item['title']} {item['description']}".lower()
                            if any(keyword in text_to_check for keyword in self.financial_keywords):
                                items.append(item)
                    
                    logger.info(f"Scraped {len(items)} financial news items from Reuters")
                    return items
                else:
                    logger.warning(f"Reuters RSS fetch failed: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error scraping Reuters RSS: {e}")
            return []
    
    async def scrape_market_headlines(self) -> List[Dict[str, Any]]:
        """Scrape market headlines from public sources"""
        headlines = []
        
        # Simulate market headlines (in production, would scrape from actual sources)
        sample_headlines = [
            {
                'title': 'Major Banks Report Strong Q4 Earnings',
                'source': 'Market Watch',
                'category': 'banking',
                'sentiment': 'positive',
                'timestamp': datetime.now(timezone.utc).isoformat()
            },
            {
                'title': 'Federal Reserve Signals Interest Rate Policy Changes',
                'source': 'Financial Times',
                'category': 'monetary_policy',
                'sentiment': 'neutral',
                'timestamp': datetime.now(timezone.utc).isoformat()
            },
            {
                'title': 'Technology Stocks Show Mixed Performance',
                'source': 'CNBC',
                'category': 'technology',
                'sentiment': 'mixed',
                'timestamp': datetime.now(timezone.utc).isoformat()
            },
            {
                'title': 'Cryptocurrency Markets Experience Volatility',
                'source': 'CoinDesk',
                'category': 'cryptocurrency',
                'sentiment': 'volatile',
                'timestamp': datetime.now(timezone.utc).isoformat()
            },
            {
                'title': 'Oil Prices Fluctuate on Global Supply Concerns',
                'source': 'Energy News',
                'category': 'commodities',
                'sentiment': 'uncertain',
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
        ]
        
        return sample_headlines
    
    async def analyze_sentiment(self, text: str) -> str:
        """Simple sentiment analysis (would use NLP library in production)"""
        positive_words = ['strong', 'gains', 'up', 'positive', 'growth', 'profit']
        negative_words = ['down', 'loss', 'decline', 'negative', 'drop', 'fall']
        
        text_lower = text.lower()
        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)
        
        if positive_count > negative_count:
            return 'positive'
        elif negative_count > positive_count:
            return 'negative'
        else:
            return 'neutral'
    
    def calculate_news_quality_score(self, news_item: Dict[str, Any]) -> float:
        """Calculate quality score for news item"""
        score = 0.0
        
        # Check required fields
        if news_item.get('title'):
            score += 0.3
        if news_item.get('source'):
            score += 0.2
        if news_item.get('category'):
            score += 0.2
        if news_item.get('timestamp'):
            score += 0.1
        
        # Check content quality
        title = news_item.get('title', '')
        if len(title) > 20:  # Meaningful title length
            score += 0.1
        
        # Check financial relevance
        text_to_check = f"{title} {news_item.get('description', '')}".lower()
        if any(keyword in text_to_check for keyword in self.financial_keywords):
            score += 0.1
        
        return min(1.0, score)
    
    async def fetch_data(self, **kwargs) -> Dict[str, Any]:
        """Fetch news data from sources"""
        all_news = []
        
        # Scrape Reuters RSS
        reuters_news = await self.scrape_reuters_rss()
        all_news.extend(reuters_news)
        
        # Add delay between requests
        await asyncio.sleep(self.request_delay)
        
        # Scrape market headlines
        headlines = await self.scrape_market_headlines()
        all_news.extend(headlines)
        
        return {
            'reuters_news': reuters_news,
            'headlines': headlines,
            'all_news': all_news
        }
    
    async def parse_data(self, raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse and process news data"""
        processed_news = []
        
        for news_item in raw_data.get('all_news', []):
            # Add sentiment analysis
            if 'sentiment' not in news_item:
                text_for_sentiment = f"{news_item.get('title', '')} {news_item.get('description', '')}"
                news_item['sentiment'] = await self.analyze_sentiment(text_for_sentiment)
            
            # Add unique ID
            content_hash = hashlib.md5(
                f"{news_item.get('title', '')}{news_item.get('source', '')}".encode()
            ).hexdigest()[:16]
            news_item['news_id'] = content_hash
            
            # Add timestamp if missing
            if 'timestamp' not in news_item:
                news_item['timestamp'] = datetime.now(timezone.utc).isoformat()
            
            processed_news.append(news_item)
            
        return processed_news

    async def run_scrape(self, **kwargs) -> Dict[str, Any]:
        """Main scraping method"""
        await self.setup_session()
        
        try:
            all_news = []
            
            # Scrape Reuters RSS
            reuters_news = await self.scrape_reuters_rss()
            all_news.extend(reuters_news)
            
            # Add delay between requests
            await asyncio.sleep(self.request_delay)
            
            # Scrape market headlines
            headlines = await self.scrape_market_headlines()
            all_news.extend(headlines)
            
            # Process and enrich news data
            processed_news = []
            for news_item in all_news:
                # Add sentiment analysis
                if 'sentiment' not in news_item:
                    text_for_sentiment = f"{news_item.get('title', '')} {news_item.get('description', '')}"
                    news_item['sentiment'] = await self.analyze_sentiment(text_for_sentiment)
                
                # Add unique ID
                content_hash = hashlib.md5(
                    f"{news_item.get('title', '')}{news_item.get('source', '')}".encode()
                ).hexdigest()[:16]
                news_item['news_id'] = content_hash
                
                # Add timestamp if missing
                if 'timestamp' not in news_item:
                    news_item['timestamp'] = datetime.now(timezone.utc).isoformat()
                
                # Calculate quality score
                quality_score = self.calculate_news_quality_score(news_item)
                
                # Create scraped data object
                scraped_data = ScrapedData(
                    source=self.name,
                    data_type='financial_news',
                    symbol=news_item.get('category', 'general'),
                    timestamp=datetime.now(timezone.utc),
                    raw_data=news_item,
                    metadata={
                        'news_id': news_item['news_id'],
                        'source_name': news_item.get('source', 'unknown'),
                        'sentiment': news_item.get('sentiment', 'neutral'),
                        'category': news_item.get('category', 'general')
                    },
                    quality_score=quality_score
                )
                
                # Push to queue
                await self.queue_manager.publish(
                    topic='raw.news.financial',
                    message=scraped_data.__dict__,
                    key=news_item['news_id']
                )
                
                processed_news.append(news_item)
                
                logger.info(
                    f"News item processed",
                    topic='raw.news.financial',
                    news_id=news_item['news_id'],
                    quality_score=quality_score
                )
            
            return {
                'success': True,
                'items_processed': len(processed_news),
                'errors': [],
                'summary': {
                    'reuters_items': len(reuters_news),
                    'headline_items': len(headlines),
                    'total_processed': len(processed_news)
                }
            }
            
        except Exception as e:
            logger.error(f"News scraping failed: {e}")
            return {
                'success': False,
                'items_processed': 0,
                'errors': [str(e)]
            }
        
        finally:
            await self.cleanup_session()
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for news scraper"""
        try:
            # Test basic connectivity
            await self.setup_session()
            
            # Test Reuters RSS availability
            async with self.session.get('https://www.reuters.com/business/finance/rss/', 
                                       timeout=aiohttp.ClientTimeout(total=10)) as response:
                reuters_status = response.status == 200
            
            await self.cleanup_session()
            
            return {
                'status': 'healthy' if reuters_status else 'degraded',
                'reuters_rss': 'available' if reuters_status else 'unavailable',
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'configured_sources': len(self.news_sources)
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }