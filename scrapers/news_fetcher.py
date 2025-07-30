"""
News Fetcher for Market Intelligence
Collects and processes market news from various sources
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import feedparser
import json

logger = logging.getLogger(__name__)

class NewsFetcher:
    """Fetches market news from multiple sources"""
    
    def __init__(self):
        self.sources = {
            'reuters_business': 'http://feeds.reuters.com/reuters/businessNews',
            'bloomberg_markets': 'https://feeds.bloomberg.com/markets/news.rss',
            'financial_times': 'https://www.ft.com/rss/home/uk',
            'business_day': 'https://www.businesslive.co.za/rss/',
            'fin24': 'https://www.fin24.com/rss'
        }
        
    async def get_latest_news(self, limit: int = 50, sources: List[str] = None) -> List[Dict[str, Any]]:
        """Get latest news articles from specified sources"""
        try:
            if sources is None:
                sources = list(self.sources.keys())
            
            all_articles = []
            
            async with aiohttp.ClientSession() as session:
                tasks = []
                for source in sources:
                    if source in self.sources:
                        task = self._fetch_source_news(session, source, self.sources[source])
                        tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, list):
                        all_articles.extend(result)
                    elif isinstance(result, Exception):
                        logger.warning(f"Failed to fetch from source: {result}")
            
            # Sort by publication date and limit
            all_articles.sort(key=lambda x: x.get('published_at', ''), reverse=True)
            return all_articles[:limit]
            
        except Exception as e:
            logger.error(f"Error fetching news: {str(e)}")
            return []
    
    async def _fetch_source_news(self, session: aiohttp.ClientSession, source_name: str, url: str) -> List[Dict[str, Any]]:
        """Fetch news from a specific RSS source"""
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    content = await response.text()
                    return self._parse_rss_feed(content, source_name)
                else:
                    logger.warning(f"HTTP {response.status} from {source_name}")
                    return []
                    
        except Exception as e:
            logger.warning(f"Error fetching from {source_name}: {str(e)}")
            return []
    
    def _parse_rss_feed(self, content: str, source_name: str) -> List[Dict[str, Any]]:
        """Parse RSS feed content"""
        try:
            feed = feedparser.parse(content)
            articles = []
            
            for entry in feed.entries:
                article = {
                    'title': entry.get('title', ''),
                    'summary': entry.get('summary', entry.get('description', '')),
                    'url': entry.get('link', ''),
                    'source': source_name,
                    'published_at': self._parse_date(entry.get('published', '')),
                    'symbols': self._extract_symbols(entry.get('title', '') + ' ' + entry.get('summary', '')),
                    'categories': entry.get('tags', []),
                    'author': entry.get('author', ''),
                    'content': entry.get('content', [{}])[0].get('value', '') if entry.get('content') else ''
                }
                articles.append(article)
            
            return articles
            
        except Exception as e:
            logger.error(f"Error parsing RSS feed from {source_name}: {str(e)}")
            return []
    
    def _parse_date(self, date_str: str) -> str:
        """Parse publication date to ISO format"""
        try:
            if date_str:
                # feedparser usually handles date parsing
                import time
                parsed_time = feedparser._parse_date(date_str)
                if parsed_time:
                    return datetime(*parsed_time[:6]).isoformat()
            return datetime.now().isoformat()
        except:
            return datetime.now().isoformat()
    
    def _extract_symbols(self, text: str) -> List[str]:
        """Extract stock symbols from news text"""
        import re
        
        symbols = []
        
        # Common JSE symbols
        jse_symbols = ['NPN', 'BHP', 'SOL', 'SBK', 'FSR', 'AGL', 'MTN', 'VOD', 'SHP', 'TKG']
        
        # Look for JSE symbols in text
        for symbol in jse_symbols:
            if symbol in text.upper():
                symbols.append(f'JSE:{symbol}')
        
        # Look for US symbols (pattern: 3-4 uppercase letters)
        us_matches = re.findall(r'\b[A-Z]{3,4}\b', text)
        for match in us_matches:
            if match not in ['THE', 'AND', 'FOR', 'ARE', 'BUT', 'NOT', 'YOU', 'ALL', 'CAN', 'HER', 'WAS', 'ONE', 'OUR', 'HAD', 'DID', 'GET', 'HAS', 'HIM', 'HIS', 'HOW', 'ITS', 'NEW', 'NOW', 'OLD', 'SEE', 'TWO', 'WHO', 'BOY', 'DID', 'USE', 'HER', 'DAY', 'GET', 'MAN', 'NEW', 'NOW', 'OLD', 'SEE', 'TWO', 'WAY', 'WHO', 'BOY', 'DID', 'USE']:
                symbols.append(f'NASDAQ:{match}')
        
        return list(set(symbols))  # Remove duplicates

class ESGDataCollector:
    """Collects ESG (Environmental, Social, Governance) data"""
    
    def __init__(self):
        self.sources = {
            'sustainability_reports': 'company_sustainability_reports',
            'carbon_footprint': 'carbon_emissions_data',
            'social_impact': 'social_responsibility_metrics',
            'governance_scores': 'corporate_governance_ratings'
        }
    
    async def collect_all_sources(self) -> List[Dict[str, Any]]:
        """Collect ESG data from all available sources"""
        try:
            # This is a placeholder implementation
            # In a real system, this would integrate with ESG data providers
            
            companies = [
                'JSE:NPN', 'JSE:BHP', 'JSE:SOL', 'JSE:SBK', 'JSE:FSR',
                'NASDAQ:AAPL', 'NASDAQ:MSFT', 'NASDAQ:GOOGL', 'NYSE:TSLA', 'NYSE:JPM'
            ]
            
            esg_data = []
            
            for company in companies:
                data = await self._collect_company_esg(company)
                if data:
                    esg_data.append(data)
                
                # Rate limiting
                await asyncio.sleep(0.1)
            
            logger.info(f"Collected ESG data for {len(esg_data)} companies")
            return esg_data
            
        except Exception as e:
            logger.error(f"Error collecting ESG data: {str(e)}")
            return []
    
    async def _collect_company_esg(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Collect ESG data for a specific company"""
        try:
            # Simulated ESG data - in reality, this would fetch from ESG data providers
            import random
            
            return {
                'symbol': symbol,
                'esg_score': round(random.uniform(20, 100), 1),
                'environmental_score': round(random.uniform(20, 100), 1),
                'social_score': round(random.uniform(20, 100), 1),
                'governance_score': round(random.uniform(20, 100), 1),
                'carbon_emissions': random.randint(1000, 100000),  # tonnes CO2
                'renewable_energy_percent': round(random.uniform(0, 100), 1),
                'diversity_score': round(random.uniform(0, 100), 1),
                'board_independence': round(random.uniform(30, 90), 1),
                'last_updated': datetime.now().isoformat(),
                'data_quality': random.choice(['high', 'medium', 'low']),
                'reporting_standard': random.choice(['GRI', 'SASB', 'TCFD', 'CDP'])
            }
            
        except Exception as e:
            logger.warning(f"Error collecting ESG data for {symbol}: {str(e)}")
            return None
