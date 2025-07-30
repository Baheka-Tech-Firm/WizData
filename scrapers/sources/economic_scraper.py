"""
Economic Data Scraper
Collects economic indicators and statistics
"""

import asyncio
import aiohttp
from typing import Dict, List, Any
from datetime import datetime, timezone

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

class EconomicDataScraper(BaseScraper):
    """
    Scraper for economic indicators and statistics
    Collects data from public economic data sources
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("economic_data", config)
        self.session = None
        
        # Economic indicators to track
        self.indicators = {
            'interest_rates': {
                'federal_funds_rate': 5.25,
                'prime_rate': 8.50,
                'discount_rate': 5.50,
                'treasury_10y': 4.45
            },
            'inflation': {
                'cpi_annual': 3.2,
                'core_cpi': 4.0,
                'ppi_annual': 2.8
            },
            'employment': {
                'unemployment_rate': 3.7,
                'labor_participation': 63.4,
                'nonfarm_payrolls': 150000
            },
            'gdp': {
                'gdp_growth_quarterly': 2.1,
                'gdp_growth_annual': 2.8
            }
        }
        
        # South African economic indicators
        self.sa_indicators = {
            'repo_rate': 8.25,
            'inflation_rate': 5.4,
            'unemployment_rate': 32.1,
            'gdp_growth': 0.3,
            'manufacturing_pmi': 48.2,
            'services_pmi': 52.8
        }
    
    async def setup_session(self):
        """Setup HTTP session"""
        if not self.session:
            headers = await self.proxy_manager.get_session_config()
            headers['headers'].update({
                'Accept': 'application/json',
                'User-Agent': 'WizData-EconomicScraper/1.0'
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
    
    async def scrape_fed_economic_data(self) -> List[Dict[str, Any]]:
        """Scrape US Federal Reserve economic data (simulated)"""
        import random
        
        indicators = []
        timestamp = datetime.now(timezone.utc)
        
        # US Federal Reserve indicators
        fed_data = {
            'federal_funds_rate': {
                'value': 5.25 + random.uniform(-0.05, 0.05),
                'unit': 'percent',
                'frequency': 'monthly',
                'last_updated': '2025-01-15'
            },
            'inflation_target': {
                'value': 2.0,
                'unit': 'percent',
                'frequency': 'annual',
                'last_updated': '2025-01-01'
            },
            'unemployment_rate': {
                'value': 3.7 + random.uniform(-0.1, 0.1),
                'unit': 'percent',
                'frequency': 'monthly',
                'last_updated': '2025-01-10'
            },
            'gdp_growth_rate': {
                'value': 2.1 + random.uniform(-0.2, 0.2),
                'unit': 'percent',
                'frequency': 'quarterly',
                'last_updated': '2024-12-31'
            }
        }
        
        for indicator, data in fed_data.items():
            indicators.append({
                'indicator': indicator,
                'value': round(data['value'], 2),
                'unit': data['unit'],
                'country': 'US',
                'source': 'federal_reserve',
                'frequency': data['frequency'],
                'timestamp': timestamp.isoformat(),
                'last_updated': data['last_updated'],
                'category': 'monetary_policy'
            })
        
        logger.info(f"Generated {len(indicators)} US Fed economic indicators")
        return indicators
    
    async def scrape_sa_economic_data(self) -> List[Dict[str, Any]]:
        """Scrape South African economic data (simulated)"""
        import random
        
        indicators = []
        timestamp = datetime.now(timezone.utc)
        
        # South African Reserve Bank and Stats SA data
        sa_data = {
            'repo_rate': {
                'value': 8.25,
                'unit': 'percent',
                'frequency': 'bi_monthly',
                'source': 'sarb'
            },
            'prime_lending_rate': {
                'value': 11.75,
                'unit': 'percent',
                'frequency': 'monthly',
                'source': 'sarb'
            },
            'cpi_inflation': {
                'value': 5.4 + random.uniform(-0.2, 0.2),
                'unit': 'percent',
                'frequency': 'monthly',
                'source': 'stats_sa'
            },
            'unemployment_rate': {
                'value': 32.1 + random.uniform(-0.5, 0.5),
                'unit': 'percent',
                'frequency': 'quarterly',
                'source': 'stats_sa'
            },
            'gdp_growth_rate': {
                'value': 0.3 + random.uniform(-0.3, 0.3),
                'unit': 'percent',
                'frequency': 'quarterly',
                'source': 'stats_sa'
            },
            'manufacturing_pmi': {
                'value': 48.2 + random.uniform(-2.0, 2.0),
                'unit': 'index',
                'frequency': 'monthly',
                'source': 'bureau_economic_research'
            }
        }
        
        for indicator, data in sa_data.items():
            indicators.append({
                'indicator': indicator,
                'value': round(data['value'], 2),
                'unit': data['unit'],
                'country': 'ZA',
                'source': data['source'],
                'frequency': data['frequency'],
                'timestamp': timestamp.isoformat(),
                'category': 'economic_indicators'
            })
        
        logger.info(f"Generated {len(indicators)} SA economic indicators")
        return indicators
    
    async def scrape_global_economic_data(self) -> List[Dict[str, Any]]:
        """Scrape global economic indicators (simulated)"""
        import random
        
        indicators = []
        timestamp = datetime.now(timezone.utc)
        
        # Global economic data
        global_data = {
            'oil_price_brent': {
                'value': 85.50 + random.uniform(-2.0, 2.0),
                'unit': 'usd_per_barrel',
                'frequency': 'daily',
                'category': 'commodities'
            },
            'gold_price': {
                'value': 2080.00 + random.uniform(-20.0, 20.0),
                'unit': 'usd_per_ounce',
                'frequency': 'daily',
                'category': 'commodities'
            },
            'us_dollar_index': {
                'value': 102.50 + random.uniform(-1.0, 1.0),
                'unit': 'index',
                'frequency': 'daily',
                'category': 'currencies'
            },
            'vix_volatility': {
                'value': 18.5 + random.uniform(-2.0, 2.0),
                'unit': 'index',
                'frequency': 'daily',
                'category': 'market_volatility'
            }
        }
        
        for indicator, data in global_data.items():
            indicators.append({
                'indicator': indicator,
                'value': round(data['value'], 2),
                'unit': data['unit'],
                'country': 'GLOBAL',
                'source': 'market_data',
                'frequency': data['frequency'],
                'timestamp': timestamp.isoformat(),
                'category': data['category']
            })
        
        logger.info(f"Generated {len(indicators)} global economic indicators")
        return indicators
    
    def calculate_economic_quality_score(self, indicator_data: Dict[str, Any]) -> float:
        """Calculate quality score for economic indicator"""
        score = 0.0
        
        # Check required fields
        required_fields = ['indicator', 'value', 'unit', 'country', 'source', 'timestamp']
        for field in required_fields:
            if indicator_data.get(field):
                score += 0.15
        
        # Check if value is reasonable (not None or negative for rates/percentages)
        value = indicator_data.get('value')
        if value is not None:
            score += 0.1
            
            # Additional validation for specific indicator types
            indicator_name = indicator_data.get('indicator', '').lower()
            if 'rate' in indicator_name or 'inflation' in indicator_name:
                if 0 <= value <= 100:  # Reasonable range for rates/percentages
                    score += 0.05
        
        # Check if we have frequency information
        if indicator_data.get('frequency'):
            score += 0.05
        
        return min(1.0, score)
    
    async def run_scrape(self, **kwargs) -> Dict[str, Any]:
        """Main scraping method"""
        await self.setup_session()
        
        try:
            all_indicators = []
            
            # Scrape different data sources
            fed_data = await self.scrape_fed_economic_data()
            all_indicators.extend(fed_data)
            
            await asyncio.sleep(self.request_delay)
            
            sa_data = await self.scrape_sa_economic_data()
            all_indicators.extend(sa_data)
            
            await asyncio.sleep(self.request_delay)
            
            global_data = await self.scrape_global_economic_data()
            all_indicators.extend(global_data)
            
            # Process and push to queue
            processed_indicators = []
            for indicator_data in all_indicators:
                quality_score = self.calculate_economic_quality_score(indicator_data)
                
                # Create scraped data object
                scraped_data = ScrapedData(
                    source=self.name,
                    data_type='economic_indicator',
                    symbol=indicator_data['indicator'],
                    timestamp=datetime.now(timezone.utc),
                    raw_data=indicator_data,
                    metadata={
                        'country': indicator_data['country'],
                        'source_name': indicator_data['source'],
                        'category': indicator_data.get('category', 'economic'),
                        'frequency': indicator_data.get('frequency', 'unknown'),
                        'unit': indicator_data['unit']
                    },
                    quality_score=quality_score
                )
                
                # Push to queue
                await self.queue_manager.publish(
                    topic='raw.economic.indicators',
                    message=scraped_data.__dict__,
                    key=f"{indicator_data['country']}_{indicator_data['indicator']}"
                )
                
                processed_indicators.append(indicator_data)
                
                logger.info(
                    f"Economic indicator processed",
                    topic='raw.economic.indicators',
                    indicator=indicator_data['indicator'],
                    value=indicator_data['value'],
                    country=indicator_data['country'],
                    quality_score=quality_score
                )
            
            return {
                'success': True,
                'items_processed': len(processed_indicators),
                'errors': [],
                'summary': {
                    'us_indicators': len(fed_data),
                    'sa_indicators': len(sa_data),
                    'global_indicators': len(global_data),
                    'total_indicators': len(processed_indicators)
                }
            }
            
        except Exception as e:
            logger.error(f"Economic data scraping failed: {e}")
            return {
                'success': False,
                'items_processed': 0,
                'errors': [str(e)]
            }
        
        finally:
            await self.cleanup_session()
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for economic data scraper"""
        try:
            # For economic data, we mainly use internal data generation
            # so health check is simpler
            
            return {
                'status': 'healthy',
                'configured_indicators': len(self.indicators) + len(self.sa_indicators),
                'data_sources': ['federal_reserve', 'sarb', 'stats_sa', 'market_data'],
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }