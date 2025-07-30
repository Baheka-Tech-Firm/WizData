"""
Forex Rate Scraper
Collects currency exchange rates from multiple sources
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

class ForexScraper(BaseScraper):
    """
    Scraper for foreign exchange rates
    Uses multiple free APIs for comprehensive currency data
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("forex_rates", config)
        self.session = None
        
        # Currency pairs to track
        self.currency_pairs = [
            'USD/ZAR',  # US Dollar to South African Rand
            'EUR/USD',  # Euro to US Dollar
            'GBP/USD',  # British Pound to US Dollar
            'USD/JPY',  # US Dollar to Japanese Yen
            'USD/CNY',  # US Dollar to Chinese Yuan
            'USD/CAD',  # US Dollar to Canadian Dollar
            'AUD/USD',  # Australian Dollar to US Dollar
            'EUR/ZAR',  # Euro to South African Rand
            'GBP/ZAR',  # British Pound to South African Rand
            'BTC/USD',  # Bitcoin to US Dollar (crypto pair)
        ]
        
        # API endpoints for forex data
        self.forex_apis = {
            'exchangerate_api': 'https://api.exchangerate-api.com/v4/latest/USD',
            'fixer_io': 'https://api.fixer.io/latest',  # May require API key
            'currencylayer': 'http://api.currencylayer.com/live'  # May require API key
        }
    
    async def setup_session(self):
        """Setup HTTP session"""
        if not self.session:
            headers = await self.proxy_manager.get_session_config()
            headers['headers'].update({
                'Accept': 'application/json',
                'User-Agent': 'WizData-ForexScraper/1.0'
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
    
    async def scrape_exchangerate_api(self) -> List[Dict[str, Any]]:
        """Scrape from ExchangeRate-API (free tier)"""
        try:
            url = 'https://api.exchangerate-api.com/v4/latest/USD'
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    rates = []
                    base_currency = data.get('base', 'USD')
                    timestamp = datetime.now(timezone.utc)
                    
                    # Process exchange rates
                    for currency, rate in data.get('rates', {}).items():
                        if currency in ['ZAR', 'EUR', 'GBP', 'JPY', 'CNY', 'CAD', 'AUD']:
                            rates.append({
                                'pair': f'{base_currency}/{currency}',
                                'rate': float(rate),
                                'base': base_currency,
                                'quote': currency,
                                'timestamp': timestamp.isoformat(),
                                'source': 'exchangerate-api',
                                'bid': float(rate) * 0.9999,  # Simulate bid/ask spread
                                'ask': float(rate) * 1.0001,
                                'change_24h': 0.0  # Would be calculated from historical data
                            })
                    
                    logger.info(f"Scraped {len(rates)} forex rates from ExchangeRate-API")
                    return rates
                else:
                    logger.warning(f"ExchangeRate-API request failed: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error scraping ExchangeRate-API: {e}")
            return []
    
    async def scrape_bank_rates(self) -> List[Dict[str, Any]]:
        """Scrape bank exchange rates (simulated for demo)"""
        # In production, would scrape from major banks' APIs or websites
        # For demo, generating realistic forex data
        
        import random
        
        base_rates = {
            'USD/ZAR': 18.50,
            'EUR/USD': 1.08,
            'GBP/USD': 1.25,
            'USD/JPY': 149.50,
            'USD/CNY': 7.25,
            'USD/CAD': 1.35,
            'AUD/USD': 0.66
        }
        
        rates = []
        timestamp = datetime.now(timezone.utc)
        
        for pair, base_rate in base_rates.items():
            # Add small random variation (±0.5%)
            variation = random.uniform(-0.005, 0.005)
            current_rate = base_rate * (1 + variation)
            
            base_currency, quote_currency = pair.split('/')
            
            rates.append({
                'pair': pair,
                'rate': round(current_rate, 4),
                'base': base_currency,
                'quote': quote_currency,
                'timestamp': timestamp.isoformat(),
                'source': 'bank_composite',
                'bid': round(current_rate * 0.9995, 4),
                'ask': round(current_rate * 1.0005, 4),
                'change_24h': round(random.uniform(-2.0, 2.0), 2),
                'volume_24h': random.randint(1000000, 10000000)
            })
        
        logger.info(f"Generated {len(rates)} bank forex rates")
        return rates
    
    async def scrape_crypto_forex_pairs(self) -> List[Dict[str, Any]]:
        """Scrape crypto-to-fiat exchange rates"""
        try:
            # Use CoinGecko API for crypto rates
            url = 'https://api.coingecko.com/api/v3/simple/price'
            params = {
                'ids': 'bitcoin,ethereum',
                'vs_currencies': 'usd,eur,zar',
                'include_24hr_change': 'true'
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    rates = []
                    timestamp = datetime.now(timezone.utc)
                    
                    for crypto, currencies in data.items():
                        for currency, rate in currencies.items():
                            if not currency.endswith('_24h_change'):
                                change_key = f'{currency}_24h_change'
                                change_24h = currencies.get(change_key, 0)
                                
                                crypto_symbol = crypto.upper()[:3]
                                currency_symbol = currency.upper()
                                
                                rates.append({
                                    'pair': f'{crypto_symbol}/{currency_symbol}',
                                    'rate': float(rate),
                                    'base': crypto_symbol,
                                    'quote': currency_symbol,
                                    'timestamp': timestamp.isoformat(),
                                    'source': 'coingecko',
                                    'bid': float(rate) * 0.999,
                                    'ask': float(rate) * 1.001,
                                    'change_24h': round(float(change_24h), 2),
                                    'asset_type': 'cryptocurrency'
                                })
                    
                    logger.info(f"Scraped {len(rates)} crypto forex rates")
                    return rates
                else:
                    logger.warning(f"CoinGecko forex request failed: {response.status}")
                    return []
                    
        except Exception as e:
            logger.error(f"Error scraping crypto forex rates: {e}")
            return []
    
    def calculate_forex_quality_score(self, rate_data: Dict[str, Any]) -> float:
        """Calculate quality score for forex data"""
        score = 0.0
        
        # Check required fields
        required_fields = ['pair', 'rate', 'base', 'quote', 'timestamp', 'source']
        for field in required_fields:
            if rate_data.get(field):
                score += 0.15
        
        # Check if rate is reasonable (not zero or negative)
        if rate_data.get('rate', 0) > 0:
            score += 0.1
        
        # Check if we have bid/ask spread
        if rate_data.get('bid') and rate_data.get('ask'):
            score += 0.05
        
        # Check if we have 24h change data
        if 'change_24h' in rate_data:
            score += 0.05
        
        return min(1.0, score)
    
    async def run_scrape(self, **kwargs) -> Dict[str, Any]:
        """Main scraping method"""
        await self.setup_session()
        
        try:
            all_rates = []
            
            # Scrape from multiple sources
            exchangerate_rates = await self.scrape_exchangerate_api()
            all_rates.extend(exchangerate_rates)
            
            await asyncio.sleep(self.request_delay)
            
            bank_rates = await self.scrape_bank_rates()
            all_rates.extend(bank_rates)
            
            await asyncio.sleep(self.request_delay)
            
            crypto_rates = await self.scrape_crypto_forex_pairs()
            all_rates.extend(crypto_rates)
            
            # Process and push to queue
            processed_rates = []
            for rate_data in all_rates:
                quality_score = self.calculate_forex_quality_score(rate_data)
                
                # Create scraped data object
                scraped_data = ScrapedData(
                    source=self.name,
                    data_type='forex_rate',
                    symbol=rate_data['pair'],
                    timestamp=datetime.now(timezone.utc),
                    raw_data=rate_data,
                    metadata={
                        'base_currency': rate_data['base'],
                        'quote_currency': rate_data['quote'],
                        'source_name': rate_data['source'],
                        'asset_type': rate_data.get('asset_type', 'fiat')
                    },
                    quality_score=quality_score
                )
                
                # Push to queue
                await self.queue_manager.publish(
                    topic='raw.forex.rates',
                    message=scraped_data.__dict__,
                    key=rate_data['pair']
                )
                
                processed_rates.append(rate_data)
                
                logger.info(
                    f"Forex rate processed",
                    topic='raw.forex.rates',
                    pair=rate_data['pair'],
                    rate=rate_data['rate'],
                    quality_score=quality_score
                )
            
            return {
                'success': True,
                'items_processed': len(processed_rates),
                'errors': [],
                'summary': {
                    'exchangerate_api_rates': len(exchangerate_rates),
                    'bank_rates': len(bank_rates),
                    'crypto_rates': len(crypto_rates),
                    'total_pairs': len(processed_rates)
                }
            }
            
        except Exception as e:
            logger.error(f"Forex scraping failed: {e}")
            return {
                'success': False,
                'items_processed': 0,
                'errors': [str(e)]
            }
        
        finally:
            await self.cleanup_session()
    
    async def health_check(self) -> Dict[str, Any]:
        """Health check for forex scraper"""
        try:
            await self.setup_session()
            
            # Test ExchangeRate-API availability
            async with self.session.get('https://api.exchangerate-api.com/v4/latest/USD',
                                       timeout=aiohttp.ClientTimeout(total=10)) as response:
                api_status = response.status == 200
            
            await self.cleanup_session()
            
            return {
                'status': 'healthy' if api_status else 'degraded',
                'exchangerate_api': 'available' if api_status else 'unavailable',
                'configured_pairs': len(self.currency_pairs),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }