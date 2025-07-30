"""
CoinGecko cryptocurrency scraper
Fetches crypto prices, market data, and trends
"""

import asyncio
import aiohttp
from typing import Dict, List, Any
from datetime import datetime, timezone
try:
    from scrapers.base.scraper_base import BaseScraper, ScrapedData
except ImportError:
    # Fallback for development
    class BaseScraper:
        def __init__(self, name, config): pass
    class ScrapedData:
        def __init__(self, **kwargs): pass

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

class CoinGeckoScraper(BaseScraper):
    """
    Scraper for CoinGecko cryptocurrency data
    Fetches crypto prices, market cap, volume, and trending data
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("coingecko", config)
        
        # CoinGecko API configuration
        self.base_url = "https://api.coingecko.com/api/v3"
        self.api_key = config.get('api_key')  # Pro API key (optional)
        
        # Default cryptocurrencies to monitor
        self.default_coins = [
            "bitcoin", "ethereum", "binancecoin", "cardano", "solana",
            "avalanche-2", "polygon", "chainlink", "uniswap", "litecoin"
        ]
        
        # Request configuration
        self.session = None
        self.rate_limit_delay = 1.2  # CoinGecko free tier: 50 calls/minute
    
    async def fetch_data(self, coins: List[str] = None, vs_currency: str = "usd", **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch cryptocurrency data from CoinGecko
        """
        coins = coins or self.default_coins
        
        if not self.session:
            session_config = await self.proxy_manager.get_session_config()
            self.session = aiohttp.ClientSession(**session_config)
        
        all_data = []
        
        try:
            # Fetch data in batches to respect rate limits
            batch_size = 10  # CoinGecko allows up to 250 coins per request
            
            for i in range(0, len(coins), batch_size):
                batch = coins[i:i + batch_size]
                await self._rate_limit()
                
                batch_data = await self._fetch_coins_batch(batch, vs_currency)
                if batch_data:
                    all_data.extend(batch_data)
                
                self.logger.debug(
                    "Fetched coin batch",
                    batch_size=len(batch),
                    data_count=len(batch_data) if batch_data else 0
                )
            
            # Also fetch trending data
            await self._rate_limit()
            trending_data = await self._fetch_trending()
            if trending_data:
                all_data.append(trending_data)
            
            self.logger.info(
                "CoinGecko data fetch completed",
                coins_requested=len(coins),
                total_data_items=len(all_data)
            )
            
        except Exception as e:
            self.logger.error(
                "Error during CoinGecko fetch",
                error=str(e)
            )
        
        return all_data
    
    async def _fetch_coins_batch(self, coins: List[str], vs_currency: str = "usd") -> List[Dict[str, Any]]:
        """Fetch data for a batch of coins"""
        try:
            coins_param = ",".join(coins)
            
            params = {
                'ids': coins_param,
                'vs_currencies': vs_currency,
                'include_market_cap': 'true',
                'include_24hr_vol': 'true',
                'include_24hr_change': 'true',
                'include_last_updated_at': 'true'
            }
            
            # Add API key if available
            if self.api_key:
                params['x_cg_pro_api_key'] = self.api_key
            
            url = f"{self.base_url}/simple/price"
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Convert to list format with metadata
                    result = []
                    for coin_id, coin_data in data.items():
                        coin_data['_fetch_metadata'] = {
                            'coin_id': coin_id,
                            'fetch_time': datetime.now(timezone.utc).isoformat(),
                            'url': str(response.url),
                            'status_code': response.status,
                            'data_type': 'price_data'
                        }
                        result.append(coin_data)
                    
                    return result
                    
                elif response.status == 429:
                    self.logger.warning("Rate limit hit, backing off")
                    await asyncio.sleep(60)  # Wait 1 minute on rate limit
                    return []
                    
                else:
                    self.logger.warning(
                        "Non-200 response for coins batch",
                        status_code=response.status,
                        coins=coins
                    )
                    return []
                    
        except Exception as e:
            self.logger.error(
                "Error fetching coins batch",
                coins=coins,
                error=str(e)
            )
            return []
    
    async def _fetch_trending(self) -> Dict[str, Any]:
        """Fetch trending cryptocurrency data"""
        try:
            url = f"{self.base_url}/search/trending"
            
            params = {}
            if self.api_key:
                params['x_cg_pro_api_key'] = self.api_key
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    data['_fetch_metadata'] = {
                        'fetch_time': datetime.now(timezone.utc).isoformat(),
                        'url': str(response.url),
                        'status_code': response.status,
                        'data_type': 'trending_data'
                    }
                    
                    return data
                    
                else:
                    self.logger.warning(
                        "Failed to fetch trending data",
                        status_code=response.status
                    )
                    return None
                    
        except Exception as e:
            self.logger.error(
                "Error fetching trending data",
                error=str(e)
            )
            return None
    
    def parse_data(self, raw_data: Dict[str, Any]) -> List[ScrapedData]:
        """
        Parse CoinGecko raw data into standardized format
        """
        try:
            fetch_metadata = raw_data.get('_fetch_metadata', {})
            data_type = fetch_metadata.get('data_type', 'unknown')
            
            if data_type == 'price_data':
                return self._parse_price_data(raw_data, fetch_metadata)
            elif data_type == 'trending_data':
                return self._parse_trending_data(raw_data, fetch_metadata)
            else:
                self.logger.warning(
                    "Unknown data type",
                    data_type=data_type
                )
                return []
                
        except Exception as e:
            self.logger.error(
                "Error parsing CoinGecko data",
                error=str(e),
                raw_data_preview=str(raw_data)[:200]
            )
            return []
    
    def _parse_price_data(self, raw_data: Dict[str, Any], fetch_metadata: Dict[str, Any]) -> List[ScrapedData]:
        """Parse cryptocurrency price data"""
        coin_id = fetch_metadata.get('coin_id', 'unknown')
        
        scraped_data = ScrapedData(
            source="coingecko",
            data_type="crypto_price",
            symbol=coin_id,
            timestamp=datetime.now(timezone.utc),
            raw_data=raw_data,
            metadata={
                'data_provider': 'coingecko_api',
                'fetch_url': fetch_metadata.get('url'),
                'fetch_time': fetch_metadata.get('fetch_time'),
                'coin_id': coin_id
            }
        )
        
        return [scraped_data]
    
    def _parse_trending_data(self, raw_data: Dict[str, Any], fetch_metadata: Dict[str, Any]) -> List[ScrapedData]:
        """Parse trending cryptocurrency data"""
        scraped_data = ScrapedData(
            source="coingecko",
            data_type="crypto_trending",
            symbol=None,  # No specific symbol for trending data
            timestamp=datetime.now(timezone.utc),
            raw_data=raw_data,
            metadata={
                'data_provider': 'coingecko_api',
                'fetch_url': fetch_metadata.get('url'),
                'fetch_time': fetch_metadata.get('fetch_time')
            }
        )
        
        return [scraped_data]
    
    def normalize_data(self, parsed_data: List[ScrapedData]) -> List[ScrapedData]:
        """
        Apply CoinGecko-specific normalization and quality checks
        """
        normalized = []
        
        for data in parsed_data:
            try:
                if data.data_type == "crypto_price":
                    normalized_item = self._normalize_price_data(data)
                elif data.data_type == "crypto_trending":
                    normalized_item = self._normalize_trending_data(data)
                else:
                    continue
                
                if normalized_item:
                    normalized.append(normalized_item)
                    
            except Exception as e:
                self.logger.error(
                    "Error normalizing CoinGecko data",
                    symbol=data.symbol,
                    data_type=data.data_type,
                    error=str(e)
                )
                continue
        
        return normalized
    
    def _normalize_price_data(self, data: ScrapedData) -> ScrapedData:
        """Normalize cryptocurrency price data"""
        raw_data = data.raw_data
        
        # Extract price information (assuming USD)
        usd_price = raw_data.get('usd')
        market_cap = raw_data.get('usd_market_cap')
        volume_24h = raw_data.get('usd_24h_vol')
        change_24h = raw_data.get('usd_24h_change')
        
        # Validate essential data
        if not usd_price or usd_price <= 0:
            self.logger.warning(
                "Invalid price data",
                symbol=data.symbol,
                price=usd_price
            )
            return None
        
        # Add normalized fields to metadata
        data.metadata.update({
            'normalized_price': float(usd_price),
            'normalized_market_cap': float(market_cap) if market_cap else None,
            'normalized_volume_24h': float(volume_24h) if volume_24h else None,
            'normalized_change_24h': float(change_24h) if change_24h else None,
            'price_currency': 'USD',
            'last_updated': raw_data.get('last_updated_at')
        })
        
        # Calculate quality score
        quality_factors = {
            'has_price': bool(usd_price),
            'has_market_cap': bool(market_cap),
            'has_volume': bool(volume_24h),
            'has_change': change_24h is not None
        }
        
        data.quality_score = sum(quality_factors.values()) / len(quality_factors)
        
        return data
    
    def _normalize_trending_data(self, data: ScrapedData) -> ScrapedData:
        """Normalize trending cryptocurrency data"""
        raw_data = data.raw_data
        
        # Extract trending coins
        trending_coins = raw_data.get('coins', [])
        
        if not trending_coins:
            return None
        
        data.metadata.update({
            'trending_count': len(trending_coins),
            'trending_coins': [coin.get('item', {}).get('id') for coin in trending_coins],
            'data_freshness': 'real_time'
        })
        
        # Trending data is always high quality if present
        data.quality_score = 1.0 if trending_coins else 0.0
        
        return data
    
    async def health_check(self) -> Dict[str, Any]:
        """Check if CoinGecko API is accessible"""
        try:
            if not self.session:
                session_config = await self.proxy_manager.get_session_config()
                self.session = aiohttp.ClientSession(**session_config)
            
            # Test with ping endpoint
            test_url = f"{self.base_url}/ping"
            
            async with self.session.get(test_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    data = await response.json()
                    return {
                        'source': 'coingecko',
                        'status': 'healthy',
                        'api_accessible': True,
                        'api_response': data.get('gecko_says'),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                else:
                    return {
                        'source': 'coingecko',
                        'status': 'degraded',
                        'api_accessible': False,
                        'status_code': response.status,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    
        except Exception as e:
            return {
                'source': 'coingecko',
                'status': 'unhealthy',
                'api_accessible': False,
                'error': str(e),
                'timestamp': datetime.now(timezone.utc).isoformat()
            }
    
    async def __aenter__(self):
        """Async context manager entry"""
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()