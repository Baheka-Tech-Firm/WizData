"""
JSE (Johannesburg Stock Exchange) scraper
Fetches real-time and historical stock data from JSE website
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

class JSEScraper(BaseScraper):
    """
    Scraper for Johannesburg Stock Exchange data
    Fetches stock prices, volumes, and market data
    """
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__("jse", config)
        
        # JSE-specific configuration
        self.base_url = "https://www.jse.co.za"
        self.api_base = "https://api.jse.co.za"
        
        # Common JSE symbols to monitor
        self.default_symbols = [
            "AGL", "BHP", "BTI", "CFR", "FSR", "GFI", "IMP", "MTN", 
            "NPN", "SBK", "SHP", "SLM", "SOL", "TKG", "VOD"
        ]
        
        # Request configuration
        self.session = None
    
    async def fetch_data(self, symbols: List[str] = None, **kwargs) -> List[Dict[str, Any]]:
        """
        Fetch stock data from JSE
        """
        symbols = symbols or self.default_symbols
        
        if not self.session:
            session_config = await self.proxy_manager.get_session_config()
            self.session = aiohttp.ClientSession(**session_config)
        
        all_data = []
        
        for symbol in symbols:
            try:
                await self._rate_limit()
                
                # Fetch stock data for this symbol
                stock_data = await self._fetch_symbol_data(symbol)
                if stock_data:
                    all_data.append(stock_data)
                
                self.logger.debug(
                    "Fetched symbol data",
                    symbol=symbol,
                    data_available=bool(stock_data)
                )
                
            except Exception as e:
                self.logger.error(
                    "Failed to fetch symbol data",
                    symbol=symbol,
                    error=str(e)
                )
                continue
        
        self.logger.info(
            "JSE data fetch completed",
            symbols_requested=len(symbols),
            symbols_fetched=len(all_data)
        )
        
        return all_data
    
    async def _fetch_symbol_data(self, symbol: str) -> Dict[str, Any]:
        """Fetch data for a specific JSE symbol"""
        try:
            # JSE API endpoint for stock data
            url = f"{self.api_base}/api/equity/summary/{symbol}"
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # Add metadata
                    data['_fetch_metadata'] = {
                        'symbol': symbol,
                        'fetch_time': datetime.now(timezone.utc).isoformat(),
                        'url': url,
                        'status_code': response.status
                    }
                    
                    return data
                else:
                    self.logger.warning(
                        "Non-200 response for symbol",
                        symbol=symbol,
                        status_code=response.status
                    )
                    return None
                    
        except Exception as e:
            self.logger.error(
                "Error fetching symbol data",
                symbol=symbol,
                error=str(e)
            )
            return None
    
    def parse_data(self, raw_data: Dict[str, Any]) -> List[ScrapedData]:
        """
        Parse JSE raw data into standardized format
        """
        try:
            fetch_metadata = raw_data.get('_fetch_metadata', {})
            symbol = fetch_metadata.get('symbol', 'UNKNOWN')
            
            # Extract price data
            price_data = raw_data.get('lastTrade', {})
            market_data = raw_data.get('marketData', {})
            
            if not price_data:
                self.logger.warning(
                    "No price data found",
                    symbol=symbol,
                    raw_data_keys=list(raw_data.keys())
                )
                return []
            
            # Create standardized data structure
            scraped_data = ScrapedData(
                source="jse",
                data_type="stock_price",
                symbol=symbol,
                timestamp=datetime.now(timezone.utc),
                raw_data=raw_data,
                metadata={
                    'exchange': 'JSE',
                    'currency': 'ZAR',
                    'data_provider': 'jse_api',
                    'fetch_url': fetch_metadata.get('url'),
                    'fetch_time': fetch_metadata.get('fetch_time')
                }
            )
            
            self.logger.debug(
                "Parsed JSE data",
                symbol=symbol,
                price=price_data.get('price'),
                volume=market_data.get('volume')
            )
            
            return [scraped_data]
            
        except Exception as e:
            self.logger.error(
                "Error parsing JSE data",
                error=str(e),
                raw_data_preview=str(raw_data)[:200]
            )
            return []
    
    def normalize_data(self, parsed_data: List[ScrapedData]) -> List[ScrapedData]:
        """
        Apply JSE-specific normalization and quality checks
        """
        normalized = []
        
        for data in parsed_data:
            try:
                # Extract price information
                last_trade = data.raw_data.get('lastTrade', {})
                market_data = data.raw_data.get('marketData', {})
                
                # Validate price data
                price = last_trade.get('price')
                if not price or price <= 0:
                    self.logger.warning(
                        "Invalid price data",
                        symbol=data.symbol,
                        price=price
                    )
                    continue
                
                # Normalize volume (handle different formats)
                volume = market_data.get('volume', 0)
                if isinstance(volume, str):
                    volume = self._parse_volume_string(volume)
                
                # Add normalized fields to metadata
                data.metadata.update({
                    'normalized_price': float(price),
                    'normalized_volume': int(volume) if volume else 0,
                    'price_currency': 'ZAR',
                    'market_open': market_data.get('isOpen', False),
                    'last_trade_time': last_trade.get('time'),
                })
                
                # Calculate quality score based on data completeness
                quality_factors = {
                    'has_price': bool(price),
                    'has_volume': bool(volume),
                    'has_timestamp': bool(last_trade.get('time')),
                    'recent_data': True  # Assume recent for now
                }
                
                quality_score = sum(quality_factors.values()) / len(quality_factors)
                data.quality_score = quality_score
                
                normalized.append(data)
                
                self.logger.debug(
                    "Normalized JSE data",
                    symbol=data.symbol,
                    quality_score=quality_score,
                    price=data.metadata['normalized_price']
                )
                
            except Exception as e:
                self.logger.error(
                    "Error normalizing JSE data",
                    symbol=data.symbol,
                    error=str(e)
                )
                continue
        
        return normalized
    
    def _parse_volume_string(self, volume_str: str) -> int:
        """Parse volume string (e.g., '1.2M', '500K') to integer"""
        try:
            volume_str = str(volume_str).upper().strip()
            
            if 'M' in volume_str:
                return int(float(volume_str.replace('M', '')) * 1_000_000)
            elif 'K' in volume_str:
                return int(float(volume_str.replace('K', '')) * 1_000)
            else:
                return int(float(volume_str))
                
        except (ValueError, TypeError):
            return 0
    
    async def health_check(self) -> Dict[str, Any]:
        """Check if JSE API is accessible"""
        try:
            if not self.session:
                session_config = await self.proxy_manager.get_session_config()
                self.session = aiohttp.ClientSession(**session_config)
            
            # Test with a common stock
            test_url = f"{self.api_base}/api/equity/summary/AGL"
            
            async with self.session.get(test_url, timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    return {
                        'source': 'jse',
                        'status': 'healthy',
                        'api_accessible': True,
                        'response_time_ms': response.headers.get('X-Response-Time'),
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                else:
                    return {
                        'source': 'jse',
                        'status': 'degraded',
                        'api_accessible': False,
                        'status_code': response.status,
                        'timestamp': datetime.now(timezone.utc).isoformat()
                    }
                    
        except Exception as e:
            return {
                'source': 'jse',
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