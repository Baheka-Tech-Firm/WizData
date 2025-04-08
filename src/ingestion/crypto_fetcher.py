import aiohttp
import pandas as pd
import logging
import json
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any
import time

from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class CryptoFetcher(BaseFetcher):
    """Fetcher for cryptocurrency data from CoinGecko and other sources"""
    
    def __init__(self):
        """Initialize the cryptocurrency fetcher"""
        super().__init__("Crypto Fetcher")
        self.coingecko_base_url = "https://api.coingecko.com/api/v3"
        self.binance_base_url = "https://api.binance.com/api/v3"
        
        # Cache for coin IDs and symbols
        self.coin_id_map = {}
        self.symbols_cache = []
        self.symbols_last_updated = None
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available cryptocurrency symbols
        
        Returns:
            List[str]: List of cryptocurrency symbols
        """
        # Return cached symbols if available and not too old
        if self.symbols_cache and self.symbols_last_updated and \
           (datetime.now() - self.symbols_last_updated).total_seconds() < 86400:  # 24 hours
            logger.info(f"Returning {len(self.symbols_cache)} cached crypto symbols")
            return self.symbols_cache
        
        try:
            # For demonstration, use a limited set of popular cryptocurrencies
            # In production, this would fetch the complete list from CoinGecko
            symbols = [
                "BTC", "ETH", "BNB", "SOL", "XRP", "ADA", "DOGE", "DOT", "AVAX", "LINK",
                "MATIC", "UNI", "ATOM", "LTC", "FTM", "ALGO", "XLM", "NEAR", "HBAR", "MANA"
            ]
            
            self.symbols_cache = symbols
            self.symbols_last_updated = datetime.now()
            logger.info(f"Retrieved {len(symbols)} crypto symbols")
            return symbols
        except Exception as e:
            logger.error(f"Error retrieving crypto symbols: {str(e)}")
            # Return cached symbols if available, otherwise an empty list
            return self.symbols_cache if self.symbols_cache else []
    
    async def fetch_data(self, symbol: Optional[str] = None, 
                         source: str = "coingecko",
                         interval: str = "daily",
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         **kwargs) -> pd.DataFrame:
        """
        Fetch cryptocurrency data from the specified source
        
        Args:
            symbol (Optional[str]): Cryptocurrency symbol (e.g., "BTC")
            source (str): Data source ("coingecko" or "binance")
            interval (str): Time interval ("daily", "hourly", "minute")
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with cryptocurrency data
        """
        self.log_fetch_attempt({
            "symbol": symbol, 
            "source": source, 
            "interval": interval,
            "start_date": start_date, 
            "end_date": end_date
        })
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 30 days before end date
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")
        
        try:
            if symbol:
                # Fetch data for a specific symbol
                if source.lower() == "coingecko":
                    coin_id = await self._get_coin_id_from_symbol(symbol)
                    if coin_id:
                        df = await self._fetch_from_coingecko(coin_id, start_date, end_date)
                    else:
                        logger.error(f"Could not find CoinGecko ID for symbol {symbol}")
                        df = pd.DataFrame()
                elif source.lower() == "binance":
                    df = await self._fetch_from_binance(symbol, interval, start_date, end_date)
                else:
                    logger.error(f"Invalid source: {source}")
                    df = pd.DataFrame()
            else:
                # Fetch data for multiple symbols
                symbols = await self.get_symbols()
                # Limit to first 5 symbols for demo purposes
                symbols = symbols[:5]
                
                all_dfs = []
                for sym in symbols:
                    try:
                        if source.lower() == "coingecko":
                            coin_id = await self._get_coin_id_from_symbol(sym)
                            if coin_id:
                                sym_df = await self._fetch_from_coingecko(coin_id, start_date, end_date)
                                if not sym_df.empty:
                                    all_dfs.append(sym_df)
                            else:
                                logger.error(f"Could not find CoinGecko ID for symbol {sym}")
                        elif source.lower() == "binance":
                            sym_df = await self._fetch_from_binance(sym, interval, start_date, end_date)
                            if not sym_df.empty:
                                all_dfs.append(sym_df)
                    except Exception as e:
                        logger.error(f"Error fetching data for symbol {sym}: {str(e)}")
                
                if all_dfs:
                    df = pd.concat(all_dfs, ignore_index=True)
                else:
                    df = pd.DataFrame()
            
            if not df.empty:
                self.log_fetch_success(len(df))
            else:
                logger.warning(f"No data retrieved for crypto {symbol if symbol else 'symbols'}")
            
            return df
        
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()  # Return empty DataFrame on error
    
    async def _fetch_from_coingecko(self, coin_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch cryptocurrency data from CoinGecko
        
        Args:
            coin_id (str): CoinGecko coin ID
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with cryptocurrency data
        """
        # For now, return simulated data (in a real implementation, we would fetch from CoinGecko API)
        # Note: CoinGecko free API has rate limits, so we're using simulation for this demo
        
        # Convert dates to unix timestamps
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_timestamp = int(start_dt.timestamp())
        end_timestamp = int(end_dt.timestamp())
        
        # Calculate number of days
        days = (end_dt - start_dt).days + 1
        
        # Base values based on coin_id (to create different but stable values for different coins)
        coin_seed = sum(ord(c) for c in coin_id) % 100
        
        # Different base price for each coin
        if "bitcoin" in coin_id:
            base_price = 28000 + coin_seed * 100
        elif "ethereum" in coin_id:
            base_price = 1800 + coin_seed * 10
        else:
            base_price = 10 + coin_seed / 5
        
        # Generate simulated price data
        dates = []
        prices = []
        market_caps = []
        volumes = []
        
        current_dt = start_dt
        price = base_price
        
        while current_dt <= end_dt:
            # Daily change - random fluctuation
            daily_change_pct = (coin_seed % 15 - 7) / 100  # -7% to +7% based on coin
            
            # Price changes more dramatically for cryptocurrencies
            price = price * (1 + daily_change_pct)
            
            # Volume and market cap based on price
            volume = int(price * 10000 + coin_seed * 1000000)
            market_cap = int(price * (1000000 + coin_seed * 100000))
            
            # Add to lists
            dates.append(current_dt.strftime("%Y-%m-%d"))
            prices.append(round(price, 2))
            market_caps.append(market_cap)
            volumes.append(volume)
            
            current_dt += timedelta(days=1)
        
        # Get symbol from coin_id
        symbol = "".join([c[0].upper() for c in coin_id.split("-")])[:5]  # Simple conversion
        if coin_id == "bitcoin":
            symbol = "BTC"
        elif coin_id == "ethereum":
            symbol = "ETH"
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'symbol': symbol,
            'open': prices,
            'high': [p * 1.05 for p in prices],
            'low': [p * 0.95 for p in prices],
            'close': prices,
            'volume': volumes,
            'market_cap': market_caps,
            'exchange': 'CoinGecko',
            'asset_type': 'crypto'
        })
        
        return df
    
    async def _fetch_from_binance(self, symbol: str, interval: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch cryptocurrency data from Binance
        
        Args:
            symbol (str): Cryptocurrency symbol
            interval (str): Time interval
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with cryptocurrency data
        """
        # For now, using simulated data instead of actual API call
        # This is similar to the CoinGecko simulation with minor differences
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate number of days
        days = (end_dt - start_dt).days + 1
        
        # Adjust symbol for Binance format (add USDT if not already a pair)
        binance_symbol = symbol if "USDT" in symbol else f"{symbol}USDT"
        
        # Base price varies by symbol
        symbol_seed = sum(ord(c) for c in symbol) % 100
        
        if symbol == "BTC":
            base_price = 28000 + symbol_seed
        elif symbol == "ETH":
            base_price = 1800 + symbol_seed
        else:
            base_price = 10 + symbol_seed / 2
        
        # Generate simulated price data
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        current_dt = start_dt
        close_price = base_price
        
        while current_dt <= end_dt:
            # Daily change - random fluctuation
            daily_change_pct = (symbol_seed % 12 - 6) / 100  # -6% to +5% based on symbol
            
            # Calculate prices
            open_price = close_price
            close_price = open_price * (1 + daily_change_pct)
            high_price = max(open_price, close_price) * 1.03
            low_price = min(open_price, close_price) * 0.97
            
            # Volume varies by symbol
            volume = int(1000000 + symbol_seed * 50000)
            
            # Add to lists
            dates.append(current_dt.strftime("%Y-%m-%d"))
            opens.append(round(open_price, 2))
            highs.append(round(high_price, 2))
            lows.append(round(low_price, 2))
            closes.append(round(close_price, 2))
            volumes.append(volume)
            
            current_dt += timedelta(days=1)
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'symbol': symbol,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes,
            'exchange': 'Binance',
            'asset_type': 'crypto'
        })
        
        return df
    
    async def _get_coin_id_from_symbol(self, symbol: str) -> Optional[str]:
        """
        Get CoinGecko coin ID from symbol
        
        Args:
            symbol (str): Cryptocurrency symbol
            
        Returns:
            Optional[str]: CoinGecko coin ID or None if not found
        """
        # Check cache first
        if symbol in self.coin_id_map:
            return self.coin_id_map[symbol]
        
        # For demo purposes, use a manual mapping for common coins
        symbol_to_id = {
            "BTC": "bitcoin",
            "ETH": "ethereum",
            "BNB": "binancecoin",
            "SOL": "solana",
            "XRP": "ripple",
            "ADA": "cardano",
            "DOGE": "dogecoin",
            "DOT": "polkadot",
            "AVAX": "avalanche-2",
            "LINK": "chainlink",
            "MATIC": "matic-network",
            "UNI": "uniswap",
            "ATOM": "cosmos",
            "LTC": "litecoin",
            "FTM": "fantom",
            "ALGO": "algorand",
            "XLM": "stellar",
            "NEAR": "near",
            "HBAR": "hedera-hashgraph",
            "MANA": "decentraland"
        }
        
        if symbol in symbol_to_id:
            coin_id = symbol_to_id[symbol]
            self.coin_id_map[symbol] = coin_id
            return coin_id
        
        # In a production environment, we would query the CoinGecko API
        # to get the mapping for all coins
        
        logger.warning(f"Could not find CoinGecko ID for symbol {symbol}")
        return None