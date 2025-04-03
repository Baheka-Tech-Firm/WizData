import aiohttp
import pandas as pd
from typing import Dict, List, Optional, Any
import asyncio
from datetime import datetime, timedelta

from src.ingestion.base_fetcher import BaseFetcher
from src.utils.config import COINMARKETCAP_API_KEY

class CryptoFetcher(BaseFetcher):
    """Fetcher for cryptocurrency data from CoinGecko and other sources"""
    
    def __init__(self):
        super().__init__("crypto")
        self.coingecko_base_url = "https://api.coingecko.com/api/v3"
        self.binance_base_url = "https://api.binance.com/api/v3"
        self.cmk_base_url = "https://pro-api.coinmarketcap.com/v1"
        self.cmk_api_key = COINMARKETCAP_API_KEY
        
        self.column_mapping = {
            'prices.0': 'timestamp',
            'prices.1': 'price',
            'market_caps.0': 'timestamp',
            'market_caps.1': 'market_cap',
            'total_volumes.0': 'timestamp',
            'total_volumes.1': 'volume',
            'symbol': 'symbol',
            'name': 'name',
            'id': 'coin_id',
            'current_price': 'price',
            'market_cap': 'market_cap',
            'market_cap_rank': 'rank',
            'high_24h': 'high_24h',
            'low_24h': 'low_24h',
            'price_change_24h': 'price_change_24h',
            'price_change_percentage_24h': 'price_change_percentage_24h',
            'circulating_supply': 'circulating_supply',
            'total_supply': 'total_supply',
            'max_supply': 'max_supply',
            'last_updated': 'last_updated',
        }
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available cryptocurrency symbols
        
        Returns:
            List[str]: List of cryptocurrency symbols
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.coingecko_base_url}/coins/list"
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to get crypto symbols, status code: {response.status}")
                        return []
                    
                    data = await response.json()
                    symbols = [item['symbol'].upper() for item in data if 'symbol' in item]
                    self.logger.info(f"Retrieved {len(symbols)} crypto symbols")
                    return symbols
        except Exception as e:
            self.logger.error(f"Error retrieving crypto symbols: {str(e)}")
            return []
    
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
        # Default to BTC if no symbol is provided
        if not symbol:
            symbol = "BTC"
            
        # Default date range if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
        self.log_fetch_attempt({
            "symbol": symbol, 
            "source": source, 
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date
        })
        
        try:
            if source.lower() == "coingecko":
                result = await self._fetch_from_coingecko(symbol, start_date, end_date)
            elif source.lower() == "binance":
                result = await self._fetch_from_binance(symbol, interval, start_date, end_date)
            else:
                self.logger.error(f"Unsupported source: {source}")
                return pd.DataFrame()
                
            if not result.empty:
                self.log_fetch_success(len(result))
            return result
                
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()
    
    async def _fetch_from_coingecko(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch cryptocurrency data from CoinGecko
        
        Args:
            symbol (str): Cryptocurrency symbol
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with cryptocurrency data
        """
        # Convert dates to Unix timestamp (milliseconds)
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
        
        # First get the coin ID from the symbol
        coin_id = await self._get_coin_id_from_symbol(symbol)
        if not coin_id:
            self.logger.error(f"Could not find coin ID for symbol: {symbol}")
            return pd.DataFrame()
        
        async with aiohttp.ClientSession() as session:
            # Fetch market data
            url = f"{self.coingecko_base_url}/coins/{coin_id}/market_chart/range"
            params = {
                'vs_currency': 'usd',
                'from': start_timestamp // 1000,  # CoinGecko requires seconds
                'to': end_timestamp // 1000
            }
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to get data for {symbol}, status code: {response.status}")
                    return pd.DataFrame()
                
                data = await response.json()
                
                # Process the data
                price_df = pd.DataFrame(data['prices'], columns=['timestamp', 'price'])
                market_cap_df = pd.DataFrame(data['market_caps'], columns=['timestamp', 'market_cap'])
                volume_df = pd.DataFrame(data['total_volumes'], columns=['timestamp', 'volume'])
                
                # Convert timestamp to datetime
                price_df['timestamp'] = pd.to_datetime(price_df['timestamp'], unit='ms')
                market_cap_df['timestamp'] = pd.to_datetime(market_cap_df['timestamp'], unit='ms')
                volume_df['timestamp'] = pd.to_datetime(volume_df['timestamp'], unit='ms')
                
                # Merge dataframes
                result = price_df.merge(market_cap_df, on='timestamp', how='outer')
                result = result.merge(volume_df, on='timestamp', how='outer')
                
                # Add symbol and source columns
                result['symbol'] = symbol
                result['source'] = 'coingecko'
                
                # Convert timestamp to string format
                result['date'] = result['timestamp'].dt.strftime('%Y-%m-%d')
                
                # Rename columns to standard format
                result.rename(columns={'timestamp': 'datetime'}, inplace=True)
                
                return result
    
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
        # Convert interval to Binance format
        interval_map = {
            'minute': '1m',
            'hourly': '1h',
            'daily': '1d',
        }
        binance_interval = interval_map.get(interval.lower(), '1d')
        
        # Convert dates to millisecond timestamps
        start_timestamp = int(datetime.strptime(start_date, "%Y-%m-%d").timestamp() * 1000)
        end_timestamp = int(datetime.strptime(end_date, "%Y-%m-%d").timestamp() * 1000)
        
        # Format symbol for Binance (usually with USDT pair)
        binance_symbol = f"{symbol.upper()}USDT"
        
        async with aiohttp.ClientSession() as session:
            url = f"{self.binance_base_url}/klines"
            params = {
                'symbol': binance_symbol,
                'interval': binance_interval,
                'startTime': start_timestamp,
                'endTime': end_timestamp,
                'limit': 1000  # Maximum allowed by Binance
            }
            
            async with session.get(url, params=params) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to get Binance data for {binance_symbol}, status code: {response.status}")
                    return pd.DataFrame()
                
                data = await response.json()
                
                # Binance klines format:
                # [Open time, Open, High, Low, Close, Volume, Close time, Quote asset volume, ...]
                columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 
                           'close_time', 'quote_asset_volume', 'number_of_trades',
                           'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']
                
                df = pd.DataFrame(data, columns=columns)
                
                # Convert timestamps to datetime
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
                
                # Convert numeric columns
                numeric_columns = ['open', 'high', 'low', 'close', 'volume', 'quote_asset_volume',
                                  'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume']
                for col in numeric_columns:
                    df[col] = pd.to_numeric(df[col])
                
                # Add symbol and source columns
                df['symbol'] = symbol
                df['source'] = 'binance'
                
                # Create date column
                df['date'] = df['timestamp'].dt.strftime('%Y-%m-%d')
                
                # Rename columns to standard format
                df.rename(columns={'timestamp': 'datetime'}, inplace=True)
                
                return df
                
    async def _get_coin_id_from_symbol(self, symbol: str) -> Optional[str]:
        """
        Get CoinGecko coin ID from symbol
        
        Args:
            symbol (str): Cryptocurrency symbol
            
        Returns:
            Optional[str]: CoinGecko coin ID or None if not found
        """
        try:
            async with aiohttp.ClientSession() as session:
                url = f"{self.coingecko_base_url}/coins/list"
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to get coin list, status code: {response.status}")
                        return None
                    
                    data = await response.json()
                    for coin in data:
                        if coin['symbol'].lower() == symbol.lower():
                            return coin['id']
                    
                    return None
        except Exception as e:
            self.logger.error(f"Error getting coin ID for symbol {symbol}: {str(e)}")
            return None
