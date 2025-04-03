import aiohttp
import pandas as pd
from typing import Dict, List, Optional, Any
import asyncio
from datetime import datetime, timedelta
import json

from src.ingestion.base_fetcher import BaseFetcher
from src.utils.config import ALPHA_VANTAGE_API_KEY

class ForexFetcher(BaseFetcher):
    """Fetcher for foreign exchange (forex) data"""
    
    def __init__(self):
        super().__init__("forex")
        self.alpha_vantage_base_url = "https://www.alphavantage.co/query"
        self.alpha_vantage_api_key = ALPHA_VANTAGE_API_KEY
        
        # Default major currency pairs
        self.major_pairs = [
            "EURUSD", "USDJPY", "GBPUSD", "AUDUSD", 
            "USDCHF", "NZDUSD", "USDCAD", "EURJPY",
            "EURGBP", "USDZAR"  # ZAR is South African Rand
        ]
        
        self.column_mapping = {
            '1. open': 'open',
            '2. high': 'high',
            '3. low': 'low',
            '4. close': 'close',
            'timestamp': 'datetime',
            'date': 'date',
            'from_currency': 'base_currency',
            'to_currency': 'quote_currency',
            'exchange_rate': 'rate',
            'bid_price': 'bid',
            'ask_price': 'ask'
        }
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available forex currency pairs
        
        Returns:
            List[str]: List of currency pairs
        """
        # For Alpha Vantage we use predefined major pairs
        # A more comprehensive list would require additional API calls or a static list
        return self.major_pairs
    
    async def fetch_data(self, symbol: Optional[str] = None, 
                         interval: str = "daily",
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         **kwargs) -> pd.DataFrame:
        """
        Fetch forex data from Alpha Vantage
        
        Args:
            symbol (Optional[str]): Currency pair (e.g., "EURUSD")
            interval (str): Time interval ("daily", "weekly", "monthly", "intraday")
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with forex data
        """
        # Default to EURUSD if no symbol is provided
        if not symbol:
            symbol = "EURUSD"
            
        # Default date range if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        self.log_fetch_attempt({
            "symbol": symbol, 
            "interval": interval,
            "start_date": start_date,
            "end_date": end_date
        })
        
        try:
            # Parse currency pair
            if len(symbol) == 6:
                from_currency = symbol[:3]
                to_currency = symbol[3:]
            else:
                parts = symbol.split('/')
                if len(parts) == 2:
                    from_currency, to_currency = parts
                else:
                    self.logger.error(f"Invalid currency pair format: {symbol}")
                    return pd.DataFrame()
            
            # Determine function based on interval
            function = "FX_DAILY"
            if interval.lower() == "weekly":
                function = "FX_WEEKLY"
            elif interval.lower() == "monthly":
                function = "FX_MONTHLY"
            elif interval.lower() == "intraday":
                function = "FX_INTRADAY"
                
            # Fetch data from Alpha Vantage
            result = await self._fetch_from_alpha_vantage(
                function, from_currency, to_currency, interval
            )
            
            if not result.empty:
                # Filter by date range if data is available
                if 'date' in result.columns:
                    result['date'] = pd.to_datetime(result['date'])
                    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
                    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
                    result = result[(result['date'] >= start_date_obj) & (result['date'] <= end_date_obj)]
                    result['date'] = result['date'].dt.strftime('%Y-%m-%d')
                
                self.log_fetch_success(len(result))
            return result
                
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()
    
    async def _fetch_from_alpha_vantage(self, function: str, from_currency: str, 
                                        to_currency: str, interval: str = "daily") -> pd.DataFrame:
        """
        Fetch forex data from Alpha Vantage API
        
        Args:
            function (str): Alpha Vantage function name
            from_currency (str): Base currency code
            to_currency (str): Quote currency code
            interval (str): Time interval for intraday data
            
        Returns:
            pd.DataFrame: DataFrame with forex data
        """
        params = {
            "function": function,
            "from_symbol": from_currency,
            "to_symbol": to_currency,
            "apikey": self.alpha_vantage_api_key,
            "outputsize": "full"
        }
        
        # Add interval parameter for intraday data
        if function == "FX_INTRADAY":
            params["interval"] = "60min"  # Default to 1-hour
            if interval.lower() in ["1min", "5min", "15min", "30min", "60min"]:
                params["interval"] = interval.lower()
        
        async with aiohttp.ClientSession() as session:
            async with session.get(self.alpha_vantage_base_url, params=params) as response:
                if response.status != 200:
                    self.logger.error(f"Failed to get data from Alpha Vantage, status code: {response.status}")
                    return pd.DataFrame()
                
                data = await response.json()
                
                # Check for error response
                if "Error Message" in data:
                    self.logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                    return pd.DataFrame()
                
                # Extract time series data
                time_series_key = None
                for key in data.keys():
                    if "Time Series" in key:
                        time_series_key = key
                        break
                
                if not time_series_key:
                    self.logger.error("No time series data found in Alpha Vantage response")
                    return pd.DataFrame()
                
                # Convert to DataFrame
                time_series_data = data[time_series_key]
                df = pd.DataFrame.from_dict(time_series_data, orient="index")
                
                # Add date as column
                df['date'] = df.index
                
                # Add symbol information
                df['symbol'] = f"{from_currency}{to_currency}"
                df['from_currency'] = from_currency
                df['to_currency'] = to_currency
                df['source'] = 'alpha_vantage'
                
                # Standardize column names
                df = self.standardize_dataframe(df, self.column_mapping)
                
                # Convert numeric columns
                for col in ['open', 'high', 'low', 'close']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col])
                
                return df
