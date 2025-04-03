import asyncio
import pandas as pd
import aiohttp
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import io

from src.ingestion.base_fetcher import BaseFetcher
from src.utils.logger import get_ingestion_logger

class JSEScraper(BaseFetcher):
    """Scraper for Johannesburg Stock Exchange (JSE) data"""
    
    def __init__(self):
        super().__init__("jse")
        self.base_url = "https://www.jse.co.za"
        self.listings_url = f"{self.base_url}/content/JSEPricingListItems/GetAllPricingItems"
        self.price_history_url = f"{self.base_url}/downloadable-files/price-history/equity-"
        self.column_mapping = {
            'Symbol': 'symbol',
            'Date': 'date',
            'Open': 'open',
            'High': 'high',
            'Low': 'low',
            'Close': 'close',
            'Volume': 'volume',
            'Value': 'value',
            'Number of Trades': 'trades',
            'Company': 'company_name',
            'Sector': 'sector'
        }
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available JSE symbols
        
        Returns:
            List[str]: List of JSE symbols
        """
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.listings_url) as response:
                    if response.status != 200:
                        self.logger.error(f"Failed to get JSE symbols, status code: {response.status}")
                        return []
                    
                    data = await response.json()
                    symbols = [item['code'] for item in data if 'code' in item]
                    self.logger.info(f"Retrieved {len(symbols)} JSE symbols")
                    return symbols
        except Exception as e:
            self.logger.error(f"Error retrieving JSE symbols: {str(e)}")
            return []
    
    async def fetch_data(self, symbol: Optional[str] = None, 
                        start_date: Optional[str] = None, 
                        end_date: Optional[str] = None,
                        **kwargs) -> pd.DataFrame:
        """
        Fetch JSE data for a specific symbol or all symbols
        
        Args:
            symbol (Optional[str]): JSE symbol to fetch data for (e.g., "SOL")
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with JSE data
        """
        # Default date range if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
            
        self.log_fetch_attempt({"symbol": symbol, "start_date": start_date, "end_date": end_date})
        
        try:
            # If no symbol is provided, fetch data for all symbols
            if not symbol:
                symbols = await self.get_symbols()
                all_data = []
                for sym in symbols:
                    try:
                        df = await self._fetch_symbol_data(sym, start_date, end_date)
                        if not df.empty:
                            all_data.append(df)
                    except Exception as e:
                        self.logger.error(f"Error fetching data for symbol {sym}: {str(e)}")
                        continue
                
                if not all_data:
                    return pd.DataFrame()
                    
                result = pd.concat(all_data)
                self.log_fetch_success(len(result))
                return result
            else:
                result = await self._fetch_symbol_data(symbol, start_date, end_date)
                if not result.empty:
                    self.log_fetch_success(len(result))
                return result
                
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()
    
    async def _fetch_symbol_data(self, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch historical data for a specific JSE symbol
        
        Args:
            symbol (str): JSE symbol
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with historical data
        """
        # Format dates for the JSE API
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        url = f"{self.price_history_url}{symbol.lower()}.csv"
        
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url) as response:
                    if response.status != 200:
                        self.logger.warning(f"Failed to get data for {symbol}, status code: {response.status}")
                        return pd.DataFrame()
                    
                    # Read CSV content
                    content = await response.read()
                    df = pd.read_csv(io.BytesIO(content))
                    
                    # Filter by date range
                    if 'Date' in df.columns:
                        df['Date'] = pd.to_datetime(df['Date'])
                        df = df[(df['Date'] >= start_date_obj) & (df['Date'] <= end_date_obj)]
                    
                    # Add symbol column if not present
                    if 'Symbol' not in df.columns:
                        df['Symbol'] = symbol
                    
                    # Standardize column names
                    df = self.standardize_dataframe(df, self.column_mapping)
                    
                    # Ensure date is in string format YYYY-MM-DD
                    if 'date' in df.columns:
                        df['date'] = df['date'].dt.strftime('%Y-%m-%d')
                    
                    return df
            except Exception as e:
                self.logger.error(f"Error processing data for symbol {symbol}: {str(e)}")
                return pd.DataFrame()
