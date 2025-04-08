import aiohttp
import pandas as pd
import logging
import json
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any
import re
from bs4 import BeautifulSoup

import trafilatura
from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class JSEScraper(BaseFetcher):
    """Scraper for Johannesburg Stock Exchange (JSE) data"""
    
    def __init__(self):
        """Initialize the JSE scraper"""
        super().__init__("JSE Scraper")
        self.base_url = "https://www.jse.co.za"
        self.symbols_cache = []
        self.symbols_last_updated = None
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available JSE symbols
        
        Returns:
            List[str]: List of JSE symbols
        """
        # Return cached symbols if available and not too old
        if self.symbols_cache and self.symbols_last_updated and \
           (datetime.now() - self.symbols_last_updated).total_seconds() < 86400:  # 24 hours
            logger.info(f"Returning {len(self.symbols_cache)} cached JSE symbols")
            return self.symbols_cache
        
        try:
            # Hardcoded list of major JSE symbols for now
            # In a production environment, we would scrape the JSE website for a complete list
            symbols = [
                "SOL", "NPN", "SLM", "FSR", "SBK", "AGL", "BHG", "MTN", "ABG", "SHP",
                "ANH", "BTI", "CPI", "DSY", "GLN", "INL", "INP", "MRP", "NED", "REM"
            ]
            
            self.symbols_cache = symbols
            self.symbols_last_updated = datetime.now()
            logger.info(f"Retrieved {len(symbols)} JSE symbols")
            return symbols
        except Exception as e:
            logger.error(f"Error retrieving JSE symbols: {str(e)}")
            # Return cached symbols if available, otherwise an empty list
            return self.symbols_cache if self.symbols_cache else []
    
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
        self.log_fetch_attempt({"symbol": symbol, "start_date": start_date, "end_date": end_date})
        
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
                df = await self._fetch_symbol_data(symbol, start_date, end_date)
            else:
                # Fetch data for all available symbols
                symbols = await self.get_symbols()
                # Limit to first 5 symbols for demo purposes
                symbols = symbols[:5]
                
                all_dfs = []
                for sym in symbols:
                    try:
                        sym_df = await self._fetch_symbol_data(sym, start_date, end_date)
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
                logger.warning(f"No data retrieved for JSE {symbol if symbol else 'symbols'}")
            
            return df
        
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()  # Return empty DataFrame on error
    
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
        # In a real implementation, this would scrape data from the JSE website
        # For this example, we'll generate some sample data
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate number of business days
        days = (end_dt - start_dt).days + 1
        
        # Base values based on symbol (to create different but stable values for different symbols)
        symbol_seed = sum(ord(c) for c in symbol) % 100
        base_price = 100 + symbol_seed * 5  # Different base price for each symbol
        
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
            # Skip weekends (simple implementation)
            if current_dt.weekday() < 5:  # 0-4 are Monday to Friday
                # Daily change - small random fluctuation around previous close
                daily_change_pct = (symbol_seed % 10 - 3) / 100  # -3% to +6% based on symbol
                
                # Calculate prices
                open_price = close_price * (1 + (daily_change_pct / 4))
                close_price = open_price * (1 + daily_change_pct)
                high_price = max(open_price, close_price) * 1.02
                low_price = min(open_price, close_price) * 0.98
                
                # Volume - also varies by symbol
                volume = int(100000 + (symbol_seed * 10000) * (0.5 + days / 100))
                
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
            'exchange': 'JSE',
            'asset_type': 'stock'
        })
        
        return df