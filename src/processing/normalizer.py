import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, timezone
import pytz
import re

from src.utils.logger import get_processing_logger

class DataNormalizer:
    """
    Class to normalize financial data from different sources
    into a consistent format
    """
    
    def __init__(self):
        self.logger = get_processing_logger()
        
        # Standard column names for OHLCV data
        self.ohlcv_columns = ['date', 'open', 'high', 'low', 'close', 'volume']
        
        # Standard column names for news data
        self.news_columns = ['date', 'title', 'content', 'source', 'url', 'sentiment']
        
        # Standard column names for fundamental data
        self.fundamental_columns = [
            'symbol', 'date', 'market_cap', 'pe_ratio', 'eps', 'dividend_yield', 
            'book_value', 'price_to_book', 'revenue', 'net_income'
        ]
    
    def normalize_ohlcv(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Normalize OHLCV (Open, High, Low, Close, Volume) data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            source (str): Data source identifier
            
        Returns:
            pd.DataFrame: Normalized DataFrame
        """
        self.logger.info(f"Normalizing OHLCV data from {source} with {len(df)} rows")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            # Ensure required columns exist
            for col in self.ohlcv_columns:
                if col not in result.columns:
                    # Try to find equivalent column
                    if col == 'date' and 'datetime' in result.columns:
                        result['date'] = pd.to_datetime(result['datetime']).dt.strftime('%Y-%m-%d')
                    elif col == 'volume' and 'vol' in result.columns:
                        result['volume'] = result['vol']
                    else:
                        self.logger.warning(f"Column {col} not found and could not be derived")
                        result[col] = None
            
            # Ensure date is in correct format
            if 'date' in result.columns:
                # First convert to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(result['date']):
                    result['date'] = pd.to_datetime(result['date'], errors='coerce')
                
                # Then convert to string in YYYY-MM-DD format
                result['date'] = result['date'].dt.strftime('%Y-%m-%d')
            
            # Ensure numeric columns are actually numeric
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            for col in numeric_cols:
                if col in result.columns:
                    result[col] = pd.to_numeric(result[col], errors='coerce')
            
            # Ensure symbol is uppercase
            if 'symbol' in result.columns:
                result['symbol'] = result['symbol'].str.upper()
            
            # Add source identifier if not present
            if 'source' not in result.columns:
                result['source'] = source
            
            # Add timestamp for when the data was normalized
            result['processed_at'] = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"Successfully normalized {len(result)} rows of OHLCV data")
            return result
            
        except Exception as e:
            self.logger.error(f"Error normalizing OHLCV data: {str(e)}")
            # Return original dataframe if normalization fails
            return df
    
    def normalize_news(self, df: pd.DataFrame, source: str) -> pd.DataFrame:
        """
        Normalize financial news data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            source (str): Data source identifier
            
        Returns:
            pd.DataFrame: Normalized DataFrame
        """
        self.logger.info(f"Normalizing news data from {source} with {len(df)} rows")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            # Ensure required columns exist
            for col in self.news_columns:
                if col not in result.columns:
                    # Try to find equivalent column
                    if col == 'date' and 'published_at' in result.columns:
                        result['date'] = pd.to_datetime(result['published_at']).dt.strftime('%Y-%m-%d')
                    elif col == 'content' and 'description' in result.columns:
                        result['content'] = result['description']
                    else:
                        self.logger.warning(f"Column {col} not found and could not be derived")
                        result[col] = None
            
            # Ensure date is in correct format
            if 'date' in result.columns:
                # First convert to datetime if it's not already
                if not pd.api.types.is_datetime64_any_dtype(result['date']):
                    result['date'] = pd.to_datetime(result['date'], errors='coerce')
                
                # Then convert to string in YYYY-MM-DD format
                result['date'] = result['date'].dt.strftime('%Y-%m-%d')
            
            # Add source identifier if not present
            if 'source' not in result.columns:
                result['source'] = source
            
            # Add timestamp for when the data was normalized
            result['processed_at'] = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"Successfully normalized {len(result)} rows of news data")
            return result
            
        except Exception as e:
            self.logger.error(f"Error normalizing news data: {str(e)}")
            # Return original dataframe if normalization fails
            return df
    
    def normalize_symbol(self, symbol: str, market: Optional[str] = None) -> str:
        """
        Normalize a financial symbol/ticker to a standard format
        
        Args:
            symbol (str): Symbol to normalize
            market (Optional[str]): Market identifier (e.g., 'JSE', 'NYSE')
            
        Returns:
            str: Normalized symbol
        """
        if not symbol:
            return ""
        
        # Convert to uppercase
        symbol = symbol.upper().strip()
        
        # Remove any special characters except dots
        symbol = re.sub(r'[^\w\.]', '', symbol)
        
        # Add market suffix if provided and not already present
        if market and not symbol.endswith(f".{market}"):
            symbol = f"{symbol}.{market}"
        
        return symbol
    
    def normalize_timeframe(self, df: pd.DataFrame, from_interval: str, 
                           to_interval: str, price_col: str = 'close') -> pd.DataFrame:
        """
        Normalize data from one timeframe to another
        
        Args:
            df (pd.DataFrame): Input DataFrame
            from_interval (str): Source interval (e.g., '1m', '1h')
            to_interval (str): Target interval (e.g., '1h', '1d')
            price_col (str): Column to use for price data
            
        Returns:
            pd.DataFrame: Normalized DataFrame with new timeframe
        """
        self.logger.info(f"Converting timeframe from {from_interval} to {to_interval}")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            # Ensure datetime column exists
            datetime_col = None
            for col in ['datetime', 'timestamp', 'date']:
                if col in result.columns:
                    datetime_col = col
                    break
            
            if not datetime_col:
                self.logger.error("No datetime column found for timeframe conversion")
                return df
            
            # Convert to datetime if not already
            if not pd.api.types.is_datetime64_any_dtype(result[datetime_col]):
                result[datetime_col] = pd.to_datetime(result[datetime_col], errors='coerce')
            
            # Set datetime as index
            result.set_index(datetime_col, inplace=True)
            
            # Define resampling rule based on target interval
            resample_rule = self._get_resample_rule(to_interval)
            
            # Resample data
            ohlc_dict = {
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }
            
            # Filter only columns that exist in the DataFrame
            ohlc_dict = {k: v for k, v in ohlc_dict.items() if k in result.columns}
            
            # If any OHLC column is missing, add it based on price_col
            for col in ['open', 'high', 'low', 'close']:
                if col not in result.columns and price_col in result.columns:
                    result[col] = result[price_col]
                    ohlc_dict[col] = 'first' if col == 'open' else 'max' if col == 'high' else 'min' if col == 'low' else 'last'
            
            # Resample
            resampled = result.resample(resample_rule).agg(ohlc_dict)
            
            # Reset index to get datetime as column again
            resampled.reset_index(inplace=True)
            
            # Rename index column back to original name
            resampled.rename(columns={'index': datetime_col}, inplace=True)
            
            # Ensure date column exists
            if 'date' not in resampled.columns and datetime_col in resampled.columns:
                resampled['date'] = pd.to_datetime(resampled[datetime_col]).dt.strftime('%Y-%m-%d')
            
            self.logger.info(f"Successfully converted timeframe from {from_interval} to {to_interval}")
            return resampled
            
        except Exception as e:
            self.logger.error(f"Error converting timeframe: {str(e)}")
            # Return original dataframe if conversion fails
            return df
    
    def _get_resample_rule(self, interval: str) -> str:
        """
        Convert a time interval string to a pandas resample rule
        
        Args:
            interval (str): Time interval (e.g., '1m', '1h', '1d')
            
        Returns:
            str: Pandas resample rule
        """
        # Extract number and unit from interval
        match = re.match(r'(\d+)([mhdwMQ])', interval)
        if not match:
            raise ValueError(f"Invalid interval format: {interval}")
        
        num, unit = match.groups()
        num = int(num)
        
        # Map unit to pandas frequency string
        unit_map = {
            'm': 'min',
            'h': 'H',
            'd': 'D',
            'w': 'W',
            'M': 'M',
            'Q': 'Q'
        }
        
        if unit not in unit_map:
            raise ValueError(f"Unsupported time unit: {unit}")
        
        return f"{num}{unit_map[unit]}"
