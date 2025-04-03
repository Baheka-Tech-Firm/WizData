from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class BaseFetcher(ABC):
    """Base class for all data fetching operations"""
    
    def __init__(self, source_name: str):
        """
        Initialize the fetcher
        
        Args:
            source_name (str): Name of the data source
        """
        self.source_name = source_name
        logger.info(f"Initialized {source_name} fetcher")
    
    @abstractmethod
    async def fetch_data(self, **kwargs) -> pd.DataFrame:
        """
        Fetch data from the source and return a pandas DataFrame
        
        Args:
            **kwargs: Additional parameters specific to the data source
            
        Returns:
            pd.DataFrame: The fetched data in a standardized format
        """
        pass
    
    @abstractmethod
    async def get_symbols(self) -> List[str]:
        """
        Get available symbols/tickers from the data source
        
        Returns:
            List[str]: List of available symbols
        """
        pass
    
    def log_fetch_attempt(self, params: Dict[str, Any]) -> None:
        """Log data fetch attempt"""
        logger.info(f"Attempting to fetch data from {self.source_name} with params: {params}")
    
    def log_fetch_success(self, data_length: int) -> None:
        """Log successful data fetch"""
        logger.info(f"Successfully fetched {data_length} records from {self.source_name}")
    
    def log_fetch_error(self, error: Exception) -> None:
        """Log error during data fetch"""
        logger.error(f"Error fetching data from {self.source_name}: {str(error)}")
    
    @staticmethod
    def standardize_dataframe(df: pd.DataFrame, mapping: Dict[str, str]) -> pd.DataFrame:
        """
        Standardize column names in DataFrame
        
        Args:
            df (pd.DataFrame): Original DataFrame
            mapping (Dict[str, str]): Mapping from source column names to standard names
            
        Returns:
            pd.DataFrame: DataFrame with standardized column names
        """
        # Create a copy to avoid modifying the original
        result_df = df.copy()
        
        # Rename columns according to mapping
        renamed_columns = {src: dst for src, dst in mapping.items() if src in df.columns}
        if renamed_columns:
            result_df = result_df.rename(columns=renamed_columns)
        
        return result_df