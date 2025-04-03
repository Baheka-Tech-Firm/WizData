from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional, Union
from src.utils.logger import get_ingestion_logger

class BaseFetcher(ABC):
    """Base class for all data fetching operations"""
    
    def __init__(self, source_name: str):
        self.source_name = source_name
        self.logger = get_ingestion_logger(source_name)
        
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
        self.logger.info(f"Attempting to fetch data from {self.source_name} with params: {params}")
    
    def log_fetch_success(self, data_length: int) -> None:
        """Log successful data fetch"""
        self.logger.info(f"Successfully fetched {data_length} records from {self.source_name}")
    
    def log_fetch_error(self, error: Exception) -> None:
        """Log error during data fetch"""
        self.logger.error(f"Error fetching data from {self.source_name}: {str(error)}")
    
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
        result = df.copy()
        
        # Rename columns based on mapping
        columns_to_rename = {k: v for k, v in mapping.items() if k in result.columns}
        result = result.rename(columns=columns_to_rename)
        
        return result
