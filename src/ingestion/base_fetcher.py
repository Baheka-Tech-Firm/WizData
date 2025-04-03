from abc import ABC, abstractmethod
import pandas as pd
from typing import Dict, List, Any, Optional, Union, TypeVar, Sequence
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define TypeVar for flexible symbol return types
T = TypeVar('T', str, Dict[str, str])
SymbolListType = Sequence[T]

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
    async def get_symbols(self, *args, **kwargs) -> SymbolListType:
        """
        Get available symbols/tickers from the data source
        
        Returns:
            SymbolListType: List of available symbols (either as strings or dictionaries)
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
        
    @staticmethod
    def export_to_json(df: pd.DataFrame, filepath: str) -> str:
        """
        Export DataFrame to JSON file
        
        Args:
            df (pd.DataFrame): DataFrame to export
            filepath (str): Path to save the JSON file
            
        Returns:
            str: Path to the saved file
        """
        df.to_json(filepath, orient='records', date_format='iso')
        return filepath
        
    @staticmethod
    def export_to_csv(df: pd.DataFrame, filepath: str) -> str:
        """
        Export DataFrame to CSV file
        
        Args:
            df (pd.DataFrame): DataFrame to export
            filepath (str): Path to save the CSV file
            
        Returns:
            str: Path to the saved file
        """
        df.to_csv(filepath, index=False)
        return filepath