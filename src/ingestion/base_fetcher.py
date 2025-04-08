"""
Base Fetcher Module

This module provides the base class for all data fetchers in the system.
It defines the common interface and functionality that all fetchers should implement.
"""

import logging
import os
import json
from abc import ABC, abstractmethod
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseFetcher(ABC):
    """Base class for all data fetchers"""
    
    def __init__(self, name, data_type='general'):
        """
        Initialize the base fetcher
        
        Args:
            name (str): Name of the fetcher
            data_type (str): Type of data being fetched (e.g., 'market', 'company', 'esg')
        """
        self.name = name
        self.data_type = data_type
        
        # Create cache directory
        self.cache_dir = os.path.join('data', 'cache', data_type)
        os.makedirs(self.cache_dir, exist_ok=True)
    
    def get_cache_path(self, key, extension='json'):
        """
        Get path for caching data
        
        Args:
            key (str): Cache key/identifier
            extension (str): File extension for the cache
            
        Returns:
            str: Path to the cache file
        """
        return os.path.join(self.cache_dir, f"{key}.{extension}")
    
    def save_to_cache(self, key, data, extension='json'):
        """
        Save data to cache
        
        Args:
            key (str): Cache key/identifier
            data: Data to cache
            extension (str): File extension for the cache
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            cache_path = self.get_cache_path(key, extension)
            
            if extension.lower() == 'json':
                with open(cache_path, 'w') as f:
                    json.dump(data, f, indent=2)
            else:
                # For other types, just save as string
                with open(cache_path, 'w') as f:
                    f.write(str(data))
                    
            return True
        except Exception as e:
            logger.error(f"Error saving to cache: {str(e)}")
            return False
    
    def load_from_cache(self, key, extension='json', max_age_hours=None):
        """
        Load data from cache
        
        Args:
            key (str): Cache key/identifier
            extension (str): File extension for the cache
            max_age_hours (int, optional): Maximum age of cache in hours
            
        Returns:
            Data from cache or None if not found or expired
        """
        cache_path = self.get_cache_path(key, extension)
        
        if not os.path.exists(cache_path):
            return None
        
        # Check cache age if max_age_hours specified
        if max_age_hours is not None:
            file_modified_time = os.path.getmtime(cache_path)
            file_modified_datetime = datetime.fromtimestamp(file_modified_time)
            age_hours = (datetime.now() - file_modified_datetime).total_seconds() / 3600
            
            if age_hours > max_age_hours:
                logger.info(f"Cache expired for {key} (age: {age_hours:.2f} hours)")
                return None
        
        try:
            if extension.lower() == 'json':
                with open(cache_path, 'r') as f:
                    return json.load(f)
            else:
                # For other types, just read as string
                with open(cache_path, 'r') as f:
                    return f.read()
        except Exception as e:
            logger.error(f"Error loading from cache: {str(e)}")
            return None
    
    @abstractmethod
    async def fetch_data(self, *args, **kwargs):
        """
        Fetch data from the source
        
        This method should be implemented by all fetcher subclasses.
        """
        pass