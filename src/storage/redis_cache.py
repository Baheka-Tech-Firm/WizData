import asyncio
import redis.asyncio as redis
from typing import Dict, List, Optional, Any, Union, Tuple
import json
import pickle
from datetime import datetime, timedelta

from src.utils.logger import get_storage_logger
from src.utils.config import REDIS_HOST, REDIS_PORT, REDIS_PASSWORD

class RedisCacheClient:
    """
    Client to interact with Redis for caching and rate limiting
    """
    
    def __init__(self, host: Optional[str] = None, port: Optional[int] = None, 
                password: Optional[str] = None, db: int = 0):
        self.logger = get_storage_logger("redis")
        self.host = host or REDIS_HOST
        self.port = port or REDIS_PORT
        self.password = password or REDIS_PASSWORD
        self.db = db
        self.redis = None
    
    async def connect(self) -> bool:
        """
        Connect to Redis
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        try:
            self.redis = redis.Redis(
                host=self.host,
                port=self.port,
                password=self.password,
                db=self.db,
                decode_responses=False,  # Keep binary data as is
                socket_timeout=5,
                socket_connect_timeout=5
            )
            
            # Test connection
            await self.redis.ping()
            self.logger.info(f"Connected to Redis at {self.host}:{self.port}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error connecting to Redis: {str(e)}")
            self.redis = None
            return False
    
    async def disconnect(self) -> None:
        """Close the Redis connection"""
        if self.redis:
            await self.redis.close()
            self.logger.info("Closed Redis connection")
    
    async def set_cache(self, key: str, value: Any, 
                      expiry_seconds: Optional[int] = None) -> bool:
        """
        Set a value in the cache
        
        Args:
            key (str): Cache key
            value (Any): Value to cache
            expiry_seconds (Optional[int]): Time to live in seconds
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.redis:
            if not await self.connect():
                return False
        
        try:
            # Serialize complex objects
            if not isinstance(value, (str, int, float, bool)):
                serialized_value = pickle.dumps(value)
            else:
                serialized_value = value
            
            if expiry_seconds:
                await self.redis.setex(key, expiry_seconds, serialized_value)
            else:
                await self.redis.set(key, serialized_value)
                
            self.logger.debug(f"Set cache for key: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error setting cache for key {key}: {str(e)}")
            return False
    
    async def get_cache(self, key: str, default_value: Any = None) -> Any:
        """
        Get a value from the cache
        
        Args:
            key (str): Cache key
            default_value (Any): Value to return if key not found
            
        Returns:
            Any: Cached value or default
        """
        if not self.redis:
            if not await self.connect():
                return default_value
        
        try:
            value = await self.redis.get(key)
            
            if value is None:
                return default_value
            
            try:
                # Try to deserialize as pickle
                return pickle.loads(value)
            except:
                # If not a pickle, return as is
                return value
                
        except Exception as e:
            self.logger.error(f"Error getting cache for key {key}: {str(e)}")
            return default_value
    
    async def delete_cache(self, key: str) -> bool:
        """
        Delete a value from the cache
        
        Args:
            key (str): Cache key
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self.redis:
            if not await self.connect():
                return False
        
        try:
            await self.redis.delete(key)
            self.logger.debug(f"Deleted cache for key: {key}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error deleting cache for key {key}: {str(e)}")
            return False
    
    async def increment_rate_limit(self, key: str, 
                                 window_seconds: int = 3600) -> Tuple[int, bool]:
        """
        Increment rate limit counter
        
        Args:
            key (str): Rate limit key
            window_seconds (int): Time window in seconds
            
        Returns:
            Tuple[int, bool]: (Current count, True if limit not exceeded)
        """
        if not self.redis:
            if not await self.connect():
                return 0, True  # If Redis is unavailable, don't block requests
        
        try:
            # Rate limit key format: rate_limit:{key}
            rate_key = f"rate_limit:{key}"
            
            # Check if key exists
            exists = await self.redis.exists(rate_key)
            
            if not exists:
                # Initialize with count 1 and set expiry
                pipe = self.redis.pipeline()
                pipe.set(rate_key, 1)
                pipe.expire(rate_key, window_seconds)
                await pipe.execute()
                return 1, True
            
            # Increment count
            count = await self.redis.incr(rate_key)
            
            # Return current count and whether limit is not exceeded (for easy checking)
            return count, True
            
        except Exception as e:
            self.logger.error(f"Error incrementing rate limit for key {key}: {str(e)}")
            return 0, True  # If error, don't block requests
    
    async def check_rate_limit(self, key: str, limit: int, 
                             window_seconds: int = 3600) -> Tuple[int, bool]:
        """
        Check if rate limit is exceeded
        
        Args:
            key (str): Rate limit key
            limit (int): Maximum allowed requests
            window_seconds (int): Time window in seconds
            
        Returns:
            Tuple[int, bool]: (Current count, True if limit not exceeded)
        """
        count, _ = await self.increment_rate_limit(key, window_seconds)
        return count, count <= limit
