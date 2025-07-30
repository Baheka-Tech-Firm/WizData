"""
Caching middleware for WizData API
Implements Redis-based caching with intelligent cache invalidation
"""

import json
import time
import hashlib
import logging
from typing import Any, Optional, Dict, Union, List
from functools import wraps
from flask import request, jsonify, current_app
from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

class CacheManager:
    """
    Redis-based cache manager with intelligent cache invalidation
    and different TTL strategies for different data types
    """
    
    def __init__(self, redis_client: Redis):
        self.redis = redis_client
        self.default_ttl = 300  # 5 minutes
        
        # Different TTLs for different data types
        self.ttl_config = {
            'market_data': 60,      # 1 minute - market data changes frequently
            'esg_data': 3600,       # 1 hour - ESG data is more stable
            'static_data': 86400,   # 24 hours - company info, symbols, etc.
            'api_responses': 300,   # 5 minutes - general API responses
            'analytics': 1800,      # 30 minutes - computed analytics
            'news': 900            # 15 minutes - news and updates
        }
    
    def _generate_cache_key(self, prefix: str, *args, **kwargs) -> str:
        """Generate a unique cache key based on parameters"""
        # Create a deterministic string from arguments
        key_data = {
            'args': args,
            'kwargs': sorted(kwargs.items())
        }
        
        key_string = json.dumps(key_data, sort_keys=True, default=str)
        key_hash = hashlib.md5(key_string.encode()).hexdigest()[:12]
        
        return f"cache:{prefix}:{key_hash}"
    
    def get(self, key: str) -> Optional[Any]:
        """Get a value from cache"""
        try:
            cached_data = self.redis.get(key)
            if cached_data:
                try:
                    return json.loads(cached_data)
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON in cache key: {key}")
                    self.delete(key)
                    return None
            return None
        except RedisError as e:
            logger.error(f"Redis error getting key {key}: {e}")
            return None
    
    def set(self, key: str, value: Any, ttl: Optional[int] = None, data_type: str = 'api_responses') -> bool:
        """Set a value in cache with optional TTL"""
        try:
            if ttl is None:
                ttl = self.ttl_config.get(data_type, self.default_ttl)
            
            # Add metadata to cached value
            cache_data = {
                'value': value,
                'cached_at': time.time(),
                'data_type': data_type,
                'ttl': ttl
            }
            
            serialized_data = json.dumps(cache_data, default=str)
            result = self.redis.setex(key, ttl, serialized_data)
            
            if result:
                logger.debug(f"Cached key {key} with TTL {ttl}s")
            
            return result
        except (RedisError, json.JSONEncodeError) as e:
            logger.error(f"Error setting cache key {key}: {e}")
            return False
    
    def delete(self, key: str) -> bool:
        """Delete a key from cache"""
        try:
            result = self.redis.delete(key)
            logger.debug(f"Deleted cache key {key}")
            return bool(result)
        except RedisError as e:
            logger.error(f"Error deleting cache key {key}: {e}")
            return False
    
    def delete_pattern(self, pattern: str) -> int:
        """Delete all keys matching a pattern"""
        try:
            keys = self.redis.keys(pattern)
            if keys:
                result = self.redis.delete(*keys)
                logger.info(f"Deleted {result} cache keys matching pattern {pattern}")
                return result
            return 0
        except RedisError as e:
            logger.error(f"Error deleting cache pattern {pattern}: {e}")
            return 0
    
    def get_cache_info(self, key: str) -> Optional[Dict[str, Any]]:
        """Get metadata about a cached item"""
        cached_data = self.get(key)
        if cached_data and isinstance(cached_data, dict):
            return {
                'cached_at': cached_data.get('cached_at'),
                'data_type': cached_data.get('data_type'),
                'ttl': cached_data.get('ttl'),
                'age_seconds': time.time() - cached_data.get('cached_at', 0)
            }
        return None
    
    def invalidate_by_data_type(self, data_type: str) -> int:
        """Invalidate all cache entries of a specific data type"""
        pattern = f"cache:*"
        deleted_count = 0
        
        try:
            # Get all cache keys
            keys = self.redis.keys(pattern)
            
            for key in keys:
                cached_data = self.redis.get(key)
                if cached_data:
                    try:
                        data = json.loads(cached_data)
                        if data.get('data_type') == data_type:
                            self.redis.delete(key)
                            deleted_count += 1
                    except json.JSONDecodeError:
                        continue
            
            logger.info(f"Invalidated {deleted_count} cache entries of type {data_type}")
            return deleted_count
            
        except RedisError as e:
            logger.error(f"Error invalidating cache by data type {data_type}: {e}")
            return 0
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        try:
            info = self.redis.info('memory')
            keyspace = self.redis.info('keyspace')
            
            # Count cache keys by type
            cache_keys = self.redis.keys("cache:*")
            type_counts = {}
            total_size = 0
            
            for key in cache_keys[:100]:  # Limit to avoid performance issues
                try:
                    cached_data = self.redis.get(key)
                    if cached_data:
                        data = json.loads(cached_data)
                        data_type = data.get('data_type', 'unknown')
                        type_counts[data_type] = type_counts.get(data_type, 0) + 1
                        total_size += len(cached_data)
                except (json.JSONDecodeError, RedisError):
                    continue
            
            return {
                'total_cache_keys': len(cache_keys),
                'cache_by_type': type_counts,
                'estimated_cache_size_bytes': total_size,
                'redis_memory_used': info.get('used_memory_human'),
                'redis_memory_peak': info.get('used_memory_peak_human'),
                'keyspace_hits': keyspace.get('keyspace_hits', 0),
                'keyspace_misses': keyspace.get('keyspace_misses', 0)
            }
            
        except RedisError as e:
            logger.error(f"Error getting cache stats: {e}")
            return {'error': 'Cache stats unavailable'}

def cached(ttl: Optional[int] = None, data_type: str = 'api_responses', key_prefix: str = None):
    """
    Decorator for caching Flask route responses
    
    Args:
        ttl: Time to live in seconds
        data_type: Type of data for TTL determination
        key_prefix: Custom prefix for cache key
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get cache manager from app context
            cache_manager = getattr(current_app, 'cache_manager', None)
            if not cache_manager:
                logger.warning("Cache manager not available, executing without cache")
                return f(*args, **kwargs)
            
            # Generate cache key
            prefix = key_prefix or f"{f.__module__}.{f.__name__}"
            
            # Include request parameters in cache key
            request_params = {}
            if request.args:
                request_params['query'] = dict(request.args)
            if request.json:
                request_params['json'] = request.json
            
            cache_key = cache_manager._generate_cache_key(prefix, *args, **dict(kwargs, **request_params))
            
            # Try to get from cache
            cached_result = cache_manager.get(cache_key)
            if cached_result and isinstance(cached_result, dict) and 'value' in cached_result:
                logger.debug(f"Cache hit for {cache_key}")
                response = jsonify(cached_result['value'])
                response.headers['X-Cache'] = 'HIT'
                response.headers['X-Cache-Age'] = str(int(time.time() - cached_result.get('cached_at', 0)))
                return response
            
            # Execute function and cache result
            logger.debug(f"Cache miss for {cache_key}")
            result = f(*args, **kwargs)
            
            # Only cache successful JSON responses
            if hasattr(result, 'get_json') and result.status_code == 200:
                try:
                    response_data = result.get_json()
                    cache_manager.set(cache_key, response_data, ttl, data_type)
                    
                    # Add cache headers
                    result.headers['X-Cache'] = 'MISS'
                    result.headers['X-Cache-TTL'] = str(ttl or cache_manager.ttl_config.get(data_type, cache_manager.default_ttl))
                    
                except Exception as e:
                    logger.error(f"Error caching response: {e}")
            
            return result
        
        return decorated_function
    return decorator

def cache_invalidate_on_update(data_types: Union[str, List[str]]):
    """
    Decorator to invalidate cache when data is updated
    
    Args:
        data_types: Data type(s) to invalidate
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            result = f(*args, **kwargs)
            
            # Invalidate cache if the operation was successful
            if hasattr(result, 'status_code') and 200 <= result.status_code < 300:
                cache_manager = getattr(current_app, 'cache_manager', None)
                if cache_manager:
                    types_to_invalidate = data_types if isinstance(data_types, list) else [data_types]
                    for data_type in types_to_invalidate:
                        cache_manager.invalidate_by_data_type(data_type)
            
            return result
        
        return decorated_function
    return decorator