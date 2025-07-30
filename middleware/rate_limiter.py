"""
Rate limiting middleware for WizData API
Implements token bucket algorithm with Redis backend for distributed rate limiting
"""

import time
import json
import logging
from typing import Optional, Dict, Any, Tuple
from functools import wraps
from flask import request, jsonify, g
from redis import Redis
from redis.exceptions import RedisError

logger = logging.getLogger(__name__)

class RateLimiter:
    """
    Distributed rate limiter using Redis as backend
    Implements token bucket algorithm for smooth rate limiting
    """
    
    def __init__(self, redis_client: Redis, default_rpm: int = 60, burst_size: int = 10):
        self.redis = redis_client
        self.default_rpm = default_rpm
        self.burst_size = burst_size
        self.window_size = 60  # 1 minute window
    
    def _get_client_id(self) -> str:
        """Generate a unique client identifier"""
        # Try to get API key from headers
        api_key = request.headers.get('X-API-Key')
        if api_key:
            return f"api_key:{api_key}"
        
        # Fall back to IP address
        client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
        if client_ip:
            # Handle comma-separated IPs from proxies
            client_ip = client_ip.split(',')[0].strip()
        
        return f"ip:{client_ip}"
    
    def _get_rate_limit_for_client(self, client_id: str) -> Tuple[int, int]:
        """Get rate limit settings for a specific client"""
        if client_id.startswith('api_key:'):
            # Authenticated users get higher limits
            return 200, 20  # 200 RPM, 20 burst
        else:
            # Anonymous users get standard limits
            return self.default_rpm, self.burst_size
    
    def is_allowed(self, endpoint: str = None) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request is allowed based on rate limiting rules
        
        Args:
            endpoint: Optional endpoint-specific rate limiting
            
        Returns:
            Tuple of (is_allowed, rate_limit_info)
        """
        client_id = self._get_client_id()
        requests_per_minute, burst_size = self._get_rate_limit_for_client(client_id)
        
        # Create Redis key for this client
        key = f"rate_limit:{client_id}"
        if endpoint:
            key = f"rate_limit:{client_id}:{endpoint}"
        
        try:
            current_time = time.time()
            window_start = int(current_time // self.window_size) * self.window_size
            
            # Use Redis pipeline for atomic operations
            pipe = self.redis.pipeline()
            
            # Get current window data
            window_key = f"{key}:{window_start}"
            pipe.get(window_key)
            pipe.ttl(window_key)
            
            results = pipe.execute()
            current_count = int(results[0] or 0)
            ttl = results[1]
            
            # Calculate if request is allowed
            tokens_per_second = requests_per_minute / 60.0
            time_since_window_start = current_time - window_start
            expected_tokens = min(burst_size, int(tokens_per_second * time_since_window_start))
            
            is_allowed = current_count < expected_tokens
            
            if is_allowed:
                # Increment counter
                pipe = self.redis.pipeline()
                pipe.incr(window_key)
                if ttl == -1:  # Key doesn't have expiration
                    pipe.expire(window_key, self.window_size + 10)  # Add buffer
                pipe.execute()
                
                current_count += 1
            
            # Calculate rate limit headers
            reset_time = window_start + self.window_size
            remaining = max(0, expected_tokens - current_count)
            
            rate_limit_info = {
                'limit': requests_per_minute,
                'remaining': remaining,
                'reset': int(reset_time),
                'reset_after': max(0, int(reset_time - current_time)),
                'burst_limit': burst_size,
                'current_window_count': current_count,
                'expected_tokens': expected_tokens
            }
            
            return is_allowed, rate_limit_info
            
        except RedisError as e:
            logger.error(f"Redis error in rate limiter: {e}")
            # Fail open - allow request if Redis is down
            return True, {
                'limit': requests_per_minute,
                'remaining': requests_per_minute,
                'reset': int(time.time() + 60),
                'reset_after': 60,
                'error': 'Rate limiter temporarily unavailable'
            }
    
    def get_rate_limit_status(self, client_id: str = None) -> Dict[str, Any]:
        """Get current rate limit status for a client"""
        if not client_id:
            client_id = self._get_client_id()
        
        requests_per_minute, burst_size = self._get_rate_limit_for_client(client_id)
        
        try:
            current_time = time.time()
            window_start = int(current_time // self.window_size) * self.window_size
            window_key = f"rate_limit:{client_id}:{window_start}"
            
            current_count = int(self.redis.get(window_key) or 0)
            reset_time = window_start + self.window_size
            
            tokens_per_second = requests_per_minute / 60.0
            time_since_window_start = current_time - window_start
            expected_tokens = min(burst_size, int(tokens_per_second * time_since_window_start))
            remaining = max(0, expected_tokens - current_count)
            
            return {
                'client_id': client_id,
                'limit': requests_per_minute,
                'remaining': remaining,
                'reset': int(reset_time),
                'reset_after': max(0, int(reset_time - current_time)),
                'burst_limit': burst_size,
                'current_count': current_count,
                'window_start': window_start
            }
            
        except RedisError as e:
            logger.error(f"Redis error getting rate limit status: {e}")
            return {
                'client_id': client_id,
                'limit': requests_per_minute,
                'error': 'Rate limiter temporarily unavailable'
            }

def rate_limit(requests_per_minute: int = None, endpoint: str = None):
    """
    Decorator for rate limiting Flask routes
    
    Args:
        requests_per_minute: Override default rate limit
        endpoint: Endpoint-specific rate limiting key
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            # Get rate limiter from app context
            rate_limiter = getattr(g, 'rate_limiter', None)
            if not rate_limiter:
                logger.warning("Rate limiter not available, allowing request")
                return f(*args, **kwargs)
            
            # Check if request is allowed
            is_allowed, rate_info = rate_limiter.is_allowed(endpoint)
            
            if not is_allowed:
                response = jsonify({
                    'error': 'Rate limit exceeded',
                    'message': f'Too many requests. Limit: {rate_info["limit"]} requests per minute',
                    'retry_after': rate_info['reset_after']
                })
                response.status_code = 429
                
                # Add rate limit headers
                response.headers['X-RateLimit-Limit'] = str(rate_info['limit'])
                response.headers['X-RateLimit-Remaining'] = str(rate_info['remaining'])
                response.headers['X-RateLimit-Reset'] = str(rate_info['reset'])
                response.headers['Retry-After'] = str(rate_info['reset_after'])
                
                return response
            
            # Add rate limit headers to successful responses
            response = f(*args, **kwargs)
            if hasattr(response, 'headers'):
                response.headers['X-RateLimit-Limit'] = str(rate_info['limit'])
                response.headers['X-RateLimit-Remaining'] = str(rate_info['remaining'])
                response.headers['X-RateLimit-Reset'] = str(rate_info['reset'])
            
            return response
        
        return decorated_function
    return decorator