from fastapi import Request, Response, HTTPException, Depends
from typing import Optional, Callable, Dict, Any, Tuple
from datetime import datetime, timedelta
import time
import hashlib
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from src.storage.redis_cache import RedisCacheClient
from src.utils.config import RATE_LIMIT_DEFAULT, RATE_LIMIT_PREMIUM
from src.utils.logger import get_api_logger

class RateLimiter:
    """
    Rate limiter for API endpoints using Redis
    """
    
    def __init__(self):
        self.redis_client = RedisCacheClient()
        self.logger = get_api_logger()
    
    async def check_rate_limit(self, request: Request, 
                              api_key: Optional[str] = None) -> Tuple[int, int, bool]:
        """
        Check if the request exceeds rate limit
        
        Args:
            request (Request): FastAPI request object
            api_key (Optional[str]): API key for premium users
            
        Returns:
            Tuple[int, int, bool]: (Current count, limit, passed)
        """
        # Determine client identifier (IP or API key)
        client_id = api_key if api_key else self._get_client_ip(request)
        
        # Generate a rate limit key that includes the endpoint path
        endpoint = request.url.path
        rate_key = f"{client_id}:{endpoint}"
        
        # Determine rate limit based on API key
        limit = RATE_LIMIT_PREMIUM if api_key else RATE_LIMIT_DEFAULT
        window = 3600  # 1 hour window
        
        try:
            # Check rate limit
            count, within_limit = await self.redis_client.check_rate_limit(
                rate_key, limit, window
            )
            
            if not within_limit:
                self.logger.warning(f"Rate limit exceeded for {client_id} on {endpoint}: {count}/{limit}")
            
            return count, limit, within_limit
            
        except Exception as e:
            self.logger.error(f"Error checking rate limit: {str(e)}")
            # If there's an error, allow the request to pass
            return 0, limit, True
    
    def _get_client_ip(self, request: Request) -> str:
        """
        Get client IP address from request
        
        Args:
            request (Request): FastAPI request object
            
        Returns:
            str: Client IP address
        """
        # Check for forwarded header first (for proxy setups)
        forwarded_for = request.headers.get("X-Forwarded-For")
        if forwarded_for:
            # Get the first IP in case of multiple proxies
            return forwarded_for.split(",")[0].strip()
        
        # Fall back to direct client
        return request.client.host if request.client else "unknown"

class RateLimitMiddleware(BaseHTTPMiddleware):
    """
    Middleware for applying rate limiting to all API requests
    """
    
    def __init__(self, app: ASGIApp):
        super().__init__(app)
        self.rate_limiter = RateLimiter()
        self.logger = get_api_logger()
    
    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        Process request with rate limiting
        
        Args:
            request (Request): FastAPI request object
            call_next (Callable): Function to call next middleware
            
        Returns:
            Response: API response
        """
        # Skip rate limiting for OPTIONS requests (CORS preflight)
        if request.method == "OPTIONS":
            return await call_next(request)
        
        # Get API key from header or query parameter
        api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
        
        # Check rate limit
        count, limit, within_limit = await self.rate_limiter.check_rate_limit(request, api_key)
        
        # Add rate limit headers to response
        response = await call_next(request)
        response.headers["X-Rate-Limit-Limit"] = str(limit)
        response.headers["X-Rate-Limit-Remaining"] = str(max(0, limit - count))
        response.headers["X-Rate-Limit-Reset"] = str(int(time.time() + 3600))
        
        return response

async def rate_limit_dependency(
    request: Request,
    api_key: Optional[str] = None
) -> None:
    """
    Dependency for manual rate limiting at the route level
    
    Args:
        request (Request): FastAPI request object
        api_key (Optional[str]): API key for premium users
        
    Raises:
        HTTPException: If rate limit is exceeded
    """
    rate_limiter = RateLimiter()
    count, limit, within_limit = await rate_limiter.check_rate_limit(request, api_key)
    
    if not within_limit:
        retry_after = int(time.time() + 3600)
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded",
            headers={
                "Retry-After": str(retry_after),
                "X-Rate-Limit-Limit": str(limit),
                "X-Rate-Limit-Remaining": "0",
                "X-Rate-Limit-Reset": str(retry_after)
            }
        )
