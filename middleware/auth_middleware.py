"""
Authentication Middleware for API Key Validation
Handles API key authentication and authorization for all requests
"""

from flask import request, jsonify, g
from functools import wraps
import time
from datetime import datetime
from typing import Optional, Dict, Any
import redis
import os
import json

# Import API key manager
import sys
sys.path.append('/home/runner/workspace')
from auth.api_key_manager import api_key_manager

class AuthenticationMiddleware:
    """Middleware for API key authentication and rate limiting"""
    
    def __init__(self, redis_client=None):
        self.redis_client = redis_client
        self.rate_limit_window = 3600  # 1 hour
        self.exempt_endpoints = {
            '/health',
            '/api-services', 
            '/docs',
            '/openapi.json',
            '/',
            '/charting',
            '/scrapers',
            '/sources',
            '/jobs',
            '/products',
            '/admin/api-keys'  # Admin dashboard doesn't need API key
        }
    
    def extract_api_key(self, request) -> Optional[str]:
        """Extract API key from request headers or query parameters"""
        
        # Check Authorization header (Bearer token)
        auth_header = request.headers.get('Authorization', '')
        if auth_header.startswith('Bearer '):
            return auth_header[7:]
        
        # Check X-API-Key header
        api_key = request.headers.get('X-API-Key')
        if api_key:
            return api_key
        
        # Check query parameter (less secure, for development)
        api_key = request.args.get('api_key')
        if api_key:
            return api_key
        
        return None
    
    def is_rate_limited(self, key_id: str, rate_limit: int) -> bool:
        """Check if API key is rate limited"""
        if not self.redis_client:
            return False  # No rate limiting without Redis
        
        try:
            rate_key = f"rate_limit:{key_id}"
            current_count = self.redis_client.get(rate_key)
            
            if current_count is None:
                # First request in this window
                self.redis_client.setex(rate_key, self.rate_limit_window, 1)
                return False
            
            current_count = int(current_count)
            if current_count >= rate_limit:
                return True
            
            # Increment counter
            self.redis_client.incr(rate_key)
            return False
            
        except Exception as e:
            print(f"Rate limit check error: {e}")
            return False  # Fail open
    
    def should_authenticate(self, endpoint: str) -> bool:
        """Check if endpoint requires authentication"""
        
        # Exempt endpoints don't require authentication
        if endpoint in self.exempt_endpoints:
            return False
        
        # Check if endpoint starts with exempt path
        for exempt in self.exempt_endpoints:
            if endpoint.startswith(exempt):
                return False
        
        # All API v1 endpoints require authentication  
        if endpoint.startswith('/api/v1/'):
            return True
        
        # Protected admin endpoints
        if endpoint.startswith('/api/admin/') and endpoint not in ['/api/admin/api-keys']:
            return True
        
        return False
    
    def authenticate_request(self, request) -> Dict[str, Any]:
        """Authenticate API request and return validation result"""
        
        endpoint = request.path
        method = request.method
        
        # Check if authentication is required
        if not self.should_authenticate(endpoint):
            return {
                "authenticated": False,
                "required": False,
                "message": "Authentication not required"
            }
        
        # Extract API key
        api_key = self.extract_api_key(request)
        if not api_key:
            return {
                "authenticated": False,
                "required": True,
                "error": "API key required",
                "code": "MISSING_API_KEY",
                "message": "Provide API key in Authorization header, X-API-Key header, or api_key parameter"
            }
        
        # Validate API key
        validation_result = api_key_manager.validate_api_key(api_key, endpoint, method)
        
        if not validation_result["valid"]:
            return {
                "authenticated": False,
                "required": True,
                "error": validation_result["error"],
                "code": validation_result["code"],
                "message": "API key validation failed"
            }
        
        # Check rate limiting
        key_id = validation_result["key_id"]
        rate_limit = validation_result["rate_limit"]
        
        if self.is_rate_limited(key_id, rate_limit):
            return {
                "authenticated": False,
                "required": True,
                "error": "Rate limit exceeded",
                "code": "RATE_LIMITED",
                "message": f"Rate limit of {rate_limit} requests per hour exceeded"
            }
        
        # Success
        return {
            "authenticated": True,
            "required": True,
            "key_id": key_id,
            "product": validation_result["product"],
            "scopes": validation_result["scopes"],
            "rate_limit": rate_limit
        }

# Create middleware instance
auth_middleware = AuthenticationMiddleware()

def require_api_key(f):
    """Decorator to require API key authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        auth_result = auth_middleware.authenticate_request(request)
        
        if auth_result["required"] and not auth_result["authenticated"]:
            return jsonify({
                "success": False,
                "error": auth_result["error"],
                "code": auth_result["code"],
                "message": auth_result["message"],
                "timestamp": datetime.now().isoformat()
            }), 401 if auth_result["code"] != "RATE_LIMITED" else 429
        
        # Store auth info in g for use in routes
        if auth_result["authenticated"]:
            g.api_key_id = auth_result["key_id"]
            g.product = auth_result["product"]
            g.scopes = auth_result["scopes"]
            g.rate_limit = auth_result["rate_limit"]
        
        return f(*args, **kwargs)
    
    return decorated_function

def init_auth_middleware(app, redis_client=None):
    """Initialize authentication middleware with Flask app"""
    
    # Update middleware with Redis client
    if redis_client:
        auth_middleware.redis_client = redis_client
    
    @app.before_request
    def before_request():
        """Check authentication for all requests"""
        start_time = time.time()
        
        auth_result = auth_middleware.authenticate_request(request)
        
        # Store auth info in g
        g.auth_required = auth_result["required"]
        g.authenticated = auth_result["authenticated"]
        g.auth_start_time = start_time
        
        if auth_result["required"] and not auth_result["authenticated"]:
            response = jsonify({
                "success": False,
                "error": auth_result["error"],
                "code": auth_result["code"],
                "message": auth_result["message"],
                "timestamp": datetime.now().isoformat(),
                "endpoint": request.path,
                "method": request.method
            })
            
            status_code = 401
            if auth_result["code"] == "RATE_LIMITED":
                status_code = 429
                response.headers['Retry-After'] = '3600'  # 1 hour
            
            return response, status_code
        
        # Store auth info for authenticated requests
        if auth_result["authenticated"]:
            g.api_key_id = auth_result["key_id"]
            g.product = auth_result["product"]
            g.scopes = auth_result["scopes"]
            g.rate_limit = auth_result["rate_limit"]
    
    @app.after_request
    def after_request(response):
        """Track API usage after request"""
        
        # Track usage for authenticated requests
        if hasattr(g, 'api_key_id') and g.api_key_id:
            response_time_ms = int((time.time() - g.auth_start_time) * 1000)
            
            try:
                api_key_manager.track_usage(
                    key_id=g.api_key_id,
                    endpoint=request.path,
                    method=request.method,
                    response_code=response.status_code,
                    response_time_ms=response_time_ms,
                    ip_address=request.remote_addr
                )
            except Exception as e:
                print(f"Usage tracking error: {e}")
        
        # Add API key info to response headers for debugging
        if hasattr(g, 'product') and g.product:
            response.headers['X-API-Product'] = g.product
            response.headers['X-Rate-Limit'] = str(g.rate_limit)
        
        return response

def get_product_access_info() -> Dict[str, Any]:
    """Get product access information for current request"""
    return {
        "authenticated": getattr(g, 'authenticated', False),
        "required": getattr(g, 'auth_required', False),
        "product": getattr(g, 'product', None),
        "scopes": getattr(g, 'scopes', []),
        "key_id": getattr(g, 'api_key_id', None),
        "rate_limit": getattr(g, 'rate_limit', None)
    }