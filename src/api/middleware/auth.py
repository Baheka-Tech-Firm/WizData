from fastapi import Request, Response, HTTPException, Depends, Security
from fastapi.security import APIKeyHeader, APIKeyQuery
from typing import Optional, Callable, Dict, Any, Tuple, List
import time
import hashlib
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from src.utils.logger import get_api_logger

# Define API key security schemes
API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)
API_KEY_QUERY = APIKeyQuery(name="api_key", auto_error=False)

class AuthMiddleware:
    """
    Authentication middleware for API endpoints
    """
    
    def __init__(self):
        self.logger = get_api_logger()
        
        # Mock API key store - in production, this would be in a database
        # Format: {api_key: {"user_id": str, "tier": str, "scopes": List[str]}}
        self.api_keys = {
            "test_api_key": {
                "user_id": "test_user",
                "tier": "basic",
                "scopes": ["read:prices", "read:fundamentals"]
            },
            "premium_api_key": {
                "user_id": "premium_user",
                "tier": "premium",
                "scopes": ["read:prices", "read:fundamentals", "read:news", "write:data"]
            }
        }
    
    def verify_api_key(self, api_key: Optional[str]) -> Optional[Dict[str, Any]]:
        """
        Verify API key and return user information
        
        Args:
            api_key (Optional[str]): API key to verify
            
        Returns:
            Optional[Dict[str, Any]]: User info if valid, None otherwise
        """
        if not api_key:
            return None
        
        # Check if API key exists in our store
        user_info = self.api_keys.get(api_key)
        
        if user_info:
            self.logger.debug(f"Authenticated user {user_info['user_id']} with tier {user_info['tier']}")
            return user_info
        
        self.logger.warning(f"Invalid API key attempted: {api_key[:6]}...")
        return None
    
    def check_scope(self, user_info: Dict[str, Any], required_scope: str) -> bool:
        """
        Check if user has the required scope
        
        Args:
            user_info (Dict[str, Any]): User information
            required_scope (str): Required permission scope
            
        Returns:
            bool: True if user has the required scope
        """
        if not user_info:
            return False
        
        return required_scope in user_info.get("scopes", [])

async def get_api_key(
    api_key_header: str = Security(API_KEY_HEADER),
    api_key_query: str = Security(API_KEY_QUERY)
) -> Optional[str]:
    """
    Get API key from header or query parameter
    
    Args:
        api_key_header (str): API key from header
        api_key_query (str): API key from query parameter
        
    Returns:
        Optional[str]: API key if found, None otherwise
    """
    return api_key_header or api_key_query

async def get_current_user(
    api_key: Optional[str] = Depends(get_api_key)
) -> Optional[Dict[str, Any]]:
    """
    Get current user based on API key
    
    Args:
        api_key (Optional[str]): API key
        
    Returns:
        Optional[Dict[str, Any]]: User info if authenticated
    """
    auth = AuthMiddleware()
    user_info = auth.verify_api_key(api_key)
    return user_info

def require_auth(required_scope: Optional[str] = None):
    """
    Dependency for requiring authentication with optional scope
    
    Args:
        required_scope (Optional[str]): Required permission scope
        
    Returns:
        User info if authenticated and authorized
    """
    
    async def auth_dependency(
        user_info: Optional[Dict[str, Any]] = Depends(get_current_user)
    ) -> Dict[str, Any]:
        auth = AuthMiddleware()
        
        if not user_info:
            raise HTTPException(
                status_code=401,
                detail="Not authenticated",
                headers={"WWW-Authenticate": "ApiKey"}
            )
        
        if required_scope and not auth.check_scope(user_info, required_scope):
            raise HTTPException(
                status_code=403,
                detail=f"Not authorized for scope: {required_scope}"
            )
        
        return user_info
    
    return auth_dependency
