"""
Authentication and API Key handling for WizData
"""

import logging
import secrets
import functools
from flask import request, jsonify, current_app, g
from datetime import datetime
from models import APIKey, APIRequest, db

logger = logging.getLogger(__name__)

def generate_api_key(name, user_id, permissions=None):
    """
    Generate a new API key for a user
    
    Args:
        name: Name of the API key
        user_id: User ID
        permissions: Optional JSON string of permissions
        
    Returns:
        The generated API key
    """
    # Generate a secure random key
    key = secrets.token_hex(32)
    
    # Create the API key record
    api_key = APIKey(
        key=key,
        name=name,
        user_id=user_id,
        permissions=permissions
    )
    
    # Save to database
    db.session.add(api_key)
    db.session.commit()
    
    return key

def get_api_key():
    """
    Get the API key from the request
    
    Returns:
        APIKey object or None
    """
    # Check header first
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        key = auth_header[7:]  # Remove 'Bearer ' prefix
    else:
        # Check query parameter as fallback
        key = request.args.get('api_key')
        
    if not key:
        return None
        
    # Find the API key in the database
    return APIKey.query.filter_by(key=key, is_active=True).first()

def require_api_key(func):
    """
    Decorator to require a valid API key for a route
    
    Args:
        func: The route function to decorate
        
    Returns:
        Decorated function that checks for a valid API key
    """
    @functools.wraps(func)
    def decorated_function(*args, **kwargs):
        # Get the API key
        api_key = get_api_key()
        
        # If no API key found, return error
        if not api_key:
            return jsonify({
                "status": "error",
                "message": "Missing or invalid API key"
            }), 401
            
        # Update last used timestamp
        api_key.last_used = datetime.utcnow()
        
        # Log the API request
        try:
            api_request = APIRequest(
                endpoint=request.path,
                method=request.method,
                user_id=api_key.user_id,
                api_key_id=api_key.id,
                ip_address=request.remote_addr,
                status_code=200,  # Will be updated if needed
                parameters=str(request.args)
            )
            db.session.add(api_request)
            
            # Store in g for potential later use
            g.api_key = api_key
            g.api_request = api_request
            
            # Call the original function
            response = func(*args, **kwargs)
            
            # Update status code if needed
            if isinstance(response, tuple) and len(response) > 1:
                api_request.status_code = response[1]
            
            # Commit the changes
            db.session.commit()
            
            return response
            
        except Exception as e:
            logger.error(f"Error in require_api_key: {str(e)}")
            db.session.rollback()
            # Still call the original function in case of logging error
            return func(*args, **kwargs)
            
    return decorated_function