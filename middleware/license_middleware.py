"""
License Checking Middleware for Dataset Access Control
Validates dataset access permissions and enforces rate limits
"""

from functools import wraps
from flask import request, jsonify, current_app, g
from datetime import datetime
import logging

from models import User, Dataset, Subscription, APIKey
from services.licensing_service import LicensingService
from services.usage_tracker import UsageTracker

logger = logging.getLogger(__name__)

# Initialize services
licensing_service = LicensingService()
usage_tracker = UsageTracker()

def dataset_access_required(dataset_slug_param='dataset_slug'):
    """
    Decorator to enforce dataset access control
    
    Args:
        dataset_slug_param: Name of the parameter containing the dataset slug
        
    Usage:
        @dataset_access_required()
        def get_market_data(dataset_slug):
            # Access is validated, proceed with data retrieval
            pass
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            try:
                # Get dataset slug from URL parameters or kwargs
                dataset_slug = kwargs.get(dataset_slug_param) or request.view_args.get(dataset_slug_param)
                
                if not dataset_slug:
                    return jsonify({
                        'error': 'Dataset slug is required',
                        'error_code': 'MISSING_DATASET_SLUG'
                    }), 400
                
                # Get dataset
                dataset = Dataset.query.filter(
                    Dataset.slug == dataset_slug,
                    Dataset.is_active == True
                ).first()
                
                if not dataset:
                    return jsonify({
                        'error': 'Dataset not found or inactive',
                        'error_code': 'DATASET_NOT_FOUND'
                    }), 404
                
                # Get current user (should be set by auth middleware)
                current_user = getattr(g, 'current_user', None)
                
                if not current_user:
                    return jsonify({
                        'error': 'Authentication required',
                        'error_code': 'AUTHENTICATION_REQUIRED'
                    }), 401
                
                # Get requested record count from query parameters
                requested_records = request.args.get('limit', 100, type=int)
                requested_records = min(requested_records, 10000)  # Hard limit
                
                # Validate access using licensing service
                access_result = licensing_service.validate_access(
                    user_id=current_user.id,
                    dataset_id=dataset.id,
                    requested_records=requested_records
                )
                
                if not access_result.get('allowed', False):
                    error_code = access_result.get('error_code', 'ACCESS_DENIED')
                    reason = access_result.get('reason', 'Access denied')
                    
                    response_data = {
                        'error': reason,
                        'error_code': error_code
                    }
                    
                    # Add reset time for rate limit errors
                    if 'reset_time' in access_result:
                        response_data['reset_time'] = access_result['reset_time']
                    
                    # Return appropriate status code based on error type
                    status_code = 429 if 'LIMIT_EXCEEDED' in error_code else 403
                    
                    return jsonify(response_data), status_code
                
                # Store access information for the route handler
                g.dataset = dataset
                g.subscription = access_result.get('subscription_id')
                g.license_info = access_result.get('license', {})
                g.request_cost = access_result.get('cost', 0)
                g.remaining_calls = access_result.get('remaining_calls', {})
                g.requested_records = requested_records
                
                # Call the original function
                result = f(*args, **kwargs)
                
                # Record usage after successful request
                _record_request_usage(current_user, dataset, result)
                
                return result
                
            except Exception as e:
                current_app.logger.error(f"Error in dataset access middleware: {str(e)}")
                return jsonify({
                    'error': 'Internal server error',
                    'error_code': 'INTERNAL_ERROR'
                }), 500
        
        return decorated_function
    return decorator

def license_feature_required(feature_name):
    """
    Decorator to check if the user's license supports a specific feature
    
    Args:
        feature_name: Name of the feature to check (e.g., 'real_time_access', 'bulk_download')
        
    Usage:
        @license_feature_required('real_time_access')
        def get_real_time_data():
            pass
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            license_info = getattr(g, 'license_info', {})
            
            if not license_info.get(feature_name, False):
                return jsonify({
                    'error': f'Current license does not support {feature_name}',
                    'error_code': 'FEATURE_NOT_AVAILABLE',
                    'required_feature': feature_name
                }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def subscription_tier_required(min_tier):
    """
    Decorator to check if the user has a minimum subscription tier
    
    Args:
        min_tier: Minimum required subscription tier (SubscriptionTier enum)
        
    Usage:
        @subscription_tier_required(SubscriptionTier.PROFESSIONAL)
        def get_premium_data():
            pass
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            current_user = getattr(g, 'current_user', None)
            
            if not current_user:
                return jsonify({
                    'error': 'Authentication required',
                    'error_code': 'AUTHENTICATION_REQUIRED'
                }), 401
            
            # Define tier hierarchy
            tier_hierarchy = {
                'free': 0,
                'starter': 1,
                'professional': 2,
                'enterprise': 3,
                'custom': 4
            }
            
            user_tier_level = tier_hierarchy.get(current_user.subscription_tier.value, 0)
            required_tier_level = tier_hierarchy.get(min_tier.value, 0)
            
            if user_tier_level < required_tier_level:
                return jsonify({
                    'error': f'Minimum subscription tier required: {min_tier.value}',
                    'error_code': 'INSUFFICIENT_SUBSCRIPTION_TIER',
                    'current_tier': current_user.subscription_tier.value,
                    'required_tier': min_tier.value
                }), 403
            
            return f(*args, **kwargs)
        
        return decorated_function
    return decorator

def _record_request_usage(user, dataset, response_result):
    """
    Record usage after a successful API request
    
    Args:
        user: User object
        dataset: Dataset object
        response_result: Response from the route handler
    """
    try:
        # Extract response information
        response_data = None
        status_code = 200
        records_returned = 0
        
        if hasattr(response_result, 'get_json'):
            # Flask Response object
            response_data = response_result.get_json()
            status_code = response_result.status_code
        elif isinstance(response_result, tuple):
            # Tuple response (data, status_code)
            if len(response_result) >= 2:
                response_data = response_result[0]
                status_code = response_result[1]
            else:
                response_data = response_result[0]
        else:
            # Direct data response
            response_data = response_result
        
        # Try to extract record count from response
        if isinstance(response_data, dict):
            # Look for common data count fields
            if 'data' in response_data and isinstance(response_data['data'], list):
                records_returned = len(response_data['data'])
            elif 'results' in response_data and isinstance(response_data['results'], list):
                records_returned = len(response_data['results'])
            elif 'records' in response_data and isinstance(response_data['records'], list):
                records_returned = len(response_data['records'])
            elif 'count' in response_data:
                records_returned = response_data['count']
            elif isinstance(response_data, list):
                records_returned = len(response_data)
        
        # Get additional request information
        subscription_id = getattr(g, 'subscription', None)
        api_key_id = getattr(g, 'api_key_id', None)
        
        # Calculate response size (rough estimate)
        response_size_bytes = len(str(response_data).encode('utf-8')) if response_data else 0
        
        # Record the usage
        usage_tracker.record_usage(
            user_id=user.id,
            dataset_id=dataset.id,
            endpoint=request.endpoint or request.path,
            method=request.method,
            records_returned=records_returned,
            response_size_bytes=response_size_bytes,
            response_time_ms=None,  # This would need to be calculated by wrapping the entire request
            status_code=status_code,
            subscription_id=subscription_id,
            api_key_id=api_key_id,
            ip_address=request.remote_addr,
            user_agent=request.headers.get('User-Agent'),
            query_parameters=dict(request.args)
        )
        
    except Exception as e:
        logger.error(f"Error recording usage: {str(e)}")
        # Don't fail the request if usage recording fails

def add_usage_headers(response):
    """
    Add usage information to response headers
    Can be used as an after_request handler
    """
    try:
        if hasattr(g, 'remaining_calls'):
            remaining_calls = g.remaining_calls
            
            # Add rate limit headers
            response.headers['X-RateLimit-Limit-Minute'] = str(remaining_calls.get('per_minute', {}).get('limit', 0))
            response.headers['X-RateLimit-Remaining-Minute'] = str(remaining_calls.get('per_minute', 0))
            response.headers['X-RateLimit-Limit-Daily'] = str(remaining_calls.get('daily', 0))
            response.headers['X-RateLimit-Limit-Monthly'] = str(remaining_calls.get('monthly', 0))
        
        if hasattr(g, 'request_cost'):
            response.headers['X-Request-Cost'] = str(g.request_cost)
        
        return response
        
    except Exception as e:
        logger.error(f"Error adding usage headers: {str(e)}")
        return response

def check_dataset_access(user_id: int, dataset_slug: str) -> dict:
    """
    Utility function to check dataset access without decorators
    
    Args:
        user_id: ID of the user
        dataset_slug: Slug of the dataset
        
    Returns:
        Dictionary with access check results
    """
    try:
        dataset = Dataset.query.filter(
            Dataset.slug == dataset_slug,
            Dataset.is_active == True
        ).first()
        
        if not dataset:
            return {
                'allowed': False,
                'reason': 'Dataset not found or inactive',
                'error_code': 'DATASET_NOT_FOUND'
            }
        
        return licensing_service.validate_access(
            user_id=user_id,
            dataset_id=dataset.id,
            requested_records=1
        )
        
    except Exception as e:
        logger.error(f"Error checking dataset access: {str(e)}")
        return {
            'allowed': False,
            'reason': 'Internal error',
            'error_code': 'INTERNAL_ERROR'
        }
