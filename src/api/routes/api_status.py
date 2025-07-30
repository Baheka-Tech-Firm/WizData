"""
API status and monitoring routes
"""

from flask import Blueprint, jsonify, current_app
from middleware.rate_limiter import rate_limit
from middleware.cache_manager import cached
from middleware.monitoring import monitor_function
from config import config
import time

api_status_bp = Blueprint('api_status', __name__, url_prefix='/api/status')

@api_status_bp.route('/cache', methods=['GET'])
@rate_limit(requests_per_minute=30)
@monitor_function("cache_status")
def cache_status():
    """Get cache statistics and status"""
    cache_manager = getattr(current_app, 'cache_manager', None)
    
    if not cache_manager:
        return jsonify({
            'status': 'disabled',
            'message': 'Cache manager not available'
        }), 503
    
    try:
        stats = cache_manager.get_cache_stats()
        return jsonify({
            'status': 'healthy',
            'timestamp': time.time(),
            'stats': stats
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_status_bp.route('/rate-limit', methods=['GET'])
@rate_limit(requests_per_minute=30)
@monitor_function("rate_limit_status")
def rate_limit_status():
    """Get current rate limit status"""
    rate_limiter = getattr(current_app, 'rate_limiter', None)
    
    if not rate_limiter:
        return jsonify({
            'status': 'disabled',
            'message': 'Rate limiter not available'
        }), 503
    
    try:
        status = rate_limiter.get_rate_limit_status()
        return jsonify({
            'status': 'healthy',
            'timestamp': time.time(),
            'rate_limit': status
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500

@api_status_bp.route('/config', methods=['GET'])
@rate_limit(requests_per_minute=10)
@cached(ttl=60, data_type='static_data')
@monitor_function("config_status")
def config_status():
    """Get configuration status (non-sensitive information only)"""
    api_key_status = config.get_api_key_status()
    
    return jsonify({
        'status': 'healthy',
        'timestamp': time.time(),
        'environment': config.environment,
        'debug': config.debug,
        'api_integrations': {
            'total_configured': sum(1 for status in api_key_status.values() if status),
            'total_available': len(api_key_status),
            'details': api_key_status
        },
        'features': {
            'caching': bool(getattr(current_app, 'cache_manager', None)),
            'rate_limiting': bool(getattr(current_app, 'rate_limiter', None)),
            'monitoring': True,
            'database': True
        },
        'cache_config': {
            'default_ttl': config.cache.default_ttl,
            'market_data_ttl': config.cache.market_data_ttl,
            'esg_data_ttl': config.cache.esg_data_ttl,
            'static_data_ttl': config.cache.static_data_ttl
        },
        'rate_limits': {
            'default_rpm': config.rate_limit.default_requests_per_minute,
            'authenticated_rpm': config.rate_limit.authenticated_requests_per_minute,
            'burst_size': config.rate_limit.burst_requests
        }
    })

@api_status_bp.route('/services', methods=['GET'])
@rate_limit(requests_per_minute=20)
@cached(ttl=30, data_type='api_responses')
@monitor_function("services_status")
def services_status():
    """Get status of all external services and integrations"""
    services = {}
    
    # Check database
    try:
        from app import db
        db.session.execute('SELECT 1')
        services['database'] = {
            'status': 'healthy',
            'type': 'postgresql'
        }
    except Exception as e:
        services['database'] = {
            'status': 'unhealthy',
            'error': str(e),
            'type': 'postgresql'
        }
    
    # Check Redis
    cache_manager = getattr(current_app, 'cache_manager', None)
    if cache_manager:
        try:
            cache_manager.redis.ping()
            services['redis'] = {
                'status': 'healthy',
                'type': 'redis'
            }
        except Exception as e:
            services['redis'] = {
                'status': 'unhealthy',
                'error': str(e),
                'type': 'redis'
            }
    else:
        services['redis'] = {
            'status': 'not_configured',
            'type': 'redis'
        }
    
    # Check external API configurations
    api_status = config.get_api_key_status()
    for api_name, is_configured in api_status.items():
        services[f'{api_name}_api'] = {
            'status': 'configured' if is_configured else 'not_configured',
            'type': 'external_api'
        }
    
    # Overall status
    unhealthy_services = [name for name, info in services.items() 
                         if info['status'] == 'unhealthy']
    
    overall_status = 'healthy'
    if unhealthy_services:
        overall_status = 'degraded' if len(unhealthy_services) < len(services) / 2 else 'unhealthy'
    
    return jsonify({
        'overall_status': overall_status,
        'timestamp': time.time(),
        'services': services,
        'unhealthy_services': unhealthy_services
    })

@api_status_bp.route('/cache/clear', methods=['POST'])
@rate_limit(requests_per_minute=5)
@monitor_function("cache_clear")
def clear_cache():
    """Clear all cache entries"""
    cache_manager = getattr(current_app, 'cache_manager', None)
    
    if not cache_manager:
        return jsonify({
            'success': False,
            'message': 'Cache manager not available'
        }), 503
    
    try:
        # Clear all cache keys
        deleted_count = cache_manager.delete_pattern("cache:*")
        
        return jsonify({
            'success': True,
            'message': f'Cache cleared successfully',
            'deleted_keys': deleted_count,
            'timestamp': time.time()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500

@api_status_bp.route('/cache/clear/<data_type>', methods=['POST'])
@rate_limit(requests_per_minute=10)
@monitor_function("cache_clear_by_type")
def clear_cache_by_type(data_type):
    """Clear cache entries of a specific type"""
    cache_manager = getattr(current_app, 'cache_manager', None)
    
    if not cache_manager:
        return jsonify({
            'success': False,
            'message': 'Cache manager not available'
        }), 503
    
    try:
        deleted_count = cache_manager.invalidate_by_data_type(data_type)
        
        return jsonify({
            'success': True,
            'message': f'Cache cleared for data type: {data_type}',
            'deleted_keys': deleted_count,
            'data_type': data_type,
            'timestamp': time.time()
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        }), 500