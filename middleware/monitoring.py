"""
Monitoring and observability middleware for WizData API
Implements metrics collection, health checks, and structured logging
"""

import time
import logging
import json
import traceback
from typing import Dict, Any, Optional
from functools import wraps
from flask import request, g, current_app, jsonify
from prometheus_client import Counter, Histogram, Gauge, generate_latest, CONTENT_TYPE_LATEST
import structlog

# Prometheus metrics
REQUEST_COUNT = Counter(
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

REQUEST_DURATION = Histogram(
    'http_request_duration_seconds',
    'HTTP request duration in seconds',
    ['method', 'endpoint']
)

ACTIVE_REQUESTS = Gauge(
    'http_requests_active',
    'Number of active HTTP requests'
)

API_KEY_USAGE = Counter(
    'api_key_requests_total',
    'Total requests by API key',
    ['api_key_hash', 'endpoint']
)

CACHE_OPERATIONS = Counter(
    'cache_operations_total',
    'Total cache operations',
    ['operation', 'result']
)

RATE_LIMIT_HITS = Counter(
    'rate_limit_hits_total',
    'Total rate limit hits',
    ['client_type']
)

DATABASE_OPERATIONS = Counter(
    'database_operations_total',
    'Total database operations',
    ['operation', 'table', 'status']
)

EXTERNAL_API_CALLS = Counter(
    'external_api_calls_total',
    'Total external API calls',
    ['provider', 'endpoint', 'status']
)

ERROR_COUNT = Counter(
    'application_errors_total',
    'Total application errors',
    ['error_type', 'endpoint']
)

# Configure structured logging
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

class MonitoringMiddleware:
    """
    Middleware for comprehensive application monitoring
    """
    
    def __init__(self, app=None):
        self.app = app
        self.logger = structlog.get_logger(__name__)
        if app:
            self.init_app(app)
    
    def init_app(self, app):
        """Initialize monitoring with Flask app"""
        app.before_request(self.before_request)
        app.after_request(self.after_request)
        app.teardown_appcontext(self.teardown_request)
        
        # Add error handler
        app.errorhandler(Exception)(self.handle_exception)
        
        # Add metrics endpoint
        app.add_url_rule('/metrics', 'metrics', self.metrics_endpoint)
        app.add_url_rule('/health', 'health', self.health_check)
        app.add_url_rule('/health/detailed', 'health_detailed', self.detailed_health_check)
    
    def before_request(self):
        """Called before each request"""
        g.start_time = time.time()
        g.request_id = self._generate_request_id()
        
        ACTIVE_REQUESTS.inc()
        
        # Log request start with structured logging
        self.logger.info(
            "Request started",
            request_id=g.request_id,
            method=request.method,
            path=request.path,
            remote_addr=request.remote_addr,
            user_agent=request.headers.get('User-Agent', ''),
            api_key_present=bool(request.headers.get('X-API-Key'))
        )
    
    def after_request(self, response):
        """Called after each request"""
        if hasattr(g, 'start_time'):
            duration = time.time() - g.start_time
            
            # Update Prometheus metrics
            REQUEST_COUNT.labels(
                method=request.method,
                endpoint=request.endpoint or 'unknown',
                status_code=response.status_code
            ).inc()
            
            REQUEST_DURATION.labels(
                method=request.method,
                endpoint=request.endpoint or 'unknown'
            ).observe(duration)
            
            ACTIVE_REQUESTS.dec()
            
            # Track API key usage
            api_key = request.headers.get('X-API-Key')
            if api_key:
                api_key_hash = self._hash_api_key(api_key)
                API_KEY_USAGE.labels(
                    api_key_hash=api_key_hash,
                    endpoint=request.endpoint or 'unknown'
                ).inc()
            
            # Log request completion
            self.logger.info(
                "Request completed",
                request_id=getattr(g, 'request_id', 'unknown'),
                method=request.method,
                path=request.path,
                status_code=response.status_code,
                duration_seconds=duration,
                response_size_bytes=len(response.get_data()) if response.get_data() else 0
            )
        
        return response
    
    def teardown_request(self, exception):
        """Called when request context is torn down"""
        if exception:
            self.logger.error(
                "Request exception",
                request_id=getattr(g, 'request_id', 'unknown'),
                exception=str(exception),
                exc_info=True
            )
    
    def handle_exception(self, error):
        """Global exception handler"""
        ERROR_COUNT.labels(
            error_type=type(error).__name__,
            endpoint=request.endpoint or 'unknown'
        ).inc()
        
        self.logger.error(
            "Unhandled exception",
            request_id=getattr(g, 'request_id', 'unknown'),
            error_type=type(error).__name__,
            error_message=str(error),
            traceback=traceback.format_exc()
        )
        
        # Return JSON error response
        response = jsonify({
            'error': 'Internal server error',
            'request_id': getattr(g, 'request_id', 'unknown'),
            'message': str(error) if current_app.debug else 'An unexpected error occurred'
        })
        response.status_code = 500
        return response
    
    def metrics_endpoint(self):
        """Prometheus metrics endpoint"""
        return generate_latest(), 200, {'Content-Type': CONTENT_TYPE_LATEST}
    
    def health_check(self):
        """Basic health check endpoint"""
        return jsonify({
            'status': 'healthy',
            'timestamp': time.time(),
            'version': '1.0.0'
        })
    
    def detailed_health_check(self):
        """Detailed health check with dependency status"""
        health_status = {
            'status': 'healthy',
            'timestamp': time.time(),
            'version': '1.0.0',
            'dependencies': {}
        }
        
        # Check database
        try:
            from app import db
            db.session.execute('SELECT 1')
            health_status['dependencies']['database'] = 'healthy'
        except Exception as e:
            health_status['dependencies']['database'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check Redis (if available)
        try:
            cache_manager = getattr(current_app, 'cache_manager', None)
            if cache_manager:
                cache_manager.redis.ping()
                health_status['dependencies']['redis'] = 'healthy'
            else:
                health_status['dependencies']['redis'] = 'not_configured'
        except Exception as e:
            health_status['dependencies']['redis'] = f'unhealthy: {str(e)}'
            health_status['status'] = 'degraded'
        
        # Check external APIs
        from config import config
        api_status = config.get_api_key_status()
        health_status['dependencies']['external_apis'] = {
            'configured_apis': sum(1 for status in api_status.values() if status),
            'total_apis': len(api_status),
            'details': api_status
        }
        
        status_code = 200 if health_status['status'] == 'healthy' else 503
        return jsonify(health_status), status_code
    
    def _generate_request_id(self) -> str:
        """Generate a unique request ID"""
        import uuid
        return str(uuid.uuid4())[:8]
    
    def _hash_api_key(self, api_key: str) -> str:
        """Hash API key for metrics (privacy)"""
        import hashlib
        return hashlib.sha256(api_key.encode()).hexdigest()[:8]
    
    @staticmethod
    def track_cache_operation(operation: str, result: str):
        """Track cache operations"""
        CACHE_OPERATIONS.labels(operation=operation, result=result).inc()
    
    @staticmethod
    def track_rate_limit_hit(client_type: str):
        """Track rate limit hits"""
        RATE_LIMIT_HITS.labels(client_type=client_type).inc()
    
    @staticmethod
    def track_database_operation(operation: str, table: str, status: str):
        """Track database operations"""
        DATABASE_OPERATIONS.labels(operation=operation, table=table, status=status).inc()
    
    @staticmethod
    def track_external_api_call(provider: str, endpoint: str, status: str):
        """Track external API calls"""
        EXTERNAL_API_CALLS.labels(provider=provider, endpoint=endpoint, status=status).inc()

def monitor_function(operation_type: str = "function", include_args: bool = False):
    """
    Decorator to monitor function execution
    
    Args:
        operation_type: Type of operation being monitored
        include_args: Whether to include function arguments in logs
    """
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            logger = structlog.get_logger(__name__)
            start_time = time.time()
            
            log_data = {
                'function': f.__name__,
                'operation_type': operation_type,
                'request_id': getattr(g, 'request_id', 'unknown')
            }
            
            if include_args:
                log_data.update({
                    'args': args,
                    'kwargs': kwargs
                })
            
            logger.info("Function started", **log_data)
            
            try:
                result = f(*args, **kwargs)
                duration = time.time() - start_time
                
                logger.info(
                    "Function completed",
                    function=f.__name__,
                    operation_type=operation_type,
                    duration_seconds=duration,
                    request_id=getattr(g, 'request_id', 'unknown')
                )
                
                return result
                
            except Exception as e:
                duration = time.time() - start_time
                
                logger.error(
                    "Function failed",
                    function=f.__name__,
                    operation_type=operation_type,
                    duration_seconds=duration,
                    error=str(e),
                    request_id=getattr(g, 'request_id', 'unknown'),
                    exc_info=True
                )
                
                raise
        
        return decorated_function
    return decorator