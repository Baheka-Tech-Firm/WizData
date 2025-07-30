"""
Simple monitoring middleware for WizData API
"""

import time
import logging
from flask import request, g

logger = logging.getLogger(__name__)

class MonitoringMiddleware:
    def __init__(self, app=None):
        self.app = app
        if app is not None:
            self.init_app(app)
    
    def init_app(self, app):
        @app.before_request
        def before_request():
            g.start_time = time.time()
            
        @app.after_request
        def after_request(response):
            if hasattr(g, 'start_time'):
                duration = time.time() - g.start_time
                logger.info(f"{request.method} {request.path} - {response.status_code} - {duration:.3f}s")
            return response

def monitor_function(func):
    """Decorator for monitoring function execution"""
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info(f"{func.__name__} completed in {duration:.3f}s")
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"{func.__name__} failed after {duration:.3f}s: {str(e)}")
            raise
    return wrapper
