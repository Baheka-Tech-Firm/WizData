"""
Simple monitoring middleware for WizData API
"""

import time
import logging
import functools
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
                logger.info("{} {} - {} - {:.3f}s".format(request.method, request.path, response.status_code, duration))
            return response

def monitor_function(func):
    """Decorator for monitoring function execution"""
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            duration = time.time() - start_time
            logger.info("{} completed in {:.3f}s".format(func.__name__, duration))
            return result
        except Exception as e:
            duration = time.time() - start_time
            logger.error("{} failed after {:.3f}s: {}".format(func.__name__, duration, str(e)))
            raise
    return wrapper
