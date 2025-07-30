"""
Enhanced Flask Application Integration
Integrates all enhanced API features with the main Flask app
"""

import logging
from flask import Flask
import redis
from redis.exceptions import RedisError
import os

# Import enhanced modules
from api.market_data_enhanced import register_market_data_api
from middleware.auth_middleware import init_auth, register_auth_api
from middleware.rate_limiter import RateLimiter
from services.job_scheduler import init_job_scheduler
from jobs.etl_jobs import initialize_etl_jobs

logger = logging.getLogger(__name__)

def create_enhanced_app():
    """Create Flask app with all enhanced features"""
    app = Flask(__name__)
    
    # Configuration
    app.config['SECRET_KEY'] = os.getenv('SECRET_KEY', 'your-secret-key-change-in-production')
    app.config['REDIS_URL'] = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    
    # Initialize Redis connection
    try:
        redis_client = redis.from_url(app.config['REDIS_URL'])
        redis_client.ping()  # Test connection
        logger.info("Connected to Redis successfully")
    except RedisError as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        redis_client = None
    
    # Initialize enhanced authentication
    if redis_client:
        init_auth(app, redis_client)
        logger.info("Authentication middleware initialized")
    
    # Initialize rate limiter
    if redis_client:
        rate_limiter = RateLimiter(redis_client)
        app.rate_limiter = rate_limiter
        logger.info("Rate limiter initialized")
    
    # Initialize job scheduler
    if redis_client:
        job_scheduler = init_job_scheduler(redis_client)
        app.job_scheduler = job_scheduler
        logger.info("Job scheduler initialized")
        
        # Start the scheduler
        job_scheduler.start_scheduler()
        logger.info("Job scheduler started")
        
        # Initialize ETL jobs
        if initialize_etl_jobs(redis_client):
            logger.info("ETL jobs initialized successfully")
        else:
            logger.warning("Some ETL jobs failed to initialize")
    
    # Register API blueprints
    register_market_data_api(app)
    register_auth_api(app)
    
    # Add health check endpoint
    @app.route('/api/v2/health')
    def enhanced_health_check():
        """Enhanced health check with service status"""
        status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'redis': 'healthy' if redis_client else 'unavailable',
                'authentication': 'healthy' if hasattr(app, 'rate_limiter') else 'unavailable',
                'job_scheduler': 'healthy' if hasattr(app, 'job_scheduler') else 'unavailable',
                'rate_limiter': 'healthy' if hasattr(app, 'rate_limiter') else 'unavailable'
            }
        }
        
        # Check if any critical services are down
        critical_services = ['redis', 'authentication']
        if any(status['services'][service] != 'healthy' for service in critical_services):
            status['status'] = 'degraded'
        
        return status
    
    # Error handlers
    @app.errorhandler(429)
    def rate_limit_handler(e):
        return {
            'error': 'Rate limit exceeded',
            'message': 'Too many requests. Please try again later.',
            'status': 'error'
        }, 429
    
    @app.errorhandler(401)
    def auth_error_handler(e):
        return {
            'error': 'Authentication required',
            'message': 'Valid API key or JWT token required',
            'status': 'error'
        }, 401
    
    @app.errorhandler(403)
    def permission_error_handler(e):
        return {
            'error': 'Insufficient permissions',
            'message': 'You do not have permission to access this resource',
            'status': 'error'
        }, 403
    
    # Cleanup on app shutdown
    @app.teardown_appcontext
    def cleanup(error):
        if hasattr(app, 'job_scheduler') and app.job_scheduler:
            app.job_scheduler.stop_scheduler()
    
    logger.info("Enhanced Flask app created successfully")
    return app

if __name__ == '__main__':
    from datetime import datetime
    app = create_enhanced_app()
    app.run(debug=True, host='0.0.0.0', port=5000)
