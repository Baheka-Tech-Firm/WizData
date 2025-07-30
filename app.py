import os
import logging
from datetime import datetime
from flask import Flask, g, jsonify
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_socketio import SocketIO
from redis import Redis
from redis.exceptions import RedisError

# Import configuration and middleware
from config import config
from middleware.monitoring_simple import MonitoringMiddleware
from middleware.cache_manager import CacheManager
from middleware.rate_limiter import RateLimiter

# Import enhanced API features
try:
    from api.market_data_enhanced import register_market_data_api
    from middleware.auth_middleware import init_auth, register_auth_api
    from services.job_scheduler import init_job_scheduler
    from jobs.etl_jobs import initialize_etl_jobs
    ENHANCED_FEATURES_AVAILABLE = True
except ImportError as e:
    logging.warning("Enhanced features not available: {}".format(e))
    ENHANCED_FEATURES_AVAILABLE = False

# Configure logging
logging.basicConfig(level=getattr(logging, config.monitoring.log_level))
logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

def create_app():
    """Create and configure the Flask application"""
    # Create the Flask application
    app = Flask(__name__)
    app.secret_key = config.secret_key
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)  # needed for url_for to generate with https

    # Configure database from config
    app.config["SQLALCHEMY_DATABASE_URI"] = config.database.url
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_size": config.database.pool_size,
        "pool_timeout": config.database.pool_timeout,
        "pool_recycle": config.database.pool_recycle,
        "pool_pre_ping": config.database.pool_pre_ping,
        "echo": config.database.echo
    }

    # Configure data export directory
    app.config["DATA_EXPORT_DIR"] = config.data_export_dir
    os.makedirs(app.config["DATA_EXPORT_DIR"], exist_ok=True)

    # Initialize database with app
    db.init_app(app)
    
    # Initialize Redis for caching and rate limiting
    redis_client = None
    try:
        redis_client = Redis.from_url(
            config.redis.url,
            decode_responses=config.redis.decode_responses,
            socket_connect_timeout=config.redis.socket_connect_timeout,
            socket_timeout=config.redis.socket_timeout,
            retry_on_timeout=config.redis.retry_on_timeout,
            health_check_interval=config.redis.health_check_interval
        )
        redis_client.ping()  # Test connection
        
        # Initialize cache manager and rate limiter
        app.cache_manager = CacheManager(redis_client)
        app.rate_limiter = RateLimiter(
            redis_client, 
            config.rate_limit.default_requests_per_minute,
            config.rate_limit.burst_requests
        )
        
        logger.info("Redis connection established successfully")
        
    except (RedisError, Exception) as e:
        logger.warning("Redis connection failed: {}. Caching and rate limiting will be disabled.".format(e))
        app.cache_manager = None
        app.rate_limiter = None
    
    # Initialize monitoring middleware
    monitoring = MonitoringMiddleware(app)
    
    # Initialize authentication middleware
    try:
        # init_auth_middleware(app, redis_client if 'redis_client' in locals() else None)
        logger.info("Authentication middleware skipped for now")
    except Exception as e:
        logger.warning(f"Could not initialize auth middleware: {e}")
    
    # Add rate limiter to request context
    @app.before_request
    def add_rate_limiter_to_context():
        g.rate_limiter = app.rate_limiter

    # Initialize Socket.IO
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

    with app.app_context():
        # Make sure to import the models here or their tables won't be created
        import models  # noqa: F401
        
        try:
            db.create_all()
            logger.info("Database setup complete")
        except Exception as e:
            logger.error(f"Error setting up database: {str(e)}")

    # Register blueprints
    try:
        from src.api.routes.prices import prices_bp
        from src.api.routes.quality import quality_bp
        from src.api.routes.esg import esg_bp, africa_esg_bp
        from src.api.routes.insights import insights_bp
        from src.api.routes.realtime import realtime_bp
        from src.api.routes.integrated_data_routes import integrated_data_bp
        from src.api.routes.api_status import api_status_bp
        from scrapers.api import scrapers_bp
        
        app.register_blueprint(prices_bp)
        app.register_blueprint(quality_bp)
        app.register_blueprint(esg_bp)
        app.register_blueprint(africa_esg_bp)
        app.register_blueprint(insights_bp)
        app.register_blueprint(realtime_bp)
        app.register_blueprint(integrated_data_bp)
        app.register_blueprint(api_status_bp)
        app.register_blueprint(scrapers_bp)
        
        # Register charting API
        try:
            from api.charting import charting_api
            app.register_blueprint(charting_api)
        except ImportError:
            pass
        
        # Register modular data services
        try:
            from api.data_services import data_services_api
            app.register_blueprint(data_services_api)
        except ImportError:
            pass
        
        # Register authentication admin API
        try:
            from api.auth_admin import auth_admin_api
            app.register_blueprint(auth_admin_api)
        except ImportError:
            pass
        
        # Register dataset registry and licensing APIs
        try:
            from api.dataset_api import dataset_api
            from api.data_access_api import data_access_api
            app.register_blueprint(dataset_api)
            app.register_blueprint(data_access_api)
        except ImportError as e:
            logger.warning(f"Could not register dataset registry APIs: {e}")
        
        logger.info("All blueprints registered successfully")
        
    except ImportError as e:
        logger.warning(f"Some blueprints could not be imported: {e}")
        # Register only the essential ones
        from src.api.routes.api_status import api_status_bp
        app.register_blueprint(api_status_bp)

    # Initialize Socket.IO handlers
    try:
        from src.api.routes.realtime.socket_routes import init_socket_handlers
        init_socket_handlers(socketio, db_url=app.config["SQLALCHEMY_DATABASE_URI"])
    except ImportError as e:
        logger.warning(f"Could not initialize socket handlers: {e}")

    # Register main routes
    from routes import register_routes
    register_routes(app)
    
    # Add enhanced health check
    @app.route('/api/v2/health')
    def enhanced_health_check():
        """Enhanced health check with service status"""
        status = {
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'services': {
                'database': 'healthy',
                'redis': 'healthy' if redis_client else 'unavailable',
                'authentication': 'healthy' if ENHANCED_FEATURES_AVAILABLE else 'unavailable',
                'job_scheduler': 'healthy' if hasattr(app, 'job_scheduler') and app.job_scheduler else 'unavailable',
                'rate_limiter': 'healthy' if app.rate_limiter else 'unavailable'
            },
            'features': {
                'enhanced_api': ENHANCED_FEATURES_AVAILABLE,
                'background_jobs': hasattr(app, 'job_scheduler') and app.job_scheduler is not None,
                'caching': app.cache_manager is not None,
                'rate_limiting': app.rate_limiter is not None
            }
        }
        
        # Check if any critical services are down
        critical_services = ['database']
        if any(status['services'][service] != 'healthy' for service in critical_services):
            status['status'] = 'degraded'
        
        return jsonify(status)
    
    # Enhanced error handlers
    @app.errorhandler(429)
    def rate_limit_handler(e):
        return jsonify({
            'error': 'Rate limit exceeded',
            'message': 'Too many requests. Please try again later.',
            'status': 'error'
        }), 429
    
    @app.errorhandler(401)
    def auth_error_handler(e):
        return jsonify({
            'error': 'Authentication required',
            'message': 'Valid API key or JWT token required',
            'status': 'error'
        }), 401
    
    @app.errorhandler(403)
    def permission_error_handler(e):
        return jsonify({
            'error': 'Insufficient permissions',
            'message': 'You do not have permission to access this resource',
            'status': 'error'
        }), 403

    # Initialize enhanced API features if available
    if ENHANCED_FEATURES_AVAILABLE and redis_client:
        try:
            # Initialize enhanced authentication
            init_auth(app, redis_client)
            logger.info("Enhanced authentication initialized")
            
            # Initialize job scheduler
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
            
            # Register enhanced API blueprints
            register_market_data_api(app)
            register_auth_api(app)
            
            # Register API documentation
            from api.api_documentation import register_api_docs
            register_api_docs(app)
            
            logger.info("Enhanced API endpoints registered")
            
        except Exception as e:
            logger.error(f"Failed to initialize enhanced features: {e}")
            app.job_scheduler = None

    return app, socketio

# Create the app and socketio instances
app, socketio = create_app()