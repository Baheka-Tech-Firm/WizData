import os
import logging
from flask import Flask, g
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_socketio import SocketIO
from redis import Redis
from redis.exceptions import RedisError

# Import configuration and middleware
from config import config
from middleware.monitoring import MonitoringMiddleware
from middleware.cache_manager import CacheManager
from middleware.rate_limiter import RateLimiter

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
        logger.warning(f"Redis connection failed: {e}. Caching and rate limiting will be disabled.")
        app.cache_manager = None
        app.rate_limiter = None
    
    # Initialize monitoring middleware
    monitoring = MonitoringMiddleware(app)
    
    # Add rate limiter to request context
    @app.before_request
    def add_rate_limiter_to_context():
        g.rate_limiter = app.rate_limiter

    # Initialize Socket.IO
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='gevent')

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

    return app, socketio

# Create the app and socketio instances
app, socketio = create_app()