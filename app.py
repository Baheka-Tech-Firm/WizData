import os
import logging
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase
from werkzeug.middleware.proxy_fix import ProxyFix
from flask_socketio import SocketIO

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

def create_app():
    """Create and configure the Flask application"""
    # Create the Flask application
    app = Flask(__name__)
    app.secret_key = os.environ.get("SESSION_SECRET", "dev_secret_key_for_development_only")
    app.wsgi_app = ProxyFix(app.wsgi_app, x_proto=1, x_host=1)  # needed for url_for to generate with https

    # Configure database
    app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
    app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
        "pool_recycle": 300,
        "pool_pre_ping": True,
    }

    # Configure data export directory
    app.config["DATA_EXPORT_DIR"] = os.environ.get("DATA_EXPORT_DIR", "data/exports")
    os.makedirs(app.config["DATA_EXPORT_DIR"], exist_ok=True)

    # Initialize database with app
    db.init_app(app)

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
    from src.api.routes.prices import prices_bp
    from src.api.routes.quality import quality_bp
    from src.api.routes.esg import esg_bp, africa_esg_bp
    from src.api.routes.insights import insights_bp
    from src.api.routes.realtime import realtime_bp
    from src.api.routes.integrated_data_routes import integrated_data_bp
    
    app.register_blueprint(prices_bp)
    app.register_blueprint(quality_bp)
    app.register_blueprint(esg_bp)
    app.register_blueprint(africa_esg_bp)
    app.register_blueprint(insights_bp)
    app.register_blueprint(realtime_bp)
    app.register_blueprint(integrated_data_bp)

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