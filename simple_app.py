#!/usr/bin/env python3
"""
Simple Flask app runner for testing the new UI/UX pages
"""

from flask import Flask
import os
import sys
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_simple_app():
    """Create a simplified Flask app for testing"""
    app = Flask(__name__)
    
    # Basic configuration
    app.config['SECRET_KEY'] = 'dev-secret-key-change-in-production'
    app.config['DEBUG'] = True
    
    # Register routes
    try:
        from routes import register_routes
        register_routes(app)
        logger.info("Routes registered successfully")
    except Exception as e:
        logger.error(f"Error registering routes: {e}")
        sys.exit(1)
    
    return app

if __name__ == "__main__":
    app = create_simple_app()
    
    # Run the app
    port = int(os.environ.get('PORT', 5000))
    logger.info(f"Starting Flask app on port {port}")
    
    try:
        app.run(
            host='0.0.0.0',
            port=port,
            debug=True,
            use_reloader=False  # Disable reloader to avoid conflicts
        )
    except Exception as e:
        logger.error(f"Error starting app: {e}")
        sys.exit(1)
