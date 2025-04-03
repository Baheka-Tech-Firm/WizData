import os
import logging
import asyncio
from flask import Flask, render_template, jsonify, request, Blueprint
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import DeclarativeBase

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Database setup
class Base(DeclarativeBase):
    pass

db = SQLAlchemy(model_class=Base)

# Create the Flask application
app = Flask(__name__)
app.secret_key = os.environ.get("SESSION_SECRET", "dev_secret_key")

# Configure database
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL")
app.config["SQLALCHEMY_ENGINE_OPTIONS"] = {
    "pool_recycle": 300,
    "pool_pre_ping": True,
}

# Initialize database with app
db.init_app(app)

# Import models (must come after db init but before create_all)
import models  # noqa

# Register API routes
from src.api.routes.prices import prices_bp
app.register_blueprint(prices_bp)

# Web UI Routes
@app.route('/')
def index():
    """Render the dashboard page"""
    return render_template('dashboard.html', title="WizData Dashboard")

@app.route('/jobs')
def jobs():
    """Render the jobs page"""
    return render_template('jobs.html', title="Data Collection Jobs")

@app.route('/sources')
def sources():
    """Render the data sources page"""
    return render_template('sources.html', title="Data Sources")

@app.route('/products')
def products():
    """Render the data products page"""
    return render_template('products.html', title="Data Products")
    
@app.route('/api-services')
def api_services():
    """Render the API services page"""
    return render_template('api_services.html', title="API Services")

@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "version": "1.0.0",
        "service": "WizData Web Dashboard"
    })

# API routes for direct usage
@app.route('/api/symbols')
def get_symbols():
    """
    Get available symbols for different asset types
    
    Query parameters:
    - asset_type: The asset type ("jse", "crypto", "forex")
    
    Returns:
        JSON response with available symbols
    """
    try:
        from src.ingestion.forex_fetcher import ForexFetcher
        from src.ingestion.crypto_fetcher import CryptoFetcher
        from src.ingestion.jse_scraper import JSEScraper
        
        asset_type = request.args.get('asset_type', 'jse').lower()
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get the appropriate fetcher
        if asset_type == "forex":
            fetcher = ForexFetcher()
        elif asset_type == "crypto":
            fetcher = CryptoFetcher()
        elif asset_type == "jse":
            fetcher = JSEScraper()
        else:
            return jsonify({
                "error": f"Invalid asset_type: {asset_type}. Supported types: jse, crypto, forex"
            }), 400
        
        # Fetch symbols asynchronously
        symbols = loop.run_until_complete(fetcher.get_symbols())
        loop.close()
        
        return jsonify({
            "asset_type": asset_type,
            "count": len(symbols),
            "symbols": symbols
        })
        
    except Exception as e:
        logger.error(f"Error fetching symbols: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

# Create database tables
with app.app_context():
    logger.info("Creating database tables if they don't exist")
    db.create_all()
    logger.info("Database setup complete")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)