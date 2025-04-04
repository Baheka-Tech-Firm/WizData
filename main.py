import os
import logging
import asyncio
import json
from datetime import datetime, timedelta
from flask import Flask, render_template, jsonify, request, Blueprint, send_file, redirect, url_for, flash
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
    "pool_recycle": 60,
    "pool_pre_ping": True,
    "connect_args": {
        "connect_timeout": 10
    }
}

# Configure data export directory
app.config["DATA_EXPORT_DIR"] = os.environ.get("DATA_EXPORT_DIR", "data/exports")
os.makedirs(app.config["DATA_EXPORT_DIR"], exist_ok=True)

# Initialize database with app
db.init_app(app)

# Import models (must come after db init but before create_all)
import models  # noqa

# Set up database tables with the app context
logger.info("Creating database tables if they don't exist")
with app.app_context():
    try:
        db.create_all()
        logger.info("Database setup complete")
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        # Continue without failing - the app can still function with reduced features

# Register API routes
from src.api.routes.prices import prices_bp
from src.api.routes.quality import quality_bp
from src.api.routes.esg import esg_bp
from src.api.routes.insights import insights_bp
app.register_blueprint(prices_bp)
app.register_blueprint(quality_bp)
app.register_blueprint(esg_bp)
app.register_blueprint(insights_bp)

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
    market = request.args.get('market', 'jse')
    return render_template('sources.html', title="Data Sources", active_market=market)

@app.route('/products')
def products():
    """Render the data products page"""
    return render_template('products.html', title="Data Products")
    
@app.route('/api-services')
def api_services():
    """Render the API services page"""
    return render_template('api_services.html', title="API Services")
    
@app.route('/quality')
def quality_dashboard():
    """Render the data quality dashboard page"""
    return render_template('quality/dashboard.html', title="Data Quality Dashboard")
    
@app.route('/insights')
def insights_wizard():
    """Render the AI insights wizard page"""
    return render_template('insights/wizard.html', title="AI Insights Wizard")

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
    - asset_type: The asset type ("jse", "crypto", "forex", "african", "global", "dividend", "earnings")
    - market: For specific markets (e.g., "jse", "ngx", "egx", "nse", "cse", "asx", "lse", "nasdaq", "nyse", "tyo")
    
    Returns:
        JSON response with available symbols
    """
    try:
        from src.ingestion.forex_fetcher import ForexFetcher
        from src.ingestion.crypto_fetcher import CryptoFetcher
        from src.ingestion.jse_scraper import JSEScraper
        from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
        from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
        from src.ingestion.dividend_fetcher import DividendFetcher
        from src.ingestion.earnings_fetcher import EarningsFetcher
        
        asset_type = request.args.get('asset_type', 'jse').lower()
        market = request.args.get('market', 'jse').lower()
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get the appropriate fetcher based on asset type
        if asset_type == 'african':
            fetcher = AfricanMarketsFetcher()
            symbols = loop.run_until_complete(fetcher.get_symbols(market))
            
            loop.close()
            
            return jsonify({
                "market": market,
                "asset_type": "stock",
                "count": len(symbols),
                "symbols": symbols
            })
        elif asset_type == 'global':
            fetcher = GlobalMarketsFetcher()
            symbols = loop.run_until_complete(fetcher.get_symbols(market))
            
            loop.close()
            
            return jsonify({
                "market": market,
                "asset_type": "stock",
                "count": len(symbols),
                "symbols": symbols
            })
        elif asset_type == 'dividend':
            fetcher = DividendFetcher()
            symbols = loop.run_until_complete(fetcher.get_symbols(market))
            
            loop.close()
            
            return jsonify({
                "market": market,
                "asset_type": "dividend",
                "count": len(symbols),
                "symbols": symbols
            })
        elif asset_type == 'earnings':
            fetcher = EarningsFetcher()
            symbols = loop.run_until_complete(fetcher.get_symbols(market))
            
            loop.close()
            
            return jsonify({
                "market": market,
                "asset_type": "earnings",
                "count": len(symbols),
                "symbols": symbols
            })
        else:
            # Get the appropriate fetcher for other asset types
            if asset_type == "forex":
                fetcher = ForexFetcher()
            elif asset_type == "crypto":
                fetcher = CryptoFetcher()
            elif asset_type == "jse":
                fetcher = JSEScraper()
            else:
                return jsonify({
                    "error": f"Invalid asset_type: {asset_type}. Supported types: jse, crypto, forex, african, global, dividend, earnings"
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
        
@app.route('/api/african/markets')
def get_african_markets():
    """
    Get available African markets
    
    Returns:
        JSON response with available African markets
    """
    try:
        from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get markets
        fetcher = AfricanMarketsFetcher()
        markets = loop.run_until_complete(fetcher.get_markets())
        loop.close()
        
        return jsonify({
            "count": len(markets),
            "markets": markets
        })
        
    except Exception as e:
        logger.error(f"Error fetching African markets: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/api/african/data')
def get_african_market_data():
    """
    Get data for African markets
    
    Query parameters:
    - market: The market code (jse, ngx, egx, nse, cse)
    - symbol: Symbol to fetch (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with market data or a file download
    """
    try:
        from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
        
        market = request.args.get('market', 'jse').lower()
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get data
        fetcher = AfricanMarketsFetcher()
        
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            symbol_part = f"_{symbol}" if symbol else "_all"
            filename = f"{market.upper()}{symbol_part}_{timestamp}.{export_format}"
            output_path = os.path.join(app.config["DATA_EXPORT_DIR"], filename)
            
            # Fetch and export data
            result = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                save_format=export_format,
                output_path=app.config["DATA_EXPORT_DIR"]
            ))
            loop.close()
            
            if result.empty:
                return jsonify({
                    "error": "No data found for the specified parameters"
                }), 404
                
            # Return the file for download
            return send_file(
                os.path.join(app.config["DATA_EXPORT_DIR"], filename),
                as_attachment=True,
                download_name=filename,
                mimetype='application/json' if export_format == 'json' else 'text/csv'
            )
        else:
            # Return JSON response
            df = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            ))
            loop.close()
            
            if df.empty:
                return jsonify({
                    "error": "No data found for the specified parameters"
                }), 404
                
            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')
            
            return jsonify({
                "market": market,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(data),
                "data": data
            })
            
    except Exception as e:
        logger.error(f"Error fetching African market data: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/api/global/markets')
def get_global_markets():
    """
    Get available global markets
    
    Returns:
        JSON response with available global markets
    """
    try:
        from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get markets
        fetcher = GlobalMarketsFetcher()
        markets = loop.run_until_complete(fetcher.get_markets())
        loop.close()
        
        return jsonify({
            "count": len(markets),
            "markets": markets
        })
        
    except Exception as e:
        logger.error(f"Error fetching global markets: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/api/global/data')
def get_global_market_data():
    """
    Get data for global markets
    
    Query parameters:
    - market: The market code (asx, lse, nasdaq, nyse, tyo)
    - symbol: Symbol to fetch (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with market data or a file download
    """
    try:
        from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
        
        market = request.args.get('market', 'nasdaq').lower()
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get data
        fetcher = GlobalMarketsFetcher()
        
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            symbol_part = f"_{symbol}" if symbol else "_all"
            filename = f"{market.upper()}{symbol_part}_{timestamp}.{export_format}"
            output_path = os.path.join(app.config["DATA_EXPORT_DIR"], filename)
            
            # Fetch and export data
            result = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                save_format=export_format,
                output_path=app.config["DATA_EXPORT_DIR"]
            ))
            loop.close()
            
            if result.empty:
                return jsonify({
                    "error": "No data found for the specified parameters"
                }), 404
                
            # Return the file for download
            return send_file(
                os.path.join(app.config["DATA_EXPORT_DIR"], filename),
                as_attachment=True,
                download_name=filename,
                mimetype='application/json' if export_format == 'json' else 'text/csv'
            )
        else:
            # Return JSON response
            df = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            ))
            loop.close()
            
            if df.empty:
                return jsonify({
                    "error": "No data found for the specified parameters"
                }), 404
                
            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')
            
            return jsonify({
                "market": market,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(data),
                "data": data
            })
            
    except Exception as e:
        logger.error(f"Error fetching global market data: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/api/dividend/data')
def get_dividend_data():
    """
    Get dividend data for a specific market and symbol
    
    Query parameters:
    - market: The market code (asx, lse, nasdaq, nyse, jse, tyo)
    - symbol: Symbol to fetch (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with dividend data or a file download
    """
    try:
        from src.ingestion.dividend_fetcher import DividendFetcher
        
        market = request.args.get('market', 'nasdaq').lower()
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get data
        fetcher = DividendFetcher()
        
        if export_format in ['json', 'csv']:
            # Fetch and export data
            result = loop.run_until_complete(fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                format=export_format,
                output_path=app.config["DATA_EXPORT_DIR"]
            ))
            loop.close()
            
            if not result:
                return jsonify({
                    "error": "No dividend data found for the specified parameters"
                }), 404
                
            # Return the file for download
            return send_file(
                result,
                as_attachment=True,
                download_name=os.path.basename(result),
                mimetype='application/json' if export_format == 'json' else 'text/csv'
            )
        else:
            # Return JSON response
            df = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            ))
            loop.close()
            
            if df.empty:
                return jsonify({
                    "error": "No dividend data found for the specified parameters"
                }), 404
                
            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')
            
            return jsonify({
                "market": market,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(data),
                "data": data
            })
            
    except Exception as e:
        logger.error(f"Error fetching dividend data: {str(e)}")
        return jsonify({
            "error": "Internal server error",
            "message": str(e)
        }), 500

@app.route('/api/earnings/data')
def get_earnings_data():
    """
    Get earnings data for a specific market and symbol
    
    Query parameters:
    - market: The market code (asx, lse, nasdaq, nyse, jse)
    - symbol: Symbol to fetch (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with earnings data or a file download
    """
    try:
        from src.ingestion.earnings_fetcher import EarningsFetcher
        
        market = request.args.get('market', 'nasdaq').lower()
        symbol = request.args.get('symbol')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Create event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Get data
        fetcher = EarningsFetcher()
        
        if export_format in ['json', 'csv']:
            # Fetch and export data
            result = loop.run_until_complete(fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                format=export_format,
                output_path=app.config["DATA_EXPORT_DIR"]
            ))
            loop.close()
            
            if not result:
                return jsonify({
                    "error": "No earnings data found for the specified parameters"
                }), 404
                
            # Return the file for download
            return send_file(
                result,
                as_attachment=True,
                download_name=os.path.basename(result),
                mimetype='application/json' if export_format == 'json' else 'text/csv'
            )
        else:
            # Return JSON response
            df = loop.run_until_complete(fetcher.fetch_data(
                market=market,
                symbol=symbol,
                start_date=start_date,
                end_date=end_date
            ))
            loop.close()
            
            if df.empty:
                return jsonify({
                    "error": "No earnings data found for the specified parameters"
                }), 404
                
            # Convert DataFrame to list of dictionaries
            data = df.to_dict(orient='records')
            
            return jsonify({
                "market": market,
                "symbol": symbol,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(data),
                "data": data
            })
            
    except Exception as e:
        logger.error(f"Error fetching earnings data: {str(e)}")
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