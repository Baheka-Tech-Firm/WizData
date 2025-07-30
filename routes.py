import os
import asyncio
import logging
from datetime import datetime
from flask import render_template, jsonify, request, send_file

logger = logging.getLogger(__name__)

def register_routes(app):
    """Register all main routes with the Flask app"""
    
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
        
    @app.route('/websocket')
    def websocket_demo():
        """Render the WebSocket demo page"""
        return render_template('websocket_demo.html', title="Real-time Data Demo")

    @app.route('/esg')
    def esg_dashboard():
        """Render the ESG data dashboard page"""
        return render_template('esg/dashboard.html', title="ESG Data Dashboard")

    @app.route('/esg/africa')
    def africa_esg_dashboard():
        """Render the Africa ESG data dashboard page"""
        return render_template('esg/africa_dashboard.html', title="African ESG Data Dashboard")

    @app.route('/integrated-data')
    def integrated_data_dashboard():
        """Render the integrated data dashboard page"""
        return render_template('integrated/dashboard.html', title="Integrated Financial & ESG Data")

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