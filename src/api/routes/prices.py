from flask import Blueprint, jsonify, request
from datetime import datetime, timedelta
import logging
import pandas as pd
import asyncio

from src.ingestion.jse_scraper import JSEScraper
from src.ingestion.crypto_fetcher import CryptoFetcher
from src.ingestion.forex_fetcher import ForexFetcher

logger = logging.getLogger(__name__)

# Create blueprint with URL prefix
prices_bp = Blueprint('prices', __name__, url_prefix='/api')

# Initialize fetchers
jse_scraper = JSEScraper()
crypto_fetcher = CryptoFetcher()
forex_fetcher = ForexFetcher()

@prices_bp.route('/symbols', methods=['GET'])
def get_symbols():
    """
    Get available symbols for different asset types
    
    Query parameters:
    - asset_type: The asset type ("jse", "crypto", "forex")
    
    Returns:
        JSON response with available symbols
    """
    asset_type = request.args.get('asset_type', 'jse').lower()
    
    try:
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch symbols based on asset type
        if asset_type == 'jse':
            symbols = loop.run_until_complete(jse_scraper.get_symbols())
        elif asset_type == 'crypto':
            symbols = loop.run_until_complete(crypto_fetcher.get_symbols())
        elif asset_type == 'forex':
            symbols = loop.run_until_complete(forex_fetcher.get_symbols())
        else:
            return jsonify({
                'status': 'error',
                'message': f"Invalid asset type: {asset_type}. Valid types are 'jse', 'crypto', 'forex'"
            }), 400
        
        # Clean up event loop
        loop.close()
        
        return jsonify({
            'status': 'success',
            'asset_type': asset_type,
            'count': len(symbols),
            'symbols': symbols
        })
        
    except Exception as e:
        logger.error(f"Error retrieving symbols for {asset_type}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving symbols: {str(e)}"
        }), 500

@prices_bp.route('/prices', methods=['GET'])
def get_prices():
    """
    Get price data for a symbol
    
    Query parameters:
    - asset_type: The asset type ("jse", "crypto", "forex")
    - symbol: The ticker symbol (e.g., "SOL", "BTC", "EURUSD")
    - interval: The time interval ("daily", "weekly", "monthly")
    - start_date: Start date in YYYY-MM-DD format
    - end_date: End date in YYYY-MM-DD format
    
    Returns:
        JSON response with price data
    """
    # Get query parameters
    asset_type = request.args.get('asset_type', 'jse').lower()
    symbol = request.args.get('symbol')
    interval = request.args.get('interval', 'daily').lower()
    start_date = request.args.get('start_date')
    end_date = request.args.get('end_date')
    
    # Validate required parameters
    if not asset_type:
        return jsonify({
            'status': 'error',
            'message': "Missing required parameter: asset_type"
        }), 400
    
    # Set default dates if not provided
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    if not start_date:
        # Default to 30 days before end date
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        start_dt = end_dt - timedelta(days=30)
        start_date = start_dt.strftime("%Y-%m-%d")
    
    try:
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch data based on asset type
        if asset_type == 'jse':
            df = loop.run_until_complete(jse_scraper.fetch_data(symbol=symbol, start_date=start_date, end_date=end_date))
        elif asset_type == 'crypto':
            df = loop.run_until_complete(crypto_fetcher.fetch_data(symbol=symbol, interval=interval, start_date=start_date, end_date=end_date))
        elif asset_type == 'forex':
            df = loop.run_until_complete(forex_fetcher.fetch_data(symbol=symbol, interval=interval, start_date=start_date, end_date=end_date))
        else:
            return jsonify({
                'status': 'error',
                'message': f"Invalid asset type: {asset_type}. Valid types are 'jse', 'crypto', 'forex'"
            }), 400
        
        # Clean up event loop
        loop.close()
        
        if df.empty:
            return jsonify({
                'status': 'warning',
                'message': f"No data found for {symbol if symbol else 'requested symbols'} in {asset_type}",
                'count': 0,
                'data': []
            })
        
        # Convert DataFrame to list of dictionaries for JSON response
        # Ensure all date columns are converted to string format
        if 'date' in df.columns:
            df['date'] = df['date'].astype(str)
        
        data_records = df.to_dict(orient='records')
        
        return jsonify({
            'status': 'success',
            'asset_type': asset_type,
            'symbol': symbol if symbol else 'multiple',
            'interval': interval,
            'start_date': start_date,
            'end_date': end_date,
            'count': len(data_records),
            'data': data_records
        })
        
    except Exception as e:
        logger.error(f"Error retrieving prices for {asset_type} {symbol}: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': f"Error retrieving prices: {str(e)}"
        }), 500