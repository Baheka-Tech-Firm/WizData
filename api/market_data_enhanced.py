"""
Enhanced Market Data API with REST endpoints
Provides comprehensive market data services with authentication and rate limiting
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging
import asyncio
from functools import wraps

from middleware.auth_middleware import require_api_key, get_current_user
from middleware.rate_limiter import rate_limit
from middleware.cache_manager import cached
from middleware.monitoring_simple import monitor_function
from services.market_data_service import MarketDataService
from services.data_validation import validate_request_data

logger = logging.getLogger(__name__)

# Create blueprint for enhanced market data API
market_data_api = Blueprint('market_data_api', __name__, url_prefix='/api/v2/market-data')

class MarketDataAPIError(Exception):
    """Custom exception for market data API errors"""
    def __init__(self, message: str, status_code: int = 400):
        self.message = message
        self.status_code = status_code
        super().__init__(self.message)

def handle_api_error(error):
    """Error handler for API exceptions"""
    if isinstance(error, MarketDataAPIError):
        return jsonify({
            'error': error.message,
            'timestamp': datetime.now().isoformat(),
            'status': 'error'
        }), error.status_code
    
    logger.error(f"Unexpected error in market data API: {str(error)}")
    return jsonify({
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat(),
        'status': 'error'
    }), 500

# Register error handler
market_data_api.register_error_handler(MarketDataAPIError, handle_api_error)
market_data_api.register_error_handler(Exception, handle_api_error)

@market_data_api.route('/quotes', methods=['GET'])
@require_api_key()
@rate_limit(requests_per_minute=120, burst_size=30)
@cached(ttl=60, data_type='market_data')
@monitor_function
def get_quotes():
    """
    Get real-time quotes for specified symbols
    
    Query Parameters:
    - symbols: Comma-separated list of symbols (required)
    - market: Market identifier (jse, nasdaq, nyse, etc.)
    - include_fundamentals: Include fundamental data (true/false)
    
    Response:
    {
        "status": "success",
        "data": {
            "JSE:NPN": {
                "symbol": "NPN",
                "price": 285000,
                "change": 2500,
                "change_percent": 0.88,
                "volume": 1250000,
                "timestamp": "2025-07-30T10:30:00Z"
            }
        },
        "metadata": {
            "count": 1,
            "timestamp": "2025-07-30T10:30:15Z",
            "cached": false
        }
    }
    """
    try:
        # Validate required parameters
        symbols_param = request.args.get('symbols')
        if not symbols_param:
            raise MarketDataAPIError("Parameter 'symbols' is required", 400)
        
        symbols = [s.strip().upper() for s in symbols_param.split(',')]
        if len(symbols) > 50:  # Limit to prevent abuse
            raise MarketDataAPIError("Maximum 50 symbols allowed per request", 400)
        
        market = request.args.get('market', 'auto')
        include_fundamentals = request.args.get('include_fundamentals', 'false').lower() == 'true'
        
        # Get current user for analytics
        user = get_current_user()
        
        # Fetch market data
        market_service = MarketDataService()
        data = market_service.get_quotes(
            symbols=symbols,
            market=market,
            include_fundamentals=include_fundamentals,
            user_id=user.get('id') if user else None
        )
        
        return jsonify({
            'status': 'success',
            'data': data['quotes'],
            'metadata': {
                'count': len(data['quotes']),
                'timestamp': datetime.now().isoformat(),
                'cached': data.get('cached', False),
                'market': market,
                'user_id': user.get('id') if user else None
            }
        })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in get_quotes: {str(e)}")
        raise MarketDataAPIError("Failed to fetch quotes", 500)

@market_data_api.route('/historical', methods=['GET'])
@require_api_key()
@rate_limit(requests_per_minute=60, burst_size=10)
@cached(ttl=300, data_type='historical_data')
@monitor_function
def get_historical_data():
    """
    Get historical OHLCV data for symbols
    
    Query Parameters:
    - symbols: Comma-separated list of symbols (required)
    - period: Time period (1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max)
    - interval: Data interval (1m, 2m, 5m, 15m, 30m, 60m, 90m, 1h, 1d, 5d, 1wk, 1mo, 3mo)
    - start_date: Start date (YYYY-MM-DD)
    - end_date: End date (YYYY-MM-DD)
    """
    try:
        symbols_param = request.args.get('symbols')
        if not symbols_param:
            raise MarketDataAPIError("Parameter 'symbols' is required", 400)
        
        symbols = [s.strip().upper() for s in symbols_param.split(',')]
        if len(symbols) > 10:  # Historical data is more expensive
            raise MarketDataAPIError("Maximum 10 symbols allowed for historical data", 400)
        
        period = request.args.get('period', '1mo')
        interval = request.args.get('interval', '1d')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Validate date parameters
        if start_date and end_date:
            try:
                start_dt = datetime.strptime(start_date, '%Y-%m-%d')
                end_dt = datetime.strptime(end_date, '%Y-%m-%d')
                if start_dt >= end_dt:
                    raise MarketDataAPIError("start_date must be before end_date", 400)
            except ValueError:
                raise MarketDataAPIError("Invalid date format. Use YYYY-MM-DD", 400)
        
        user = get_current_user()
        market_service = MarketDataService()
        
        data = market_service.get_historical_data(
            symbols=symbols,
            period=period,
            interval=interval,
            start_date=start_date,
            end_date=end_date,
            user_id=user.get('id') if user else None
        )
        
        return jsonify({
            'status': 'success',
            'data': data['historical'],
            'metadata': {
                'symbols': symbols,
                'period': period,
                'interval': interval,
                'start_date': start_date,
                'end_date': end_date,
                'timestamp': datetime.now().isoformat(),
                'cached': data.get('cached', False)
            }
        })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in get_historical_data: {str(e)}")
        raise MarketDataAPIError("Failed to fetch historical data", 500)

@market_data_api.route('/fundamentals', methods=['GET'])
@require_api_key()
@rate_limit(requests_per_minute=30, burst_size=5)
@cached(ttl=3600, data_type='fundamental_data')
@monitor_function
def get_fundamentals():
    """
    Get fundamental data for symbols
    
    Query Parameters:
    - symbols: Comma-separated list of symbols (required)
    - metrics: Specific metrics to fetch (optional)
    """
    try:
        symbols_param = request.args.get('symbols')
        if not symbols_param:
            raise MarketDataAPIError("Parameter 'symbols' is required", 400)
        
        symbols = [s.strip().upper() for s in symbols_param.split(',')]
        if len(symbols) > 20:
            raise MarketDataAPIError("Maximum 20 symbols allowed for fundamental data", 400)
        
        metrics = request.args.get('metrics')
        metrics_list = [m.strip() for m in metrics.split(',')] if metrics else None
        
        user = get_current_user()
        market_service = MarketDataService()
        
        data = market_service.get_fundamentals(
            symbols=symbols,
            metrics=metrics_list,
            user_id=user.get('id') if user else None
        )
        
        return jsonify({
            'status': 'success',
            'data': data['fundamentals'],
            'metadata': {
                'symbols': symbols,
                'metrics': metrics_list,
                'timestamp': datetime.now().isoformat(),
                'cached': data.get('cached', False)
            }
        })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in get_fundamentals: {str(e)}")
        raise MarketDataAPIError("Failed to fetch fundamental data", 500)

@market_data_api.route('/news', methods=['GET'])
@require_api_key()
@rate_limit(requests_per_minute=60, burst_size=15)
@cached(ttl=300, data_type='news_data')
@monitor_function
def get_market_news():
    """
    Get market news for symbols or general market news
    
    Query Parameters:
    - symbols: Comma-separated list of symbols (optional)
    - category: News category (earnings, mergers, analyst, general)
    - limit: Number of articles to return (max 100)
    - since: Get news since timestamp (ISO format)
    """
    try:
        symbols_param = request.args.get('symbols')
        symbols = [s.strip().upper() for s in symbols_param.split(',')] if symbols_param else []
        
        category = request.args.get('category', 'general')
        limit = min(int(request.args.get('limit', 10)), 100)
        since = request.args.get('since')
        
        if since:
            try:
                since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
            except ValueError:
                raise MarketDataAPIError("Invalid 'since' timestamp format", 400)
        else:
            since_dt = None
        
        user = get_current_user()
        market_service = MarketDataService()
        
        data = market_service.get_market_news(
            symbols=symbols,
            category=category,
            limit=limit,
            since=since_dt,
            user_id=user.get('id') if user else None
        )
        
        return jsonify({
            'status': 'success',
            'data': data['news'],
            'metadata': {
                'symbols': symbols,
                'category': category,
                'limit': limit,
                'since': since,
                'count': len(data['news']),
                'timestamp': datetime.now().isoformat(),
                'cached': data.get('cached', False)
            }
        })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in get_market_news: {str(e)}")
        raise MarketDataAPIError("Failed to fetch market news", 500)

@market_data_api.route('/screener', methods=['POST'])
@require_api_key()
@rate_limit(requests_per_minute=20, burst_size=5)
@monitor_function
def stock_screener():
    """
    Screen stocks based on criteria
    
    POST Body:
    {
        "filters": {
            "market_cap": {"min": 1000000000, "max": 50000000000},
            "pe_ratio": {"min": 5, "max": 25},
            "dividend_yield": {"min": 0.02},
            "volume": {"min": 100000}
        },
        "sort_by": "market_cap",
        "sort_order": "desc",
        "limit": 50
    }
    """
    try:
        if not request.is_json:
            raise MarketDataAPIError("Request must contain JSON data", 400)
        
        data = request.get_json()
        filters = data.get('filters', {})
        sort_by = data.get('sort_by', 'market_cap')
        sort_order = data.get('sort_order', 'desc')
        limit = min(data.get('limit', 50), 100)
        
        # Validate filters
        valid_filters = ['market_cap', 'pe_ratio', 'dividend_yield', 'volume', 'price', 'change_percent']
        for filter_key in filters.keys():
            if filter_key not in valid_filters:
                raise MarketDataAPIError(f"Invalid filter: {filter_key}", 400)
        
        user = get_current_user()
        market_service = MarketDataService()
        
        results = market_service.screen_stocks(
            filters=filters,
            sort_by=sort_by,
            sort_order=sort_order,
            limit=limit,
            user_id=user.get('id') if user else None
        )
        
        return jsonify({
            'status': 'success',
            'data': results['stocks'],
            'metadata': {
                'filters': filters,
                'sort_by': sort_by,
                'sort_order': sort_order,
                'limit': limit,
                'count': len(results['stocks']),
                'timestamp': datetime.now().isoformat()
            }
        })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in stock_screener: {str(e)}")
        raise MarketDataAPIError("Failed to screen stocks", 500)

@market_data_api.route('/watchlist', methods=['GET', 'POST', 'DELETE'])
@require_api_key()
@rate_limit(requests_per_minute=60, burst_size=20)
@monitor_function
def manage_watchlist():
    """
    Manage user watchlists
    
    GET: Get user's watchlists
    POST: Create or update watchlist
    DELETE: Remove symbols from watchlist
    """
    try:
        user = get_current_user()
        if not user:
            raise MarketDataAPIError("Authentication required", 401)
        
        market_service = MarketDataService()
        
        if request.method == 'GET':
            watchlist_name = request.args.get('name', 'default')
            watchlist = market_service.get_watchlist(user['id'], watchlist_name)
            
            return jsonify({
                'status': 'success',
                'data': watchlist,
                'metadata': {
                    'user_id': user['id'],
                    'watchlist_name': watchlist_name,
                    'timestamp': datetime.now().isoformat()
                }
            })
        
        elif request.method == 'POST':
            if not request.is_json:
                raise MarketDataAPIError("Request must contain JSON data", 400)
            
            data = request.get_json()
            watchlist_name = data.get('name', 'default')
            symbols = data.get('symbols', [])
            action = data.get('action', 'add')  # add or replace
            
            if not symbols:
                raise MarketDataAPIError("Symbols list is required", 400)
            
            watchlist = market_service.update_watchlist(
                user['id'], watchlist_name, symbols, action
            )
            
            return jsonify({
                'status': 'success',
                'data': watchlist,
                'metadata': {
                    'user_id': user['id'],
                    'watchlist_name': watchlist_name,
                    'action': action,
                    'timestamp': datetime.now().isoformat()
                }
            })
        
        elif request.method == 'DELETE':
            watchlist_name = request.args.get('name', 'default')
            symbols = request.args.get('symbols', '').split(',')
            symbols = [s.strip().upper() for s in symbols if s.strip()]
            
            if not symbols:
                raise MarketDataAPIError("Symbols parameter is required", 400)
            
            watchlist = market_service.remove_from_watchlist(
                user['id'], watchlist_name, symbols
            )
            
            return jsonify({
                'status': 'success',
                'data': watchlist,
                'metadata': {
                    'user_id': user['id'],
                    'watchlist_name': watchlist_name,
                    'removed_symbols': symbols,
                    'timestamp': datetime.now().isoformat()
                }
            })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in manage_watchlist: {str(e)}")
        raise MarketDataAPIError("Failed to manage watchlist", 500)

@market_data_api.route('/alerts', methods=['GET', 'POST', 'DELETE'])
@require_api_key()
@rate_limit(requests_per_minute=30, burst_size=10)
@monitor_function
def manage_alerts():
    """
    Manage price alerts
    
    GET: Get user's alerts
    POST: Create new alert
    DELETE: Remove alert
    """
    try:
        user = get_current_user()
        if not user:
            raise MarketDataAPIError("Authentication required", 401)
        
        market_service = MarketDataService()
        
        if request.method == 'GET':
            alerts = market_service.get_user_alerts(user['id'])
            
            return jsonify({
                'status': 'success',
                'data': alerts,
                'metadata': {
                    'user_id': user['id'],
                    'count': len(alerts),
                    'timestamp': datetime.now().isoformat()
                }
            })
        
        elif request.method == 'POST':
            if not request.is_json:
                raise MarketDataAPIError("Request must contain JSON data", 400)
            
            data = request.get_json()
            required_fields = ['symbol', 'condition', 'value']
            for field in required_fields:
                if field not in data:
                    raise MarketDataAPIError(f"Field '{field}' is required", 400)
            
            alert = market_service.create_alert(
                user_id=user['id'],
                symbol=data['symbol'].upper(),
                condition=data['condition'],  # 'above', 'below', 'change_percent'
                value=float(data['value']),
                message=data.get('message', ''),
                notification_method=data.get('notification_method', 'email')
            )
            
            return jsonify({
                'status': 'success',
                'data': alert,
                'metadata': {
                    'user_id': user['id'],
                    'timestamp': datetime.now().isoformat()
                }
            })
        
        elif request.method == 'DELETE':
            alert_id = request.args.get('alert_id')
            if not alert_id:
                raise MarketDataAPIError("alert_id parameter is required", 400)
            
            success = market_service.delete_alert(user['id'], alert_id)
            
            return jsonify({
                'status': 'success' if success else 'error',
                'message': 'Alert deleted successfully' if success else 'Alert not found',
                'metadata': {
                    'user_id': user['id'],
                    'alert_id': alert_id,
                    'timestamp': datetime.now().isoformat()
                }
            })
        
    except MarketDataAPIError:
        raise
    except Exception as e:
        logger.error(f"Error in manage_alerts: {str(e)}")
        raise MarketDataAPIError("Failed to manage alerts", 500)

@market_data_api.route('/markets/status', methods=['GET'])
@require_api_key()
@rate_limit(requests_per_minute=60, burst_size=15)
@cached(ttl=120, data_type='market_status')
@monitor_function
def get_market_status():
    """
    Get market status and trading hours for different exchanges
    """
    try:
        markets = request.args.get('markets', 'all')
        if markets != 'all':
            markets = [m.strip().upper() for m in markets.split(',')]
        
        market_service = MarketDataService()
        status_data = market_service.get_market_status(markets)
        
        return jsonify({
            'status': 'success',
            'data': status_data,
            'metadata': {
                'timestamp': datetime.now().isoformat(),
                'timezone': 'UTC'
            }
        })
        
    except Exception as e:
        logger.error(f"Error in get_market_status: {str(e)}")
        raise MarketDataAPIError("Failed to fetch market status", 500)

# Register the blueprint
def register_market_data_api(app):
    """Register the market data API blueprint with the Flask app"""
    app.register_blueprint(market_data_api)
