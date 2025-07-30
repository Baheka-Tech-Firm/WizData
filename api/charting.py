"""
Professional Charting API
Provides OHLCV data, technical indicators, and market metadata for charting platforms
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
import json
import asyncio
from typing import Dict, List, Any, Optional

# Create blueprint
charting_api = Blueprint('charting_api', __name__, url_prefix='/api/v1/charting')

def get_ohlcv_data(symbol: str, interval: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Generate OHLCV (Open, High, Low, Close, Volume) data for charting
    Production implementation would fetch from database/cache
    """
    import random
    from datetime import datetime, timedelta
    
    # Base prices for different symbols
    base_prices = {
        'BTC/USDT': 67500,
        'ETH/USDT': 3750,
        'AAPL': 185,
        'GOOGL': 155,
        'JSE:NPN': 2850,
        'JSE:BHP': 4200,
        'JSE:SOL': 12500,
        'USD/ZAR': 18.50,
        'EUR/USD': 1.08,
        'GBP/USD': 1.25
    }
    
    base_price = base_prices.get(symbol, 100)
    current_price = base_price
    
    # Calculate time intervals
    interval_minutes = {
        '1m': 1, '5m': 5, '15m': 15, '30m': 30,
        '1h': 60, '4h': 240, '1d': 1440, '1w': 10080
    }
    
    minutes = interval_minutes.get(interval, 60)
    data = []
    
    # Generate historical data
    for i in range(limit, 0, -1):
        timestamp = datetime.now() - timedelta(minutes=minutes * i)
        
        # Generate realistic price movement
        change_percent = random.uniform(-0.02, 0.02)  # ±2% change
        open_price = current_price
        
        # Generate high/low based on volatility
        volatility = abs(change_percent) * 2
        high_price = open_price * (1 + random.uniform(0, volatility))
        low_price = open_price * (1 - random.uniform(0, volatility))
        
        # Close price with trend
        close_price = open_price * (1 + change_percent)
        current_price = close_price
        
        # Generate volume
        base_volume = 1000000 if 'BTC' in symbol else 500000
        volume = int(base_volume * random.uniform(0.5, 2.0))
        
        data.append({
            "time": int(timestamp.timestamp()),
            "open": round(open_price, 2),
            "high": round(high_price, 2),
            "low": round(low_price, 2),
            "close": round(close_price, 2),
            "volume": volume
        })
    
    return data

def calculate_technical_indicators(ohlcv_data: List[Dict], symbol: str) -> Dict[str, Any]:
    """
    Calculate technical indicators for charting
    Production would use TA-Lib or similar library
    """
    if not ohlcv_data:
        return {}
    
    closes = [candle['close'] for candle in ohlcv_data]
    highs = [candle['high'] for candle in ohlcv_data]
    lows = [candle['low'] for candle in ohlcv_data]
    
    indicators = {}
    
    # Simple Moving Average (SMA)
    if len(closes) >= 20:
        sma_20 = []
        for i in range(19, len(closes)):
            sma_value = sum(closes[i-19:i+1]) / 20
            sma_20.append(round(sma_value, 2))
        indicators['sma_20'] = sma_20
    
    # RSI (simplified calculation)
    if len(closes) >= 14:
        rsi_values = []
        for i in range(14, len(closes)):
            gains = []
            losses = []
            for j in range(i-13, i+1):
                change = closes[j] - closes[j-1] if j > 0 else 0
                if change > 0:
                    gains.append(change)
                    losses.append(0)
                else:
                    gains.append(0)
                    losses.append(abs(change))
            
            avg_gain = sum(gains) / 14
            avg_loss = sum(losses) / 14
            
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(round(rsi, 2))
        
        indicators['rsi'] = rsi_values
    
    # MACD (simplified)
    if len(closes) >= 26:
        ema_12 = []
        ema_26 = []
        
        # Calculate EMAs
        multiplier_12 = 2 / (12 + 1)
        multiplier_26 = 2 / (26 + 1)
        
        ema_12.append(closes[0])
        ema_26.append(closes[0])
        
        for i in range(1, len(closes)):
            ema_12_val = (closes[i] * multiplier_12) + (ema_12[-1] * (1 - multiplier_12))
            ema_26_val = (closes[i] * multiplier_26) + (ema_26[-1] * (1 - multiplier_26))
            
            ema_12.append(ema_12_val)
            ema_26.append(ema_26_val)
        
        # MACD line
        macd_line = [ema_12[i] - ema_26[i] for i in range(len(ema_12))]
        
        # Signal line (9-day EMA of MACD)
        signal_multiplier = 2 / (9 + 1)
        signal_line = [macd_line[0]]
        
        for i in range(1, len(macd_line)):
            signal_val = (macd_line[i] * signal_multiplier) + (signal_line[-1] * (1 - signal_multiplier))
            signal_line.append(signal_val)
        
        # Histogram
        histogram = [macd_line[i] - signal_line[i] for i in range(len(macd_line))]
        
        indicators['macd'] = {
            'line': [round(x, 4) for x in macd_line[-50:]],  # Last 50 values
            'signal': [round(x, 4) for x in signal_line[-50:]],
            'histogram': [round(x, 4) for x in histogram[-50:]]
        }
    
    return indicators

@charting_api.route('/ohlcv/<path:symbol>')
def get_symbol_ohlcv(symbol):
    """
    Get OHLCV data for a symbol
    
    Query parameters:
    - interval: 1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w (default: 1h)
    - limit: number of candles (default: 100, max: 1000)
    - from: start timestamp
    - to: end timestamp
    """
    try:
        interval = request.args.get('interval', '1h')
        limit = min(int(request.args.get('limit', 100)), 1000)
        
        # Get OHLCV data
        ohlcv_data = get_ohlcv_data(symbol, interval, limit)
        
        # Calculate technical indicators
        indicators = calculate_technical_indicators(ohlcv_data, symbol)
        
        return jsonify({
            'success': True,
            'symbol': symbol,
            'interval': interval,
            'data': ohlcv_data,
            'indicators': indicators,
            'timestamp': datetime.now().isoformat(),
            'count': len(ohlcv_data)
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 400

@charting_api.route('/symbols')
def get_available_symbols():
    """Get available symbols for charting"""
    symbols = {
        'crypto': [
            {
                'symbol': 'BTC/USDT',
                'name': 'Bitcoin',
                'base': 'BTC',
                'quote': 'USDT',
                'exchange': 'Binance',
                'type': 'cryptocurrency',
                'sector': 'Digital Assets'
            },
            {
                'symbol': 'ETH/USDT',
                'name': 'Ethereum',
                'base': 'ETH',
                'quote': 'USDT',
                'exchange': 'Binance',
                'type': 'cryptocurrency',
                'sector': 'Digital Assets'
            }
        ],
        'stocks': [
            {
                'symbol': 'AAPL',
                'name': 'Apple Inc.',
                'exchange': 'NASDAQ',
                'currency': 'USD',
                'sector': 'Technology',
                'industry': 'Consumer Electronics',
                'type': 'stock'
            },
            {
                'symbol': 'GOOGL',
                'name': 'Alphabet Inc.',
                'exchange': 'NASDAQ',
                'currency': 'USD',
                'sector': 'Technology',
                'industry': 'Internet Services',
                'type': 'stock'
            }
        ],
        'jse': [
            {
                'symbol': 'JSE:NPN',
                'name': 'Naspers Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Technology',
                'industry': 'Internet Services',
                'type': 'stock',
                'market_cap': 1500000000000  # ZAR cents
            },
            {
                'symbol': 'JSE:PRX',
                'name': 'Prosus N.V.',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Technology',
                'industry': 'Internet Services',
                'type': 'stock',
                'market_cap': 2000000000000
            },
            {
                'symbol': 'JSE:BHP',
                'name': 'BHP Group Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Mining',
                'industry': 'Diversified Mining',
                'type': 'stock',
                'market_cap': 850000000000
            },
            {
                'symbol': 'JSE:AGL',
                'name': 'Anglo American plc',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Mining',
                'industry': 'Diversified Mining',
                'type': 'stock',
                'market_cap': 680000000000
            },
            {
                'symbol': 'JSE:SOL',
                'name': 'Sasol Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Energy',
                'industry': 'Chemicals',
                'type': 'stock',
                'market_cap': 320000000000
            },
            {
                'symbol': 'JSE:SBK',
                'name': 'Standard Bank Group Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Banking',
                'industry': 'Commercial Banking',
                'type': 'stock',
                'market_cap': 450000000000
            },
            {
                'symbol': 'JSE:MTN',
                'name': 'MTN Group Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Telecommunications',
                'industry': 'Mobile Communications',
                'type': 'stock',
                'market_cap': 280000000000
            },
            {
                'symbol': 'JSE:CFR',
                'name': 'Capitec Bank Holdings Limited',
                'exchange': 'JSE',
                'currency': 'ZAR',
                'sector': 'Banking',
                'industry': 'Retail Banking',
                'type': 'stock',
                'market_cap': 380000000000
            }
        ],
        'forex': [
            {
                'symbol': 'USD/ZAR',
                'name': 'US Dollar / South African Rand',
                'base': 'USD',
                'quote': 'ZAR',
                'type': 'forex',
                'sector': 'Currency'
            },
            {
                'symbol': 'EUR/USD',
                'name': 'Euro / US Dollar',
                'base': 'EUR',
                'quote': 'USD',
                'type': 'forex',
                'sector': 'Currency'
            }
        ]
    }
    
    return jsonify({
        'success': True,
        'symbols': symbols,
        'count': sum(len(category) for category in symbols.values()),
        'timestamp': datetime.now().isoformat()
    })

@charting_api.route('/market-data/<path:symbol>')
def get_market_data(symbol):
    """Get current market data for a symbol"""
    try:
        # Get latest OHLCV data
        ohlcv_data = get_ohlcv_data(symbol, '1d', 2)
        
        if not ohlcv_data:
            return jsonify({
                'success': False,
                'error': 'No data available for symbol'
            }), 404
        
        current = ohlcv_data[-1]
        previous = ohlcv_data[-2] if len(ohlcv_data) > 1 else current
        
        # Calculate change
        price_change = current['close'] - previous['close']
        change_percent = (price_change / previous['close']) * 100
        
        market_data = {
            'symbol': symbol,
            'price': current['close'],
            'change': round(price_change, 2),
            'change_percent': round(change_percent, 2),
            'volume': current['volume'],
            'high_24h': current['high'],
            'low_24h': current['low'],
            'open_24h': current['open'],
            'timestamp': datetime.now().isoformat(),
            'market_cap': current['close'] * current['volume'] if 'BTC' in symbol else None
        }
        
        return jsonify({
            'success': True,
            'data': market_data,
            'timestamp': datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }), 400

@charting_api.route('/events/<path:symbol>')
def get_symbol_events(symbol):
    """Get corporate actions and events for a symbol"""
    # Sample events data
    events = {
        'JSE:NPN': [
            {
                'type': 'dividend',
                'date': '2025-08-15',
                'amount': 28.50,
                'currency': 'ZAR',
                'description': 'Interim dividend payment'
            },
            {
                'type': 'earnings',
                'date': '2025-07-25',
                'eps': 45.80,
                'currency': 'ZAR',
                'description': 'Q2 2025 earnings report'
            }
        ],
        'AAPL': [
            {
                'type': 'dividend',
                'date': '2025-08-10',
                'amount': 0.25,
                'currency': 'USD',
                'description': 'Quarterly dividend'
            },
            {
                'type': 'earnings',
                'date': '2025-07-28',
                'eps': 1.53,
                'currency': 'USD',
                'description': 'Q3 2025 earnings beat'
            }
        ],
        'BTC/USDT': [
            {
                'type': 'news',
                'date': '2025-07-29',
                'description': 'Bitcoin ETF approval drives institutional adoption'
            },
            {
                'type': 'news',
                'date': '2025-07-25',
                'description': 'Major mining pool implements new sustainability measures'
            }
        ]
    }
    
    symbol_events = events.get(symbol, [])
    
    return jsonify({
        'success': True,
        'symbol': symbol,
        'events': symbol_events,
        'count': len(symbol_events),
        'timestamp': datetime.now().isoformat()
    })

@charting_api.route('/news/<path:symbol>')
def get_symbol_news(symbol):
    """Get financial news for a symbol"""
    # Sample news data
    news_data = {
        'JSE:NPN': [
            {
                'title': 'Naspers reports strong Q2 growth in e-commerce segment',
                'source': 'Reuters',
                'time': '2025-07-30T08:15:00Z',
                'sentiment': 'positive',
                'relevance': 0.95
            },
            {
                'title': 'Technology stocks lead JSE gains amid global tech rally',
                'source': 'BusinessDay',
                'time': '2025-07-30T06:30:00Z',
                'sentiment': 'positive',
                'relevance': 0.78
            }
        ],
        'BTC/USDT': [
            {
                'title': 'Bitcoin breaks key resistance level as institutional demand surges',
                'source': 'CoinDesk',
                'time': '2025-07-30T09:45:00Z',
                'sentiment': 'positive',
                'relevance': 0.98
            },
            {
                'title': 'Cryptocurrency market cap reaches new all-time high',
                'source': 'CryptoNews',
                'time': '2025-07-30T07:20:00Z',
                'sentiment': 'positive',
                'relevance': 0.85
            }
        ]
    }
    
    symbol_news = news_data.get(symbol, [])
    
    return jsonify({
        'success': True,
        'symbol': symbol,
        'news': symbol_news,
        'count': len(symbol_news),
        'timestamp': datetime.now().isoformat()
    })

@charting_api.route('/screener')
def market_screener():
    """Get market screener data - top movers, most active, etc."""
    
    # Generate dynamic screener data
    import random
    
    symbols = ['JSE:NPN', 'JSE:BHP', 'JSE:SOL', 'BTC/USDT', 'ETH/USDT', 'AAPL', 'GOOGL']
    
    screener_data = {
        'top_gainers': [],
        'top_losers': [],
        'most_active': [],
        'trending': []
    }
    
    for symbol in symbols:
        change_percent = random.uniform(-8, 12)
        volume = random.randint(500000, 5000000)
        price = random.uniform(50, 5000)
        
        item = {
            'symbol': symbol,
            'price': round(price, 2),
            'change_percent': round(change_percent, 2),
            'volume': volume,
            'market_cap': round(price * volume / 1000, 2) if 'JSE' in symbol else None
        }
        
        if change_percent > 3:
            screener_data['top_gainers'].append(item)
        elif change_percent < -3:
            screener_data['top_losers'].append(item)
        
        if volume > 2000000:
            screener_data['most_active'].append(item)
        
        if abs(change_percent) > 5:
            screener_data['trending'].append(item)
    
    # Sort lists
    screener_data['top_gainers'].sort(key=lambda x: x['change_percent'], reverse=True)
    screener_data['top_losers'].sort(key=lambda x: x['change_percent'])
    screener_data['most_active'].sort(key=lambda x: x['volume'], reverse=True)
    
    return jsonify({
        'success': True,
        'data': screener_data,
        'timestamp': datetime.now().isoformat(),
        'updated': datetime.now().strftime('%H:%M:%S')
    })

@charting_api.route('/sectors')
def get_sector_performance():
    """Get sector performance data"""
    sectors = [
        {'name': 'Technology', 'change': 2.4, 'volume': 15000000},
        {'name': 'Mining', 'change': 1.8, 'volume': 12000000},
        {'name': 'Banking', 'change': -0.3, 'volume': 8000000},
        {'name': 'Energy', 'change': 3.2, 'volume': 6000000},
        {'name': 'Healthcare', 'change': 0.7, 'volume': 4000000},
        {'name': 'Retail', 'change': -1.2, 'volume': 3000000}
    ]
    
    return jsonify({
        'success': True,
        'sectors': sectors,
        'timestamp': datetime.now().isoformat()
    })

@charting_api.route('/currency-rates')
def get_currency_rates():
    """Get currency conversion rates"""
    rates = {
        'USD': 1.0,
        'ZAR': 18.52,
        'EUR': 0.92,
        'GBP': 0.80,
        'JPY': 149.45,
        'AUD': 1.51,
        'CAD': 1.35
    }
    
    return jsonify({
        'success': True,
        'base': 'USD',
        'rates': rates,
        'timestamp': datetime.now().isoformat()
    })

# Register error handlers
@charting_api.errorhandler(404)
def not_found(error):
    return jsonify({
        'success': False,
        'error': 'Endpoint not found',
        'timestamp': datetime.now().isoformat()
    }), 404

@charting_api.errorhandler(500)
def internal_error(error):
    return jsonify({
        'success': False,
        'error': 'Internal server error',
        'timestamp': datetime.now().isoformat()
    }), 500