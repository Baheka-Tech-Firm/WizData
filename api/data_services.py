"""
Modular Data Services for WizData Ecosystem
Implements the microservices architecture for supporting multiple products
"""

from flask import Blueprint, request, jsonify, g
from datetime import datetime, timedelta
import json
from typing import Dict, List, Any, Optional

# Create blueprint for data services
data_services_api = Blueprint('data_services_api', __name__, url_prefix='/api/v1/data-services')

class MarketDataService:
    """Market data service for OHLCV, quotes, and real-time data"""
    
    @staticmethod
    def get_market_data(symbols: List[str], data_type: str = 'quote') -> Dict[str, Any]:
        """Get market data for multiple symbols"""
        data = {}
        
        for symbol in symbols:
            if data_type == 'quote':
                data[symbol] = MarketDataService.get_current_quote(symbol)
            elif data_type == 'ohlcv':
                data[symbol] = MarketDataService.get_ohlcv_data(symbol)
            elif data_type == 'fundamentals':
                data[symbol] = MarketDataService.get_fundamentals(symbol)
        
        return {
            'success': True,
            'data': data,
            'timestamp': datetime.now().isoformat(),
            'count': len(data)
        }
    
    @staticmethod
    def get_current_quote(symbol: str) -> Dict[str, Any]:
        """Get current quote for a symbol"""
        # Integration point for real data sources
        import random
        
        base_prices = {
            'JSE:NPN': 285000,
            'JSE:BHP': 42000,
            'JSE:SOL': 12500,
            'BTC/USDT': 67500,
            'EUR/USD': 1.08
        }
        
        base_price = base_prices.get(symbol, 10000)
        change = random.uniform(-0.05, 0.05)
        current_price = base_price * (1 + change)
        
        return {
            'symbol': symbol,
            'price': round(current_price, 2),
            'change': round(base_price * change, 2),
            'change_percent': round(change * 100, 2),
            'volume': random.randint(100000, 2000000),
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def get_ohlcv_data(symbol: str, interval: str = '1d', limit: int = 30) -> List[Dict[str, Any]]:
        """Get OHLCV data for charting"""
        import random
        from datetime import datetime, timedelta
        
        base_price = 10000
        data = []
        
        for i in range(limit):
            timestamp = datetime.now() - timedelta(days=limit-i)
            change = random.uniform(-0.02, 0.02)
            
            open_price = base_price
            close_price = base_price * (1 + change)
            high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.01))
            low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.01))
            
            data.append({
                'time': int(timestamp.timestamp()),
                'open': round(open_price, 2),
                'high': round(high_price, 2),
                'low': round(low_price, 2),
                'close': round(close_price, 2),
                'volume': random.randint(500000, 2000000)
            })
            
            base_price = close_price
        
        return data
    
    @staticmethod
    def get_fundamentals(symbol: str) -> Dict[str, Any]:
        """Get fundamental data for a symbol"""
        import random
        
        return {
            'symbol': symbol,
            'market_cap': random.randint(1000000, 100000000),
            'pe_ratio': round(random.uniform(8, 25), 2),
            'dividend_yield': round(random.uniform(0, 8), 2),
            'eps': round(random.uniform(50, 2000), 2),
            'book_value': round(random.uniform(5000, 50000), 2),
            'debt_to_equity': round(random.uniform(0.2, 2.0), 2),
            'roe': round(random.uniform(5, 25), 2),
            'timestamp': datetime.now().isoformat()
        }

class IndicatorEngine:
    """Technical indicators calculation service"""
    
    @staticmethod
    def calculate_indicators(symbol: str, indicators: List[str]) -> Dict[str, Any]:
        """Calculate multiple technical indicators"""
        ohlcv_data = MarketDataService.get_ohlcv_data(symbol, limit=50)
        closes = [candle['close'] for candle in ohlcv_data]
        
        results = {}
        
        for indicator in indicators:
            if indicator.upper() == 'RSI':
                results['rsi'] = IndicatorEngine.calculate_rsi(closes)
            elif indicator.upper() == 'MACD':
                results['macd'] = IndicatorEngine.calculate_macd(closes)
            elif indicator.upper() == 'SMA':
                results['sma'] = IndicatorEngine.calculate_sma(closes, 20)
            elif indicator.upper() == 'EMA':
                results['ema'] = IndicatorEngine.calculate_ema(closes, 20)
            elif indicator.upper() == 'BOLLINGER':
                results['bollinger'] = IndicatorEngine.calculate_bollinger_bands(closes)
        
        return {
            'success': True,
            'symbol': symbol,
            'indicators': results,
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def calculate_rsi(closes: List[float], period: int = 14) -> List[float]:
        """Calculate RSI indicator"""
        if len(closes) < period + 1:
            return []
        
        gains = []
        losses = []
        
        for i in range(1, len(closes)):
            change = closes[i] - closes[i-1]
            if change > 0:
                gains.append(change)
                losses.append(0)
            else:
                gains.append(0)
                losses.append(abs(change))
        
        rsi_values = []
        
        for i in range(period - 1, len(gains)):
            avg_gain = sum(gains[i-period+1:i+1]) / period
            avg_loss = sum(losses[i-period+1:i+1]) / period
            
            if avg_loss == 0:
                rsi = 100
            else:
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
            
            rsi_values.append(round(rsi, 2))
        
        return rsi_values
    
    @staticmethod
    def calculate_macd(closes: List[float]) -> Dict[str, List[float]]:
        """Calculate MACD indicator"""
        if len(closes) < 26:
            return {'line': [], 'signal': [], 'histogram': []}
        
        # Calculate EMAs
        ema_12 = IndicatorEngine.calculate_ema(closes, 12)
        ema_26 = IndicatorEngine.calculate_ema(closes, 26)
        
        # MACD line
        macd_line = []
        for i in range(len(ema_26)):
            if i < len(ema_12):
                macd_line.append(ema_12[i] - ema_26[i])
        
        # Signal line (9-day EMA of MACD)
        signal_line = IndicatorEngine.calculate_ema(macd_line, 9)
        
        # Histogram
        histogram = []
        for i in range(len(signal_line)):
            if i < len(macd_line):
                histogram.append(macd_line[i] - signal_line[i])
        
        return {
            'line': [round(x, 4) for x in macd_line[-20:]],
            'signal': [round(x, 4) for x in signal_line[-20:]],
            'histogram': [round(x, 4) for x in histogram[-20:]]
        }
    
    @staticmethod
    def calculate_sma(closes: List[float], period: int) -> List[float]:
        """Calculate Simple Moving Average"""
        sma_values = []
        
        for i in range(period - 1, len(closes)):
            sma = sum(closes[i-period+1:i+1]) / period
            sma_values.append(round(sma, 2))
        
        return sma_values
    
    @staticmethod
    def calculate_ema(closes: List[float], period: int) -> List[float]:
        """Calculate Exponential Moving Average"""
        if len(closes) < period:
            return []
        
        multiplier = 2 / (period + 1)
        ema_values = [closes[0]]  # Start with first price
        
        for i in range(1, len(closes)):
            ema = (closes[i] * multiplier) + (ema_values[-1] * (1 - multiplier))
            ema_values.append(ema)
        
        return [round(x, 2) for x in ema_values]
    
    @staticmethod
    def calculate_bollinger_bands(closes: List[float], period: int = 20, std_dev: int = 2) -> Dict[str, List[float]]:
        """Calculate Bollinger Bands"""
        import statistics
        
        if len(closes) < period:
            return {'upper': [], 'middle': [], 'lower': []}
        
        upper_band = []
        middle_band = []
        lower_band = []
        
        for i in range(period - 1, len(closes)):
            slice_data = closes[i-period+1:i+1]
            sma = sum(slice_data) / period
            std = statistics.stdev(slice_data)
            
            upper_band.append(round(sma + (std_dev * std), 2))
            middle_band.append(round(sma, 2))
            lower_band.append(round(sma - (std_dev * std), 2))
        
        return {
            'upper': upper_band,
            'middle': middle_band,
            'lower': lower_band
        }

class EventEngine:
    """Corporate events and announcements service"""
    
    @staticmethod
    def get_events(symbol: str = None, event_type: str = None, days_ahead: int = 30) -> Dict[str, Any]:
        """Get upcoming corporate events"""
        events = EventEngine.generate_sample_events()
        
        # Filter by symbol if provided
        if symbol:
            events = [e for e in events if e.get('symbol') == symbol]
        
        # Filter by event type if provided
        if event_type:
            events = [e for e in events if e.get('type') == event_type]
        
        return {
            'success': True,
            'events': events,
            'count': len(events),
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def generate_sample_events() -> List[Dict[str, Any]]:
        """Generate sample corporate events"""
        from datetime import datetime, timedelta
        
        events = [
            {
                'symbol': 'JSE:NPN',
                'type': 'earnings',
                'date': (datetime.now() + timedelta(days=7)).strftime('%Y-%m-%d'),
                'description': 'Quarterly earnings announcement',
                'expected_eps': 125.50,
                'currency': 'ZAR'
            },
            {
                'symbol': 'JSE:SBK',
                'type': 'dividend',
                'date': (datetime.now() + timedelta(days=14)).strftime('%Y-%m-%d'),
                'description': 'Ex-dividend date',
                'dividend_amount': 890,
                'currency': 'ZAR'
            },
            {
                'symbol': 'JSE:MTN',
                'type': 'agm',
                'date': (datetime.now() + timedelta(days=21)).strftime('%Y-%m-%d'),
                'description': 'Annual General Meeting',
                'time': '10:00 AM SAST'
            }
        ]
        
        return events

class MetadataService:
    """Company and instrument metadata service"""
    
    @staticmethod
    def get_company_profile(symbol: str) -> Dict[str, Any]:
        """Get comprehensive company profile"""
        profiles = {
            'JSE:NPN': {
                'symbol': 'JSE:NPN',
                'name': 'Naspers Limited',
                'description': 'Naspers is a global consumer internet group and technology investor.',
                'sector': 'Technology',
                'industry': 'Internet Services',
                'employees': 28000,
                'founded': 1915,
                'headquarters': 'Cape Town, South Africa',
                'website': 'https://www.naspers.com',
                'ceo': 'Fabricio Bloisi',
                'market_cap': 1500000000000,  # ZAR cents
                'shares_outstanding': 430000000
            },
            'JSE:BHP': {
                'symbol': 'JSE:BHP',
                'name': 'BHP Group Limited',
                'description': 'BHP is a leading global resources company.',
                'sector': 'Mining',
                'industry': 'Diversified Mining',
                'employees': 80000,
                'founded': 1885,
                'headquarters': 'Melbourne, Australia',
                'website': 'https://www.bhp.com',
                'ceo': 'Mike Henry',
                'market_cap': 850000000000,
                'shares_outstanding': 507000000
            }
        }
        
        profile = profiles.get(symbol, {
            'symbol': symbol,
            'name': f'{symbol} Company',
            'description': 'Company information not available',
            'sector': 'Unknown',
            'industry': 'Unknown'
        })
        
        return {
            'success': True,
            'profile': profile,
            'timestamp': datetime.now().isoformat()
        }

class ScreenerEngine:
    """Market screening and filtering service"""
    
    @staticmethod
    def run_screen(criteria: Dict[str, Any]) -> Dict[str, Any]:
        """Run market screen with specified criteria"""
        # Generate sample screening results
        results = ScreenerEngine.generate_screen_results(criteria)
        
        return {
            'success': True,
            'criteria': criteria,
            'results': results,
            'count': len(results),
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def generate_screen_results(criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate sample screening results"""
        import random
        
        symbols = ['JSE:NPN', 'JSE:BHP', 'JSE:SOL', 'JSE:SBK', 'JSE:MTN']
        results = []
        
        for symbol in symbols:
            # Apply basic filtering logic
            market_cap = random.randint(100000000, 2000000000)
            pe_ratio = round(random.uniform(5, 30), 2)
            dividend_yield = round(random.uniform(0, 8), 2)
            
            # Check criteria
            include = True
            if criteria.get('min_market_cap') and market_cap < criteria['min_market_cap']:
                include = False
            if criteria.get('max_pe_ratio') and pe_ratio > criteria['max_pe_ratio']:
                include = False
            if criteria.get('min_dividend_yield') and dividend_yield < criteria['min_dividend_yield']:
                include = False
            
            if include:
                results.append({
                    'symbol': symbol,
                    'market_cap': market_cap,
                    'pe_ratio': pe_ratio,
                    'dividend_yield': dividend_yield,
                    'score': round(random.uniform(60, 95), 1)
                })
        
        return results

class NewsService:
    """Financial news and sentiment service"""
    
    @staticmethod
    def get_news(symbol: str = None, category: str = None, limit: int = 20) -> Dict[str, Any]:
        """Get financial news with optional filtering"""
        news_items = NewsService.generate_sample_news()
        
        # Filter by symbol if provided
        if symbol:
            news_items = [n for n in news_items if symbol in n.get('related_symbols', [])]
        
        # Filter by category if provided
        if category:
            news_items = [n for n in news_items if n.get('category') == category]
        
        # Apply limit
        news_items = news_items[:limit]
        
        return {
            'success': True,
            'news': news_items,
            'count': len(news_items),
            'timestamp': datetime.now().isoformat()
        }
    
    @staticmethod
    def generate_sample_news() -> List[Dict[str, Any]]:
        """Generate sample financial news"""
        news = [
            {
                'id': 'news_001',
                'title': 'JSE sees strong trading volumes as investors return',
                'summary': 'The Johannesburg Stock Exchange recorded strong trading volumes this week...',
                'source': 'Business Day',
                'category': 'market',
                'sentiment': 'positive',
                'related_symbols': ['JSE:NPN', 'JSE:BHP', 'JSE:SOL'],
                'published_at': datetime.now().isoformat(),
                'url': 'https://example.com/news/1'
            },
            {
                'id': 'news_002',
                'title': 'Mining sector shows resilience amid global uncertainty',
                'summary': 'South African mining companies continue to perform well...',
                'source': 'Mining Weekly',
                'category': 'sector',
                'sentiment': 'positive',
                'related_symbols': ['JSE:BHP', 'JSE:AGL'],
                'published_at': (datetime.now() - timedelta(hours=2)).isoformat(),
                'url': 'https://example.com/news/2'
            }
        ]
        
        return news

# API Routes
@data_services_api.route('/market-data')
def get_market_data():
    """Get market data for specified symbols"""
    symbols = request.args.getlist('symbols')
    data_type = request.args.get('type', 'quote')
    
    if not symbols:
        return jsonify({
            'success': False,
            'error': 'No symbols provided'
        }), 400
    
    return jsonify(MarketDataService.get_market_data(symbols, data_type))

@data_services_api.route('/indicators/<path:symbol>')
def get_indicators(symbol):
    """Get technical indicators for a symbol"""
    indicators = request.args.getlist('indicators')
    
    if not indicators:
        indicators = ['RSI', 'MACD', 'SMA']
    
    return jsonify(IndicatorEngine.calculate_indicators(symbol, indicators))

@data_services_api.route('/events')
def get_events():
    """Get corporate events"""
    symbol = request.args.get('symbol')
    event_type = request.args.get('type')
    days_ahead = int(request.args.get('days_ahead', 30))
    
    return jsonify(EventEngine.get_events(symbol, event_type, days_ahead))

@data_services_api.route('/profile/<path:symbol>')
def get_company_profile(symbol):
    """Get company profile"""
    return jsonify(MetadataService.get_company_profile(symbol))

@data_services_api.route('/screener')
def run_screener():
    """Run market screener"""
    criteria = {}
    
    # Parse criteria from query parameters
    if request.args.get('min_market_cap'):
        criteria['min_market_cap'] = int(request.args.get('min_market_cap'))
    if request.args.get('max_pe_ratio'):
        criteria['max_pe_ratio'] = float(request.args.get('max_pe_ratio'))
    if request.args.get('min_dividend_yield'):
        criteria['min_dividend_yield'] = float(request.args.get('min_dividend_yield'))
    
    return jsonify(ScreenerEngine.run_screen(criteria))

@data_services_api.route('/news')
def get_news():
    """Get financial news"""
    symbol = request.args.get('symbol')
    category = request.args.get('category')
    limit = int(request.args.get('limit', 20))
    
    return jsonify(NewsService.get_news(symbol, category, limit))

@data_services_api.route('/health')
def health_check():
    """Health check for data services"""
    return jsonify({
        'success': True,
        'status': 'healthy',
        'services': {
            'market_data': 'operational',
            'indicators': 'operational',
            'events': 'operational',
            'metadata': 'operational',
            'screener': 'operational',
            'news': 'operational'
        },
        'timestamp': datetime.now().isoformat()
    })