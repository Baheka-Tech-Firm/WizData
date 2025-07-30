"""
Real-time WebSocket API for Live Data Streaming
Provides real-time market data, price ticks, and live updates for charting platforms
"""

from flask import Blueprint
from flask_socketio import SocketIO, emit, join_room, leave_room, rooms
import asyncio
import json
import random
import threading
import time
from datetime import datetime
from typing import Dict, List, Set

# WebSocket blueprint
websocket_api = Blueprint('websocket_api', __name__)

# Global SocketIO instance (will be initialized in app.py)
socketio = None

# Track active subscriptions
active_subscriptions: Dict[str, Set[str]] = {}  # room_id -> set of symbols
connected_clients: Set[str] = set()

def init_socketio(app):
    """Initialize SocketIO with the Flask app"""
    global socketio
    socketio = SocketIO(
        app, 
        cors_allowed_origins="*",
        async_mode='threading',
        logger=True,
        engineio_logger=True
    )
    
    register_websocket_events()
    start_price_streaming_thread()
    return socketio

def register_websocket_events():
    """Register WebSocket event handlers"""
    
    @socketio.on('connect')
    def handle_connect():
        """Handle client connection"""
        client_id = request.sid
        connected_clients.add(client_id)
        
        emit('connected', {
            'message': 'Connected to WizData real-time feed',
            'client_id': client_id,
            'timestamp': datetime.now().isoformat(),
            'available_channels': [
                'price_updates',
                'market_overview',
                'news_feed',
                'order_book'
            ]
        })
        
        print(f"Client connected: {client_id}")
    
    @socketio.on('disconnect')
    def handle_disconnect():
        """Handle client disconnection"""
        client_id = request.sid
        connected_clients.discard(client_id)
        
        # Remove from all subscriptions
        for room_symbols in active_subscriptions.values():
            room_symbols.discard(client_id)
        
        print(f"Client disconnected: {client_id}")
    
    @socketio.on('subscribe')
    def handle_subscribe(data):
        """Handle subscription to symbol updates"""
        try:
            symbols = data.get('symbols', [])
            channel = data.get('channel', 'price_updates')
            client_id = request.sid
            
            for symbol in symbols:
                room_name = f"{channel}_{symbol}"
                join_room(room_name)
                
                if room_name not in active_subscriptions:
                    active_subscriptions[room_name] = set()
                active_subscriptions[room_name].add(client_id)
            
            emit('subscription_confirmed', {
                'symbols': symbols,
                'channel': channel,
                'timestamp': datetime.now().isoformat()
            })
            
            print(f"Client {client_id} subscribed to {symbols} on {channel}")
            
        except Exception as e:
            emit('error', {
                'message': f'Subscription failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
    
    @socketio.on('unsubscribe')
    def handle_unsubscribe(data):
        """Handle unsubscription from symbol updates"""
        try:
            symbols = data.get('symbols', [])
            channel = data.get('channel', 'price_updates')
            client_id = request.sid
            
            for symbol in symbols:
                room_name = f"{channel}_{symbol}"
                leave_room(room_name)
                
                if room_name in active_subscriptions:
                    active_subscriptions[room_name].discard(client_id)
            
            emit('unsubscription_confirmed', {
                'symbols': symbols,
                'channel': channel,
                'timestamp': datetime.now().isoformat()
            })
            
        except Exception as e:
            emit('error', {
                'message': f'Unsubscription failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })
    
    @socketio.on('request_snapshot')
    def handle_snapshot_request(data):
        """Handle request for current market snapshot"""
        try:
            symbol = data.get('symbol')
            
            if symbol:
                snapshot = generate_market_snapshot(symbol)
                emit('market_snapshot', snapshot)
            else:
                emit('error', {
                    'message': 'Symbol required for snapshot',
                    'timestamp': datetime.now().isoformat()
                })
                
        except Exception as e:
            emit('error', {
                'message': f'Snapshot request failed: {str(e)}',
                'timestamp': datetime.now().isoformat()
            })

def generate_market_snapshot(symbol: str) -> Dict:
    """Generate current market snapshot for a symbol"""
    base_prices = {
        'BTC/USDT': 67500,
        'ETH/USDT': 3750,
        'AAPL': 185,
        'JSE:NPN': 2850,
        'USD/ZAR': 18.50
    }
    
    base_price = base_prices.get(symbol, 100)
    
    # Generate realistic market data
    price_change = random.uniform(-0.02, 0.02)
    current_price = base_price * (1 + price_change)
    
    return {
        'symbol': symbol,
        'price': round(current_price, 2),
        'change': round(base_price * price_change, 2),
        'change_percent': round(price_change * 100, 2),
        'volume': random.randint(500000, 5000000),
        'high_24h': round(current_price * 1.05, 2),
        'low_24h': round(current_price * 0.95, 2),
        'bid': round(current_price * 0.9995, 2),
        'ask': round(current_price * 1.0005, 2),
        'timestamp': datetime.now().isoformat(),
        'type': 'snapshot'
    }

def generate_price_tick(symbol: str, last_price: float) -> Dict:
    """Generate realistic price tick for a symbol"""
    # Small price movement (±0.1%)
    change_percent = random.uniform(-0.001, 0.001)
    new_price = last_price * (1 + change_percent)
    
    return {
        'symbol': symbol,
        'price': round(new_price, 2),
        'change': round(new_price - last_price, 2),
        'volume': random.randint(1000, 50000),
        'timestamp': datetime.now().isoformat(),
        'type': 'tick'
    }

def start_price_streaming_thread():
    """Start background thread for streaming live price updates"""
    
    def price_streaming_worker():
        """Background worker for price streaming"""
        symbols = ['BTC/USDT', 'ETH/USDT', 'AAPL', 'JSE:NPN', 'USD/ZAR', 'EUR/USD']
        
        # Initialize last prices
        last_prices = {
            'BTC/USDT': 67500,
            'ETH/USDT': 3750,
            'AAPL': 185,
            'JSE:NPN': 2850,
            'USD/ZAR': 18.50,
            'EUR/USD': 1.08
        }
        
        while True:
            try:
                if socketio and connected_clients:
                    for symbol in symbols:
                        room_name = f"price_updates_{symbol}"
                        
                        if room_name in active_subscriptions and active_subscriptions[room_name]:
                            # Generate price tick
                            tick_data = generate_price_tick(symbol, last_prices[symbol])
                            last_prices[symbol] = tick_data['price']
                            
                            # Emit to subscribed clients
                            socketio.emit('price_tick', tick_data, room=room_name)
                
                # Sleep for realistic tick frequency
                time.sleep(random.uniform(1, 3))  # 1-3 seconds between ticks
                
            except Exception as e:
                print(f"Price streaming error: {e}")
                time.sleep(5)
    
    # Start background thread
    streaming_thread = threading.Thread(target=price_streaming_worker, daemon=True)
    streaming_thread.start()

def broadcast_news_update(news_item: Dict):
    """Broadcast news update to all connected clients"""
    if socketio:
        socketio.emit('news_update', {
            'title': news_item.get('title', ''),
            'source': news_item.get('source', ''),
            'symbol': news_item.get('symbol', ''),
            'sentiment': news_item.get('sentiment', 'neutral'),
            'timestamp': datetime.now().isoformat(),
            'type': 'news'
        }, room='news_feed')

def broadcast_market_alert(alert_data: Dict):
    """Broadcast market alert to all connected clients"""
    if socketio:
        socketio.emit('market_alert', {
            'message': alert_data.get('message', ''),
            'symbol': alert_data.get('symbol', ''),
            'severity': alert_data.get('severity', 'info'),
            'timestamp': datetime.now().isoformat(),
            'type': 'alert'
        })

# WebSocket route decorators for Flask integration
@websocket_api.route('/ws-status')
def websocket_status():
    """Get WebSocket service status"""
    return {
        'websocket_enabled': socketio is not None,
        'connected_clients': len(connected_clients),
        'active_subscriptions': len(active_subscriptions),
        'subscription_rooms': list(active_subscriptions.keys()),
        'timestamp': datetime.now().isoformat()
    }

@websocket_api.route('/ws-test')
def websocket_test():
    """Test WebSocket broadcasting"""
    if socketio:
        test_data = {
            'message': 'WebSocket test broadcast',
            'timestamp': datetime.now().isoformat(),
            'test': True
        }
        socketio.emit('test_message', test_data)
        
        return {
            'success': True,
            'message': 'Test broadcast sent',
            'connected_clients': len(connected_clients)
        }
    else:
        return {
            'success': False,
            'message': 'WebSocket not initialized'
        }, 500