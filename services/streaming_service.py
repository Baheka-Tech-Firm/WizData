"""
Streaming Service - High-Performance Real-time Data Streaming
Quart-based async WebSocket service for real-time market data
"""

from quart import Quart, websocket, request, jsonify
from quart_cors import cors
import asyncio
import aiohttp
import json
import redis
from datetime import datetime, timedelta
from typing import Dict, List, Set, Any, Optional
import uuid
import os
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Quart app with CORS
app = Quart(__name__)
app = cors(app, allow_origin="*")

class StreamingService:
    """High-performance real-time data streaming service"""
    
    def __init__(self):
        self.redis_client = None
        self.session = None
        
        # Connection management
        self.connections: Dict[str, Dict] = {}  # connection_id -> connection_info
        self.subscriptions: Dict[str, Set[str]] = {}  # symbol -> set of connection_ids
        
        # Service URLs
        self.market_data_url = os.getenv('MARKET_DATA_SERVICE_URL', 'http://localhost:5001')
        self.symbol_registry_url = os.getenv('SYMBOL_REGISTRY_SERVICE_URL', 'http://localhost:5002')
        
        # Streaming configuration
        self.update_interval = 1.0  # seconds
        self.max_connections = 1000
        self.heartbeat_interval = 30.0  # seconds
        
        # Initialize Redis
        try:
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()
            logger.info("Redis connected for streaming service")
        except Exception as e:
            logger.warning(f"Redis not available: {e}")
            self.redis_client = None
        
        # Background tasks will be started in setup method
    
    async def setup(self):
        """Setup async HTTP session"""
        if not self.session:
            self.session = aiohttp.ClientSession()
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
    
    async def add_connection(self, connection_id: str, websocket_obj) -> bool:
        """Add new WebSocket connection"""
        if len(self.connections) >= self.max_connections:
            logger.warning(f"Max connections reached: {self.max_connections}")
            return False
        
        self.connections[connection_id] = {
            'websocket': websocket_obj,
            'subscriptions': set(),
            'last_ping': datetime.now(),
            'connected_at': datetime.now()
        }
        
        logger.info(f"New connection added: {connection_id}")
        return True
    
    async def remove_connection(self, connection_id: str):
        """Remove WebSocket connection and cleanup subscriptions"""
        if connection_id in self.connections:
            # Remove from all symbol subscriptions
            connection_info = self.connections[connection_id]
            for symbol in connection_info['subscriptions']:
                if symbol in self.subscriptions:
                    self.subscriptions[symbol].discard(connection_id)
                    if not self.subscriptions[symbol]:
                        del self.subscriptions[symbol]
            
            del self.connections[connection_id]
            logger.info(f"Connection removed: {connection_id}")
    
    async def subscribe_to_symbol(self, connection_id: str, symbol: str) -> bool:
        """Subscribe connection to symbol updates"""
        if connection_id not in self.connections:
            return False
        
        # Validate symbol exists
        if not await self.validate_symbol(symbol):
            logger.warning(f"Invalid symbol subscription: {symbol}")
            return False
        
        # Add to subscriptions
        if symbol not in self.subscriptions:
            self.subscriptions[symbol] = set()
        
        self.subscriptions[symbol].add(connection_id)
        self.connections[connection_id]['subscriptions'].add(symbol)
        
        logger.info(f"Connection {connection_id} subscribed to {symbol}")
        return True
    
    async def unsubscribe_from_symbol(self, connection_id: str, symbol: str):
        """Unsubscribe connection from symbol updates"""
        if connection_id in self.connections:
            self.connections[connection_id]['subscriptions'].discard(symbol)
        
        if symbol in self.subscriptions:
            self.subscriptions[symbol].discard(connection_id)
            if not self.subscriptions[symbol]:
                del self.subscriptions[symbol]
        
        logger.info(f"Connection {connection_id} unsubscribed from {symbol}")
    
    async def validate_symbol(self, symbol: str) -> bool:
        """Validate symbol against registry service"""
        await self.setup()
        
        try:
            url = f"{self.symbol_registry_url}/symbols/{symbol}"
            async with self.session.get(url) as response:
                return response.status == 200
        except:
            # Fallback validation
            return True  # Allow all symbols for now
    
    async def get_market_data(self, symbols: List[str]) -> Dict[str, Any]:
        """Get market data for multiple symbols"""
        await self.setup()
        
        try:
            url = f"{self.market_data_url}/quotes"
            params = [('symbols', symbol) for symbol in symbols]
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', {})
        except Exception as e:
            logger.error(f"Error fetching market data: {e}")
        
        return {}
    
    async def broadcast_to_subscribers(self, symbol: str, data: Dict[str, Any]):
        """Broadcast market data to all subscribers of a symbol"""
        if symbol not in self.subscriptions:
            return
        
        message = {
            'type': 'market_data',
            'symbol': symbol,
            'data': data,
            'timestamp': datetime.now().isoformat()
        }
        
        # Get list of connections to avoid modification during iteration
        connection_ids = list(self.subscriptions[symbol])
        
        for connection_id in connection_ids:
            if connection_id in self.connections:
                try:
                    websocket_obj = self.connections[connection_id]['websocket']
                    await websocket_obj.send(json.dumps(message))
                except Exception as e:
                    logger.warning(f"Failed to send to {connection_id}: {e}")
                    # Remove broken connection
                    await self.remove_connection(connection_id)
    
    async def start_data_streaming(self):
        """Main data streaming loop"""
        logger.info("Starting data streaming loop")
        
        while True:
            try:
                if self.subscriptions:
                    # Get all subscribed symbols
                    symbols = list(self.subscriptions.keys())
                    
                    if symbols:
                        # Fetch market data
                        market_data = await self.get_market_data(symbols)
                        
                        # Broadcast to subscribers
                        for symbol, data in market_data.items():
                            await self.broadcast_to_subscribers(symbol, data)
                
                await asyncio.sleep(self.update_interval)
                
            except Exception as e:
                logger.error(f"Error in streaming loop: {e}")
                await asyncio.sleep(5)  # Wait before retrying
    
    async def heartbeat_task(self):
        """Send heartbeat to all connections"""
        while True:
            try:
                await asyncio.sleep(self.heartbeat_interval)
                
                heartbeat_message = {
                    'type': 'heartbeat',
                    'timestamp': datetime.now().isoformat(),
                    'connections': len(self.connections),
                    'subscriptions': len(self.subscriptions)
                }
                
                # Send to all connections
                connection_ids = list(self.connections.keys())
                for connection_id in connection_ids:
                    if connection_id in self.connections:
                        try:
                            websocket_obj = self.connections[connection_id]['websocket']
                            await websocket_obj.send(json.dumps(heartbeat_message))
                            self.connections[connection_id]['last_ping'] = datetime.now()
                        except:
                            await self.remove_connection(connection_id)
                
            except Exception as e:
                logger.error(f"Error in heartbeat task: {e}")
    
    async def handle_client_message(self, connection_id: str, message: str):
        """Handle incoming client messages"""
        try:
            data = json.loads(message)
            action = data.get('action')
            
            if action == 'subscribe':
                symbols = data.get('symbols', [])
                for symbol in symbols:
                    await self.subscribe_to_symbol(connection_id, symbol)
                
                # Send confirmation
                response = {
                    'type': 'subscription_confirmed',
                    'symbols': symbols,
                    'timestamp': datetime.now().isoformat()
                }
                websocket_obj = self.connections[connection_id]['websocket']
                await websocket_obj.send(json.dumps(response))
            
            elif action == 'unsubscribe':
                symbols = data.get('symbols', [])
                for symbol in symbols:
                    await self.unsubscribe_from_symbol(connection_id, symbol)
                
                # Send confirmation
                response = {
                    'type': 'unsubscription_confirmed',
                    'symbols': symbols,
                    'timestamp': datetime.now().isoformat()
                }
                websocket_obj = self.connections[connection_id]['websocket']
                await websocket_obj.send(json.dumps(response))
            
            elif action == 'ping':
                # Respond with pong
                pong_message = {
                    'type': 'pong',
                    'timestamp': datetime.now().isoformat()
                }
                websocket_obj = self.connections[connection_id]['websocket']
                await websocket_obj.send(json.dumps(pong_message))
        
        except Exception as e:
            logger.error(f"Error handling client message: {e}")

# Service instance
streaming_service = StreamingService()

# WebSocket endpoint
@app.websocket('/ws')
async def websocket_endpoint():
    """Main WebSocket endpoint for real-time data streaming"""
    connection_id = str(uuid.uuid4())
    
    # Add connection
    if not await streaming_service.add_connection(connection_id, websocket._get_current_object()):
        await websocket.close(code=1013, reason="Server overloaded")
        return
    
    try:
        # Send welcome message
        welcome_message = {
            'type': 'welcome',
            'connection_id': connection_id,
            'timestamp': datetime.now().isoformat(),
            'message': 'Connected to WizData streaming service'
        }
        await websocket.send(json.dumps(welcome_message))
        
        # Handle incoming messages
        async for message in websocket:
            await streaming_service.handle_client_message(connection_id, message)
    
    except Exception as e:
        logger.error(f"WebSocket error for {connection_id}: {e}")
    
    finally:
        await streaming_service.remove_connection(connection_id)

# REST API endpoints
@app.route('/status')
async def get_status():
    """Get streaming service status"""
    return jsonify({
        'success': True,
        'status': 'operational',
        'connections': len(streaming_service.connections),
        'active_subscriptions': len(streaming_service.subscriptions),
        'subscribed_symbols': list(streaming_service.subscriptions.keys()),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/connections')
async def get_connections():
    """Get active connections info"""
    connections_info = []
    
    for conn_id, conn_info in streaming_service.connections.items():
        connections_info.append({
            'connection_id': conn_id,
            'subscriptions': list(conn_info['subscriptions']),
            'connected_at': conn_info['connected_at'].isoformat(),
            'last_ping': conn_info['last_ping'].isoformat()
        })
    
    return jsonify({
        'success': True,
        'connections': connections_info,
        'total_connections': len(connections_info),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/subscriptions')
async def get_subscriptions():
    """Get active subscriptions by symbol"""
    subscriptions_info = {}
    
    for symbol, connection_ids in streaming_service.subscriptions.items():
        subscriptions_info[symbol] = {
            'subscriber_count': len(connection_ids),
            'connection_ids': list(connection_ids)
        }
    
    return jsonify({
        'success': True,
        'subscriptions': subscriptions_info,
        'total_symbols': len(subscriptions_info),
        'timestamp': datetime.now().isoformat()
    })

@app.route('/broadcast', methods=['POST'])
async def broadcast_message():
    """Broadcast custom message to all connections"""
    data = await request.get_json()
    
    message = {
        'type': 'broadcast',
        'data': data,
        'timestamp': datetime.now().isoformat()
    }
    
    # Send to all connections
    sent_count = 0
    for connection_id, connection_info in streaming_service.connections.items():
        try:
            websocket_obj = connection_info['websocket']
            await websocket_obj.send(json.dumps(message))
            sent_count += 1
        except:
            pass
    
    return jsonify({
        'success': True,
        'message': 'Broadcast sent',
        'sent_to': sent_count,
        'timestamp': datetime.now().isoformat()
    })

@app.route('/health')
async def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'healthy',
        'service': 'streaming',
        'timestamp': datetime.now().isoformat(),
        'uptime_seconds': (datetime.now() - datetime.now()).total_seconds()
    })

@app.before_serving
async def startup():
    """Setup on startup"""
    await streaming_service.setup()
    # Start background tasks
    asyncio.create_task(streaming_service.start_data_streaming())
    asyncio.create_task(streaming_service.heartbeat_task())

@app.after_serving
async def shutdown():
    """Cleanup on shutdown"""
    await streaming_service.cleanup()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5004, debug=True)