"""
WebSocket Routes for Real-time Data Streaming

This module defines the routes and event handlers for real-time data streaming
using Flask-SocketIO.
"""

import logging
import os
from datetime import datetime
from flask import Blueprint, request, jsonify, session
from flask_socketio import emit, join_room, leave_room

from src.api.realtime import MarketDataStream, AlertStream

logger = logging.getLogger(__name__)

# Create Blueprint for real-time data API
realtime_bp = Blueprint('realtime', __name__, url_prefix='/api/realtime')

# Global instances of stream managers
# These will be initialized when the Socket.IO instance is available
market_stream = None
alert_stream = None


def init_socket_handlers(socketio, db_url=None):
    """
    Initialize Socket.IO event handlers
    
    Args:
        socketio: Flask-SocketIO instance
        db_url: Database URL for storing streaming data
    """
    global market_stream, alert_stream
    
    # Initialize stream managers with Socket.IO instance
    market_stream = MarketDataStream(socketio=socketio, db_url=db_url)
    alert_stream = AlertStream(socketio=socketio)
    
    @socketio.on('connect')
    def handle_connect():
        """Handle client connection"""
        client_id = request.sid
        logger.info(f"Client connected: {client_id}")
        emit('connection_response', {'status': 'connected', 'client_id': client_id})
    
    @socketio.on('disconnect')
    def handle_disconnect():
        """Handle client disconnection"""
        client_id = request.sid
        logger.info(f"Client disconnected: {client_id}")
        
        # Stop any streams associated with this client
        for stream_id in list(market_stream.active_streams.keys()):
            if stream_id.startswith(f"stream_{client_id}"):
                market_stream.stop_stream(stream_id)
    
    @socketio.on('subscribe')
    def handle_subscribe(data):
        """
        Handle subscription to a market data stream
        
        Expected data:
        {
            'market': 'jse',  # Market code
            'symbol': 'SOL',  # Symbol to stream
            'interval': 1     # Update interval in seconds (optional)
        }
        """
        client_id = request.sid
        market = data.get('market', 'jse').lower()
        symbol = data.get('symbol')
        interval = data.get('interval', 1)
        
        if not symbol:
            emit('error', {'message': 'Symbol is required'})
            return
        
        # Generate a unique stream ID for this subscription
        stream_id = f"stream_{client_id}_{market}_{symbol}_{int(datetime.now().timestamp())}"
        
        # Start the stream
        success = market_stream.start_stream(stream_id, market, symbol, interval)
        
        if success:
            # Create a room for this stream
            join_room(stream_id)
            
            emit('subscription_response', {
                'status': 'subscribed',
                'stream_id': stream_id,
                'market': market,
                'symbol': symbol,
                'interval': interval
            })
            
            logger.info(f"Client {client_id} subscribed to {market}:{symbol}")
        else:
            emit('error', {'message': f"Failed to start stream for {market}:{symbol}"})
    
    @socketio.on('unsubscribe')
    def handle_unsubscribe(data):
        """
        Handle unsubscription from a market data stream
        
        Expected data:
        {
            'stream_id': 'stream_x123_jse_SOL_1234567890'
        }
        """
        client_id = request.sid
        stream_id = data.get('stream_id')
        
        if not stream_id:
            emit('error', {'message': 'Stream ID is required'})
            return
        
        # Stop the stream
        success = market_stream.stop_stream(stream_id)
        
        if success:
            # Leave the room for this stream
            leave_room(stream_id)
            
            emit('unsubscription_response', {
                'status': 'unsubscribed',
                'stream_id': stream_id
            })
            
            logger.info(f"Client {client_id} unsubscribed from stream {stream_id}")
        else:
            emit('error', {'message': f"Failed to unsubscribe from stream {stream_id}"})
    
    @socketio.on('add_alert')
    def handle_add_alert(data):
        """
        Handle adding a price alert
        
        Expected data:
        {
            'market': 'jse',       # Market code
            'symbol': 'SOL',       # Symbol to monitor
            'target_price': 150.0, # Target price
            'direction': 'above'   # 'above' or 'below'
        }
        """
        client_id = request.sid
        market = data.get('market', 'jse').lower()
        symbol = data.get('symbol')
        target_price = data.get('target_price')
        direction = data.get('direction', 'above').lower()
        
        if not symbol:
            emit('error', {'message': 'Symbol is required'})
            return
            
        if not target_price:
            emit('error', {'message': 'Target price is required'})
            return
            
        if direction not in ['above', 'below']:
            emit('error', {'message': 'Direction must be "above" or "below"'})
            return
        
        # Convert target price to float
        try:
            target_price = float(target_price)
        except ValueError:
            emit('error', {'message': 'Invalid target price'})
            return
        
        # Add the alert
        alert_id = alert_stream.add_price_alert(client_id, market, symbol, target_price, direction)
        
        emit('alert_response', {
            'status': 'added',
            'alert_id': alert_id,
            'market': market,
            'symbol': symbol,
            'target_price': target_price,
            'direction': direction
        })
        
        logger.info(f"Client {client_id} added alert for {market}:{symbol} {direction} {target_price}")
    
    @socketio.on('remove_alert')
    def handle_remove_alert(data):
        """
        Handle removing a price alert
        
        Expected data:
        {
            'alert_id': 'alert_x123_jse_SOL_1234567890'
        }
        """
        client_id = request.sid
        alert_id = data.get('alert_id')
        
        if not alert_id:
            emit('error', {'message': 'Alert ID is required'})
            return
        
        # Remove the alert
        success = alert_stream.remove_alert(alert_id)
        
        if success:
            emit('alert_response', {
                'status': 'removed',
                'alert_id': alert_id
            })
            
            logger.info(f"Client {client_id} removed alert {alert_id}")
        else:
            emit('error', {'message': f"Failed to remove alert {alert_id}"})


# HTTP Routes for the real-time API
@realtime_bp.route('/streams', methods=['GET'])
def get_streams():
    """
    Get all active streams
    
    Returns:
        JSON response with active streams
    """
    global market_stream
    
    if not market_stream:
        return jsonify({
            "error": "Real-time data service not initialized"
        }), 500
    
    active_streams = market_stream.get_active_streams()
    
    return jsonify({
        "count": len(active_streams),
        "streams": active_streams
    })