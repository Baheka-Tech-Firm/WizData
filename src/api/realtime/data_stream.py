"""
Real-time Data Stream Module

This module provides the functionality for streaming real-time financial data
using WebSockets. It includes:
1. Market data streaming
2. Price updates
3. Trade notifications
4. Alert triggers
"""

import asyncio
import json
import logging
import random
import time
from datetime import datetime
from threading import Thread

import aiohttp
import asyncpg

logger = logging.getLogger(__name__)

class MarketDataStream:
    """
    MarketDataStream class handles the streaming of real-time market data
    via WebSockets.
    """
    
    def __init__(self, socketio=None, db_url=None):
        """
        Initialize the MarketDataStream with a Flask-SocketIO instance
        
        Args:
            socketio: The Flask-SocketIO instance for emitting events
            db_url: Database URL for storing streaming data
        """
        self.socketio = socketio
        self.db_url = db_url
        self.active_streams = {}
        self.stream_threads = {}
        self.should_stop = {}
        
    def start_stream(self, stream_id, market, symbol, interval=1):
        """
        Start a real-time data stream for the specified market and symbol
        
        Args:
            stream_id: Unique identifier for this stream
            market: Market code (jse, crypto, forex, etc.)
            symbol: Symbol to stream
            interval: Update interval in seconds
            
        Returns:
            bool: True if stream started successfully, False otherwise
        """
        try:
            if stream_id in self.active_streams:
                logger.warning(f"Stream {stream_id} already exists. Stopping existing stream.")
                self.stop_stream(stream_id)
                
            self.should_stop[stream_id] = False
            self.active_streams[stream_id] = {
                'market': market,
                'symbol': symbol,
                'interval': interval,
                'start_time': datetime.now().isoformat(),
                'updates': 0
            }
            
            # Start a thread to simulate real-time updates
            # In production, this would connect to a real data source
            thread = Thread(target=self._stream_data, args=(stream_id, market, symbol, interval))
            thread.daemon = True
            thread.start()
            
            self.stream_threads[stream_id] = thread
            
            logger.info(f"Started data stream {stream_id} for {market}:{symbol}")
            return True
            
        except Exception as e:
            logger.error(f"Error starting stream {stream_id}: {str(e)}")
            return False
    
    def stop_stream(self, stream_id):
        """
        Stop a running data stream
        
        Args:
            stream_id: ID of the stream to stop
            
        Returns:
            bool: True if stream stopped successfully, False otherwise
        """
        if stream_id not in self.active_streams:
            logger.warning(f"Stream {stream_id} not found")
            return False
            
        self.should_stop[stream_id] = True
        
        # Wait for the thread to finish
        if stream_id in self.stream_threads and self.stream_threads[stream_id].is_alive():
            self.stream_threads[stream_id].join(timeout=2.0)
            
        # Clean up
        if stream_id in self.active_streams:
            del self.active_streams[stream_id]
        if stream_id in self.stream_threads:
            del self.stream_threads[stream_id]
        if stream_id in self.should_stop:
            del self.should_stop[stream_id]
            
        logger.info(f"Stopped data stream {stream_id}")
        return True
    
    def get_active_streams(self):
        """
        Get a list of all active streams
        
        Returns:
            dict: Dictionary of active streams and their details
        """
        return self.active_streams
    
    def _stream_data(self, stream_id, market, symbol, interval):
        """
        Internal method to simulate real-time data streaming
        In a production environment, this would connect to a real market data source
        
        Args:
            stream_id: Stream identifier
            market: Market code
            symbol: Symbol to stream
            interval: Update interval in seconds
        """
        try:
            # Initial price - in production this would come from real data
            last_price = self._get_initial_price(market, symbol)
            
            while not self.should_stop.get(stream_id, True):
                # Generate a new price update
                # In production, fetch real data from an exchange API
                price_data = self._generate_price_update(market, symbol, last_price)
                last_price = price_data['price']
                
                # Increment update counter
                if stream_id in self.active_streams:
                    self.active_streams[stream_id]['updates'] += 1
                
                # Emit the update via SocketIO
                if self.socketio:
                    self.socketio.emit('price_update', {
                        'stream_id': stream_id,
                        'data': price_data
                    })
                
                # Store in database if configured (async operation)
                if self.db_url:
                    asyncio.run(self._store_price_update(price_data))
                    
                # Sleep for the specified interval
                time.sleep(interval)
                
        except Exception as e:
            logger.error(f"Error in stream {stream_id}: {str(e)}")
            # Clean up
            if stream_id in self.active_streams:
                del self.active_streams[stream_id]
    
    def _get_initial_price(self, market, symbol):
        """
        Get initial price for a symbol
        In production, this would fetch from a real data source
        
        Args:
            market: Market code
            symbol: Symbol to get price for
            
        Returns:
            float: Initial price
        """
        # In production, this would fetch from a real data source
        # For demo purposes, we'll use a random price between 10 and 1000
        return random.uniform(10.0, 1000.0)
    
    def _generate_price_update(self, market, symbol, last_price):
        """
        Generate a simulated price update
        In production, this would fetch from a real data source
        
        Args:
            market: Market code
            symbol: Symbol to update
            last_price: Last known price
            
        Returns:
            dict: Price update data
        """
        # Simulate a small random price movement
        change_percent = random.uniform(-0.5, 0.5)  # -0.5% to +0.5%
        price_change = last_price * (change_percent / 100)
        new_price = round(last_price + price_change, 2)
        
        # Ensure price doesn't go below 1.0
        if new_price < 1.0:
            new_price = 1.0
            
        # Generate volume based on price movement
        volume = int(random.uniform(100, 10000))
        
        # Calculate traditional market data
        timestamp = datetime.now().isoformat()
        
        return {
            'market': market,
            'symbol': symbol,
            'price': new_price,
            'change': round(price_change, 2),
            'change_percent': round(change_percent, 2),
            'volume': volume,
            'timestamp': timestamp
        }
        
    async def _store_price_update(self, price_data):
        """
        Store price update in database
        
        Args:
            price_data: Price update data to store
        """
        if not self.db_url:
            return
            
        try:
            conn = await asyncpg.connect(self.db_url)
            
            # Check if table exists and create if needed
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS price_updates (
                    id SERIAL PRIMARY KEY,
                    market TEXT,
                    symbol TEXT,
                    price NUMERIC,
                    change NUMERIC,
                    change_percent NUMERIC,
                    volume INTEGER,
                    timestamp TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            
            # Insert the price update
            await conn.execute('''
                INSERT INTO price_updates (market, symbol, price, change, change_percent, volume, timestamp)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            ''',
            price_data['market'],
            price_data['symbol'],
            price_data['price'],
            price_data['change'],
            price_data['change_percent'],
            price_data['volume'],
            datetime.fromisoformat(price_data['timestamp'])
            )
            
            await conn.close()
            
        except Exception as e:
            logger.error(f"Error storing price update in database: {str(e)}")


class AlertStream:
    """
    AlertStream class handles the streaming of alerts and notifications
    via WebSockets.
    """
    
    def __init__(self, socketio=None):
        """
        Initialize the AlertStream with a Flask-SocketIO instance
        
        Args:
            socketio: The Flask-SocketIO instance for emitting events
        """
        self.socketio = socketio
        self.active_alerts = {}
        
    def add_price_alert(self, user_id, market, symbol, target_price, direction='above'):
        """
        Add a price alert for a user
        
        Args:
            user_id: User ID to send the alert to
            market: Market code (jse, crypto, forex, etc.)
            symbol: Symbol to monitor
            target_price: Target price to trigger the alert
            direction: 'above' or 'below' to specify when to trigger
            
        Returns:
            str: Alert ID
        """
        alert_id = f"alert_{user_id}_{market}_{symbol}_{int(time.time())}"
        
        self.active_alerts[alert_id] = {
            'user_id': user_id,
            'market': market,
            'symbol': symbol,
            'target_price': target_price,
            'direction': direction,
            'created_at': datetime.now().isoformat(),
            'triggered': False
        }
        
        logger.info(f"Added price alert {alert_id} for {user_id}: {symbol} {direction} {target_price}")
        return alert_id
        
    def remove_alert(self, alert_id):
        """
        Remove an alert
        
        Args:
            alert_id: ID of the alert to remove
            
        Returns:
            bool: True if alert removed successfully, False otherwise
        """
        if alert_id in self.active_alerts:
            del self.active_alerts[alert_id]
            logger.info(f"Removed alert {alert_id}")
            return True
        return False
        
    def check_alerts(self, price_data):
        """
        Check if any alerts should be triggered based on price data
        
        Args:
            price_data: Current price data
            
        Returns:
            list: List of triggered alert IDs
        """
        if not price_data:
            return []
            
        triggered_alerts = []
        
        for alert_id, alert in list(self.active_alerts.items()):
            if alert['triggered']:
                continue
                
            if (alert['market'] == price_data['market'] and 
                alert['symbol'] == price_data['symbol']):
                
                # Check if price crossed the target threshold
                current_price = price_data['price']
                target_price = alert['target_price']
                
                if (alert['direction'] == 'above' and current_price >= target_price) or \
                   (alert['direction'] == 'below' and current_price <= target_price):
                    
                    # Mark as triggered
                    self.active_alerts[alert_id]['triggered'] = True
                    self.active_alerts[alert_id]['triggered_at'] = datetime.now().isoformat()
                    
                    # Emit alert notification
                    if self.socketio:
                        self.socketio.emit('alert_triggered', {
                            'alert_id': alert_id,
                            'user_id': alert['user_id'],
                            'market': alert['market'],
                            'symbol': alert['symbol'],
                            'target_price': target_price,
                            'current_price': current_price,
                            'direction': alert['direction'],
                            'timestamp': datetime.now().isoformat()
                        })
                    
                    triggered_alerts.append(alert_id)
                    logger.info(f"Triggered alert {alert_id}: {alert['symbol']} at {current_price}")
        
        return triggered_alerts