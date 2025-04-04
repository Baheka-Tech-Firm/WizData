"""
Real-time WebSocket Data Streaming Package

This package provides functionality for streaming real-time financial data
via WebSockets. It includes modules for price streaming, trade notifications,
and alerts.
"""

from src.api.realtime.data_stream import MarketDataStream, AlertStream

__all__ = ['MarketDataStream', 'AlertStream']