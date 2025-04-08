"""
Real-time WebSocket API Routes

This package provides API routes for managing real-time data streams
via WebSockets.
"""

from src.api.routes.realtime.socket_routes import realtime_bp

__all__ = ['realtime_bp']