import logging
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio
import os

from src.api.routes import prices, news, fundamentals, dividends
from src.api.middleware.rate_limiter import RateLimitMiddleware
from src.storage.timescale_db import TimescaleDBClient
from src.storage.redis_cache import RedisCacheClient
from src.utils.config import API_HOST, API_PORT, DEBUG_MODE
from src.utils.logger import get_api_logger

# Configure logger
logger = get_api_logger()

# Initialize FastAPI application
app = FastAPI(
    title="WizData API",
    description="Financial data API for TradingWealthWiz",
    version="1.0.0"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add rate limiting middleware
app.add_middleware(RateLimitMiddleware)

# Database clients
db_client = TimescaleDBClient()
cache_client = RedisCacheClient()

@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    logger.info("Starting WizData API service")
    
    # Initialize database pool
    await db_client.init_pool()
    
    # Initialize Redis connection
    await cache_client.connect()
    
    logger.info("WizData API service started")

@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown"""
    logger.info("Shutting down WizData API service")
    
    # Close database pool
    await db_client.close_pool()
    
    # Close Redis connection
    await cache_client.disconnect()
    
    logger.info("WizData API service shut down completed")

# Exception handler for unhandled exceptions
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"detail": "An unexpected error occurred"}
    )

# Add API routes
app.include_router(prices.router, prefix="/api")
app.include_router(news.router, prefix="/api")
app.include_router(fundamentals.router, prefix="/api")
app.include_router(dividends.router, prefix="/api")

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "version": "1.0.0"}

# Root endpoint with API documentation link
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "WizData API - Financial data service for TradingWealthWiz",
        "documentation": "/docs",
        "health": "/health"
    }

if __name__ == "__main__":
    # Run the API server
    uvicorn.run(
        "src.api.main:app",
        host=API_HOST,
        port=API_PORT,
        reload=DEBUG_MODE
    )
