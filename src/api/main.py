from fastapi import FastAPI, Request, HTTPException
import logging
import uvicorn
from typing import Dict, Any
import asyncio
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("api")

# Create FastAPI app
app = FastAPI(
    title="WizData API",
    description="Financial data API for TradingWealthWiz",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize connections on startup"""
    logger.info("Starting WizData API server")

@app.on_event("shutdown")
async def shutdown_event():
    """Close connections on shutdown"""
    logger.info("Shutting down WizData API server")

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return {
        "error": "Internal server error",
        "detail": str(exc)
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "version": app.version}

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": "WizData API",
        "version": app.version,
        "docs_url": "/docs",
        "endpoints": {
            "prices": "/api/prices",
            "symbols": "/api/symbols",
            "markets": "/api/markets"
        }
    }

# Import API routes
from src.api.routes import prices

# Include routers
# app.include_router(prices.router, prefix="/api", tags=["prices"])

# If we were using FastAPI blueprints directly
# For now we're using Flask blueprints in the prices.py file

if __name__ == "__main__":
    host = os.environ.get("API_HOST", "0.0.0.0")
    port = int(os.environ.get("API_PORT", 8000))
    uvicorn.run(app, host=host, port=port)