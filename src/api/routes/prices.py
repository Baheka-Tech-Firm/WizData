from fastapi import APIRouter, Depends, Query, HTTPException, Path
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
import asyncio
import pandas as pd

from src.storage.timescale_db import TimescaleDBClient
from src.storage.redis_cache import RedisCacheClient
from src.api.middleware.rate_limiter import rate_limit_dependency
from src.api.middleware.auth import require_auth, get_current_user
from src.utils.logger import get_api_logger

router = APIRouter(
    prefix="/prices",
    tags=["prices"],
    responses={
        404: {"description": "Not found"},
        429: {"description": "Rate limit exceeded"},
    },
)

# Initialize clients
db_client = TimescaleDBClient()
cache_client = RedisCacheClient()
logger = get_api_logger()

@router.get("/{symbol}")
async def get_prices(
    symbol: str = Path(..., description="Stock symbol"),
    interval: str = Query("1d", description="Time interval (1d, 1h, etc.)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(100, description="Number of results to return", ge=1, le=1000),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get historical price data for a symbol
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 30 days for free tier, 1 year for premium
            days_back = 365 if (user_info and user_info.get("tier") == "premium") else 30
            start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"prices:{symbol}:{interval}:{start_date}:{end_date}:{limit}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        df = await db_client.get_ohlcv(symbol, start_date, end_date, limit)
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
        
        # Convert to list of dictionaries
        result = df.to_dict(orient="records")
        
        # Cache the result
        cache_ttl = 60 * 5  # 5 minutes
        await cache_client.set_cache(cache_key, result, cache_ttl)
        
        return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching prices for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/latest/{symbol}")
async def get_latest_price(
    symbol: str = Path(..., description="Stock symbol"),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get latest price for a symbol
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Generate cache key
        cache_key = f"latest_price:{symbol}"
        
        # Try to get from cache first (short TTL for latest prices)
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        df = await db_client.get_ohlcv(symbol, limit=1)
        
        if df.empty:
            raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
        
        # Get the latest record
        latest = df.iloc[0].to_dict()
        
        # Cache the result
        cache_ttl = 60  # 1 minute for latest prices
        await cache_client.set_cache(cache_key, latest, cache_ttl)
        
        return latest
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching latest price for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/batch")
async def get_batch_prices(
    symbols: str = Query(..., description="Comma-separated list of symbols"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    user_info: Dict[str, Any] = Depends(require_auth("read:prices"))
):
    """
    Get prices for multiple symbols (premium feature)
    """
    try:
        # Only available for premium tier
        if user_info.get("tier") != "premium":
            raise HTTPException(status_code=403, detail="This endpoint requires a premium subscription")
        
        # Parse symbols
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
        if not symbol_list:
            raise HTTPException(status_code=400, detail="No symbols provided")
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"batch_prices:{symbols}:{start_date}:{end_date}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Fetch data for each symbol in parallel
        results = {}
        tasks = []
        
        for symbol in symbol_list:
            task = db_client.get_ohlcv(symbol, start_date, end_date)
            tasks.append((symbol, task))
        
        for symbol, task in tasks:
            df = await task
            if not df.empty:
                results[symbol] = df.to_dict(orient="records")
        
        # Cache the result
        cache_ttl = 60 * 5  # 5 minutes
        await cache_client.set_cache(cache_key, results, cache_ttl)
        
        return results
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching batch prices: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/compare")
async def compare_symbols(
    symbols: str = Query(..., description="Comma-separated list of symbols to compare"),
    metric: str = Query("close", description="Metric to compare (close, volume, etc.)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    normalize: bool = Query(False, description="Normalize values to percentage change"),
    user_info: Dict[str, Any] = Depends(require_auth("read:prices"))
):
    """
    Compare performance of multiple symbols
    """
    try:
        # Parse symbols
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
        if not symbol_list:
            raise HTTPException(status_code=400, detail="No symbols provided")
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"compare:{symbols}:{metric}:{start_date}:{end_date}:{normalize}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Fetch data for each symbol in parallel
        results = {}
        comparison = {"dates": [], "values": {}}
        tasks = []
        
        for symbol in symbol_list:
            task = db_client.get_ohlcv(symbol, start_date, end_date)
            tasks.append((symbol, task))
        
        for symbol, task in tasks:
            df = await task
            if not df.empty:
                # Ensure the requested metric exists
                if metric not in df.columns:
                    continue
                
                # Add to comparison data
                if normalize:
                    # Calculate percentage change from first value
                    first_value = df[metric].iloc[-1]  # Oldest value (sorted by date DESC)
                    series = ((df[metric] / first_value) - 1) * 100
                    values = series.tolist()
                else:
                    values = df[metric].tolist()
                
                comparison["values"][symbol] = values
                
                # Add dates if not already added
                if not comparison["dates"]:
                    comparison["dates"] = df["date"].tolist()
        
        # Cache the result
        cache_ttl = 60 * 15  # 15 minutes
        await cache_client.set_cache(cache_key, comparison, cache_ttl)
        
        return comparison
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing symbols: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
