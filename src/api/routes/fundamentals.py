from fastapi import APIRouter, Depends, Query, HTTPException, Path
from typing import List, Optional, Dict, Any, Union
from datetime import datetime, timedelta
import asyncio

from src.storage.timescale_db import TimescaleDBClient
from src.storage.redis_cache import RedisCacheClient
from src.api.middleware.rate_limiter import rate_limit_dependency
from src.api.middleware.auth import require_auth, get_current_user
from src.utils.logger import get_api_logger

router = APIRouter(
    prefix="/fundamentals",
    tags=["fundamentals"],
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
async def get_fundamentals(
    symbol: str = Path(..., description="Stock symbol"),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get fundamental data for a symbol
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Generate cache key
        cache_key = f"fundamentals:{symbol}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        # Query the most recent fundamentals data
        query = """
            SELECT 
                symbol, 
                date, 
                market_cap, 
                pe_ratio, 
                eps, 
                dividend_yield, 
                book_value, 
                price_to_book, 
                revenue, 
                net_income,
                source
            FROM fundamentals
            WHERE symbol = $1
            ORDER BY date DESC
            LIMIT 1
        """
        
        async with db_client.pool.acquire() as conn:
            record = await conn.fetchrow(query, symbol)
            
            if not record:
                raise HTTPException(status_code=404, detail=f"No fundamental data found for symbol {symbol}")
            
            # Convert to dictionary
            result = dict(record)
            
            # Ensure date is formatted as string
            result["date"] = result["date"].strftime("%Y-%m-%d")
            
            # Cache the result
            cache_ttl = 60 * 60  # 1 hour
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fundamentals for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/{symbol}/history")
async def get_fundamental_history(
    symbol: str = Path(..., description="Stock symbol"),
    metric: str = Query("pe_ratio", description="Fundamental metric to fetch history for"),
    quarters: int = Query(8, description="Number of quarters to retrieve", ge=1, le=20),
    user_info: Dict[str, Any] = Depends(require_auth("read:fundamentals")),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get historical fundamental data for a symbol (premium feature)
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Check if metric is valid
        valid_metrics = [
            "market_cap", "pe_ratio", "eps", "dividend_yield", 
            "book_value", "price_to_book", "revenue", "net_income"
        ]
        
        if metric not in valid_metrics:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid metric. Valid options are: {', '.join(valid_metrics)}"
            )
        
        # Generate cache key
        cache_key = f"fundamental_history:{symbol}:{metric}:{quarters}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        # Query historical fundamental data
        query = f"""
            SELECT 
                date, 
                {metric}
            FROM fundamentals
            WHERE symbol = $1
            ORDER BY date DESC
            LIMIT $2
        """
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, symbol, quarters)
            
            if not records:
                raise HTTPException(status_code=404, detail=f"No fundamental history found for symbol {symbol}")
            
            result = {
                "symbol": symbol,
                "metric": metric,
                "dates": [],
                "values": []
            }
            
            for record in records:
                result["dates"].append(record["date"].strftime("%Y-%m-%d"))
                result["values"].append(float(record[metric]) if record[metric] is not None else None)
            
            # Reverse to get chronological order
            result["dates"].reverse()
            result["values"].reverse()
            
            # Cache the result
            cache_ttl = 60 * 60 * 24  # 24 hours
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching fundamental history for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/compare")
async def compare_fundamentals(
    symbols: str = Query(..., description="Comma-separated list of symbols to compare"),
    metrics: Optional[str] = Query("pe_ratio,eps,dividend_yield", description="Comma-separated metrics to compare"),
    user_info: Dict[str, Any] = Depends(require_auth("read:fundamentals")),
    _: None = Depends(rate_limit_dependency)
):
    """
    Compare fundamental metrics across multiple symbols (premium feature)
    """
    try:
        # Parse symbols and metrics
        symbol_list = [s.strip().upper() for s in symbols.split(",")]
        metric_list = [m.strip() for m in metrics.split(",")]
        
        if not symbol_list:
            raise HTTPException(status_code=400, detail="No symbols provided")
        
        # Check if metrics are valid
        valid_metrics = [
            "market_cap", "pe_ratio", "eps", "dividend_yield", 
            "book_value", "price_to_book", "revenue", "net_income"
        ]
        
        invalid_metrics = [m for m in metric_list if m not in valid_metrics]
        if invalid_metrics:
            raise HTTPException(
                status_code=400, 
                detail=f"Invalid metrics: {', '.join(invalid_metrics)}"
            )
        
        # Generate cache key
        cache_key = f"compare_fundamentals:{symbols}:{metrics}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Query for latest fundamental data for each symbol
        result = {
            "symbols": symbol_list,
            "metrics": metric_list,
            "data": {}
        }
        
        async with db_client.pool.acquire() as conn:
            for symbol in symbol_list:
                # Select only the requested metrics
                columns = "symbol, date, " + ", ".join(metric_list)
                
                query = f"""
                    SELECT {columns}
                    FROM fundamentals
                    WHERE symbol = $1
                    ORDER BY date DESC
                    LIMIT 1
                """
                
                record = await conn.fetchrow(query, symbol)
                
                if record:
                    # Convert to dict and add to result
                    symbol_data = dict(record)
                    symbol_data["date"] = symbol_data["date"].strftime("%Y-%m-%d")
                    result["data"][symbol] = symbol_data
            
            # Cache the result
            cache_ttl = 60 * 60  # 1 hour
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error comparing fundamentals: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
