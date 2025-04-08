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
    prefix="/news",
    tags=["news"],
    responses={
        404: {"description": "Not found"},
        429: {"description": "Rate limit exceeded"},
    },
)

# Initialize clients
db_client = TimescaleDBClient()
cache_client = RedisCacheClient()
logger = get_api_logger()

@router.get("/")
async def get_news(
    symbol: Optional[str] = Query(None, description="Stock symbol filter"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(10, description="Number of results to return", ge=1, le=100),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get financial news articles, optionally filtered by symbol
    """
    try:
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"news:{symbol or 'all'}:{start_date}:{end_date}:{limit}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        # This is a mock implementation as we don't have the actual table schema
        # In a real implementation, you would query the news table
        
        # Simulated query
        query = """
            SELECT id, date, title, content, source, url, sentiment, symbols
            FROM news
            WHERE date BETWEEN $1 AND $2
        """
        
        params = [start_date, end_date]
        
        if symbol:
            # Filter by symbol (assuming symbols is a text array in PostgreSQL)
            symbol = symbol.upper()
            query += " AND $3 = ANY(symbols)"
            params.append(symbol)
        
        query += " ORDER BY date DESC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, *params)
            
            result = []
            for record in records:
                result.append({
                    "id": record["id"],
                    "date": record["date"].strftime("%Y-%m-%d"),
                    "title": record["title"],
                    "content": record["content"],
                    "source": record["source"],
                    "url": record["url"],
                    "sentiment": record["sentiment"],
                    "symbols": record["symbols"]
                })
            
            # Cache the result
            cache_ttl = 60 * 10  # 10 minutes
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except Exception as e:
        logger.error(f"Error fetching news: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/sentiment/{symbol}")
async def get_sentiment(
    symbol: str = Path(..., description="Stock symbol"),
    days: int = Query(30, description="Number of days to analyze", ge=1, le=90),
    user_info: Dict[str, Any] = Depends(require_auth("read:news")),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get sentiment analysis for a symbol over time
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Calculate date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"sentiment:{symbol}:{days}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # In a real implementation, you would query the database
        # for sentiment data over time, possibly aggregating by day
        
        # Simulated query
        query = """
            SELECT date, AVG(sentiment) as avg_sentiment, COUNT(*) as article_count
            FROM news
            WHERE $1 = ANY(symbols) AND date BETWEEN $2 AND $3
            GROUP BY date
            ORDER BY date
        """
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, symbol, start_date, end_date)
            
            result = {
                "symbol": symbol,
                "period_days": days,
                "dates": [],
                "sentiment": [],
                "article_count": []
            }
            
            for record in records:
                result["dates"].append(record["date"].strftime("%Y-%m-%d"))
                result["sentiment"].append(float(record["avg_sentiment"]))
                result["article_count"].append(record["article_count"])
            
            # Add summary statistics
            if result["sentiment"]:
                result["average_sentiment"] = sum(result["sentiment"]) / len(result["sentiment"])
                result["total_articles"] = sum(result["article_count"])
            else:
                result["average_sentiment"] = 0
                result["total_articles"] = 0
            
            # Cache the result
            cache_ttl = 60 * 60  # 1 hour
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except Exception as e:
        logger.error(f"Error fetching sentiment for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/trending")
async def get_trending_news(
    days: int = Query(7, description="Number of days to analyze", ge=1, le=30),
    limit: int = Query(5, description="Number of trending topics to return", ge=1, le=20),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get trending news topics and symbols
    """
    try:
        # Calculate date range
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"trending:{days}:{limit}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # In a real implementation, you would query the most frequently mentioned
        # symbols and topics in recent news articles
        
        # Simulated query for trending symbols
        symbol_query = """
            SELECT unnest(symbols) as symbol, COUNT(*) as mentions
            FROM news
            WHERE date BETWEEN $1 AND $2
            GROUP BY symbol
            ORDER BY mentions DESC
            LIMIT $3
        """
        
        async with db_client.pool.acquire() as conn:
            symbol_records = await conn.fetch(symbol_query, start_date, end_date, limit)
            
            result = {
                "trending_symbols": [
                    {"symbol": record["symbol"], "mentions": record["mentions"]}
                    for record in symbol_records
                ],
                "time_period_days": days
            }
            
            # Cache the result
            cache_ttl = 60 * 30  # 30 minutes
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except Exception as e:
        logger.error(f"Error fetching trending news: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
