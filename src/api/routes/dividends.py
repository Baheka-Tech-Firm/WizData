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
    prefix="/dividends",
    tags=["dividends"],
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
async def get_dividends(
    symbol: str = Path(..., description="Stock symbol"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    limit: int = Query(10, description="Number of dividend events to return", ge=1, le=100),
    user_info: Optional[Dict[str, Any]] = Depends(get_current_user),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get dividend history for a specific symbol
    """
    try:
        # Validate symbol
        symbol = symbol.upper()
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 2 years for dividend history
            start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"dividends:{symbol}:{start_date}:{end_date}:{limit}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        query = """
            SELECT 
                symbol, 
                ex_date, 
                payment_date, 
                amount, 
                currency,
                source
            FROM dividends
            WHERE symbol = $1
            AND ex_date BETWEEN $2 AND $3
            ORDER BY ex_date DESC
            LIMIT $4
        """
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, symbol, start_date, end_date, limit)
            
            if not records:
                # Return empty list instead of 404 for dividend history
                return []
            
            result = []
            for record in records:
                dividend = {
                    "symbol": record["symbol"],
                    "ex_date": record["ex_date"].strftime("%Y-%m-%d"),
                    "payment_date": record["payment_date"].strftime("%Y-%m-%d") if record["payment_date"] else None,
                    "amount": float(record["amount"]),
                    "currency": record["currency"] or "USD",
                    "source": record["source"]
                }
                result.append(dividend)
            
            # Cache the result
            cache_ttl = 60 * 60 * 24  # 24 hours (dividends don't change often)
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except Exception as e:
        logger.error(f"Error fetching dividends for {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/upcoming")
async def get_upcoming_dividends(
    days: int = Query(30, description="Number of days to look ahead", ge=1, le=90),
    limit: int = Query(20, description="Number of upcoming dividends to return", ge=1, le=100),
    exchange: Optional[str] = Query(None, description="Filter by exchange (e.g., JSE, NYSE)"),
    user_info: Dict[str, Any] = Depends(require_auth("read:dividends")),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get upcoming dividend events (premium feature)
    """
    try:
        # Calculate date range
        start_date = datetime.now().strftime("%Y-%m-%d")
        end_date = (datetime.now() + timedelta(days=days)).strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"upcoming_dividends:{start_date}:{end_date}:{exchange or 'all'}:{limit}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        query = """
            SELECT 
                d.symbol, 
                d.ex_date, 
                d.payment_date, 
                d.amount, 
                d.currency,
                s.name as company_name,
                s.exchange
            FROM dividends d
            JOIN symbols s ON d.symbol = s.symbol
            WHERE d.ex_date BETWEEN $1 AND $2
        """
        
        params = [start_date, end_date]
        
        if exchange:
            query += " AND s.exchange = $3"
            params.append(exchange)
        
        query += " ORDER BY d.ex_date ASC LIMIT $" + str(len(params) + 1)
        params.append(limit)
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, *params)
            
            result = {
                "upcoming_dividends": [],
                "time_period_days": days,
                "total_count": len(records)
            }
            
            for record in records:
                dividend = {
                    "symbol": record["symbol"],
                    "company_name": record["company_name"],
                    "exchange": record["exchange"],
                    "ex_date": record["ex_date"].strftime("%Y-%m-%d"),
                    "payment_date": record["payment_date"].strftime("%Y-%m-%d") if record["payment_date"] else None,
                    "amount": float(record["amount"]),
                    "currency": record["currency"] or "USD",
                    "days_until": (record["ex_date"] - datetime.now().date()).days
                }
                result["upcoming_dividends"].append(dividend)
            
            # Cache the result
            cache_ttl = 60 * 60 * 12  # 12 hours
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except Exception as e:
        logger.error(f"Error fetching upcoming dividends: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get("/calendar")
async def get_dividend_calendar(
    month: Optional[int] = Query(None, description="Month (1-12)"),
    year: Optional[int] = Query(None, description="Year (e.g., 2024)"),
    exchange: Optional[str] = Query(None, description="Filter by exchange (e.g., JSE, NYSE)"),
    user_info: Dict[str, Any] = Depends(require_auth("read:dividends")),
    _: None = Depends(rate_limit_dependency)
):
    """
    Get dividend calendar for a specific month (premium feature)
    """
    try:
        # Default to current month if not specified
        if not month or not year:
            now = datetime.now()
            month = month or now.month
            year = year or now.year
        
        # Validate month
        if month < 1 or month > 12:
            raise HTTPException(status_code=400, detail="Month must be between 1 and 12")
        
        # Calculate date range for the month
        start_date = datetime(year, month, 1).strftime("%Y-%m-%d")
        
        # Calculate end date (last day of the month)
        if month == 12:
            end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            end_date = datetime(year, month + 1, 1) - timedelta(days=1)
        
        end_date = end_date.strftime("%Y-%m-%d")
        
        # Generate cache key
        cache_key = f"dividend_calendar:{year}:{month}:{exchange or 'all'}"
        
        # Try to get from cache first
        cached_data = await cache_client.get_cache(cache_key)
        if cached_data is not None:
            logger.debug(f"Cache hit for {cache_key}")
            return cached_data
        
        # Not in cache, fetch from database
        query = """
            SELECT 
                d.symbol, 
                d.ex_date, 
                d.payment_date, 
                d.amount, 
                d.currency,
                s.name as company_name,
                s.exchange
            FROM dividends d
            JOIN symbols s ON d.symbol = s.symbol
            WHERE d.ex_date BETWEEN $1 AND $2
        """
        
        params = [start_date, end_date]
        
        if exchange:
            query += " AND s.exchange = $3"
            params.append(exchange)
        
        query += " ORDER BY d.ex_date ASC"
        
        async with db_client.pool.acquire() as conn:
            records = await conn.fetch(query, *params)
            
            # Organize by day of month
            calendar = {}
            for day in range(1, 32):
                # Skip invalid days for the month
                try:
                    datetime(year, month, day)
                    calendar[day] = []
                except ValueError:
                    continue
            
            for record in records:
                day = record["ex_date"].day
                
                dividend = {
                    "symbol": record["symbol"],
                    "company_name": record["company_name"],
                    "exchange": record["exchange"],
                    "ex_date": record["ex_date"].strftime("%Y-%m-%d"),
                    "payment_date": record["payment_date"].strftime("%Y-%m-%d") if record["payment_date"] else None,
                    "amount": float(record["amount"]),
                    "currency": record["currency"] or "USD"
                }
                
                calendar[day].append(dividend)
            
            result = {
                "year": year,
                "month": month,
                "month_name": datetime(year, month, 1).strftime("%B"),
                "exchange": exchange,
                "calendar": calendar,
                "total_events": len(records)
            }
            
            # Cache the result
            cache_ttl = 60 * 60 * 24  # 24 hours
            await cache_client.set_cache(cache_key, result, cache_ttl)
            
            return result
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching dividend calendar: {str(e)}")
        raise HTTPException(status_code=500, detail="Internal server error")
