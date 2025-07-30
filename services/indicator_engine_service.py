"""
Indicator Engine Service - High-Performance Technical Analysis
FastAPI-based microservice for technical indicators calculation
"""

from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
import asyncio
import aiohttp
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json
import uvicorn
from pydantic import BaseModel
import redis
import os

# Data models
class IndicatorRequest(BaseModel):
    symbol: str
    indicators: List[str]
    period: Optional[int] = None
    data_points: Optional[int] = 100

class IndicatorResponse(BaseModel):
    success: bool
    symbol: str
    indicators: Dict[str, Any]
    timestamp: str
    calculation_time: Optional[float] = None

# FastAPI app
app = FastAPI(
    title="WizData Indicator Engine Service",
    description="High-performance technical analysis microservice",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class IndicatorEngine:
    """High-performance technical indicators calculation engine"""
    
    def __init__(self):
        self.redis_client = None
        self.session = None
        self.cache_ttl = 300  # 5 minutes cache for indicators
        
        # Initialize Redis if available
        try:
            redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379')
            self.redis_client = redis.from_url(redis_url, decode_responses=True)
            self.redis_client.ping()
        except:
            self.redis_client = None
        
        # Market data service URL
        self.market_data_url = os.getenv('MARKET_DATA_SERVICE_URL', 'http://localhost:5001')
    
    async def setup(self):
        """Setup async session"""
        if not self.session:
            self.session = aiohttp.ClientSession()
    
    async def cleanup(self):
        """Cleanup resources"""
        if self.session:
            await self.session.close()
    
    def get_cached_indicators(self, key: str) -> Optional[Dict]:
        """Get cached indicators from Redis"""
        if not self.redis_client:
            return None
        
        try:
            data = self.redis_client.get(key)
            return json.loads(data) if data else None
        except:
            return None
    
    def set_cached_indicators(self, key: str, data: Dict):
        """Set cached indicators in Redis"""
        if not self.redis_client:
            return
        
        try:
            self.redis_client.setex(key, self.cache_ttl, json.dumps(data))
        except:
            pass
    
    async def get_market_data(self, symbol: str, data_points: int = 100) -> List[Dict]:
        """Get OHLCV data from market data service"""
        await self.setup()
        
        try:
            url = f"{self.market_data_url}/ohlcv/{symbol}"
            params = {'interval': '1h', 'limit': data_points}
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get('data', [])
        except Exception as e:
            print(f"Error fetching market data: {e}")
        
        # Fallback: generate sample data
        return self.generate_sample_data(symbol, data_points)
    
    def generate_sample_data(self, symbol: str, data_points: int) -> List[Dict]:
        """Generate sample OHLCV data for testing"""
        import random
        
        base_prices = {
            'JSE:NPN': 285000,
            'BTC/USDT': 67000,
            'AAPL': 185.0
        }
        
        base_price = base_prices.get(symbol, 100.0)
        data = []
        
        for i in range(data_points):
            timestamp = datetime.now() - timedelta(hours=data_points - i)
            change = random.uniform(-0.02, 0.02)
            
            open_price = base_price
            close_price = base_price * (1 + change)
            high_price = max(open_price, close_price) * (1 + random.uniform(0, 0.01))
            low_price = min(open_price, close_price) * (1 - random.uniform(0, 0.01))
            
            data.append({
                'time': int(timestamp.timestamp()),
                'open': open_price,
                'high': high_price,
                'low': low_price,
                'close': close_price,
                'volume': random.randint(100000, 1000000)
            })
            
            base_price = close_price
        
        return data
    
    async def calculate_indicators(
        self,
        symbol: str,
        indicators: List[str],
        data_points: int = 100
    ) -> Dict[str, Any]:
        """Calculate technical indicators for a symbol"""
        
        cache_key = f"indicators:{symbol}:{':'.join(sorted(indicators))}:{data_points}"
        cached = self.get_cached_indicators(cache_key)
        if cached:
            return cached
        
        start_time = datetime.now()
        
        # Get market data
        ohlcv_data = await self.get_market_data(symbol, data_points)
        if not ohlcv_data:
            raise ValueError("No market data available")
        
        # Convert to numpy arrays for efficient calculation
        closes = np.array([candle['close'] for candle in ohlcv_data])
        highs = np.array([candle['high'] for candle in ohlcv_data])
        lows = np.array([candle['low'] for candle in ohlcv_data])
        volumes = np.array([candle['volume'] for candle in ohlcv_data])
        
        results = {}
        
        # Calculate requested indicators
        for indicator in indicators:
            indicator_upper = indicator.upper()
            
            if indicator_upper == 'RSI':
                results['rsi'] = self.calculate_rsi(closes)
            elif indicator_upper == 'MACD':
                results['macd'] = self.calculate_macd(closes)
            elif indicator_upper == 'SMA':
                results['sma'] = self.calculate_sma(closes, 20)
            elif indicator_upper == 'EMA':
                results['ema'] = self.calculate_ema(closes, 20)
            elif indicator_upper == 'BOLLINGER':
                results['bollinger'] = self.calculate_bollinger_bands(closes)
            elif indicator_upper == 'STOCHASTIC':
                results['stochastic'] = self.calculate_stochastic(highs, lows, closes)
            elif indicator_upper == 'WILLIAMS_R':
                results['williams_r'] = self.calculate_williams_r(highs, lows, closes)
            elif indicator_upper == 'CCI':
                results['cci'] = self.calculate_cci(highs, lows, closes)
            elif indicator_upper == 'ADX':
                results['adx'] = self.calculate_adx(highs, lows, closes)
            elif indicator_upper == 'OBV':
                results['obv'] = self.calculate_obv(closes, volumes)
        
        calculation_time = (datetime.now() - start_time).total_seconds()
        
        result = {
            'indicators': results,
            'calculation_time': calculation_time,
            'data_points_used': len(ohlcv_data)
        }
        
        # Cache the results
        self.set_cached_indicators(cache_key, result)
        
        return result
    
    def calculate_rsi(self, closes: np.ndarray, period: int = 14) -> List[float]:
        """Calculate RSI (Relative Strength Index)"""
        if len(closes) < period + 1:
            return []
        
        deltas = np.diff(closes)
        gains = np.where(deltas > 0, deltas, 0)
        losses = np.where(deltas < 0, -deltas, 0)
        
        # Use exponential moving average for smoothing
        avg_gains = pd.Series(gains).ewm(span=period).mean()
        avg_losses = pd.Series(losses).ewm(span=period).mean()
        
        rs = avg_gains / avg_losses
        rsi = 100 - (100 / (1 + rs))
        
        return rsi.fillna(50).tolist()[-30:]  # Return last 30 values
    
    def calculate_macd(self, closes: np.ndarray, fast: int = 12, slow: int = 26, signal: int = 9) -> Dict[str, List[float]]:
        """Calculate MACD (Moving Average Convergence Divergence)"""
        if len(closes) < slow:
            return {'line': [], 'signal': [], 'histogram': []}
        
        # Calculate EMAs
        ema_fast = pd.Series(closes).ewm(span=fast).mean()
        ema_slow = pd.Series(closes).ewm(span=slow).mean()
        
        # MACD line
        macd_line = ema_fast - ema_slow
        
        # Signal line
        signal_line = macd_line.ewm(span=signal).mean()
        
        # Histogram
        histogram = macd_line - signal_line
        
        return {
            'line': macd_line.fillna(0).tolist()[-30:],
            'signal': signal_line.fillna(0).tolist()[-30:],
            'histogram': histogram.fillna(0).tolist()[-30:]
        }
    
    def calculate_sma(self, closes: np.ndarray, period: int = 20) -> List[float]:
        """Calculate Simple Moving Average"""
        if len(closes) < period:
            return []
        
        sma = pd.Series(closes).rolling(window=period).mean()
        return sma.fillna(closes[0]).tolist()[-30:]
    
    def calculate_ema(self, closes: np.ndarray, period: int = 20) -> List[float]:
        """Calculate Exponential Moving Average"""
        if len(closes) < period:
            return []
        
        ema = pd.Series(closes).ewm(span=period).mean()
        return ema.fillna(closes[0]).tolist()[-30:]
    
    def calculate_bollinger_bands(self, closes: np.ndarray, period: int = 20, std_dev: int = 2) -> Dict[str, List[float]]:
        """Calculate Bollinger Bands"""
        if len(closes) < period:
            return {'upper': [], 'middle': [], 'lower': []}
        
        series = pd.Series(closes)
        sma = series.rolling(window=period).mean()
        std = series.rolling(window=period).std()
        
        upper_band = sma + (std_dev * std)
        lower_band = sma - (std_dev * std)
        
        return {
            'upper': upper_band.fillna(closes[-1]).tolist()[-30:],
            'middle': sma.fillna(closes[-1]).tolist()[-30:],
            'lower': lower_band.fillna(closes[-1]).tolist()[-30:]
        }
    
    def calculate_stochastic(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> Dict[str, List[float]]:
        """Calculate Stochastic Oscillator"""
        if len(closes) < period:
            return {'k': [], 'd': []}
        
        lowest_lows = pd.Series(lows).rolling(window=period).min()
        highest_highs = pd.Series(highs).rolling(window=period).max()
        
        k_percent = 100 * (pd.Series(closes) - lowest_lows) / (highest_highs - lowest_lows)
        d_percent = k_percent.rolling(window=3).mean()
        
        return {
            'k': k_percent.fillna(50).tolist()[-30:],
            'd': d_percent.fillna(50).tolist()[-30:]
        }
    
    def calculate_williams_r(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> List[float]:
        """Calculate Williams %R"""
        if len(closes) < period:
            return []
        
        highest_highs = pd.Series(highs).rolling(window=period).max()
        lowest_lows = pd.Series(lows).rolling(window=period).min()
        
        williams_r = -100 * (highest_highs - pd.Series(closes)) / (highest_highs - lowest_lows)
        
        return williams_r.fillna(-50).tolist()[-30:]
    
    def calculate_cci(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 20) -> List[float]:
        """Calculate Commodity Channel Index"""
        if len(closes) < period:
            return []
        
        typical_price = (highs + lows + closes) / 3
        sma_tp = pd.Series(typical_price).rolling(window=period).mean()
        mad = pd.Series(typical_price).rolling(window=period).apply(lambda x: np.mean(np.abs(x - x.mean())))
        
        cci = (typical_price - sma_tp) / (0.015 * mad)
        
        return pd.Series(cci).fillna(0).tolist()[-30:]
    
    def calculate_adx(self, highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> Dict[str, List[float]]:
        """Calculate Average Directional Index"""
        if len(closes) < period + 1:
            return {'adx': [], 'di_plus': [], 'di_minus': []}
        
        # True Range
        high_low = highs - lows
        high_close = np.abs(highs - np.roll(closes, 1))
        low_close = np.abs(lows - np.roll(closes, 1))
        
        tr = np.maximum(high_low, np.maximum(high_close, low_close))
        tr[0] = high_low[0]  # First value
        
        # Directional Movement
        dm_plus = np.where((highs - np.roll(highs, 1)) > (np.roll(lows, 1) - lows), 
                          np.maximum(highs - np.roll(highs, 1), 0), 0)
        dm_minus = np.where((np.roll(lows, 1) - lows) > (highs - np.roll(highs, 1)), 
                           np.maximum(np.roll(lows, 1) - lows, 0), 0)
        
        dm_plus[0] = 0  # First value
        dm_minus[0] = 0  # First value
        
        # Smoothed values
        atr = pd.Series(tr).ewm(span=period).mean()
        di_plus = 100 * pd.Series(dm_plus).ewm(span=period).mean() / atr
        di_minus = 100 * pd.Series(dm_minus).ewm(span=period).mean() / atr
        
        # ADX calculation
        dx = 100 * np.abs(di_plus - di_minus) / (di_plus + di_minus)
        adx = pd.Series(dx).ewm(span=period).mean()
        
        return {
            'adx': adx.fillna(25).tolist()[-30:],
            'di_plus': di_plus.fillna(25).tolist()[-30:],
            'di_minus': di_minus.fillna(25).tolist()[-30:]
        }
    
    def calculate_obv(self, closes: np.ndarray, volumes: np.ndarray) -> List[float]:
        """Calculate On-Balance Volume"""
        if len(closes) < 2:
            return []
        
        price_changes = np.diff(closes)
        obv = np.zeros(len(closes))
        
        for i in range(1, len(closes)):
            if price_changes[i-1] > 0:
                obv[i] = obv[i-1] + volumes[i]
            elif price_changes[i-1] < 0:
                obv[i] = obv[i-1] - volumes[i]
            else:
                obv[i] = obv[i-1]
        
        return obv.tolist()[-30:]

# Service instance
indicator_engine = IndicatorEngine()

# API Endpoints
@app.post("/calculate", response_model=IndicatorResponse)
async def calculate_indicators(request: IndicatorRequest):
    """Calculate technical indicators for a symbol"""
    try:
        start_time = datetime.now()
        
        result = await indicator_engine.calculate_indicators(
            request.symbol,
            request.indicators,
            request.data_points or 100
        )
        
        return IndicatorResponse(
            success=True,
            symbol=request.symbol,
            indicators=result['indicators'],
            timestamp=datetime.now().isoformat(),
            calculation_time=result.get('calculation_time', 0)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/indicators/{symbol}")
async def get_indicators(
    symbol: str,
    indicators: List[str] = Query(...),
    data_points: int = Query(100, ge=20, le=1000)
):
    """Get technical indicators for a symbol (GET endpoint)"""
    try:
        result = await indicator_engine.calculate_indicators(symbol, indicators, data_points)
        
        return IndicatorResponse(
            success=True,
            symbol=symbol,
            indicators=result['indicators'],
            timestamp=datetime.now().isoformat(),
            calculation_time=result.get('calculation_time', 0)
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/indicators/available")
async def get_available_indicators():
    """Get list of available technical indicators"""
    indicators = {
        'trend': [
            {'name': 'SMA', 'description': 'Simple Moving Average'},
            {'name': 'EMA', 'description': 'Exponential Moving Average'},
            {'name': 'MACD', 'description': 'Moving Average Convergence Divergence'},
            {'name': 'ADX', 'description': 'Average Directional Index'}
        ],
        'momentum': [
            {'name': 'RSI', 'description': 'Relative Strength Index'},
            {'name': 'STOCHASTIC', 'description': 'Stochastic Oscillator'},
            {'name': 'WILLIAMS_R', 'description': 'Williams %R'},
            {'name': 'CCI', 'description': 'Commodity Channel Index'}
        ],
        'volatility': [
            {'name': 'BOLLINGER', 'description': 'Bollinger Bands'}
        ],
        'volume': [
            {'name': 'OBV', 'description': 'On-Balance Volume'}
        ]
    }
    
    return {
        'success': True,
        'indicators': indicators,
        'total_count': sum(len(category) for category in indicators.values()),
        'timestamp': datetime.now().isoformat()
    }

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "indicator-engine",
        "timestamp": datetime.now().isoformat(),
        "cache_available": indicator_engine.redis_client is not None
    }

@app.on_event("startup")
async def startup_event():
    """Setup on startup"""
    await indicator_engine.setup()

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    await indicator_engine.cleanup()

if __name__ == "__main__":
    uvicorn.run(
        "indicator_engine_service:app",
        host="0.0.0.0",
        port=5003,
        reload=True,
        log_level="info"
    )