import aiohttp
import pandas as pd
import logging
import json
import os
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any

from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class ForexFetcher(BaseFetcher):
    """Fetcher for foreign exchange (forex) data"""
    
    def __init__(self):
        """Initialize the forex fetcher"""
        super().__init__("Forex Fetcher")
        self.api_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
        self.base_url = "https://www.alphavantage.co/query"
        
        # Cache for symbols
        self.symbols_cache = []
        self.symbols_last_updated = None
    
    async def get_symbols(self) -> List[str]:
        """
        Get list of available forex currency pairs
        
        Returns:
            List[str]: List of currency pairs
        """
        # Return cached symbols if available and not too old
        if self.symbols_cache and self.symbols_last_updated and \
           (datetime.now() - self.symbols_last_updated).total_seconds() < 86400:  # 24 hours
            logger.info(f"Returning {len(self.symbols_cache)} cached forex symbols")
            return self.symbols_cache
        
        try:
            # Common forex pairs 
            # In a production environment, we would get this dynamically from Alpha Vantage
            symbols = [
                "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "AUDUSD", 
                "USDCAD", "NZDUSD", "EURGBP", "EURJPY", "GBPJPY",
                "AUDJPY", "EURAUD", "EURCHF", "GBPCHF", "CADJPY"
            ]
            
            self.symbols_cache = symbols
            self.symbols_last_updated = datetime.now()
            logger.info(f"Retrieved {len(symbols)} forex symbols")
            return symbols
        except Exception as e:
            logger.error(f"Error retrieving forex symbols: {str(e)}")
            # Return cached symbols if available, otherwise an empty list
            return self.symbols_cache if self.symbols_cache else []
    
    async def fetch_data(self, symbol: Optional[str] = None, 
                         interval: str = "daily",
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None,
                         **kwargs) -> pd.DataFrame:
        """
        Fetch forex data from Alpha Vantage
        
        Args:
            symbol (Optional[str]): Currency pair (e.g., "EURUSD")
            interval (str): Time interval ("daily", "weekly", "monthly", "intraday")
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with forex data
        """
        self.log_fetch_attempt({
            "symbol": symbol, 
            "interval": interval,
            "start_date": start_date, 
            "end_date": end_date
        })
        
        if not self.api_key:
            logger.error("Alpha Vantage API key not found in environment variables")
            return pd.DataFrame()
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 30 days before end date
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=30)
            start_date = start_dt.strftime("%Y-%m-%d")
        
        try:
            if symbol:
                # Split symbol into from_currency and to_currency
                if len(symbol) == 6:  # Standard forex pair format
                    from_currency = symbol[:3]
                    to_currency = symbol[3:]
                else:
                    # Handle other formats or use default
                    from_currency = "EUR"
                    to_currency = "USD"
                    logger.warning(f"Invalid forex symbol format: {symbol}, using EURUSD")
                
                # Determine Alpha Vantage function based on interval
                if interval.lower() == "intraday":
                    function = "FX_INTRADAY"
                    av_interval = "60min"  # Default to hourly for intraday
                elif interval.lower() == "weekly":
                    function = "FX_WEEKLY"
                    av_interval = ""
                elif interval.lower() == "monthly":
                    function = "FX_MONTHLY"
                    av_interval = ""
                else:  # Default to daily
                    function = "FX_DAILY"
                    av_interval = ""
                
                # Fetch data from Alpha Vantage
                df = await self._fetch_from_alpha_vantage(
                    function=function,
                    from_currency=from_currency,
                    to_currency=to_currency,
                    interval=av_interval
                )
                
                # Filter by date range if data was fetched
                if not df.empty and 'date' in df.columns:
                    mask = (df['date'] >= start_date) & (df['date'] <= end_date)
                    df = df[mask].copy()
            else:
                # Fetch data for multiple symbols
                symbols = await self.get_symbols()
                # Limit to first 5 symbols for demo purposes
                symbols = symbols[:5]
                
                all_dfs = []
                for sym in symbols:
                    try:
                        # Split symbol
                        from_currency = sym[:3]
                        to_currency = sym[3:]
                        
                        # Fetch data
                        sym_df = await self._fetch_from_alpha_vantage(
                            function="FX_DAILY",
                            from_currency=from_currency,
                            to_currency=to_currency
                        )
                        
                        # Filter by date range
                        if not sym_df.empty and 'date' in sym_df.columns:
                            mask = (sym_df['date'] >= start_date) & (sym_df['date'] <= end_date)
                            sym_df = sym_df[mask].copy()
                            
                            if not sym_df.empty:
                                all_dfs.append(sym_df)
                    except Exception as e:
                        logger.error(f"Error fetching data for symbol {sym}: {str(e)}")
                
                if all_dfs:
                    df = pd.concat(all_dfs, ignore_index=True)
                else:
                    df = pd.DataFrame()
            
            if not df.empty:
                self.log_fetch_success(len(df))
            else:
                logger.warning(f"No data retrieved for forex {symbol if symbol else 'symbols'}")
            
            return df
        
        except Exception as e:
            self.log_fetch_error(e)
            return pd.DataFrame()  # Return empty DataFrame on error
    
    async def _fetch_from_alpha_vantage(self, function: str, from_currency: str, 
                                       to_currency: str, interval: str = "") -> pd.DataFrame:
        """
        Fetch forex data from Alpha Vantage API
        
        Args:
            function (str): Alpha Vantage function name
            from_currency (str): Base currency code
            to_currency (str): Quote currency code
            interval (str): Time interval for intraday data
            
        Returns:
            pd.DataFrame: DataFrame with forex data
        """
        # For now, simulate API response to avoid hitting rate limits
        # In a production environment, this would make actual API requests
        
        # Create a realistic DataFrame based on the currency pair
        symbol = f"{from_currency}{to_currency}"
        
        # Starting date - go back 100 days from today
        end_date = datetime.now()
        start_date = end_date - timedelta(days=100)
        
        # Generate simulated exchange rate data
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        
        # Base rate depends on the currency pair
        base_rate = 1.0
        if symbol == "EURUSD":
            base_rate = 1.08
        elif symbol == "GBPUSD":
            base_rate = 1.25
        elif symbol == "USDJPY":
            base_rate = 145.0
        elif symbol == "USDCHF":
            base_rate = 0.93
        elif symbol == "AUDUSD":
            base_rate = 0.66
        elif symbol == "USDCAD":
            base_rate = 1.36
        elif symbol == "NZDUSD":
            base_rate = 0.61
        else:
            # For other pairs, generate a random base rate between 0.5 and 2.0
            seed = sum(ord(c) for c in symbol) % 100
            base_rate = 0.5 + (seed / 100.0) * 1.5
        
        current_date = start_date
        current_rate = base_rate
        
        while current_date <= end_date:
            # Skip weekends for forex data
            if current_date.weekday() < 5:  # 0-4 are Monday to Friday
                # Small daily variation in exchange rate
                # Different currency pairs have different volatility
                volatility = 0.002  # Default volatility
                if "JPY" in symbol:
                    volatility = 0.004
                elif "GBP" in symbol:
                    volatility = 0.003
                
                # Calculate daily change
                daily_change = (hash(str(current_date)) % 100 - 50) / 10000.0  # -0.5% to +0.5%
                daily_change *= (1 + volatility * 10)
                
                # Calculate prices
                open_rate = current_rate
                close_rate = current_rate * (1 + daily_change)
                high_rate = max(open_rate, close_rate) * (1 + volatility)
                low_rate = min(open_rate, close_rate) * (1 - volatility)
                
                # Update for next day
                current_rate = close_rate
                
                # Round rates to appropriate decimal places
                if "JPY" in symbol:  # JPY pairs typically have 3 decimal places
                    precision = 3
                else:  # Most other pairs have 5 decimal places
                    precision = 5
                
                # Add to lists
                dates.append(current_date.strftime("%Y-%m-%d"))
                opens.append(round(open_rate, precision))
                highs.append(round(high_rate, precision))
                lows.append(round(low_rate, precision))
                closes.append(round(close_rate, precision))
            
            current_date += timedelta(days=1)
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'symbol': symbol,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'exchange': 'Alpha Vantage',
            'asset_type': 'forex'
        })
        
        return df