import aiohttp
import pandas as pd
import logging
import json
import os
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any, Tuple, Union, Sequence
import traceback
import random
from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class EarningsFetcher(BaseFetcher):
    """Fetcher for earnings data (EPS, revenue, projections) across major global markets"""
    
    def __init__(self):
        """Initialize the Earnings data fetcher"""
        super().__init__("Earnings Fetcher")
        
        # Markets supported for earnings data
        self.markets = {
            "asx": "Australian Securities Exchange",
            "lse": "London Stock Exchange",
            "nasdaq": "NASDAQ",
            "nyse": "New York Stock Exchange",
            "jse": "Johannesburg Stock Exchange"
        }
        
        self.earnings_cache = {}
        self.cache_last_updated = {}
        
    async def get_symbols(self, market: str = "nasdaq", *args, **kwargs) -> Sequence[Dict[str, str]]:
        """
        Get list of stocks with earnings data for the specified market
        
        Args:
            market (str): Market code (asx, lse, nasdaq, nyse, jse)
        
        Returns:
            Sequence[Dict[str, str]]: List of symbols with details
        """
        # Similar to dividends, we'll reuse the market-specific fetchers
        from .global_markets_fetcher import GlobalMarketsFetcher
        from .africa_markets_fetcher import AfricanMarketsFetcher
        
        market = market.lower()
        
        try:
            if market in ["asx", "lse", "nasdaq", "nyse"]:
                fetcher = GlobalMarketsFetcher()
                return await fetcher.get_symbols(market)
            elif market in ["jse"]:
                fetcher = AfricanMarketsFetcher()
                return await fetcher.get_symbols(market)
            else:
                logger.error(f"Unsupported market for earnings data: {market}")
                return []
        except Exception as e:
            logger.error(f"Error getting symbols for earnings data in {market}: {str(e)}")
            return []
        
    async def fetch_data(self, 
                      market: str = "nasdaq", 
                      symbol: Optional[str] = None,
                      start_date: Optional[str] = None,
                      end_date: Optional[str] = None,
                      save_format: Optional[str] = None,
                      output_path: Optional[str] = None,
                      **kwargs) -> pd.DataFrame:
        """
        Fetch earnings data for specified market and symbol
        
        Args:
            market (str): Market code
            symbol (Optional[str]): Symbol to fetch earnings data for
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            save_format (Optional[str]): Format to save data ('json', 'csv', None)
            output_path (Optional[str]): Path to save the output file
            
        Returns:
            pd.DataFrame: DataFrame with earnings data
        """
        market = market.lower()
        
        self.log_fetch_attempt({
            "type": "earnings",
            "market": market,
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date
        })
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 2 years before end date for earnings data (8 quarters)
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=730)
            start_date = start_dt.strftime("%Y-%m-%d")
        
        try:
            # Check for cached data
            cache_key = f"{market}_{symbol}_{start_date}_{end_date}"
            if (cache_key in self.earnings_cache and 
                cache_key in self.cache_last_updated and
                (datetime.now() - self.cache_last_updated[cache_key]).total_seconds() < 86400):  # 24 hours
                logger.info(f"Using cached earnings data for {symbol} in {market}")
                df = self.earnings_cache[cache_key]
            else:
                if symbol:
                    # Fetch earnings data for a specific symbol
                    df = await self._fetch_earnings_data(market, symbol, start_date, end_date)
                    if not df.empty:
                        self.earnings_cache[cache_key] = df
                        self.cache_last_updated[cache_key] = datetime.now()
                else:
                    # Fetch earnings data for multiple symbols
                    symbols_data = await self.get_symbols(market)
                    
                    # Limit to first 10 symbols for performance
                    symbols_data = symbols_data[:10]
                    
                    all_dfs = []
                    for symbol_info in symbols_data:
                        try:
                            current_symbol = symbol_info["symbol"]
                            sym_df = await self._fetch_earnings_data(market, current_symbol, start_date, end_date)
                            
                            if not sym_df.empty:
                                # Add company info
                                if "name" in symbol_info:
                                    sym_df["company_name"] = symbol_info["name"]
                                if "sector" in symbol_info:
                                    sym_df["sector"] = symbol_info["sector"]
                                
                                all_dfs.append(sym_df)
                        except Exception as e:
                            if "symbol" in symbol_info:
                                logger.error(f"Error fetching earnings data for {symbol_info['symbol']}: {str(e)}")
                            else:
                                logger.error(f"Error fetching earnings data: {str(e)}")
                    
                    if all_dfs:
                        df = pd.concat(all_dfs, ignore_index=True)
                    else:
                        df = pd.DataFrame()
            
            if not df.empty:
                self.log_fetch_success(len(df))
                
                # Save to file if requested
                if save_format and not df.empty:
                    saved_path = self.save_data(df, market, symbol, start_date, end_date, save_format, output_path)
                    logger.info(f"Saved earnings data to {saved_path}")
            else:
                logger.warning(f"No earnings data found for {symbol if symbol else 'symbols'} in {market}")
            
            return df
            
        except Exception as e:
            self.log_fetch_error(e)
            logger.debug(traceback.format_exc())
            return pd.DataFrame()
    
    async def _fetch_earnings_data(self, market: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch quarterly earnings history for a specific symbol
        
        Args:
            market (str): Market code
            symbol (str): Symbol to fetch
            start_date (str): Start date
            end_date (str): End date
            
        Returns:
            pd.DataFrame: DataFrame with earnings data
        """
        # In a real implementation, this would use market-specific APIs
        # or a financial data provider API
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Different markets have different fiscal quarters
        # For simplicity, assume all markets follow calendar quarters
        
        # Generate simulated earnings data based on symbol characteristics
        symbol_seed = sum(ord(c) for c in symbol) % 100
        
        # Base revenue depends on market and company size (symbol seed)
        market_revenue_ranges = {
            "nasdaq": (500e6, 50e9),  # Tech companies tend to have higher revenue range
            "nyse": (1e9, 100e9),     # Large traditional companies
            "asx": (50e6, 10e9),      # Australian companies tend to be smaller
            "lse": (100e6, 50e9),     # UK companies
            "jse": (100e6, 20e9)      # South African companies
        }
        
        min_rev, max_rev = market_revenue_ranges.get(market, (100e6, 10e9))
        base_revenue = min_rev + (symbol_seed / 100) * (max_rev - min_rev)
        
        # Different growth characteristics based on market and symbol
        market_growth_ranges = {
            "nasdaq": (0.02, 0.15),  # Tech higher growth
            "nyse": (0.01, 0.08),    # More established companies
            "asx": (0.01, 0.07),
            "lse": (0.01, 0.06),
            "jse": (0.01, 0.09)
        }
        
        min_growth, max_growth = market_growth_ranges.get(market, (0.01, 0.05))
        avg_growth_rate = min_growth + (symbol_seed / 100) * (max_growth - min_growth)
        
        # Different margins based on industry and market
        market_margin_ranges = {
            "nasdaq": (0.1, 0.3),    # Tech higher margins
            "nyse": (0.05, 0.15),    # Traditional companies
            "asx": (0.05, 0.12),
            "lse": (0.05, 0.13),
            "jse": (0.04, 0.12)
        }
        
        min_margin, max_margin = market_margin_ranges.get(market, (0.05, 0.1))
        base_margin = min_margin + (symbol_seed / 100) * (max_margin - min_margin)
        
        # Generate quarterly earnings records
        records = []
        
        # Find start quarter
        start_year = start_dt.year
        start_quarter = (start_dt.month - 1) // 3 + 1
        
        # Find end quarter
        end_year = end_dt.year
        end_quarter = (end_dt.month - 1) // 3 + 1
        
        # Generate records for each quarter
        current_year = start_year
        current_quarter = start_quarter
        
        while current_year < end_year or (current_year == end_year and current_quarter <= end_quarter):
            # Determine quarter end date
            if current_quarter == 1:
                quarter_end = datetime(current_year, 3, 31)
            elif current_quarter == 2:
                quarter_end = datetime(current_year, 6, 30)
            elif current_quarter == 3:
                quarter_end = datetime(current_year, 9, 30)
            else:
                quarter_end = datetime(current_year, 12, 31)
            
            # Adjust growth based on economic cycles
            # Simple 5-year economic cycle model
            cycle_phase = (current_year % 5) / 5
            cycle_factor = 1 + 0.3 * (cycle_phase - 0.5)  # Cyclical adjustment
            
            # Calculate random quarterly variations
            random.seed(f"{symbol}_{current_year}_{current_quarter}")
            quarterly_variation = 0.8 + 0.4 * random.random()  # 0.8 to 1.2
            
            # Seasonal variations (Q4 usually higher for retail, etc.)
            seasonal_factors = {1: 0.9, 2: 1.0, 3: 0.95, 4: 1.15}
            seasonal_factor = seasonal_factors.get(current_quarter, 1.0)
            
            # Years since start for growth calculation
            years_since_start = (current_year - start_year) + (current_quarter - start_quarter)/4
            
            # Apply compounding growth
            growth_factor = (1 + avg_growth_rate) ** years_since_start
            
            # Calculate quarterly revenue
            quarterly_revenue = base_revenue * growth_factor * cycle_factor * quarterly_variation * seasonal_factor / 4
            
            # Margin also varies by quarter and economic cycle
            quarterly_margin = base_margin * (0.9 + 0.2 * random.random()) * (0.9 + 0.2 * cycle_phase)
            
            # Calculate earnings metrics
            eps = round((quarterly_revenue * quarterly_margin) / (1e7 + symbol_seed * 1e5), 2)
            expected_eps = round(eps * (0.9 + 0.2 * random.random()), 2)
            surprise_pct = round(((eps - expected_eps) / expected_eps) * 100, 1)
            
            # Add earnings record
            records.append({
                "symbol": symbol,
                "market": market.upper(),
                "fiscal_year": current_year,
                "fiscal_quarter": current_quarter,
                "report_date": (quarter_end + timedelta(days=25)).strftime("%Y-%m-%d"),
                "quarter_end_date": quarter_end.strftime("%Y-%m-%d"),
                "revenue": round(quarterly_revenue, 0),
                "eps": eps,
                "expected_eps": expected_eps,
                "surprise_pct": surprise_pct,
                "currency": self._get_currency(market),
            })
            
            # Move to next quarter
            current_quarter += 1
            if current_quarter > 4:
                current_quarter = 1
                current_year += 1
        
        # Create DataFrame
        if records:
            df = pd.DataFrame(records)
            return df
        else:
            return pd.DataFrame()
    
    def _get_currency(self, market: str) -> str:
        """Get currency code for a market"""
        currency_map = {
            "asx": "AUD",
            "lse": "GBP",
            "nasdaq": "USD",
            "nyse": "USD",
            "jse": "ZAR"
        }
        return currency_map.get(market, "USD")
    
    def save_data(self, 
               df: pd.DataFrame, 
               market: str, 
               symbol: Optional[str], 
               start_date: str, 
               end_date: str, 
               format: str = 'json',
               output_path: Optional[str] = None) -> str:
        """
        Save earnings data to a file
        
        Args:
            df (pd.DataFrame): DataFrame with earnings data
            market (str): Market code
            symbol (Optional[str]): Symbol or None if multiple symbols
            start_date (str): Start date
            end_date (str): End date
            format (str): Output format ('json' or 'csv')
            output_path (Optional[str]): Path to save the output file
            
        Returns:
            str: Path to the saved file
        """
        if format not in ['json', 'csv']:
            raise ValueError("Format must be 'json' or 'csv'")
            
        # Create directory if needed
        if output_path:
            os.makedirs(output_path, exist_ok=True)
        else:
            output_path = 'data/exports'
            os.makedirs(output_path, exist_ok=True)
            
        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        symbol_part = f"_{symbol}" if symbol else "_all"
        filename = f"EARNINGS_{market.upper()}{symbol_part}_{start_date}_to_{end_date}_{timestamp}.{format}"
        filepath = os.path.join(output_path, filename)
        
        # Save to file
        if format == 'json':
            df.to_json(filepath, orient='records', date_format='iso')
        else:  # CSV
            df.to_csv(filepath, index=False)
            
        return filepath
    
    async def export_data(self, 
                      market: str = "nasdaq",
                      symbol: Optional[str] = None, 
                      start_date: Optional[str] = None, 
                      end_date: Optional[str] = None,
                      format: str = 'json',
                      output_path: Optional[str] = None) -> str:
        """
        Fetch and export earnings data to a file
        
        Args:
            market (str): Market code
            symbol (Optional[str]): Symbol to fetch data for
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            format (str): Output format ('json' or 'csv')
            output_path (Optional[str]): Path to save the output file
            
        Returns:
            str: Path to the saved file
        """
        df = await self.fetch_data(
            market=market, 
            symbol=symbol, 
            start_date=start_date,
            end_date=end_date
        )
        
        if df.empty:
            logger.error(f"No earnings data to export for {symbol if symbol else 'all symbols'} in {market}")
            return ""
            
        return self.save_data(df, market, symbol, 
                           start_date or (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d"),
                           end_date or datetime.now().strftime("%Y-%m-%d"),
                           format, output_path)