import aiohttp
import pandas as pd
import logging
import json
import os
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any, Tuple, Union, Sequence
import traceback
from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class DividendFetcher(BaseFetcher):
    """Fetcher for dividend data across various global markets"""
    
    def __init__(self):
        """Initialize the Dividend data fetcher"""
        super().__init__("Dividend Fetcher")
        
        # Markets supported for dividend data
        self.markets = {
            "asx": "Australian Securities Exchange",
            "lse": "London Stock Exchange",
            "nasdaq": "NASDAQ",
            "nyse": "New York Stock Exchange",
            "jse": "Johannesburg Stock Exchange",
            "tyo": "Tokyo Stock Exchange"
        }
        
        self.dividends_cache = {}
        self.cache_last_updated = {}
        
    async def get_symbols(self, market: str = "nasdaq", *args, **kwargs) -> Sequence[Dict[str, str]]:
        """
        Get list of stocks with dividend data for the specified market
        
        Args:
            market (str): Market code (asx, lse, nasdaq, nyse, jse, tyo)
        
        Returns:
            Sequence[Dict[str, str]]: List of symbols with details
        """
        # For dividend data, we redirect to the appropriate market fetcher
        # In a real implementation, this would filter for dividend-paying stocks
        from .global_markets_fetcher import GlobalMarketsFetcher
        from .africa_markets_fetcher import AfricanMarketsFetcher
        
        market = market.lower()
        
        try:
            if market in ["asx", "lse", "nasdaq", "nyse", "tyo"]:
                fetcher = GlobalMarketsFetcher()
                return await fetcher.get_symbols(market)
            elif market in ["jse", "ngx", "egx", "nse", "cse"]:
                fetcher = AfricanMarketsFetcher()
                return await fetcher.get_symbols(market)
            else:
                logger.error(f"Unsupported market for dividend data: {market}")
                return []
        except Exception as e:
            logger.error(f"Error getting symbols for dividend data in {market}: {str(e)}")
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
        Fetch dividend data for specified market and symbol
        
        Args:
            market (str): Market code
            symbol (Optional[str]): Symbol to fetch dividend data for
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            save_format (Optional[str]): Format to save data ('json', 'csv', None)
            output_path (Optional[str]): Path to save the output file
            
        Returns:
            pd.DataFrame: DataFrame with dividend data
        """
        market = market.lower()
        
        self.log_fetch_attempt({
            "type": "dividend",
            "market": market,
            "symbol": symbol,
            "start_date": start_date,
            "end_date": end_date
        })
        
        # Set default dates if not provided
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # Default to 1 year before end date for dividend data
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
            start_dt = end_dt - timedelta(days=365)
            start_date = start_dt.strftime("%Y-%m-%d")
        
        try:
            # Check if we have cached data we can use
            cache_key = f"{market}_{symbol}_{start_date}_{end_date}"
            if (cache_key in self.dividends_cache and 
                cache_key in self.cache_last_updated and
                (datetime.now() - self.cache_last_updated[cache_key]).total_seconds() < 86400):  # 24 hours
                logger.info(f"Using cached dividend data for {symbol} in {market}")
                df = self.dividends_cache[cache_key]
            else:
                if symbol:
                    # Fetch dividend data for a specific symbol
                    df = await self._fetch_dividend_data(market, symbol, start_date, end_date)
                    if not df.empty:
                        self.dividends_cache[cache_key] = df
                        self.cache_last_updated[cache_key] = datetime.now()
                else:
                    # Fetch dividend data for multiple symbols
                    symbols_data = await self.get_symbols(market)
                    
                    # Limit to first 10 symbols for performance
                    symbols_data = symbols_data[:10]
                    
                    all_dfs = []
                    for symbol_info in symbols_data:
                        try:
                            current_symbol = symbol_info["symbol"]
                            sym_df = await self._fetch_dividend_data(market, current_symbol, start_date, end_date)
                            
                            if not sym_df.empty:
                                # Add company info
                                if "name" in symbol_info:
                                    sym_df["company_name"] = symbol_info["name"]
                                if "sector" in symbol_info:
                                    sym_df["sector"] = symbol_info["sector"]
                                
                                all_dfs.append(sym_df)
                        except Exception as e:
                            if "symbol" in symbol_info:
                                logger.error(f"Error fetching dividend data for {symbol_info['symbol']}: {str(e)}")
                            else:
                                logger.error(f"Error fetching dividend data: {str(e)}")
                    
                    if all_dfs:
                        df = pd.concat(all_dfs, ignore_index=True)
                    else:
                        df = pd.DataFrame()
            
            if not df.empty:
                self.log_fetch_success(len(df))
                
                # Save to file if requested
                if save_format and not df.empty:
                    saved_path = self.save_data(df, market, symbol, start_date, end_date, save_format, output_path)
                    logger.info(f"Saved dividend data to {saved_path}")
            else:
                logger.warning(f"No dividend data found for {symbol if symbol else 'symbols'} in {market}")
            
            return df
            
        except Exception as e:
            self.log_fetch_error(e)
            logger.debug(traceback.format_exc())
            return pd.DataFrame()
    
    async def _fetch_dividend_data(self, market: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch dividend history for a specific symbol
        
        Args:
            market (str): Market code
            symbol (str): Symbol to fetch
            start_date (str): Start date
            end_date (str): End date
            
        Returns:
            pd.DataFrame: DataFrame with dividend data
        """
        # In a real implementation, this would use market-specific APIs
        # or a financial data provider API
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Generate simulated dividend data based on symbol characteristics
        symbol_seed = sum(ord(c) for c in symbol) % 100
        
        # Determine frequency of dividends (quarterly, semi-annual, annual)
        frequencies = ["quarterly", "semi-annual", "annual"]
        frequency_seed = symbol_seed % 3  # 0, 1, or 2
        frequency = frequencies[frequency_seed]
        
        # Determine dividend amount (as yield percentage)
        # Different markets have different typical yield ranges
        market_yield_ranges = {
            "asx": (0.03, 0.06),    # 3-6%
            "lse": (0.025, 0.05),   # 2.5-5%
            "nasdaq": (0.01, 0.03), # 1-3%
            "nyse": (0.015, 0.04),  # 1.5-4%
            "jse": (0.035, 0.07),   # 3.5-7%
            "tyo": (0.01, 0.025)    # 1-2.5%
        }
        
        min_yield, max_yield = market_yield_ranges.get(market, (0.02, 0.05))
        base_yield = min_yield + (symbol_seed / 100) * (max_yield - min_yield)
        
        # For demonstration purposes, get a stock price range
        # In a real implementation, would use actual price data
        price_ranges = {
            "asx": (10, 80),
            "lse": (5, 50),  # GBP
            "nasdaq": (50, 300),
            "nyse": (40, 200),
            "jse": (50, 400),
            "tyo": (1000, 8000) # JPY
        }
        min_price, max_price = price_ranges.get(market, (50, 200))
        base_price = min_price + (symbol_seed / 100) * (max_price - min_price)
        
        # Generate dividend records
        records = []
        
        current_dt = start_dt
        
        # Determine interval between dividends based on frequency
        if frequency == "quarterly":
            interval_months = 3
        elif frequency == "semi-annual":
            interval_months = 6
        else:  # annual
            interval_months = 12
            
        # Adjust start date to align with the nearest previous dividend date
        # For simplicity, assume dividends are paid at the end of Jan, Apr, Jul, Oct for quarterly
        # End of Jun, Dec for semi-annual, and end of Dec for annual
        dividend_months = []
        if frequency == "quarterly":
            dividend_months = [1, 4, 7, 10]
        elif frequency == "semi-annual":
            dividend_months = [6, 12]
        else:  # annual
            dividend_months = [12]
            
        # Find next dividend date after start_dt
        next_dividend_dt = None
        year = start_dt.year
        
        while not next_dividend_dt or next_dividend_dt < start_dt:
            for month in dividend_months:
                dividend_dt = datetime(year=year, month=month, day=28)
                if dividend_dt >= start_dt:
                    next_dividend_dt = dividend_dt
                    break
            year += 1
            
        if not next_dividend_dt:
            next_dividend_dt = start_dt
        
        # Generate dividend records from next_dividend_dt to end_dt
        current_dt = next_dividend_dt
        
        while current_dt <= end_dt:
            # Simulate a modest change in dividend amount over time (slight growth)
            # More established companies tend to have more stable dividends
            stability_factor = 1 - (symbol_seed % 5) / 10  # 0.5 to 0.9
            growth_factor = 1 + (current_dt.year - start_dt.year) * 0.03 * stability_factor
            
            # Calculate dividend amount
            # In a real implementation, use actual stock prices or dividend announcements
            dividend_amount = round(base_price * base_yield / 4 * growth_factor, 2)
            
            # Add dividend record
            records.append({
                "symbol": symbol,
                "ex_date": current_dt.strftime("%Y-%m-%d"),
                "payment_date": (current_dt + timedelta(days=14)).strftime("%Y-%m-%d"),
                "dividend_amount": dividend_amount,
                "currency": self._get_currency(market),
                "frequency": frequency,
                "market": market.upper(),
            })
            
            # Move to next dividend date
            if frequency == "quarterly":
                # Go to the next quarter
                month = current_dt.month
                next_month = month + 3
                if next_month > 12:
                    next_month -= 12
                    current_dt = current_dt.replace(year=current_dt.year + 1, month=next_month)
                else:
                    current_dt = current_dt.replace(month=next_month)
            elif frequency == "semi-annual":
                # Go to next half-year
                month = current_dt.month
                if month < 7:
                    current_dt = current_dt.replace(month=7)
                else:
                    current_dt = current_dt.replace(year=current_dt.year + 1, month=1)
            else:  # annual
                # Go to next year
                current_dt = current_dt.replace(year=current_dt.year + 1)
                
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
            "jse": "ZAR",
            "tyo": "JPY"
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
        Save dividend data to a file
        
        Args:
            df (pd.DataFrame): DataFrame with dividend data
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
        filename = f"DIVIDEND_{market.upper()}{symbol_part}_{start_date}_to_{end_date}_{timestamp}.{format}"
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
        Fetch and export dividend data to a file
        
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
            logger.error(f"No dividend data to export for {symbol if symbol else 'all symbols'} in {market}")
            return ""
            
        return self.save_data(df, market, symbol, 
                           start_date or (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d"),
                           end_date or datetime.now().strftime("%Y-%m-%d"),
                           format, output_path)