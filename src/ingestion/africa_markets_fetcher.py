import aiohttp
import pandas as pd
import logging
import json
import os
import csv
from datetime import datetime, timedelta
import asyncio
from typing import List, Dict, Optional, Any, Tuple, Union, Sequence
import re
from bs4 import BeautifulSoup
import traceback
import trafilatura
from .base_fetcher import BaseFetcher

logger = logging.getLogger(__name__)

class AfricanMarketsFetcher(BaseFetcher):
    """
    Comprehensive fetcher for African stock markets including:
    - Johannesburg Stock Exchange (JSE)
    - Nigerian Stock Exchange (NGX)
    - Egyptian Exchange (EGX)
    - Nairobi Securities Exchange (NSE)
    - Casablanca Stock Exchange (CSE)
    """
    
    def __init__(self):
        """Initialize the African Markets fetcher"""
        super().__init__("African Markets Fetcher")
        self.markets = {
            "jse": {
                "name": "Johannesburg Stock Exchange",
                "base_url": "https://www.jse.co.za",
                "country": "South Africa",
                "currency": "ZAR"
            },
            "ngx": {
                "name": "Nigerian Stock Exchange",
                "base_url": "https://ngxgroup.com",
                "country": "Nigeria",
                "currency": "NGN"
            },
            "egx": {
                "name": "Egyptian Exchange",
                "base_url": "https://www.egx.com.eg",
                "country": "Egypt",
                "currency": "EGP"
            },
            "nse": {
                "name": "Nairobi Securities Exchange",
                "base_url": "https://www.nse.co.ke",
                "country": "Kenya",
                "currency": "KES"
            },
            "cse": {
                "name": "Casablanca Stock Exchange",
                "base_url": "https://www.casablanca-bourse.com",
                "country": "Morocco",
                "currency": "MAD"
            }
        }
        
        self.symbols_cache = {}
        self.symbols_last_updated = {}
        
    async def get_markets(self) -> List[Dict[str, str]]:
        """
        Get list of available African markets
        
        Returns:
            List[Dict[str, str]]: List of markets with code and name
        """
        return [{"code": code, **info} for code, info in self.markets.items()]

    async def get_symbols(self, market: str = "jse") -> Sequence[Dict[str, str]]:
        """
        Get list of available symbols for a specific African market
        
        Args:
            market (str): Market code (jse, ngx, egx, nse, cse)
        
        Returns:
            Sequence[Dict[str, str]]: List of symbols with details
        """
        market = market.lower()
        if market not in self.markets:
            logger.error(f"Invalid market code: {market}")
            return []
            
        # Return cached symbols if available and not too old
        if (market in self.symbols_cache and 
            market in self.symbols_last_updated and 
            (datetime.now() - self.symbols_last_updated[market]).total_seconds() < 86400):  # 24 hours
            logger.info(f"Returning {len(self.symbols_cache[market])} cached {market.upper()} symbols")
            return self.symbols_cache[market]
        
        try:
            # In a production environment, we would scrape each market's website
            # For now, using predefined lists of major symbols
            symbols_data = []
            
            if market == "jse":
                symbols_data = [
                    {"symbol": "SOL", "name": "Sasol Limited", "sector": "Energy", "country": "South Africa"},
                    {"symbol": "NPN", "name": "Naspers Limited", "sector": "Technology", "country": "South Africa"},
                    {"symbol": "SLM", "name": "Sanlam Limited", "sector": "Financial Services", "country": "South Africa"},
                    {"symbol": "FSR", "name": "FirstRand Limited", "sector": "Banking", "country": "South Africa"},
                    {"symbol": "SBK", "name": "Standard Bank Group", "sector": "Banking", "country": "South Africa"},
                    {"symbol": "AGL", "name": "Anglo American PLC", "sector": "Mining", "country": "South Africa"},
                    {"symbol": "BHG", "name": "BHP Group PLC", "sector": "Mining", "country": "South Africa"},
                    {"symbol": "MTN", "name": "MTN Group Limited", "sector": "Telecommunications", "country": "South Africa"},
                    {"symbol": "ABG", "name": "Absa Group Limited", "sector": "Banking", "country": "South Africa"},
                    {"symbol": "SHP", "name": "Shoprite Holdings", "sector": "Retail", "country": "South Africa"},
                    {"symbol": "ANH", "name": "Anheuser-Busch InBev", "sector": "Consumer Goods", "country": "South Africa"},
                    {"symbol": "BTI", "name": "British American Tobacco", "sector": "Consumer Goods", "country": "South Africa"},
                    {"symbol": "CPI", "name": "Capitec Bank Holdings", "sector": "Banking", "country": "South Africa"},
                    {"symbol": "DSY", "name": "Discovery Limited", "sector": "Financial Services", "country": "South Africa"},
                    {"symbol": "GLN", "name": "Glencore PLC", "sector": "Mining", "country": "South Africa"}
                ]
            elif market == "ngx":
                symbols_data = [
                    {"symbol": "DANGCEM", "name": "Dangote Cement PLC", "sector": "Industrial", "country": "Nigeria"},
                    {"symbol": "MTNN", "name": "MTN Nigeria", "sector": "Telecommunications", "country": "Nigeria"},
                    {"symbol": "ZENITHBANK", "name": "Zenith Bank PLC", "sector": "Banking", "country": "Nigeria"},
                    {"symbol": "GUARANTY", "name": "Guaranty Trust Bank", "sector": "Banking", "country": "Nigeria"},
                    {"symbol": "NESTLE", "name": "Nestle Nigeria PLC", "sector": "Consumer Goods", "country": "Nigeria"},
                    {"symbol": "NB", "name": "Nigerian Breweries", "sector": "Consumer Goods", "country": "Nigeria"},
                    {"symbol": "FBNH", "name": "FBN Holdings PLC", "sector": "Banking", "country": "Nigeria"},
                    {"symbol": "UBA", "name": "United Bank for Africa", "sector": "Banking", "country": "Nigeria"},
                    {"symbol": "ACCESSCORP", "name": "Access Holdings PLC", "sector": "Banking", "country": "Nigeria"},
                    {"symbol": "SEPLAT", "name": "Seplat Energy PLC", "sector": "Energy", "country": "Nigeria"}
                ]
            elif market == "egx":
                symbols_data = [
                    {"symbol": "COMI", "name": "Commercial International Bank", "sector": "Banking", "country": "Egypt"},
                    {"symbol": "HRHO", "name": "EFG Hermes Holding", "sector": "Financial Services", "country": "Egypt"},
                    {"symbol": "TMGH", "name": "Talaat Moustafa Group", "sector": "Real Estate", "country": "Egypt"},
                    {"symbol": "EFIC", "name": "Edita Food Industries", "sector": "Consumer Goods", "country": "Egypt"},
                    {"symbol": "EAST", "name": "Eastern Company", "sector": "Consumer Goods", "country": "Egypt"},
                    {"symbol": "SWDY", "name": "Elsewedy Electric", "sector": "Industrial", "country": "Egypt"},
                    {"symbol": "ETEL", "name": "Telecom Egypt", "sector": "Telecommunications", "country": "Egypt"},
                    {"symbol": "EKHO", "name": "Egypt Kuwait Holding", "sector": "Investments", "country": "Egypt"},
                    {"symbol": "ORWE", "name": "Oriental Weavers", "sector": "Consumer Goods", "country": "Egypt"},
                    {"symbol": "ABUK", "name": "Abou Kir Fertilizers", "sector": "Materials", "country": "Egypt"}
                ]
            elif market == "nse":
                symbols_data = [
                    {"symbol": "SCOM", "name": "Safaricom PLC", "sector": "Telecommunications", "country": "Kenya"},
                    {"symbol": "EQTY", "name": "Equity Group Holdings", "sector": "Banking", "country": "Kenya"},
                    {"symbol": "KCB", "name": "KCB Group PLC", "sector": "Banking", "country": "Kenya"},
                    {"symbol": "COOP", "name": "Co-operative Bank", "sector": "Banking", "country": "Kenya"},
                    {"symbol": "EABL", "name": "East African Breweries", "sector": "Consumer Goods", "country": "Kenya"},
                    {"symbol": "BAT", "name": "British American Tobacco Kenya", "sector": "Consumer Goods", "country": "Kenya"},
                    {"symbol": "ABSA", "name": "Absa Bank Kenya", "sector": "Banking", "country": "Kenya"},
                    {"symbol": "SBIC", "name": "Stanbic Holdings", "sector": "Banking", "country": "Kenya"},
                    {"symbol": "JUB", "name": "Jubilee Holdings", "sector": "Insurance", "country": "Kenya"},
                    {"symbol": "SCBK", "name": "Standard Chartered Bank Kenya", "sector": "Banking", "country": "Kenya"}
                ]
            elif market == "cse":
                symbols_data = [
                    {"symbol": "IAM", "name": "Maroc Telecom", "sector": "Telecommunications", "country": "Morocco"},
                    {"symbol": "ATW", "name": "Attijariwafa Bank", "sector": "Banking", "country": "Morocco"},
                    {"symbol": "BOA", "name": "BMCE Bank of Africa", "sector": "Banking", "country": "Morocco"},
                    {"symbol": "BCP", "name": "Banque Centrale Populaire", "sector": "Banking", "country": "Morocco"},
                    {"symbol": "ADH", "name": "Douja Promotion Groupe Addoha", "sector": "Real Estate", "country": "Morocco"},
                    {"symbol": "CIH", "name": "CIH Bank", "sector": "Banking", "country": "Morocco"},
                    {"symbol": "COL", "name": "Colorado", "sector": "Consumer Goods", "country": "Morocco"},
                    {"symbol": "CMT", "name": "Compagnie Minière de Touissit", "sector": "Mining", "country": "Morocco"},
                    {"symbol": "LBV", "name": "Label Vie", "sector": "Retail", "country": "Morocco"},
                    {"symbol": "MNG", "name": "Managem", "sector": "Mining", "country": "Morocco"}
                ]
            
            # Add market and currency information to each symbol
            for symbol_data in symbols_data:
                symbol_data["market"] = market.upper()
                symbol_data["currency"] = self.markets[market]["currency"]
            
            self.symbols_cache[market] = symbols_data
            self.symbols_last_updated[market] = datetime.now()
            logger.info(f"Retrieved {len(symbols_data)} {market.upper()} symbols")
            return symbols_data
            
        except Exception as e:
            logger.error(f"Error retrieving {market.upper()} symbols: {str(e)}")
            # Return cached symbols if available, otherwise an empty list
            return self.symbols_cache.get(market, [])

    async def fetch_data(self, 
                     market: str = "jse",
                     symbol: Optional[str] = None, 
                     start_date: Optional[str] = None, 
                     end_date: Optional[str] = None,
                     save_format: Optional[str] = None,
                     output_path: Optional[str] = None,
                     **kwargs) -> pd.DataFrame:
        """
        Fetch data for African markets
        
        Args:
            market (str): Market code (jse, ngx, egx, nse, cse)
            symbol (Optional[str]): Symbol to fetch data for
            start_date (Optional[str]): Start date in YYYY-MM-DD format
            end_date (Optional[str]): End date in YYYY-MM-DD format
            save_format (Optional[str]): Format to save data ('json', 'csv', None)
            output_path (Optional[str]): Path to save the output file
            
        Returns:
            pd.DataFrame: DataFrame with market data
        """
        market = market.lower()
        if market not in self.markets:
            logger.error(f"Invalid market code: {market}")
            return pd.DataFrame()
            
        self.log_fetch_attempt({
            "market": market, 
            "symbol": symbol, 
            "start_date": start_date, 
            "end_date": end_date
        })
        
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
                # Fetch data for a specific symbol
                df = await self._fetch_symbol_data(market, symbol, start_date, end_date)
            else:
                # Fetch data for all available symbols in the market
                symbols_data = await self.get_symbols(market)
                
                # Limit to first 10 symbols for demo/performance purposes
                symbols_data = symbols_data[:10]
                
                all_dfs = []
                for symbol_info in symbols_data:
                    try:
                        # Get symbol from info dictionary
                        current_symbol = symbol_info["symbol"]
                        
                        # Fetch data for this symbol
                        sym_df = await self._fetch_symbol_data(market, current_symbol, start_date, end_date)
                        
                        # Add data to our collection if not empty
                        if not sym_df.empty:
                            # Add additional symbol information to the dataframe
                            for key in ["name", "sector", "country", "currency"]:
                                if key in symbol_info:
                                    sym_df[key] = symbol_info[key]
                            all_dfs.append(sym_df)
                    except Exception as e:
                        # In case of error, log using the symbol from the info dictionary
                        if "symbol" in symbol_info:
                            logger.error(f"Error fetching data for symbol {symbol_info['symbol']} in {market.upper()}: {str(e)}")
                        else:
                            logger.error(f"Error fetching data for unknown symbol in {market.upper()}: {str(e)}")
                        logger.debug(traceback.format_exc())
                
                if all_dfs:
                    df = pd.concat(all_dfs, ignore_index=True)
                else:
                    df = pd.DataFrame()
            
            if not df.empty:
                self.log_fetch_success(len(df))
                
                # Save data to file if requested
                if save_format and df is not None and not df.empty:
                    saved_path = self.save_data(df, market, symbol, start_date, end_date, save_format, output_path)
                    logger.info(f"Saved {market.upper()} data to {saved_path}")
            else:
                logger.warning(f"No data retrieved for {market.upper()} {symbol if symbol else 'symbols'}")
            
            return df
        
        except Exception as e:
            self.log_fetch_error(e)
            logger.debug(traceback.format_exc())
            return pd.DataFrame()  # Return empty DataFrame on error
    
    async def _fetch_symbol_data(self, market: str, symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch historical data for a specific symbol in an African market
        
        Args:
            market (str): Market code
            symbol (str): Symbol to fetch
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with historical data
        """
        # Real implementation would use different APIs or web scraping methods for each market
        # For now, we'll use a modified version of the JSE data generation
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate number of business days
        days = (end_dt - start_dt).days + 1
        
        # Base values depend on symbol and market
        symbol_seed = sum(ord(c) for c in symbol) % 100
        market_seed = sum(ord(c) for c in market) % 50
        
        # Different base price ranges for different markets (based on typical price ranges in those markets)
        market_price_ranges = {
            "jse": (50, 1000),    # South African stocks often trade in hundreds of Rand
            "ngx": (5, 100),      # Nigerian stocks typically in tens of Naira
            "egx": (5, 50),       # Egyptian stocks typically in tens of Pounds
            "nse": (10, 200),     # Kenyan stocks typically in tens of Shillings
            "cse": (100, 2000)    # Moroccan stocks can be in hundreds or thousands of Dirhams
        }
        
        min_price, max_price = market_price_ranges.get(market, (50, 500))
        price_range = max_price - min_price
        
        # Calculate base price within the appropriate range for this market
        base_price = min_price + ((symbol_seed * market_seed) % price_range)
        
        # Generate simulated price data
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        current_dt = start_dt
        close_price = base_price
        
        while current_dt <= end_dt:
            # Skip weekends (simple implementation)
            if current_dt.weekday() < 5:  # 0-4 are Monday to Friday
                # Daily change - small random fluctuation around previous close
                # Use both symbol and market seeds to add variability
                daily_change_pct = ((symbol_seed + market_seed) % 15 - 5) / 100  # -5% to +9%
                
                # Calculate prices
                open_price = close_price * (1 + (daily_change_pct / 5))
                close_price = open_price * (1 + daily_change_pct)
                high_price = max(open_price, close_price) * 1.02
                low_price = min(open_price, close_price) * 0.98
                
                # Volume - also varies by symbol and market
                base_volume = 10000 + (symbol_seed * 1000)
                if market in ["jse", "egx"]:  # More liquid markets
                    base_volume *= 5
                volume = int(base_volume * (0.7 + days / 200))
                
                # Add to lists
                dates.append(current_dt.strftime("%Y-%m-%d"))
                opens.append(round(open_price, 2))
                highs.append(round(high_price, 2))
                lows.append(round(low_price, 2))
                closes.append(round(close_price, 2))
                volumes.append(volume)
            
            current_dt += timedelta(days=1)
        
        # Create DataFrame
        df = pd.DataFrame({
            'date': dates,
            'symbol': symbol,
            'open': opens,
            'high': highs,
            'low': lows,
            'close': closes,
            'volume': volumes,
            'market': market.upper(),
            'exchange': self.markets[market]["name"],
            'asset_type': 'stock'
        })
        
        return df
    
    def save_data(self, 
               df: pd.DataFrame, 
               market: str, 
               symbol: Optional[str], 
               start_date: str, 
               end_date: str, 
               format: str = 'json',
               output_path: Optional[str] = None) -> str:
        """
        Save fetched data to a file in JSON or CSV format
        
        Args:
            df (pd.DataFrame): DataFrame with market data
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
        filename = f"{market.upper()}{symbol_part}_{start_date}_to_{end_date}_{timestamp}.{format}"
        filepath = os.path.join(output_path, filename)
        
        # Save to file
        if format == 'json':
            # Convert to JSON (use date_format to make dates JSON serializable)
            df.to_json(filepath, orient='records', date_format='iso')
        else:  # CSV
            df.to_csv(filepath, index=False)
            
        return filepath
    
    async def export_data(self, 
                      market: str = "jse",
                      symbol: Optional[str] = None, 
                      start_date: Optional[str] = None, 
                      end_date: Optional[str] = None,
                      format: str = 'json',
                      output_path: Optional[str] = None) -> str:
        """
        Fetch and export data to a file
        
        Args:
            market (str): Market code (jse, ngx, egx, nse, cse)
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
            logger.error(f"No data to export for {market.upper()} {symbol if symbol else 'all symbols'}")
            return ""
            
        return self.save_data(df, market, symbol, 
                           start_date or (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
                           end_date or datetime.now().strftime("%Y-%m-%d"),
                           format, output_path)