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

class GlobalMarketsFetcher(BaseFetcher):
    """
    Comprehensive fetcher for global stock markets including:
    - Australian Securities Exchange (ASX)
    - London Stock Exchange (LSE)
    - NASDAQ
    - New York Stock Exchange (NYSE)
    - Tokyo Stock Exchange (TSE/TYO)
    """
    
    def __init__(self):
        """Initialize the Global Markets fetcher"""
        super().__init__("Global Markets Fetcher")
        self.markets = {
            "asx": {
                "name": "Australian Securities Exchange",
                "base_url": "https://www.asx.com.au",
                "country": "Australia",
                "currency": "AUD"
            },
            "lse": {
                "name": "London Stock Exchange",
                "base_url": "https://www.londonstockexchange.com",
                "country": "United Kingdom",
                "currency": "GBP"
            },
            "nasdaq": {
                "name": "NASDAQ",
                "base_url": "https://www.nasdaq.com",
                "country": "United States",
                "currency": "USD"
            },
            "nyse": {
                "name": "New York Stock Exchange",
                "base_url": "https://www.nyse.com",
                "country": "United States",
                "currency": "USD"
            },
            "tyo": {
                "name": "Tokyo Stock Exchange",
                "base_url": "https://www.jpx.co.jp",
                "country": "Japan",
                "currency": "JPY"
            }
        }
        
        self.symbols_cache = {}
        self.symbols_last_updated = {}
        
    async def get_markets(self) -> List[Dict[str, str]]:
        """
        Get list of available global markets
        
        Returns:
            List[Dict[str, str]]: List of markets with code and name
        """
        return [{"code": code, **info} for code, info in self.markets.items()]

    async def get_symbols(self, market: str = "nasdaq", *args, **kwargs) -> Sequence[Dict[str, str]]:
        """
        Get list of available symbols for a specific global market
        
        Args:
            market (str): Market code (asx, lse, nasdaq, nyse, tyo)
        
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
            # In a production environment, we would use APIs for each market
            # For now, using predefined lists of major symbols
            symbols_data = []
            
            if market == "asx":
                symbols_data = [
                    {"symbol": "BHP", "name": "BHP Group Ltd", "sector": "Materials", "country": "Australia"},
                    {"symbol": "CBA", "name": "Commonwealth Bank of Australia", "sector": "Financials", "country": "Australia"},
                    {"symbol": "CSL", "name": "CSL Limited", "sector": "Health Care", "country": "Australia"},
                    {"symbol": "NAB", "name": "National Australia Bank", "sector": "Financials", "country": "Australia"},
                    {"symbol": "WBC", "name": "Westpac Banking Corporation", "sector": "Financials", "country": "Australia"},
                    {"symbol": "ANZ", "name": "Australia and New Zealand Banking", "sector": "Financials", "country": "Australia"},
                    {"symbol": "MQG", "name": "Macquarie Group", "sector": "Financials", "country": "Australia"},
                    {"symbol": "WES", "name": "Wesfarmers Limited", "sector": "Consumer Staples", "country": "Australia"},
                    {"symbol": "WOW", "name": "Woolworths Group", "sector": "Consumer Staples", "country": "Australia"},
                    {"symbol": "RIO", "name": "Rio Tinto Limited", "sector": "Materials", "country": "Australia"},
                    {"symbol": "TCL", "name": "Transurban Group", "sector": "Industrials", "country": "Australia"},
                    {"symbol": "TLS", "name": "Telstra Corporation", "sector": "Communication Services", "country": "Australia"},
                    {"symbol": "GMG", "name": "Goodman Group", "sector": "Real Estate", "country": "Australia"},
                    {"symbol": "FMG", "name": "Fortescue Metals Group", "sector": "Materials", "country": "Australia"},
                    {"symbol": "NCM", "name": "Newcrest Mining", "sector": "Materials", "country": "Australia"}
                ]
            elif market == "lse":
                symbols_data = [
                    {"symbol": "HSBA", "name": "HSBC Holdings plc", "sector": "Financials", "country": "United Kingdom"},
                    {"symbol": "BP", "name": "BP plc", "sector": "Energy", "country": "United Kingdom"},
                    {"symbol": "BATS", "name": "British American Tobacco", "sector": "Consumer Staples", "country": "United Kingdom"},
                    {"symbol": "GSK", "name": "GlaxoSmithKline plc", "sector": "Health Care", "country": "United Kingdom"},
                    {"symbol": "AZN", "name": "AstraZeneca plc", "sector": "Health Care", "country": "United Kingdom"},
                    {"symbol": "ULVR", "name": "Unilever plc", "sector": "Consumer Staples", "country": "United Kingdom"},
                    {"symbol": "RIO", "name": "Rio Tinto plc", "sector": "Materials", "country": "United Kingdom"},
                    {"symbol": "VOD", "name": "Vodafone Group plc", "sector": "Communication Services", "country": "United Kingdom"},
                    {"symbol": "DGE", "name": "Diageo plc", "sector": "Consumer Staples", "country": "United Kingdom"},
                    {"symbol": "LLOY", "name": "Lloyds Banking Group plc", "sector": "Financials", "country": "United Kingdom"},
                    {"symbol": "REL", "name": "RELX plc", "sector": "Industrials", "country": "United Kingdom"},
                    {"symbol": "PRU", "name": "Prudential plc", "sector": "Financials", "country": "United Kingdom"},
                    {"symbol": "NG", "name": "National Grid plc", "sector": "Utilities", "country": "United Kingdom"},
                    {"symbol": "RB", "name": "Reckitt Benckiser Group", "sector": "Consumer Staples", "country": "United Kingdom"},
                    {"symbol": "GLEN", "name": "Glencore plc", "sector": "Materials", "country": "United Kingdom"}
                ]
            elif market == "nasdaq":
                symbols_data = [
                    {"symbol": "AAPL", "name": "Apple Inc.", "sector": "Technology", "country": "United States"},
                    {"symbol": "MSFT", "name": "Microsoft Corporation", "sector": "Technology", "country": "United States"},
                    {"symbol": "AMZN", "name": "Amazon.com Inc.", "sector": "Consumer Discretionary", "country": "United States"},
                    {"symbol": "GOOGL", "name": "Alphabet Inc. Class A", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "GOOG", "name": "Alphabet Inc. Class C", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "META", "name": "Meta Platforms Inc.", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "TSLA", "name": "Tesla Inc.", "sector": "Consumer Discretionary", "country": "United States"},
                    {"symbol": "NVDA", "name": "NVIDIA Corporation", "sector": "Technology", "country": "United States"},
                    {"symbol": "PEP", "name": "PepsiCo Inc.", "sector": "Consumer Staples", "country": "United States"},
                    {"symbol": "COST", "name": "Costco Wholesale Corporation", "sector": "Consumer Staples", "country": "United States"},
                    {"symbol": "INTC", "name": "Intel Corporation", "sector": "Technology", "country": "United States"},
                    {"symbol": "CSCO", "name": "Cisco Systems Inc.", "sector": "Technology", "country": "United States"},
                    {"symbol": "ADBE", "name": "Adobe Inc.", "sector": "Technology", "country": "United States"},
                    {"symbol": "NFLX", "name": "Netflix Inc.", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "CMCSA", "name": "Comcast Corporation", "sector": "Communication Services", "country": "United States"}
                ]
            elif market == "nyse":
                symbols_data = [
                    {"symbol": "JPM", "name": "JPMorgan Chase & Co.", "sector": "Financials", "country": "United States"},
                    {"symbol": "JNJ", "name": "Johnson & Johnson", "sector": "Health Care", "country": "United States"},
                    {"symbol": "V", "name": "Visa Inc.", "sector": "Financials", "country": "United States"},
                    {"symbol": "PG", "name": "Procter & Gamble Co.", "sector": "Consumer Staples", "country": "United States"},
                    {"symbol": "XOM", "name": "Exxon Mobil Corporation", "sector": "Energy", "country": "United States"},
                    {"symbol": "MA", "name": "Mastercard Incorporated", "sector": "Financials", "country": "United States"},
                    {"symbol": "BAC", "name": "Bank of America Corp", "sector": "Financials", "country": "United States"},
                    {"symbol": "HD", "name": "Home Depot Inc.", "sector": "Consumer Discretionary", "country": "United States"},
                    {"symbol": "DIS", "name": "Walt Disney Company", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "VZ", "name": "Verizon Communications", "sector": "Communication Services", "country": "United States"},
                    {"symbol": "CVX", "name": "Chevron Corporation", "sector": "Energy", "country": "United States"},
                    {"symbol": "KO", "name": "Coca-Cola Company", "sector": "Consumer Staples", "country": "United States"},
                    {"symbol": "MRK", "name": "Merck & Co. Inc.", "sector": "Health Care", "country": "United States"},
                    {"symbol": "PFE", "name": "Pfizer Inc.", "sector": "Health Care", "country": "United States"},
                    {"symbol": "WMT", "name": "Walmart Inc.", "sector": "Consumer Staples", "country": "United States"}
                ]
            elif market == "tyo":
                symbols_data = [
                    {"symbol": "7203", "name": "Toyota Motor Corporation", "sector": "Consumer Discretionary", "country": "Japan"},
                    {"symbol": "9984", "name": "SoftBank Group Corp.", "sector": "Communication Services", "country": "Japan"},
                    {"symbol": "6758", "name": "Sony Group Corporation", "sector": "Consumer Discretionary", "country": "Japan"},
                    {"symbol": "6861", "name": "Keyence Corporation", "sector": "Information Technology", "country": "Japan"},
                    {"symbol": "6367", "name": "Daikin Industries,Ltd.", "sector": "Industrials", "country": "Japan"},
                    {"symbol": "8306", "name": "Mitsubishi UFJ Financial Group", "sector": "Financials", "country": "Japan"},
                    {"symbol": "6501", "name": "Hitachi, Ltd.", "sector": "Industrials", "country": "Japan"},
                    {"symbol": "7267", "name": "Honda Motor Co., Ltd.", "sector": "Consumer Discretionary", "country": "Japan"},
                    {"symbol": "9433", "name": "KDDI Corporation", "sector": "Communication Services", "country": "Japan"},
                    {"symbol": "4063", "name": "Shin-Etsu Chemical Co., Ltd.", "sector": "Materials", "country": "Japan"},
                    {"symbol": "4502", "name": "Takeda Pharmaceutical Company", "sector": "Health Care", "country": "Japan"},
                    {"symbol": "6098", "name": "Recruit Holdings Co., Ltd.", "sector": "Industrials", "country": "Japan"},
                    {"symbol": "9432", "name": "Nippon Telegraph and Telephone", "sector": "Communication Services", "country": "Japan"},
                    {"symbol": "7974", "name": "Nintendo Co., Ltd.", "sector": "Communication Services", "country": "Japan"},
                    {"symbol": "7751", "name": "Canon Inc.", "sector": "Information Technology", "country": "Japan"}
                ]
            
            # Add market and currency information to each symbol
            for symbol_data in symbols_data:
                symbol_data["market"] = market.upper()
                symbol_data["currency"] = self.markets[market]["currency"]
                symbol_data["exchange"] = self.markets[market]["name"]
            
            self.symbols_cache[market] = symbols_data
            self.symbols_last_updated[market] = datetime.now()
            logger.info(f"Retrieved {len(symbols_data)} {market.upper()} symbols")
            return symbols_data
            
        except Exception as e:
            logger.error(f"Error retrieving {market.upper()} symbols: {str(e)}")
            # Return cached symbols if available, otherwise an empty list
            return self.symbols_cache.get(market, [])

    async def fetch_data(self, 
                     market: str = "nasdaq",
                     symbol: Optional[str] = None, 
                     start_date: Optional[str] = None, 
                     end_date: Optional[str] = None,
                     save_format: Optional[str] = None,
                     output_path: Optional[str] = None,
                     **kwargs) -> pd.DataFrame:
        """
        Fetch data for global markets
        
        Args:
            market (str): Market code (asx, lse, nasdaq, nyse, tyo)
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
                            for key in ["name", "sector", "country", "currency", "exchange"]:
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
        Fetch historical data for a specific symbol in a global market
        
        Args:
            market (str): Market code
            symbol (str): Symbol to fetch
            start_date (str): Start date in YYYY-MM-DD format
            end_date (str): End date in YYYY-MM-DD format
            
        Returns:
            pd.DataFrame: DataFrame with historical data
        """
        # In a real implementation, this would use market-specific APIs
        # For demonstration purposes, we're generating representative data
        
        # Convert dates to datetime objects
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        # Calculate number of business days
        days = (end_dt - start_dt).days + 1
        
        # Base values depend on symbol and market
        symbol_seed = sum(ord(c) for c in symbol) % 100
        market_seed = sum(ord(c) for c in market) % 50
        
        # Different base price ranges for different markets
        market_price_ranges = {
            "asx": (10, 100),      # AUD
            "lse": (100, 5000),    # GBP (pence)
            "nasdaq": (50, 500),   # USD 
            "nyse": (40, 400),     # USD
            "tyo": (1000, 10000)   # JPY
        }
        
        # Determine if in pence (LSE) or normal currency
        is_pence = market == "lse"
        
        min_price, max_price = market_price_ranges.get(market, (50, 500))
        price_range = max_price - min_price
        
        # Calculate base price within the appropriate range for this market
        base_price = min_price + ((symbol_seed * market_seed) % price_range)
        
        # Volume ranges also vary by market
        volume_multipliers = {
            "nasdaq": 10,
            "nyse": 8,
            "asx": 3,
            "lse": 4,
            "tyo": 5
        }
        volume_multiplier = volume_multipliers.get(market, 1)
        
        # Generate simulated price data
        dates = []
        opens = []
        highs = []
        lows = []
        closes = []
        volumes = []
        
        current_dt = start_dt
        close_price = base_price
        
        # Volatility factors for different markets (higher = more volatile)
        volatility_factors = {
            "nasdaq": 2.0,
            "nyse": 1.5,
            "asx": 1.2,
            "lse": 1.3,
            "tyo": 1.1
        }
        volatility = volatility_factors.get(market, 1.0)
        
        while current_dt <= end_dt:
            # Skip weekends (simple implementation)
            if current_dt.weekday() < 5:  # 0-4 are Monday to Friday
                # Daily change - random fluctuation with market-specific volatility
                daily_change_pct = ((symbol_seed + market_seed + current_dt.day) % 21 - 10) / 100 * volatility
                
                # Calculate prices
                open_price = close_price * (1 + (daily_change_pct / 3))
                close_price = open_price * (1 + daily_change_pct)
                high_price = max(open_price, close_price) * 1.02
                low_price = min(open_price, close_price) * 0.98
                
                # Volume - varies by market and symbol popularity
                base_volume = 50000 + (symbol_seed * 5000)
                volume = int(base_volume * volume_multiplier * (0.7 + days / 200))
                
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
                      market: str = "nasdaq",
                      symbol: Optional[str] = None, 
                      start_date: Optional[str] = None, 
                      end_date: Optional[str] = None,
                      format: str = 'json',
                      output_path: Optional[str] = None) -> str:
        """
        Fetch and export data to a file
        
        Args:
            market (str): Market code (asx, lse, nasdaq, nyse, tyo)
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