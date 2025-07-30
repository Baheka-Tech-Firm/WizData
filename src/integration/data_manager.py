"""
WizData Integration Data Manager

This module provides a unified interface for accessing financial and ESG data
from various sources. It combines data from multiple providers and offers methods
to retrieve, format, and cache data for consumption by the WizData platform.
"""

import os
import time
import logging
import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any
from pathlib import Path

from src.integration.finance.alpha_vantage_client import AlphaVantageClient
from src.integration.finance.sp_global_client import SPGlobalClient
from src.integration.finance.bloomberg_client import BloombergClient
from src.integration.finance.refinitiv_client import RefinitivClient
from src.integration.esg.world_bank_client import WorldBankClient

# Setup logging
logger = logging.getLogger(__name__)

class DataManager:
    """
    Unified data manager for financial and ESG data
    
    This class provides methods to access and combine data from various sources,
    along with caching capabilities to avoid excessive API calls.
    """
    
    # Cache directory relative to project root
    CACHE_DIR = "data/cache"
    
    def __init__(self):
        """Initialize the data manager with all required data sources"""
        # Create data source clients (with optional API keys)
        try:
            self.alpha_vantage = AlphaVantageClient()
        except Exception as e:
            logger.warning(f"Alpha Vantage client initialization failed: {e}")
            self.alpha_vantage = None
            
        try:
            self.sp_global = SPGlobalClient()
        except Exception as e:
            logger.warning(f"S&P Global client initialization failed: {e}")
            self.sp_global = None
            
        try:
            self.bloomberg = BloombergClient()
        except Exception as e:
            logger.warning(f"Bloomberg client initialization failed: {e}")
            self.bloomberg = None
            
        try:
            self.refinitiv = RefinitivClient()
        except Exception as e:
            logger.warning(f"Refinitiv client initialization failed: {e}")
            self.refinitiv = None
            
        try:
            self.world_bank = WorldBankClient()
        except Exception as e:
            logger.warning(f"World Bank client initialization failed: {e}")
            self.world_bank = None
        
        # Ensure cache directory exists
        os.makedirs(self.CACHE_DIR, exist_ok=True)
    
    def _get_cache_path(self, data_type: str, key: str) -> str:
        """
        Get the file path for a cache item
        
        Args:
            data_type: Type of data (e.g., 'finance', 'esg')
            key: Cache key
            
        Returns:
            Path to the cache file
        """
        return os.path.join(self.CACHE_DIR, f"{data_type}_{key}.json")
    
    def _cache_data(self, data_type: str, key: str, data: Any) -> None:
        """
        Cache data to disk
        
        Args:
            data_type: Type of data (e.g., 'finance', 'esg')
            key: Cache key
            data: Data to cache
        """
        try:
            cache_path = self._get_cache_path(data_type, key)
            cache_data = {
                "timestamp": datetime.now().isoformat(),
                "data": data
            }
            with open(cache_path, "w") as f:
                json.dump(cache_data, f)
        except Exception as e:
            logger.error(f"Failed to cache data: {str(e)}")
    
    def _get_cached_data(self, data_type: str, key: str, max_age_hours: int = 24) -> Optional[Any]:
        """
        Get data from cache if available and not expired
        
        Args:
            data_type: Type of data (e.g., 'finance', 'esg')
            key: Cache key
            max_age_hours: Maximum age of cached data in hours
            
        Returns:
            Cached data if available and fresh, None otherwise
        """
        cache_path = self._get_cache_path(data_type, key)
        if not os.path.exists(cache_path):
            return None
        
        try:
            with open(cache_path, "r") as f:
                cache_data = json.load(f)
            
            cache_time = datetime.fromisoformat(cache_data["timestamp"])
            if datetime.now() - cache_time > timedelta(hours=max_age_hours):
                return None
            
            return cache_data["data"]
        except Exception as e:
            logger.error(f"Failed to read cache: {str(e)}")
            return None
    
    # Financial data methods
    
    def get_stock_data(self, symbol: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get stock data for a given symbol
        
        Args:
            symbol: Stock symbol
            use_cache: Whether to use cached data if available
            
        Returns:
            Stock data including time series and company info
        """
        cache_key = f"stock_{symbol}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key)
            if cached_data:
                logger.info(f"Using cached stock data for {symbol}")
                return cached_data
        
        # Get data from source
        try:
            # Get time series data
            time_series = self.alpha_vantage.get_time_series_daily(symbol)
            
            # Get company overview
            company_info = self.alpha_vantage.get_company_overview(symbol)
            
            # Format time series data
            formatted_time_series = self.alpha_vantage.format_time_series_data(time_series)
            
            # Combine data
            result = {
                "symbol": symbol,
                "company_info": company_info,
                "time_series": formatted_time_series
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get stock data for {symbol}: {str(e)}")
            raise
    
    def get_sector_performance(self, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get sector performance data
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            Sector performance data
        """
        cache_key = "sector_performance"
        
        # Try to get from cache - use a shorter cache period for sector data
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info("Using cached sector performance data")
                return cached_data
        
        # Get data from source
        try:
            sector_data = self.alpha_vantage.get_sector_performance()
            
            # Format sector data
            result = {
                "timestamp": datetime.now().isoformat(),
                "data": sector_data
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get sector performance data: {str(e)}")
            raise
    
    def get_forex_data(self, from_currency: str, to_currency: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get forex exchange rate data
        
        Args:
            from_currency: From currency code
            to_currency: To currency code
            use_cache: Whether to use cached data if available
            
        Returns:
            Forex exchange rate data
        """
        cache_key = f"forex_{from_currency}_{to_currency}"
        
        # Try to get from cache - use a shorter cache period for forex data
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached forex data for {from_currency}/{to_currency}")
                return cached_data
        
        # Get data from source
        try:
            forex_data = self.alpha_vantage.get_forex_data(from_currency, to_currency)
            
            # Format forex data
            result = {
                "timestamp": datetime.now().isoformat(),
                "from_currency": from_currency,
                "to_currency": to_currency,
                "data": forex_data
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get forex data for {from_currency}/{to_currency}: {str(e)}")
            raise
    
    def get_crypto_data(self, symbol: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get cryptocurrency data
        
        Args:
            symbol: Cryptocurrency symbol
            use_cache: Whether to use cached data if available
            
        Returns:
            Cryptocurrency data
        """
        cache_key = f"crypto_{symbol}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached crypto data for {symbol}")
                return cached_data
        
        # Get data from source
        try:
            crypto_data = self.alpha_vantage.get_crypto_data(symbol)
            
            # Format crypto data
            result = {
                "timestamp": datetime.now().isoformat(),
                "symbol": symbol,
                "data": crypto_data
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get crypto data for {symbol}: {str(e)}")
            raise
    
    # ESG data methods
    
    def get_country_esg_data(self, country_code: str, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get ESG data for a country
        
        Args:
            country_code: Country code (ISO 3166-1 alpha-2 or alpha-3)
            use_cache: Whether to use cached data if available
            
        Returns:
            ESG data for the country
        """
        cache_key = f"esg_{country_code}"
        
        # Try to get from cache - ESG data can be cached longer
        if use_cache:
            cached_data = self._get_cached_data("esg", cache_key, max_age_hours=168)  # 7 days
            if cached_data:
                logger.info(f"Using cached ESG data for {country_code}")
                return cached_data
        
        # Get data from source
        try:
            # Get country info
            country_info = self.world_bank.get_country_data(country_code)
            
            # Get ESG data for the last 10 years
            current_year = datetime.now().year
            start_year = current_year - 10
            esg_data = self.world_bank.get_all_esg_indicators(country_code, start_year, current_year)
            
            # Calculate ESG scores for the most recent year with data
            # In reality, we'd want to find the most recent year with sufficient data
            esg_scores = self.world_bank.calculate_esg_scores(country_code, current_year - 1)
            
            # Combine data
            result = {
                "country_code": country_code,
                "country_info": country_info,
                "esg_data": esg_data,
                "esg_scores": esg_scores,
                "timestamp": datetime.now().isoformat()
            }
            
            # Cache the result
            self._cache_data("esg", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get ESG data for {country_code}: {str(e)}")
            raise
    
    def get_countries_list(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get list of countries
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            List of countries
        """
        cache_key = "countries_list"
        
        # Try to get from cache - country list rarely changes
        if use_cache:
            cached_data = self._get_cached_data("esg", cache_key, max_age_hours=720)  # 30 days
            if cached_data:
                logger.info("Using cached countries list")
                return cached_data
        
        # Get data from source
        try:
            countries = self.world_bank.get_countries()
            
            # Cache the result
            self._cache_data("esg", cache_key, countries)
            
            return countries
        except Exception as e:
            logger.error(f"Failed to get countries list: {str(e)}")
            raise
    
    def get_esg_scores_for_region(self, region_code: str, year: Optional[int] = None, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get ESG scores for a region (country or sub-region)
        
        Args:
            region_code: Region code (could be country or custom region)
            year: Year for the data (default to most recent)
            use_cache: Whether to use cached data if available
            
        Returns:
            ESG scores for the region
        """
        if year is None:
            year = datetime.now().year - 1  # Default to previous year
            
        cache_key = f"esg_scores_{region_code}_{year}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("esg", cache_key, max_age_hours=168)  # 7 days
            if cached_data:
                logger.info(f"Using cached ESG scores for {region_code} ({year})")
                return cached_data
        
        # For country-level data
        if len(region_code) == 2 or len(region_code) == 3:
            try:
                # Calculate ESG scores
                esg_scores = self.world_bank.calculate_esg_scores(region_code, year)
                
                # Get country info
                country_info = self.world_bank.get_country_data(region_code)
                
                # Format result
                result = {
                    "region_code": region_code,
                    "region_name": country_info.get("name", region_code),
                    "year": year,
                    "scores": esg_scores,
                    "timestamp": datetime.now().isoformat()
                }
                
                # Cache the result
                self._cache_data("esg", cache_key, result)
                
                return result
            except Exception as e:
                logger.error(f"Failed to get ESG scores for region {region_code} ({year}): {str(e)}")
                raise
        
        # For custom regions, would need custom logic here
        # This would be where we'd apply our special algorithms for African regions
        
        raise ValueError(f"Unsupported region code: {region_code}")
    
    def compare_esg_scores(self, region_codes: List[str], year: Optional[int] = None, 
                         dimension: Optional[str] = None) -> Dict[str, Any]:
        """
        Compare ESG scores for multiple regions
        
        Args:
            region_codes: List of region codes
            year: Year for the data (default to most recent)
            dimension: ESG dimension to compare (default to all)
            
        Returns:
            Comparative ESG scores
        """
        results = {}
        
        for region_code in region_codes:
            try:
                region_data = self.get_esg_scores_for_region(region_code, year)
                if dimension:
                    if dimension in region_data["scores"]:
                        results[region_code] = {
                            "region_name": region_data["region_name"],
                            "score": region_data["scores"][dimension]
                        }
                else:
                    results[region_code] = {
                        "region_name": region_data["region_name"],
                        "scores": region_data["scores"]
                    }
            except Exception as e:
                logger.warning(f"Failed to get ESG scores for comparison: {region_code} - {str(e)}")
                continue
        
        return {
            "year": year or datetime.now().year - 1,
            "dimension": dimension or "all",
            "regions": results,
            "timestamp": datetime.now().isoformat()
        }
    
    # S&P Global data methods
    
    def get_sp500_constituents(self, use_cache: bool = True) -> List[Dict[str, Any]]:
        """
        Get the current constituents of the S&P 500 index
        
        Args:
            use_cache: Whether to use cached data if available
            
        Returns:
            List of S&P 500 constituent companies
        """
        cache_key = "sp500_constituents"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=24)
            if cached_data:
                logger.info("Using cached S&P 500 constituents data")
                return cached_data
        
        # Get data from source
        try:
            constituents = self.sp_global.get_sp500_constituents()
            
            # Cache the result
            self._cache_data("finance", cache_key, constituents)
            
            return constituents
        except Exception as e:
            logger.error(f"Failed to get S&P 500 constituents: {str(e)}")
            raise
    
    def get_index_performance(self, index_id: str = "SP500", period: str = "1y", use_cache: bool = True) -> Dict[str, Any]:
        """
        Get performance data for a specific index
        
        Args:
            index_id: Index identifier (e.g., SP500, DJIA)
            period: Time period (1d, 5d, 1m, 3m, 6m, 1y, 5y)
            use_cache: Whether to use cached data if available
            
        Returns:
            Index performance data
        """
        cache_key = f"index_performance_{index_id}_{period}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached index performance data for {index_id}")
                return cached_data
        
        # Get data from source
        try:
            performance = self.sp_global.get_index_performance(index_id, period)
            
            # Format data
            result = {
                "index_id": index_id,
                "period": period,
                "timestamp": datetime.now().isoformat(),
                "data": performance
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get index performance for {index_id}: {str(e)}")
            raise
    
    # Bloomberg data methods
    
    def get_market_indices(self, region: str = None, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get market indices data
        
        Args:
            region: Region filter (e.g., US, EU, ASIA)
            use_cache: Whether to use cached data if available
            
        Returns:
            Market indices data
        """
        cache_key = f"market_indices_{region or 'global'}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached market indices data for {region or 'global'}")
                return cached_data
        
        # Get data from source
        try:
            if region:
                indices = self.refinitiv.get_market_indices(region)
            else:
                indices = self.bloomberg.get_global_market_indices()
            
            # Format data
            result = {
                "region": region or "global",
                "timestamp": datetime.now().isoformat(),
                "data": indices
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get market indices for {region or 'global'}: {str(e)}")
            raise
    
    def get_fixed_income_data(self, issuer_type: str = "sovereign", currency: str = "USD", use_cache: bool = True) -> Dict[str, Any]:
        """
        Get fixed income market data
        
        Args:
            issuer_type: Type of issuer (sovereign, corporate)
            currency: Currency code
            use_cache: Whether to use cached data if available
            
        Returns:
            Fixed income market data
        """
        cache_key = f"fixed_income_{issuer_type}_{currency}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached fixed income data for {issuer_type}/{currency}")
                return cached_data
        
        # Get data from source
        try:
            fixed_income_data = self.bloomberg.get_fixed_income_data(issuer_type, currency)
            
            # Format data
            result = {
                "issuer_type": issuer_type,
                "currency": currency,
                "timestamp": datetime.now().isoformat(),
                "data": fixed_income_data
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get fixed income data for {issuer_type}/{currency}: {str(e)}")
            raise
    
    # Refinitiv data methods
    
    def get_time_series(self, ric: str, interval: str = "P1D", start: str = None, end: str = None, use_cache: bool = True) -> Dict[str, Any]:
        """
        Get time series data for a RIC (Reuters Instrument Code)
        
        Args:
            ric: Reuters Instrument Code
            interval: Time interval (P1D for daily, PT1M for minute, etc.)
            start: Start date in ISO format
            end: End date in ISO format
            use_cache: Whether to use cached data if available
            
        Returns:
            Time series data
        """
        cache_key = f"time_series_{ric}_{interval}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=4)
            if cached_data:
                logger.info(f"Using cached time series data for {ric}")
                return cached_data
        
        # Get data from source
        try:
            time_series = self.refinitiv.get_time_series(ric, None, interval, start, end)
            
            # Format data
            formatted_time_series = self.refinitiv.format_time_series_data(time_series)
            
            result = {
                "ric": ric,
                "interval": interval,
                "start": start,
                "end": end,
                "timestamp": datetime.now().isoformat(),
                "data": formatted_time_series
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get time series data for {ric}: {str(e)}")
            raise
    
    def get_company_fundamentals(self, identifier: str, source: str = "refinitiv", use_cache: bool = True) -> Dict[str, Any]:
        """
        Get company fundamental data from the specified source
        
        Args:
            identifier: Company identifier (ticker or RIC)
            source: Data source provider (refinitiv, bloomberg, sp_global, alpha_vantage)
            use_cache: Whether to use cached data if available
            
        Returns:
            Company fundamental data
        """
        cache_key = f"fundamentals_{source}_{identifier}"
        
        # Try to get from cache
        if use_cache:
            cached_data = self._get_cached_data("finance", cache_key, max_age_hours=24)
            if cached_data:
                logger.info(f"Using cached fundamentals data for {identifier} from {source}")
                return cached_data
        
        # Get data from source
        try:
            if source == "refinitiv":
                fundamentals = self.refinitiv.get_fundamentals(identifier)
            elif source == "bloomberg":
                fundamentals = self.bloomberg.get_company_fundamentals(identifier)
            elif source == "sp_global":
                fundamentals = self.sp_global.get_company_financials(identifier)
            elif source == "alpha_vantage":
                fundamentals = self.alpha_vantage.get_company_overview(identifier)
            else:
                raise ValueError(f"Unsupported data source: {source}")
            
            # Format data
            result = {
                "identifier": identifier,
                "source": source,
                "timestamp": datetime.now().isoformat(),
                "data": fundamentals
            }
            
            # Cache the result
            self._cache_data("finance", cache_key, result)
            
            return result
        except Exception as e:
            logger.error(f"Failed to get fundamentals for {identifier} from {source}: {str(e)}")
            raise
    
    # Combined data methods
    
    def get_market_esg_impact(self, symbol: str, country_code: str) -> Dict[str, Any]:
        """
        Get combined market and ESG data to evaluate ESG impact
        
        Args:
            symbol: Stock symbol
            country_code: Country code for ESG data
            
        Returns:
            Combined market and ESG data with impact analysis
        """
        # Get stock data
        stock_data = self.get_stock_data(symbol)
        
        # Get ESG data
        esg_data = self.get_country_esg_data(country_code)
        
        # Here we would implement some analysis to correlate ESG factors with stock performance
        # This is a simplified placeholder implementation
        
        result = {
            "symbol": symbol,
            "company_name": stock_data["company_info"].get("Name", symbol),
            "country_code": country_code,
            "country_name": esg_data["country_info"].get("name", country_code),
            "timestamp": datetime.now().isoformat(),
            "stock_data": {
                "latest_price": stock_data["time_series"][0]["close"] if stock_data["time_series"] else None,
                "change_30d": self._calculate_price_change(stock_data["time_series"], 30) if stock_data["time_series"] else None,
                "change_90d": self._calculate_price_change(stock_data["time_series"], 90) if stock_data["time_series"] else None,
                "change_1y": self._calculate_price_change(stock_data["time_series"], 365) if stock_data["time_series"] else None
            },
            "esg_scores": esg_data["esg_scores"],
            # This would be where we'd put our proprietary correlation analysis
            "impact_analysis": {
                "correlation": 0.0,  # Placeholder
                "esg_impact_estimate": "Neutral",  # Placeholder
                "key_factors": []  # Placeholder
            }
        }
        
        return result
    
    def _calculate_price_change(self, time_series: List[Dict[str, Any]], days: int) -> float:
        """
        Calculate price change over a period
        
        Args:
            time_series: Time series data
            days: Number of days for the change
            
        Returns:
            Percentage change
        """
        if not time_series or len(time_series) < 2:
            return 0.0
        
        latest_price = time_series[0]["close"]
        
        # Find the price 'days' days ago
        older_price = latest_price
        for entry in time_series:
            entry_date = datetime.fromisoformat(entry["date"])
            if (datetime.now() - entry_date).days >= days:
                older_price = entry["close"]
                break
        
        if older_price == 0:
            return 0.0
            
        return ((latest_price - older_price) / older_price) * 100.0