"""
Alpha Vantage Financial Data Integration Client

This module provides a client for integrating with Alpha Vantage's financial data API.
Alpha Vantage provides real-time and historical financial market data that can be
resold to clients as part of the WizData platform.

Reference: https://www.alphavantage.co/documentation/
"""

import os
import time
import logging
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Any

# Setup logging
logger = logging.getLogger(__name__)

class AlphaVantageClient:
    """Client for Alpha Vantage financial data API"""
    
    BASE_URL = "https://www.alphavantage.co/query"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Alpha Vantage client
        
        Args:
            api_key: Alpha Vantage API key (defaults to environment variable if not provided)
        """
        self.api_key = api_key or os.environ.get("ALPHA_VANTAGE_API_KEY")
        self.is_enabled = bool(self.api_key)
        if not self.is_enabled:
            logger.warning("Alpha Vantage API key not found. Alpha Vantage features will be disabled.")
        
        self.session = requests.Session()
        self.request_count = 0
        self.last_request_time = 0
    
    def _make_request(self, params: Dict[str, str]) -> Dict[str, Any]:
        """
        Make a request to Alpha Vantage API with rate limiting
        
        Args:
            params: Query parameters for the request
            
        Returns:
            JSON response data
        """
        if not self.is_enabled:
            return {
                "error": "Alpha Vantage API not configured",
                "message": "ALPHA_VANTAGE_API_KEY environment variable is required"
            }
        
        # Add API key to parameters
        params["apikey"] = self.api_key
        
        # Alpha Vantage has a limit of 5 requests per minute on standard plan
        # Implement rate limiting to avoid exceeding the limit
        current_time = time.time()
        if self.request_count >= 5 and current_time - self.last_request_time < 60:
            sleep_time = 60 - (current_time - self.last_request_time)
            logger.info(f"Rate limit reached, sleeping for {sleep_time:.2f} seconds")
            time.sleep(sleep_time)
            self.request_count = 0
            self.last_request_time = time.time()
        
        # Make the request
        try:
            response = self.session.get(self.BASE_URL, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                logger.error(f"Alpha Vantage API error: {data['Error Message']}")
                raise ValueError(f"Alpha Vantage API error: {data['Error Message']}")
                
            if "Information" in data and "rate limit" in data["Information"].lower():
                logger.warning(f"Alpha Vantage rate limit message: {data['Information']}")
                
            # Update rate limiting counters
            self.request_count += 1
            if self.request_count == 1:
                self.last_request_time = time.time()
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to Alpha Vantage failed: {str(e)}")
            raise
    
    def get_time_series_daily(self, symbol: str, output_size: str = "compact") -> Dict[str, Any]:
        """
        Get daily time series for a symbol
        
        Args:
            symbol: The stock symbol
            output_size: 'compact' returns the latest 100 datapoints, 'full' returns up to 20 years of data
            
        Returns:
            Daily time series data
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": output_size
        }
        return self._make_request(params)
    
    def get_global_quote(self, symbol: str) -> Dict[str, Any]:
        """
        Get the latest price and volume information for a symbol
        
        Args:
            symbol: The stock symbol
            
        Returns:
            Latest quote data
        """
        params = {
            "function": "GLOBAL_QUOTE",
            "symbol": symbol
        }
        return self._make_request(params)
    
    def get_sector_performance(self) -> Dict[str, Any]:
        """
        Get real-time and historical sector performances
        
        Returns:
            Sector performance data
        """
        params = {
            "function": "SECTOR"
        }
        return self._make_request(params)
    
    def get_forex_data(self, from_currency: str, to_currency: str) -> Dict[str, Any]:
        """
        Get forex exchange rate data
        
        Args:
            from_currency: From currency code (e.g., 'USD')
            to_currency: To currency code (e.g., 'JPY')
            
        Returns:
            Forex exchange rate data
        """
        params = {
            "function": "CURRENCY_EXCHANGE_RATE",
            "from_currency": from_currency,
            "to_currency": to_currency
        }
        return self._make_request(params)
    
    def get_crypto_data(self, symbol: str, market: str = 'USD') -> Dict[str, Any]:
        """
        Get cryptocurrency data
        
        Args:
            symbol: The cryptocurrency symbol (e.g., 'BTC')
            market: The market to get data for (e.g., 'USD')
            
        Returns:
            Cryptocurrency data
        """
        params = {
            "function": "DIGITAL_CURRENCY_DAILY",
            "symbol": symbol,
            "market": market
        }
        return self._make_request(params)
    
    def get_company_overview(self, symbol: str) -> Dict[str, Any]:
        """
        Get company overview data including key financial metrics
        
        Args:
            symbol: The stock symbol
            
        Returns:
            Company overview data
        """
        params = {
            "function": "OVERVIEW",
            "symbol": symbol
        }
        return self._make_request(params)
    
    def get_earnings(self, symbol: str) -> Dict[str, Any]:
        """
        Get company earnings data
        
        Args:
            symbol: The stock symbol
            
        Returns:
            Company earnings data
        """
        params = {
            "function": "EARNINGS",
            "symbol": symbol
        }
        return self._make_request(params)
    
    def get_economic_indicator(self, indicator: str) -> Dict[str, Any]:
        """
        Get economic indicator data
        
        Args:
            indicator: The economic indicator (e.g., 'REAL_GDP', 'CPI')
            
        Returns:
            Economic indicator data
        """
        params = {
            "function": indicator
        }
        return self._make_request(params)
    
    def format_time_series_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format time series data into a list of dictionaries for easier consumption
        
        Args:
            data: Raw time series data from Alpha Vantage
            
        Returns:
            Formatted time series data
        """
        formatted_data = []
        
        time_series_key = next((k for k in data.keys() if "Time Series" in k), None)
        if not time_series_key:
            raise ValueError("No time series data found in response")
            
        time_series = data[time_series_key]
        
        for date, values in time_series.items():
            entry = {"date": date}
            for key, value in values.items():
                # Extract the field name from keys like "1. open", "2. high", etc.
                field = key.split(". ")[1] if ". " in key else key
                entry[field] = float(value)
            formatted_data.append(entry)
            
        # Sort by date, most recent first
        formatted_data.sort(key=lambda x: x["date"], reverse=True)
        
        return formatted_data