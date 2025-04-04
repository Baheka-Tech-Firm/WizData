"""
Bloomberg Data Integration Client

This module provides a client for integrating with Bloomberg Data Services.
Bloomberg provides comprehensive financial data including real-time and historical 
market data, news, research, and analytics that can be integrated into the WizData platform.

Reference: https://www.bloomberg.com/professional/product/market-data/
"""
import os
import time
import logging
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class BloombergClient:
    """Client for Bloomberg Data Services API"""
    
    BASE_URL = "https://api.bloomberg.com/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize Bloomberg client
        
        Args:
            api_key: Bloomberg API key (defaults to environment variable if not provided)
        """
        self.api_key = api_key or os.environ.get("BLOOMBERG_API_KEY")
        if not self.api_key:
            logger.warning("Bloomberg API key not provided. Set BLOOMBERG_API_KEY environment variable.")
        
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 10 requests per second (100ms)
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a request to Bloomberg API with rate limiting
        
        Args:
            endpoint: API endpoint
            params: Query parameters for the request
            
        Returns:
            JSON response data
        """
        # Rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        headers = {
            "X-BAPI-KEY": self.api_key,
            "Content-Type": "application/json"
        }
        
        try:
            if not self.api_key:
                raise ValueError("Bloomberg API key is required. Set BLOOMBERG_API_KEY environment variable.")
                
            response = requests.get(url, headers=headers, params=params)
            self.last_request_time = time.time()
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Bloomberg API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            
            # Return empty result on error
            return {}
    
    def get_market_data(self, symbols: List[str], fields: List[str] = None) -> Dict[str, Any]:
        """
        Get market data for a list of symbols
        
        Args:
            symbols: List of security identifiers
            fields: List of data fields to retrieve (defaults to price data)
            
        Returns:
            Market data for the requested symbols
        """
        endpoint = "market/data"
        
        if fields is None:
            fields = ["PX_LAST", "PX_VOLUME", "PX_HIGH", "PX_LOW", "PX_OPEN"]
        
        params = {
            "symbols": ",".join(symbols),
            "fields": ",".join(fields)
        }
        
        return self._make_request(endpoint, params)
    
    def get_historical_data(self, symbol: str, start_date: str, end_date: str = None, 
                          period: str = "DAILY") -> Dict[str, Any]:
        """
        Get historical market data for a symbol
        
        Args:
            symbol: Security identifier
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format (defaults to today)
            period: Data frequency (DAILY, WEEKLY, MONTHLY)
            
        Returns:
            Historical market data
        """
        endpoint = "market/historical"
        
        if end_date is None:
            end_date = datetime.now().strftime("%Y-%m-%d")
        
        params = {
            "symbol": symbol,
            "startDate": start_date,
            "endDate": end_date,
            "period": period
        }
        
        return self._make_request(endpoint, params)
    
    def get_news(self, topics: List[str] = None, tickers: List[str] = None, 
               max_items: int = 10) -> Dict[str, Any]:
        """
        Get financial news articles
        
        Args:
            topics: List of news topics
            tickers: List of tickers to filter news
            max_items: Maximum number of news items to return
            
        Returns:
            Financial news articles
        """
        endpoint = "news/headlines"
        
        params = {"limit": str(max_items)}
        
        if topics:
            params["topics"] = ",".join(topics)
        
        if tickers:
            params["tickers"] = ",".join(tickers)
        
        return self._make_request(endpoint, params)
    
    def get_company_fundamentals(self, ticker: str) -> Dict[str, Any]:
        """
        Get company fundamental data
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            Company fundamental data
        """
        endpoint = f"data/fundamentals/{ticker}"
        return self._make_request(endpoint)
    
    def get_global_market_indices(self) -> Dict[str, Any]:
        """
        Get global market indices data
        
        Returns:
            Global market indices data
        """
        endpoint = "market/indices"
        return self._make_request(endpoint)
    
    def get_fixed_income_data(self, issuer_type: str = "sovereign", 
                            currency: str = "USD") -> Dict[str, Any]:
        """
        Get fixed income market data
        
        Args:
            issuer_type: Type of issuer (sovereign, corporate)
            currency: Currency code
            
        Returns:
            Fixed income market data
        """
        endpoint = "market/fixed-income"
        
        params = {
            "issuerType": issuer_type,
            "currency": currency
        }
        
        return self._make_request(endpoint, params)
    
    def get_esg_data(self, ticker: str) -> Dict[str, Any]:
        """
        Get ESG data for a company
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            ESG data for the company
        """
        endpoint = f"data/esg/{ticker}"
        return self._make_request(endpoint)
    
    def get_economic_calendar(self, start_date: str = None, end_date: str = None, 
                           countries: List[str] = None) -> Dict[str, Any]:
        """
        Get economic calendar data
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            countries: List of country codes to filter
            
        Returns:
            Economic calendar data
        """
        endpoint = "market/economic-calendar"
        
        params = {}
        
        if start_date:
            params["startDate"] = start_date
        
        if end_date:
            params["endDate"] = end_date
        
        if countries:
            params["countries"] = ",".join(countries)
        
        return self._make_request(endpoint, params)
    
    def format_historical_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format historical data into a list of dictionaries for easier consumption
        
        Args:
            data: Raw historical data from Bloomberg
            
        Returns:
            Formatted historical data
        """
        formatted_data = []
        
        if "results" in data and "data" in data["results"]:
            for item in data["results"]["data"]:
                formatted_item = {
                    "date": item.get("date", ""),
                    "open": item.get("open", 0.0),
                    "high": item.get("high", 0.0),
                    "low": item.get("low", 0.0),
                    "close": item.get("close", 0.0),
                    "volume": item.get("volume", 0)
                }
                
                formatted_data.append(formatted_item)
        
        return formatted_data