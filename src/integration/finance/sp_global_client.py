"""
S&P Global Market Data Integration Client

This module provides a client for integrating with S&P Global Market Intelligence data.
S&P Global provides premium financial data, indices, company information, and market analysis
that can be incorporated into the WizData platform.

Reference: https://www.spglobal.com/marketintelligence/
"""
import os
import time
import logging
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class SPGlobalClient:
    """Client for S&P Global Market Intelligence data API"""
    
    BASE_URL = "https://api.marketplace.spglobal.com/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize S&P Global client
        
        Args:
            api_key: S&P Global API key (defaults to environment variable if not provided)
        """
        self.api_key = api_key or os.environ.get("SP_GLOBAL_API_KEY")
        if not self.api_key:
            logger.warning("S&P Global API key not provided. Set SP_GLOBAL_API_KEY environment variable.")
        
        self.last_request_time = 0
        self.min_request_interval = 0.2  # 5 requests per second (200ms)
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a request to S&P Global API with rate limiting
        
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
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            if not self.api_key:
                raise ValueError("S&P Global API key is required. Set SP_GLOBAL_API_KEY environment variable.")
                
            response = requests.get(url, headers=headers, params=params)
            self.last_request_time = time.time()
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"S&P Global API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            
            # Return empty result on error
            return {}
    
    def get_sp500_constituents(self) -> List[Dict[str, Any]]:
        """
        Get the current constituents of the S&P 500 index
        
        Returns:
            List of S&P 500 constituent companies
        """
        endpoint = "indices/SP500/constituents"
        response = self._make_request(endpoint)
        
        if "constituents" in response:
            return response["constituents"]
        
        return []
    
    def get_index_performance(self, index_id: str = "SP500", period: str = "1y") -> Dict[str, Any]:
        """
        Get performance data for a specific index
        
        Args:
            index_id: Index identifier (e.g., SP500, DJIA)
            period: Time period (1d, 5d, 1m, 3m, 6m, 1y, 5y)
            
        Returns:
            Index performance data
        """
        endpoint = f"indices/{index_id}/performance"
        params = {"period": period}
        return self._make_request(endpoint, params)
    
    def get_company_profile(self, ticker: str) -> Dict[str, Any]:
        """
        Get detailed company profile and information
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            Company profile data
        """
        endpoint = f"companies/{ticker}/profile"
        return self._make_request(endpoint)
    
    def get_company_financials(self, ticker: str, statement_type: str = "income") -> Dict[str, Any]:
        """
        Get company financial statements
        
        Args:
            ticker: Company ticker symbol
            statement_type: Type of financial statement (income, balance, cash_flow)
            
        Returns:
            Financial statement data
        """
        endpoint = f"companies/{ticker}/financials/{statement_type}"
        return self._make_request(endpoint)
    
    def get_sectors_performance(self) -> Dict[str, Any]:
        """
        Get performance data for market sectors
        
        Returns:
            Sector performance data
        """
        endpoint = "sectors/performance"
        return self._make_request(endpoint)
    
    def get_market_indices(self) -> List[Dict[str, Any]]:
        """
        Get available market indices
        
        Returns:
            List of available market indices
        """
        endpoint = "indices"
        response = self._make_request(endpoint)
        
        if "indices" in response:
            return response["indices"]
        
        return []
    
    def get_esg_ratings(self, ticker: str) -> Dict[str, Any]:
        """
        Get ESG ratings for a company
        
        Args:
            ticker: Company ticker symbol
            
        Returns:
            ESG ratings data
        """
        endpoint = f"companies/{ticker}/esg"
        return self._make_request(endpoint)
    
    def get_economic_indicators(self, country_code: str = "US") -> Dict[str, Any]:
        """
        Get economic indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 country code
            
        Returns:
            Economic indicators data
        """
        endpoint = f"economics/{country_code}/indicators"
        return self._make_request(endpoint)
    
    def format_time_series_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format time series data into a list of dictionaries for easier consumption
        
        Args:
            data: Raw time series data from S&P Global
            
        Returns:
            Formatted time series data
        """
        formatted_data = []
        
        if "time_series" in data and "values" in data["time_series"]:
            for item in data["time_series"]["values"]:
                formatted_item = {
                    "date": item.get("date", ""),
                    "value": item.get("value", 0.0)
                }
                
                # Add any additional fields
                for key, value in item.items():
                    if key not in ["date", "value"]:
                        formatted_item[key] = value
                
                formatted_data.append(formatted_item)
        
        return formatted_data