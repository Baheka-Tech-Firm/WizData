"""
Refinitiv Data Integration Client

This module provides a client for integrating with Refinitiv (formerly Thomson Reuters) data services.
Refinitiv provides comprehensive financial data including real-time and historical market data,
fundamentals, estimates, news, and analytics that can be integrated into the WizData platform.

Reference: https://www.refinitiv.com/en/financial-data
"""
import os
import time
import logging
import requests
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

class RefinitivClient:
    """Client for Refinitiv Data Services API"""
    
    BASE_URL = "https://api.refinitiv.com/v1"
    
    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """
        Initialize Refinitiv client
        
        Args:
            api_key: Refinitiv API key (defaults to environment variable if not provided)
            api_secret: Refinitiv API secret (defaults to environment variable if not provided)
        """
        self.api_key = api_key or os.environ.get("REFINITIV_API_KEY")
        self.api_secret = api_secret or os.environ.get("REFINITIV_API_SECRET")
        
        if not self.api_key:
            logger.warning("Refinitiv API key not provided. Set REFINITIV_API_KEY environment variable.")
        
        if not self.api_secret:
            logger.warning("Refinitiv API secret not provided. Set REFINITIV_API_SECRET environment variable.")
        
        self.access_token = None
        self.token_expiry = None
        self.last_request_time = 0
        self.min_request_interval = 0.1  # 10 requests per second (100ms)
    
    def _authenticate(self) -> None:
        """
        Authenticate with Refinitiv API and get an access token
        """
        auth_url = f"{self.BASE_URL}/auth/token"
        
        try:
            if not self.api_key or not self.api_secret:
                raise ValueError("Refinitiv API key and secret are required.")
                
            payload = {
                "grant_type": "client_credentials",
                "client_id": self.api_key,
                "client_secret": self.api_secret
            }
            
            response = requests.post(auth_url, data=payload)
            response.raise_for_status()
            
            auth_data = response.json()
            self.access_token = auth_data.get("access_token")
            
            expires_in = auth_data.get("expires_in", 3600)  # Default to 1 hour
            self.token_expiry = datetime.now() + timedelta(seconds=expires_in)
            
            logger.info("Successfully authenticated with Refinitiv API")
        except Exception as e:
            logger.error(f"Failed to authenticate with Refinitiv API: {str(e)}")
            self.access_token = None
            self.token_expiry = None
    
    def _ensure_authenticated(self) -> bool:
        """
        Ensure the client is authenticated with a valid token
        
        Returns:
            True if authenticated, False otherwise
        """
        if not self.access_token or not self.token_expiry or datetime.now() >= self.token_expiry:
            self._authenticate()
        
        return self.access_token is not None
    
    def _make_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a request to Refinitiv API with rate limiting
        
        Args:
            endpoint: API endpoint
            params: Query parameters for the request
            
        Returns:
            JSON response data
        """
        # Ensure authenticated
        if not self._ensure_authenticated():
            logger.error("Unable to authenticate with Refinitiv API")
            return {}
        
        # Rate limiting
        current_time = time.time()
        time_since_last_request = current_time - self.last_request_time
        
        if time_since_last_request < self.min_request_interval:
            sleep_time = self.min_request_interval - time_since_last_request
            time.sleep(sleep_time)
        
        url = f"{self.BASE_URL}/{endpoint}"
        
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.get(url, headers=headers, params=params)
            self.last_request_time = time.time()
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Refinitiv API request failed: {str(e)}")
            if hasattr(e.response, 'text'):
                logger.error(f"Response: {e.response.text}")
            
            # Return empty result on error
            return {}
    
    def get_time_series(self, ric: str, fields: List[str] = None, 
                       interval: str = "P1D", start: str = None, end: str = None) -> Dict[str, Any]:
        """
        Get time series data for a RIC (Reuters Instrument Code)
        
        Args:
            ric: Reuters Instrument Code
            fields: List of fields to retrieve
            interval: Time interval (P1D for daily, PT1M for minute, etc.)
            start: Start date in ISO format
            end: End date in ISO format
            
        Returns:
            Time series data
        """
        endpoint = "data/timeseries"
        
        if fields is None:
            fields = ["OPEN", "HIGH", "LOW", "CLOSE", "VOLUME"]
        
        params = {
            "universe": ric,
            "interval": interval
        }
        
        if start:
            params["start"] = start
        
        if end:
            params["end"] = end
        
        if fields:
            params["fields"] = ",".join(fields)
        
        return self._make_request(endpoint, params)
    
    def get_quote(self, rics: List[str], fields: List[str] = None) -> Dict[str, Any]:
        """
        Get real-time quotes for a list of RICs
        
        Args:
            rics: List of Reuters Instrument Codes
            fields: List of fields to retrieve
            
        Returns:
            Quote data
        """
        endpoint = "data/quotes"
        
        if fields is None:
            fields = ["BID", "ASK", "OPEN_PRC", "HIGH_1", "LOW_1", "TRDPRC_1", "ACVOL_1"]
        
        params = {
            "universe": ",".join(rics)
        }
        
        if fields:
            params["fields"] = ",".join(fields)
        
        return self._make_request(endpoint, params)
    
    def get_fundamentals(self, ric: str, fields: List[str] = None) -> Dict[str, Any]:
        """
        Get fundamental data for a RIC
        
        Args:
            ric: Reuters Instrument Code
            fields: List of fundamental fields to retrieve
            
        Returns:
            Fundamental data
        """
        endpoint = "data/fundamentals"
        
        params = {
            "universe": ric
        }
        
        if fields:
            params["fields"] = ",".join(fields)
        
        return self._make_request(endpoint, params)
    
    def get_news(self, query: str = None, rics: List[str] = None, 
               max_items: int = 10) -> Dict[str, Any]:
        """
        Get news headlines and stories
        
        Args:
            query: News search query
            rics: List of RICs to filter news
            max_items: Maximum number of news items
            
        Returns:
            News data
        """
        endpoint = "data/news"
        
        params = {
            "limit": str(max_items)
        }
        
        if query:
            params["query"] = query
        
        if rics:
            params["rics"] = ",".join(rics)
        
        return self._make_request(endpoint, params)
    
    def get_esg_scores(self, ric: str) -> Dict[str, Any]:
        """
        Get ESG scores for a company
        
        Args:
            ric: Reuters Instrument Code
            
        Returns:
            ESG scores
        """
        endpoint = "data/esg/scores"
        
        params = {
            "universe": ric
        }
        
        return self._make_request(endpoint, params)
    
    def get_estimates(self, ric: str) -> Dict[str, Any]:
        """
        Get analyst estimates for a company
        
        Args:
            ric: Reuters Instrument Code
            
        Returns:
            Analyst estimates
        """
        endpoint = "data/estimates"
        
        params = {
            "universe": ric
        }
        
        return self._make_request(endpoint, params)
    
    def get_market_indices(self, region: str = None) -> Dict[str, Any]:
        """
        Get market indices data
        
        Args:
            region: Region filter (e.g., US, EU, ASIA)
            
        Returns:
            Market indices data
        """
        endpoint = "data/indices"
        
        params = {}
        
        if region:
            params["region"] = region
        
        return self._make_request(endpoint, params)
    
    def format_time_series_data(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Format time series data into a list of dictionaries for easier consumption
        
        Args:
            data: Raw time series data from Refinitiv
            
        Returns:
            Formatted time series data
        """
        formatted_data = []
        
        if "data" in data and "timeseriesData" in data["data"]:
            for item in data["data"]["timeseriesData"]:
                date_str = item.get("date", "")
                
                formatted_item = {
                    "date": date_str
                }
                
                # Add fields
                if "fields" in item:
                    for field, value in item["fields"].items():
                        formatted_item[field.lower()] = value
                
                formatted_data.append(formatted_item)
        
        return formatted_data