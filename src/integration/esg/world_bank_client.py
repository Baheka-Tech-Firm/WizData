"""
World Bank Climate and ESG Data Integration Client

This module provides a client for integrating with the World Bank's Climate Data API
and other data APIs that provide ESG-related metrics. World Bank data is open and free
to use but is reliable and can be repackaged and sold as part of the WizData platform.

References:
- https://climatedata.worldbank.org/
- https://datacatalog.worldbank.org/
"""

import logging
import requests
import json
from typing import Dict, List, Optional, Union, Any
from datetime import datetime

# Setup logging
logger = logging.getLogger(__name__)

class WorldBankClient:
    """Client for World Bank Climate and ESG data APIs"""
    
    CLIMATE_API_BASE_URL = "https://climatedata.worldbank.org/api/v1"
    INDICATORS_API_BASE_URL = "https://api.worldbank.org/v2"
    
    def __init__(self):
        """Initialize World Bank client"""
        self.session = requests.Session()
    
    def _make_climate_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a request to World Bank Climate API
        
        Args:
            endpoint: API endpoint
            params: Query parameters for the request
            
        Returns:
            JSON response data
        """
        url = f"{self.CLIMATE_API_BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to World Bank Climate API failed: {str(e)}")
            raise
    
    def _make_indicators_request(self, endpoint: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """
        Make a request to World Bank Indicators API
        
        Args:
            endpoint: API endpoint
            params: Query parameters for the request
            
        Returns:
            JSON response data
        """
        if params is None:
            params = {}
        
        # Add format parameter for JSON response
        params["format"] = "json"
        
        url = f"{self.INDICATORS_API_BASE_URL}/{endpoint}"
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Request to World Bank Indicators API failed: {str(e)}")
            raise
    
    def get_countries(self) -> List[Dict[str, Any]]:
        """
        Get list of countries available in the World Bank data
        
        Returns:
            List of countries with their metadata
        """
        response = self._make_indicators_request("countries", {"per_page": 300})
        # Skip the first element which is metadata
        return response[1] if len(response) > 1 else []
    
    def get_country_data(self, country_code: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            
        Returns:
            Country information
        """
        response = self._make_indicators_request(f"countries/{country_code}")
        return response[1][0] if len(response) > 1 and response[1] else {}
    
    def get_climate_data(self, country_code: str, data_type: str) -> Dict[str, Any]:
        """
        Get climate data for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-3 country code
            data_type: Type of climate data (e.g., 'tas' for temperature, 'pr' for precipitation)
            
        Returns:
            Climate data for the country
        """
        endpoint = f"climate/historical/{data_type}/{country_code}"
        return self._make_climate_request(endpoint)
    
    def get_environmental_indicators(self, country_code: str, indicators: List[str], 
                                   start_year: Optional[int] = None, 
                                   end_year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get environmental indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            indicators: List of indicator codes
            start_year: Start year for data (optional)
            end_year: End year for data (optional)
            
        Returns:
            Environmental indicator data
        """
        # Common environmental indicators
        # CO2 emissions: EN.ATM.CO2E.PC
        # Forest area: AG.LND.FRST.ZS
        # Renewable energy consumption: EG.FEC.RNEW.ZS
        # Access to electricity: EG.ELC.ACCS.ZS
        
        params = {
            "per_page": 1000
        }
        
        if start_year is not None and end_year is not None:
            params["date"] = f"{start_year}:{end_year}"
        
        indicator_codes = ";".join(indicators)
        endpoint = f"countries/{country_code}/indicators/{indicator_codes}"
        
        response = self._make_indicators_request(endpoint, params)
        return response[1] if len(response) > 1 else []
    
    def get_social_indicators(self, country_code: str, indicators: List[str],
                           start_year: Optional[int] = None,
                           end_year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get social indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            indicators: List of indicator codes
            start_year: Start year for data (optional)
            end_year: End year for data (optional)
            
        Returns:
            Social indicator data
        """
        # Common social indicators
        # Literacy rate: SE.ADT.LITR.ZS
        # Life expectancy: SP.DYN.LE00.IN
        # Poverty headcount ratio: SI.POV.DDAY
        # Income share held by lowest 20%: SI.DST.FRST.20
        
        params = {
            "per_page": 1000
        }
        
        if start_year is not None and end_year is not None:
            params["date"] = f"{start_year}:{end_year}"
        
        indicator_codes = ";".join(indicators)
        endpoint = f"countries/{country_code}/indicators/{indicator_codes}"
        
        response = self._make_indicators_request(endpoint, params)
        return response[1] if len(response) > 1 else []
    
    def get_governance_indicators(self, country_code: str,
                              start_year: Optional[int] = None,
                              end_year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get governance indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            start_year: Start year for data (optional)
            end_year: End year for data (optional)
            
        Returns:
            Governance indicator data
        """
        # World Bank Governance Indicators
        governance_indicators = [
            "GE.EST",  # Government Effectiveness
            "PV.EST",  # Political Stability and Absence of Violence
            "RQ.EST",  # Regulatory Quality
            "RL.EST",  # Rule of Law
            "CC.EST",  # Control of Corruption
            "VA.EST"   # Voice and Accountability
        ]
        
        params = {
            "per_page": 1000
        }
        
        if start_year is not None and end_year is not None:
            params["date"] = f"{start_year}:{end_year}"
        
        indicator_codes = ";".join(governance_indicators)
        endpoint = f"countries/{country_code}/indicators/{indicator_codes}"
        
        response = self._make_indicators_request(endpoint, params)
        return response[1] if len(response) > 1 else []
    
    def get_infrastructure_indicators(self, country_code: str,
                                  start_year: Optional[int] = None,
                                  end_year: Optional[int] = None) -> Dict[str, Any]:
        """
        Get infrastructure indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            start_year: Start year for data (optional)
            end_year: End year for data (optional)
            
        Returns:
            Infrastructure indicator data
        """
        # Common infrastructure indicators
        infrastructure_indicators = [
            "IS.ROD.PAVE.ZS",     # Roads, paved (% of total roads)
            "IS.RRS.TOTL.KM",     # Rail lines (total route-km)
            "IS.AIR.PSGR",        # Air transport, passengers carried
            "IT.CEL.SETS.P2",     # Mobile cellular subscriptions (per 100 people)
            "IT.NET.USER.ZS",     # Individuals using the Internet (% of population)
            "EG.ELC.ACCS.ZS"      # Access to electricity (% of population)
        ]
        
        params = {
            "per_page": 1000
        }
        
        if start_year is not None and end_year is not None:
            params["date"] = f"{start_year}:{end_year}"
        
        indicator_codes = ";".join(infrastructure_indicators)
        endpoint = f"countries/{country_code}/indicators/{indicator_codes}"
        
        response = self._make_indicators_request(endpoint, params)
        return response[1] if len(response) > 1 else []
    
    def get_all_esg_indicators(self, country_code: str, 
                           start_year: Optional[int] = None,
                           end_year: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
        """
        Get all ESG indicators for a specific country
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            start_year: Start year for data (optional)
            end_year: End year for data (optional)
            
        Returns:
            All ESG indicators organized by category
        """
        # Environmental indicators
        environmental_indicators = [
            "EN.ATM.CO2E.PC",     # CO2 emissions (metric tons per capita)
            "AG.LND.FRST.ZS",     # Forest area (% of land area)
            "EG.FEC.RNEW.ZS",     # Renewable energy consumption (% of total final energy consumption)
            "EG.ELC.ACCS.ZS",     # Access to electricity (% of population)
            "ER.H2O.FWTL.ZS",     # Annual freshwater withdrawals, total (% of internal resources)
            "EN.ATM.PM25.MC.M3"   # PM2.5 air pollution, mean annual exposure (micrograms per cubic meter)
        ]
        
        # Social indicators
        social_indicators = [
            "SE.ADT.LITR.ZS",     # Literacy rate, adult total (% of people ages 15 and above)
            "SP.DYN.LE00.IN",     # Life expectancy at birth, total (years)
            "SI.POV.DDAY",        # Poverty headcount ratio at $1.90 a day (2011 PPP) (% of population)
            "SI.DST.FRST.20",     # Income share held by lowest 20%
            "SH.MED.PHYS.ZS",     # Physicians (per 1,000 people)
            "SE.PRM.ENRR"         # School enrollment, primary (% gross)
        ]
        
        # Governance indicators from World Bank Governance project
        governance_indicators = [
            "GE.EST",  # Government Effectiveness
            "PV.EST",  # Political Stability and Absence of Violence
            "RQ.EST",  # Regulatory Quality
            "RL.EST",  # Rule of Law
            "CC.EST",  # Control of Corruption
            "VA.EST"   # Voice and Accountability
        ]
        
        # Infrastructure indicators
        infrastructure_indicators = [
            "IS.ROD.PAVE.ZS",     # Roads, paved (% of total roads)
            "IS.RRS.TOTL.KM",     # Rail lines (total route-km)
            "IS.AIR.PSGR",        # Air transport, passengers carried
            "IT.CEL.SETS.P2",     # Mobile cellular subscriptions (per 100 people)
            "IT.NET.USER.ZS",     # Individuals using the Internet (% of population)
            "EG.ELC.ACCS.ZS"      # Access to electricity (% of population)
        ]
        
        # Collect all indicator data
        result = {
            "environmental": self.get_environmental_indicators(country_code, environmental_indicators, start_year, end_year),
            "social": self.get_social_indicators(country_code, social_indicators, start_year, end_year),
            "governance": self.get_governance_indicators(country_code, start_year, end_year),
            "infrastructure": self.get_infrastructure_indicators(country_code, start_year, end_year)
        }
        
        return result
    
    def calculate_esg_scores(self, country_code: str, year: int) -> Dict[str, float]:
        """
        Calculate ESG scores for a country based on the most recent data
        
        Args:
            country_code: ISO 3166-1 alpha-2 or alpha-3 country code
            year: Year for the data
            
        Returns:
            ESG scores for the country
        """
        # Get all ESG indicators
        esg_data = self.get_all_esg_indicators(country_code, year, year)
        
        # Calculate scores (simplified approach)
        scores = {
            "environmental": 0,
            "social": 0,
            "governance": 0,
            "infrastructure": 0,
            "overall": 0
        }
        
        # Count valid indicators
        count = {
            "environmental": 0,
            "social": 0,
            "governance": 0,
            "infrastructure": 0
        }
        
        # Process environmental indicators
        for indicator in esg_data["environmental"]:
            if indicator["value"] is not None:
                # This is very simplified - in real implementation would need
                # to normalize values based on global min/max and direction
                # (some indicators are better when higher, some when lower)
                count["environmental"] += 1
                # Just for illustration - real calculation would be more sophisticated
                if indicator["indicator"]["id"] == "EN.ATM.CO2E.PC":
                    # Lower is better for CO2 emissions
                    scores["environmental"] += max(0, 100 - indicator["value"] * 10)
                elif indicator["indicator"]["id"] == "AG.LND.FRST.ZS":
                    # Higher is better for forest area
                    scores["environmental"] += indicator["value"]
                elif indicator["indicator"]["id"] == "EG.FEC.RNEW.ZS":
                    # Higher is better for renewable energy
                    scores["environmental"] += indicator["value"]
                else:
                    scores["environmental"] += 50  # Default value
        
        # Process social indicators
        for indicator in esg_data["social"]:
            if indicator["value"] is not None:
                count["social"] += 1
                if indicator["indicator"]["id"] == "SE.ADT.LITR.ZS":
                    # Higher is better for literacy rate
                    scores["social"] += indicator["value"]
                elif indicator["indicator"]["id"] == "SP.DYN.LE00.IN":
                    # Higher is better for life expectancy, but needs normalization
                    scores["social"] += min(100, indicator["value"] * 1.25)
                elif indicator["indicator"]["id"] == "SI.POV.DDAY":
                    # Lower is better for poverty
                    scores["social"] += max(0, 100 - indicator["value"] * 5)
                else:
                    scores["social"] += 50  # Default value
        
        # Process governance indicators
        for indicator in esg_data["governance"]:
            if indicator["value"] is not None:
                count["governance"] += 1
                # Governance indicators are from -2.5 to 2.5, convert to 0-100
                scores["governance"] += (indicator["value"] + 2.5) * 20
        
        # Process infrastructure indicators
        for indicator in esg_data["infrastructure"]:
            if indicator["value"] is not None:
                count["infrastructure"] += 1
                if indicator["indicator"]["id"] == "IT.NET.USER.ZS":
                    # Higher is better for internet usage
                    scores["infrastructure"] += indicator["value"]
                elif indicator["indicator"]["id"] == "IT.CEL.SETS.P2":
                    # Higher is better for mobile subscribers, but cap at 100
                    scores["infrastructure"] += min(100, indicator["value"])
                elif indicator["indicator"]["id"] == "EG.ELC.ACCS.ZS":
                    # Higher is better for electricity access
                    scores["infrastructure"] += indicator["value"]
                else:
                    scores["infrastructure"] += 50  # Default value
        
        # Calculate averages
        for category in ["environmental", "social", "governance", "infrastructure"]:
            if count[category] > 0:
                scores[category] = scores[category] / count[category]
            else:
                scores[category] = 0
        
        # Calculate overall score
        valid_categories = sum(1 for category in ["environmental", "social", "governance", "infrastructure"] 
                             if scores[category] > 0)
        
        if valid_categories > 0:
            scores["overall"] = sum(scores[category] for category in ["environmental", "social", "governance", "infrastructure"]) / valid_categories
        
        return scores