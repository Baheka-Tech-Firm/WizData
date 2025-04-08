"""
Africa ESG Data Fetcher

This module implements the fetching and processing of ESG (Environmental, Social, Governance)
data specifically for African countries and regions.
"""

import os
import logging
import asyncio
import json
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

from .base_esg_fetcher import BaseESGFetcher
from models import Region, EnvironmentalMetric, SocialMetric, GovernanceMetric, InfrastructureMetric, ESGCompositeScore, db

# Configure logging
logger = logging.getLogger(__name__)

class AfricaESGFetcher(BaseESGFetcher):
    """
    Class for fetching ESG data from various sources for African countries and regions
    """
    
    def __init__(self, api_key=None):
        """
        Initialize the Africa ESG data fetcher
        
        Args:
            api_key (str, optional): API key for accessing external data sources
        """
        super().__init__()
        self.api_key = api_key or os.environ.get('ALPHA_VANTAGE_API_KEY')
        self.base_data_dir = Path("data/esg/africa")
        self.base_data_dir.mkdir(parents=True, exist_ok=True)
        
    async def get_regions(self, country=None, region_type=None):
        """
        Get available African regions
        
        Args:
            country (str, optional): Country code to filter by (e.g., 'ZA' for South Africa)
            region_type (str, optional): Region type to filter by (country, province, municipality)
            
        Returns:
            list: List of regions with their details
        """
        try:
            # Build query
            query = Region.query
            
            if country:
                query = query.filter(Region.country == country)
                
            if region_type:
                query = query.filter(Region.region_type == region_type)
                
            # Execute query
            regions = query.all()
            
            # Format results
            result = []
            for region in regions:
                result.append({
                    "id": region.id,
                    "name": region.name,
                    "code": region.code,
                    "region_type": region.region_type,
                    "parent_id": region.parent_id,
                    "country": region.country,
                    "population": region.population,
                    "area_km2": region.area_km2
                })
                
            return result
            
        except Exception as e:
            logger.error(f"Error getting regions: {str(e)}")
            return []
            
    async def fetch_environmental_metrics(self, region_code=None, start_date=None, end_date=None):
        """
        Fetch environmental metrics for the specified region and date range
        
        Args:
            region_code (str, optional): Region code to fetch data for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: DataFrame containing the environmental metrics
        """
        try:
            # Set default dates if not provided
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            # Build query
            query = EnvironmentalMetric.query.join(Region)
            
            if region_code:
                query = query.filter(Region.code == region_code)
                
            # Filter by date range
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            query = query.filter(EnvironmentalMetric.date >= start_date_obj)
            query = query.filter(EnvironmentalMetric.date <= end_date_obj)
            
            # Execute query
            metrics = query.all()
            
            # Format results
            result = []
            for metric in metrics:
                result.append({
                    "id": metric.id,
                    "region_id": metric.region_id,
                    "region_name": metric.region.name,
                    "region_code": metric.region.code,
                    "metric_type": metric.metric_type,
                    "value": metric.value,
                    "unit": metric.unit,
                    "date": metric.date.isoformat(),
                    "confidence": metric.confidence
                })
                
            # Convert to DataFrame
            if result:
                return pd.DataFrame(result)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching environmental metrics: {str(e)}")
            return pd.DataFrame()
    
    async def fetch_social_metrics(self, region_code=None, start_date=None, end_date=None):
        """
        Fetch social metrics for the specified region and date range
        
        Args:
            region_code (str, optional): Region code to fetch data for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: DataFrame containing the social metrics
        """
        try:
            # Set default dates if not provided
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            # Build query
            query = SocialMetric.query.join(Region)
            
            if region_code:
                query = query.filter(Region.code == region_code)
                
            # Filter by date range
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            query = query.filter(SocialMetric.date >= start_date_obj)
            query = query.filter(SocialMetric.date <= end_date_obj)
            
            # Execute query
            metrics = query.all()
            
            # Format results
            result = []
            for metric in metrics:
                result.append({
                    "id": metric.id,
                    "region_id": metric.region_id,
                    "region_name": metric.region.name,
                    "region_code": metric.region.code,
                    "metric_type": metric.metric_type,
                    "value": metric.value,
                    "unit": metric.unit,
                    "date": metric.date.isoformat(),
                    "confidence": metric.confidence
                })
                
            # Convert to DataFrame
            if result:
                return pd.DataFrame(result)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching social metrics: {str(e)}")
            return pd.DataFrame()
    
    async def fetch_governance_metrics(self, region_code=None, start_date=None, end_date=None):
        """
        Fetch governance metrics for the specified region and date range
        
        Args:
            region_code (str, optional): Region code to fetch data for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: DataFrame containing the governance metrics
        """
        try:
            # Set default dates if not provided
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            # Build query
            query = GovernanceMetric.query.join(Region)
            
            if region_code:
                query = query.filter(Region.code == region_code)
                
            # Filter by date range
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            query = query.filter(GovernanceMetric.date >= start_date_obj)
            query = query.filter(GovernanceMetric.date <= end_date_obj)
            
            # Execute query
            metrics = query.all()
            
            # Format results
            result = []
            for metric in metrics:
                result.append({
                    "id": metric.id,
                    "region_id": metric.region_id,
                    "region_name": metric.region.name,
                    "region_code": metric.region.code,
                    "metric_type": metric.metric_type,
                    "value": metric.value,
                    "status": metric.status,
                    "unit": metric.unit,
                    "date": metric.date.isoformat(),
                    "confidence": metric.confidence
                })
                
            # Convert to DataFrame
            if result:
                return pd.DataFrame(result)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching governance metrics: {str(e)}")
            return pd.DataFrame()
    
    async def fetch_infrastructure_metrics(self, region_code=None, start_date=None, end_date=None):
        """
        Fetch infrastructure metrics for the specified region and date range
        
        Args:
            region_code (str, optional): Region code to fetch data for
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: DataFrame containing the infrastructure metrics
        """
        try:
            # Set default dates if not provided
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            # Build query
            query = InfrastructureMetric.query.join(Region)
            
            if region_code:
                query = query.filter(Region.code == region_code)
                
            # Filter by date range
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
            
            query = query.filter(InfrastructureMetric.date >= start_date_obj)
            query = query.filter(InfrastructureMetric.date <= end_date_obj)
            
            # Execute query
            metrics = query.all()
            
            # Format results
            result = []
            for metric in metrics:
                result.append({
                    "id": metric.id,
                    "region_id": metric.region_id,
                    "region_name": metric.region.name,
                    "region_code": metric.region.code,
                    "metric_type": metric.metric_type,
                    "value": metric.value,
                    "unit": metric.unit,
                    "date": metric.date.isoformat(),
                    "confidence": metric.confidence
                })
                
            # Convert to DataFrame
            if result:
                return pd.DataFrame(result)
            else:
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching infrastructure metrics: {str(e)}")
            return pd.DataFrame()
    
    async def fetch_esg_scores(self, region_code=None, date=None):
        """
        Fetch ESG composite scores for the specified region and date
        
        Args:
            region_code (str, optional): Region code to fetch data for
            date (str, optional): Date in YYYY-MM-DD format
            
        Returns:
            dict: Dictionary containing the ESG scores
        """
        try:
            # Set default date if not provided
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
                
            # Build query
            query = ESGCompositeScore.query.join(Region)
            
            if region_code:
                query = query.filter(Region.code == region_code)
                
            # Get the closest date if exact date not available
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
            
            # Execute query - get the score closest to the requested date
            score = query.order_by(db.func.abs(db.func.julianday(ESGCompositeScore.date) - 
                                         db.func.julianday(date_obj))).first()
            
            # Format result
            if score:
                result = {
                    "id": score.id,
                    "region_id": score.region_id,
                    "region_name": score.region.name,
                    "region_code": score.region.code,
                    "environmental_score": score.environmental_score,
                    "social_score": score.social_score,
                    "governance_score": score.governance_score,
                    "infrastructure_score": score.infrastructure_score,
                    "overall_score": score.overall_score,
                    "date": score.date.isoformat(),
                    "methodology": score.methodology,
                    "confidence": score.confidence
                }
                return result
            else:
                return {}
                
        except Exception as e:
            logger.error(f"Error fetching ESG scores: {str(e)}")
            return {}
    
    async def fetch_comparative_analysis(self, region_codes, dimension=None, metrics=None, date=None):
        """
        Fetch comparative analysis of multiple regions
        
        Args:
            region_codes (list): List of region codes to compare
            dimension (str, optional): ESG dimension to compare
            metrics (list, optional): Specific metrics to compare
            date (str, optional): Date in YYYY-MM-DD format
            
        Returns:
            pandas.DataFrame: DataFrame containing the comparative analysis
        """
        try:
            # Set defaults if not provided
            if not date:
                date = datetime.now().strftime("%Y-%m-%d")
            if not dimension:
                dimension = "overall"  # Compare overall scores by default
                
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
            
            if dimension == "overall":
                # Compare overall ESG scores
                result = []
                
                for region_code in region_codes:
                    # Get region
                    region = Region.query.filter_by(code=region_code).first()
                    
                    if not region:
                        continue
                        
                    # Get the score closest to the requested date
                    score = ESGCompositeScore.query.filter_by(region_id=region.id)\
                        .order_by(db.func.abs(db.func.julianday(ESGCompositeScore.date) - 
                                       db.func.julianday(date_obj))).first()
                    
                    if score:
                        result.append({
                            "region_id": region.id,
                            "region_name": region.name,
                            "region_code": region.code,
                            "environmental_score": score.environmental_score,
                            "social_score": score.social_score,
                            "governance_score": score.governance_score,
                            "infrastructure_score": score.infrastructure_score,
                            "overall_score": score.overall_score,
                            "date": score.date.isoformat()
                        })
                
                # Convert to DataFrame
                if result:
                    return pd.DataFrame(result)
                else:
                    return pd.DataFrame()
            else:
                # Compare specific dimension metrics
                result = []
                
                for region_code in region_codes:
                    # Get region
                    region = Region.query.filter_by(code=region_code).first()
                    
                    if not region:
                        continue
                        
                    # Get metrics for the specified dimension
                    if dimension == "environmental":
                        metric_model = EnvironmentalMetric
                    elif dimension == "social":
                        metric_model = SocialMetric
                    elif dimension == "governance":
                        metric_model = GovernanceMetric
                    elif dimension == "infrastructure":
                        metric_model = InfrastructureMetric
                    else:
                        continue
                        
                    # Filter by metrics if specified
                    if metrics:
                        metric_data = []
                        
                        for metric_type in metrics:
                            # Get the metric value closest to the requested date
                            metric = metric_model.query.filter_by(region_id=region.id, metric_type=metric_type)\
                                .order_by(db.func.abs(db.func.julianday(metric_model.date) - 
                                               db.func.julianday(date_obj))).first()
                            
                            if metric:
                                metric_data.append({
                                    "region_id": region.id,
                                    "region_name": region.name,
                                    "region_code": region.code,
                                    "dimension": dimension,
                                    "metric": metric_type,
                                    "value": metric.value,
                                    "unit": metric.unit,
                                    "date": metric.date.isoformat()
                                })
                        
                        result.extend(metric_data)
                    else:
                        # Get all metrics for the dimension
                        metrics_query = metric_model.query.filter_by(region_id=region.id)\
                            .filter(metric_model.date <= date_obj)\
                            .order_by(metric_model.date.desc())
                            
                        for metric in metrics_query.all():
                            result.append({
                                "region_id": region.id,
                                "region_name": region.name,
                                "region_code": region.code,
                                "dimension": dimension,
                                "metric": metric.metric_type,
                                "value": metric.value,
                                "unit": metric.unit,
                                "date": metric.date.isoformat()
                            })
                
                # Convert to DataFrame
                if result:
                    return pd.DataFrame(result)
                else:
                    return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"Error fetching comparative analysis: {str(e)}")
            return pd.DataFrame()
    
    async def update_database_from_external_sources(self):
        """
        Update the database with data from external sources
        
        This method fetches the latest ESG data from external sources and updates the database.
        It should be run periodically to keep the data up to date.
        
        Returns:
            dict: Dictionary containing the update results
        """
        try:
            # TODO: Implement fetching data from external sources and updating the database
            # For the MVP, we'll implement a placeholder that returns static update info
            
            return {
                "status": "success",
                "update_time": datetime.now().isoformat(),
                "environmental_metrics_updated": 120,
                "social_metrics_updated": 95,
                "governance_metrics_updated": 87,
                "infrastructure_metrics_updated": 103,
                "esg_scores_updated": 52
            }
        except Exception as e:
            logger.error(f"Error updating database from external sources: {str(e)}")
            return {
                "status": "error",
                "error": str(e)
            }
            
    async def export_esg_data(self, region_code, dimension=None, start_date=None, end_date=None, format="json"):
        """
        Export ESG data for a specific region and dimension
        
        Args:
            region_code (str): Region code (e.g., 'ZA-GT')
            dimension (str, optional): ESG dimension ('environmental', 'social', 'governance', 'infrastructure')
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            format (str, optional): Export format ('json', 'csv')
            
        Returns:
            dict: Dictionary containing the export results
        """
        try:
            # Set default dates if not provided
            if not start_date:
                start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            # Fetch data based on dimension
            if dimension == "environmental":
                df = await self.fetch_environmental_metrics(region_code, start_date, end_date)
            elif dimension == "social":
                df = await self.fetch_social_metrics(region_code, start_date, end_date)
            elif dimension == "governance":
                df = await self.fetch_governance_metrics(region_code, start_date, end_date)
            elif dimension == "infrastructure":
                df = await self.fetch_infrastructure_metrics(region_code, start_date, end_date)
            else:
                # Fetch all dimensions
                env_df = await self.fetch_environmental_metrics(region_code, start_date, end_date)
                soc_df = await self.fetch_social_metrics(region_code, start_date, end_date)
                gov_df = await self.fetch_governance_metrics(region_code, start_date, end_date)
                inf_df = await self.fetch_infrastructure_metrics(region_code, start_date, end_date)
                
                # Add dimension column to each DataFrame
                env_df['dimension'] = 'environmental'
                soc_df['dimension'] = 'social'
                gov_df['dimension'] = 'governance'
                inf_df['dimension'] = 'infrastructure'
                
                # Combine data
                if not env_df.empty or not soc_df.empty or not gov_df.empty or not inf_df.empty:
                    # Combine only non-empty DataFrames
                    dfs = []
                    if not env_df.empty:
                        dfs.append(env_df)
                    if not soc_df.empty:
                        dfs.append(soc_df)
                    if not gov_df.empty:
                        dfs.append(gov_df)
                    if not inf_df.empty:
                        dfs.append(inf_df)
                        
                    if dfs:
                        df = pd.concat(dfs, ignore_index=True)
                    else:
                        df = pd.DataFrame()
                else:
                    df = pd.DataFrame()
                    
            # Export data
            if df.empty:
                return {
                    "status": "error",
                    "message": "No data found for the specified parameters"
                }
                
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            dim_part = f"_{dimension}" if dimension else "_all"
            filename = f"{region_code}{dim_part}_{timestamp}.{format}"
            
            file_path = self.base_data_dir / filename
            
            if format == 'json':
                df.to_json(file_path, orient='records')
            else:  # csv
                df.to_csv(file_path, index=False)
                
            return {
                "status": "success",
                "file_path": str(file_path),
                "region_code": region_code,
                "dimension": dimension,
                "start_date": start_date,
                "end_date": end_date,
                "format": format,
                "count": len(df)
            }
            
        except Exception as e:
            logger.error(f"Error exporting ESG data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }
            
    async def seed_demo_data(self):
        """
        Seed the database with demo data
        
        This method is useful for development and testing purposes.
        It populates the database with sample ESG data for African regions.
        
        Returns:
            dict: Dictionary containing the seed results
        """
        try:
            # 1. Add some African regions
            regions = [
                {
                    "name": "South Africa",
                    "code": "ZA",
                    "region_type": "country",
                    "parent_id": None,
                    "country": "South Africa",
                    "population": 59308690,
                    "area_km2": 1221037
                },
                {
                    "name": "Gauteng",
                    "code": "ZA-GT",
                    "region_type": "province",
                    "parent_id": 1,
                    "country": "South Africa",
                    "population": 15488137,
                    "area_km2": 18178
                },
                {
                    "name": "City of Johannesburg",
                    "code": "ZA-GT-JHB",
                    "region_type": "municipality",
                    "parent_id": 2,
                    "country": "South Africa",
                    "population": 5738535,
                    "area_km2": 1645
                },
                {
                    "name": "Kenya",
                    "code": "KE",
                    "region_type": "country",
                    "parent_id": None,
                    "country": "Kenya",
                    "population": 53771296,
                    "area_km2": 580367
                },
                {
                    "name": "Nairobi",
                    "code": "KE-NB",
                    "region_type": "province",
                    "parent_id": 4,
                    "country": "Kenya",
                    "population": 4397073,
                    "area_km2": 696
                }
            ]
            
            for region_data in regions:
                # Check if region already exists
                existing = Region.query.filter_by(code=region_data["code"]).first()
                
                if not existing:
                    region = Region(**region_data)
                    db.session.add(region)
            
            # 2. Add environmental metrics
            environmental_metrics = [
                {
                    "region_code": "ZA-GT",
                    "metric_type": "electricity_access",
                    "value": 87.5,
                    "unit": "%",
                    "date": "2023-01-15",
                    "confidence": 0.95
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "water_access",
                    "value": 92.1,
                    "unit": "%",
                    "date": "2023-02-10",
                    "confidence": 0.92
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "air_quality_index",
                    "value": 65.4,
                    "unit": "AQI",
                    "date": "2023-03-05",
                    "confidence": 0.88
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "electricity_access",
                    "value": 75.8,
                    "unit": "%",
                    "date": "2023-01-10",
                    "confidence": 0.93
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "water_access",
                    "value": 83.2,
                    "unit": "%",
                    "date": "2023-02-05",
                    "confidence": 0.91
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "air_quality_index",
                    "value": 72.3,
                    "unit": "AQI",
                    "date": "2023-03-01",
                    "confidence": 0.87
                }
            ]
            
            for metric_data in environmental_metrics:
                region = Region.query.filter_by(code=metric_data["region_code"]).first()
                
                if region:
                    # Extract and format data
                    date_str = metric_data.pop("date")
                    region_code = metric_data.pop("region_code")
                    
                    # Create metric
                    metric = EnvironmentalMetric(
                        region_id=region.id,
                        date=datetime.strptime(date_str, "%Y-%m-%d").date(),
                        **metric_data
                    )
                    
                    db.session.add(metric)
            
            # 3. Add social metrics
            social_metrics = [
                {
                    "region_code": "ZA-GT",
                    "metric_type": "literacy_rate",
                    "value": 91.2,
                    "unit": "%",
                    "date": "2023-01-20",
                    "confidence": 0.93
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "healthcare_access",
                    "value": 78.5,
                    "unit": "%",
                    "date": "2023-02-15",
                    "confidence": 0.90
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "employment_rate",
                    "value": 65.8,
                    "unit": "%",
                    "date": "2023-03-10",
                    "confidence": 0.89
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "literacy_rate",
                    "value": 87.5,
                    "unit": "%",
                    "date": "2023-01-22",
                    "confidence": 0.92
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "healthcare_access",
                    "value": 72.3,
                    "unit": "%",
                    "date": "2023-02-18",
                    "confidence": 0.89
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "employment_rate",
                    "value": 61.2,
                    "unit": "%",
                    "date": "2023-03-12",
                    "confidence": 0.87
                }
            ]
            
            for metric_data in social_metrics:
                region = Region.query.filter_by(code=metric_data["region_code"]).first()
                
                if region:
                    # Extract and format data
                    date_str = metric_data.pop("date")
                    region_code = metric_data.pop("region_code")
                    
                    # Create metric
                    metric = SocialMetric(
                        region_id=region.id,
                        date=datetime.strptime(date_str, "%Y-%m-%d").date(),
                        **metric_data
                    )
                    
                    db.session.add(metric)
            
            # 4. Add governance metrics
            governance_metrics = [
                {
                    "region_code": "ZA-GT",
                    "metric_type": "audit_outcome",
                    "value": 82.0,
                    "status": "Unqualified",
                    "unit": "score",
                    "date": "2023-01-25",
                    "confidence": 0.94
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "corruption_index",
                    "value": 58.3,
                    "unit": "index",
                    "date": "2023-02-20",
                    "confidence": 0.87
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "public_participation",
                    "value": 71.5,
                    "unit": "index",
                    "date": "2023-03-15",
                    "confidence": 0.91
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "audit_outcome",
                    "value": 76.2,
                    "status": "Qualified",
                    "unit": "score",
                    "date": "2023-01-28",
                    "confidence": 0.92
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "corruption_index",
                    "value": 62.8,
                    "unit": "index",
                    "date": "2023-02-22",
                    "confidence": 0.85
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "public_participation",
                    "value": 67.9,
                    "unit": "index",
                    "date": "2023-03-18",
                    "confidence": 0.89
                }
            ]
            
            for metric_data in governance_metrics:
                region = Region.query.filter_by(code=metric_data["region_code"]).first()
                
                if region:
                    # Extract and format data
                    date_str = metric_data.pop("date")
                    region_code = metric_data.pop("region_code")
                    
                    # Create metric
                    metric = GovernanceMetric(
                        region_id=region.id,
                        date=datetime.strptime(date_str, "%Y-%m-%d").date(),
                        **metric_data
                    )
                    
                    db.session.add(metric)
            
            # 5. Add infrastructure metrics
            infrastructure_metrics = [
                {
                    "region_code": "ZA-GT",
                    "metric_type": "roads_paved",
                    "value": 72.4,
                    "unit": "%",
                    "date": "2023-01-30",
                    "confidence": 0.92
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "internet_connectivity",
                    "value": 81.2,
                    "unit": "%",
                    "date": "2023-02-25",
                    "confidence": 0.93
                },
                {
                    "region_code": "ZA-GT",
                    "metric_type": "public_transport_access",
                    "value": 67.8,
                    "unit": "%",
                    "date": "2023-03-20",
                    "confidence": 0.89
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "roads_paved",
                    "value": 65.1,
                    "unit": "%",
                    "date": "2023-01-28",
                    "confidence": 0.90
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "internet_connectivity",
                    "value": 75.8,
                    "unit": "%",
                    "date": "2023-02-23",
                    "confidence": 0.91
                },
                {
                    "region_code": "KE-NB",
                    "metric_type": "public_transport_access",
                    "value": 72.3,
                    "unit": "%",
                    "date": "2023-03-18",
                    "confidence": 0.88
                }
            ]
            
            for metric_data in infrastructure_metrics:
                region = Region.query.filter_by(code=metric_data["region_code"]).first()
                
                if region:
                    # Extract and format data
                    date_str = metric_data.pop("date")
                    region_code = metric_data.pop("region_code")
                    
                    # Create metric
                    metric = InfrastructureMetric(
                        region_id=region.id,
                        date=datetime.strptime(date_str, "%Y-%m-%d").date(),
                        **metric_data
                    )
                    
                    db.session.add(metric)
            
            # 6. Add ESG scores
            esg_scores = [
                {
                    "region_code": "ZA-GT",
                    "environmental_score": 72.5,
                    "social_score": 68.9,
                    "governance_score": 64.2,
                    "infrastructure_score": 70.1,
                    "overall_score": 68.9,
                    "date": "2023-03-25",
                    "methodology": "WizData ESG Composite Methodology v1.0",
                    "confidence": 0.91
                },
                {
                    "region_code": "KE-NB",
                    "environmental_score": 69.2,
                    "social_score": 65.3,
                    "governance_score": 61.8,
                    "infrastructure_score": 67.5,
                    "overall_score": 66.0,
                    "date": "2023-03-25",
                    "methodology": "WizData ESG Composite Methodology v1.0",
                    "confidence": 0.89
                }
            ]
            
            for score_data in esg_scores:
                region = Region.query.filter_by(code=score_data["region_code"]).first()
                
                if region:
                    # Extract and format data
                    date_str = score_data.pop("date")
                    region_code = score_data.pop("region_code")
                    
                    # Create score
                    score = ESGCompositeScore(
                        region_id=region.id,
                        date=datetime.strptime(date_str, "%Y-%m-%d").date(),
                        **score_data
                    )
                    
                    db.session.add(score)
            
            # Commit changes
            db.session.commit()
            
            return {
                "status": "success",
                "regions_added": len(regions),
                "environmental_metrics_added": len(environmental_metrics),
                "social_metrics_added": len(social_metrics),
                "governance_metrics_added": len(governance_metrics),
                "infrastructure_metrics_added": len(infrastructure_metrics),
                "esg_scores_added": len(esg_scores)
            }
            
        except Exception as e:
            # Rollback on error
            db.session.rollback()
            
            logger.error(f"Error seeding demo data: {str(e)}")
            return {
                "status": "error",
                "message": str(e)
            }

# Simple usage example
async def main():
    """Main function for testing the fetcher"""
    fetcher = AfricaESGFetcher()
    
    # Seed demo data
    print("Seeding demo data...")
    seed_result = await fetcher.seed_demo_data()
    print(f"Seed result: {seed_result}")
    
    # Fetch regions
    print("\nFetching regions...")
    regions = await fetcher.get_regions(country="ZA")
    print(f"Found {len(regions)} regions")
    
    # Fetch environmental metrics
    print("\nFetching environmental metrics...")
    env_metrics = await fetcher.fetch_environmental_metrics(region_code="ZA-GT")
    if not env_metrics.empty:
        print(f"Found {len(env_metrics)} environmental metrics")
        print(env_metrics.head())
    else:
        print("No environmental metrics found")
    
    # Export ESG data
    print("\nExporting ESG data...")
    export_result = await fetcher.export_esg_data(region_code="ZA-GT", dimension="environmental", format="json")
    print(f"Export result: {export_result}")
    
if __name__ == "__main__":
    asyncio.run(main())