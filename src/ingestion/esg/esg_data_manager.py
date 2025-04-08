"""
ESG Data Manager Module

This module provides a unified interface for fetching and managing ESG data
from multiple sources. It handles combining, reconciling, and normalizing data 
from different fetchers.
"""

import logging
import asyncio
import json
import os
from datetime import datetime, date, timedelta
import pandas as pd
import numpy as np

from src.ingestion.esg.stats_sa_fetcher import StatsSAFetcher
from src.ingestion.esg.osm_fetcher import OSMFetcher

logger = logging.getLogger(__name__)

class ESGDataManager:
    """
    Manager for ESG data from multiple sources
    
    This class provides a unified interface for working with ESG data
    and handles the complexity of integrating data from different sources.
    """
    
    def __init__(self):
        """Initialize the ESG Data Manager"""
        self.fetchers = {}
        self._init_fetchers()
    
    def _init_fetchers(self):
        """Initialize all available ESG data fetchers"""
        self.fetchers['stats_sa'] = StatsSAFetcher()
        self.fetchers['osm'] = OSMFetcher()
        # Add more fetchers as they become available
    
    async def get_available_regions(self, country=None, region_type=None):
        """
        Get all available regions from all data sources
        
        Args:
            country (str, optional): Filter by country code
            region_type (str, optional): Filter by region type
            
        Returns:
            list: Combined and deduplicated list of regions
        """
        all_regions = []
        
        # Get regions from each fetcher
        for fetcher_name, fetcher in self.fetchers.items():
            try:
                regions = await fetcher.get_regions(country=country, region_type=region_type)
                all_regions.extend(regions)
            except Exception as e:
                logger.error(f"Error getting regions from {fetcher_name}: {str(e)}")
        
        # Remove duplicates based on region code
        unique_regions = {}
        for region in all_regions:
            code = region.get("code")
            if code and code not in unique_regions:
                unique_regions[code] = region
        
        return list(unique_regions.values())
    
    async def get_dimension_data(self, region_code, dimension, metric_types=None, start_date=None, end_date=None, sources=None):
        """
        Get ESG data for a specific dimension
        
        Args:
            region_code (str): Region code
            dimension (str): ESG dimension ('environmental', 'social', 'governance', 'infrastructure')
            metric_types (list, optional): List of specific metric types to fetch
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            sources (list, optional): List of specific sources to use
            
        Returns:
            pandas.DataFrame: Combined and reconciled DataFrame with ESG data
        """
        # Validate dimension
        if dimension not in ['environmental', 'social', 'governance', 'infrastructure']:
            raise ValueError(f"Invalid dimension: {dimension}")
        
        # Filter sources if specified
        fetchers = self.fetchers
        if sources:
            fetchers = {k: v for k, v in fetchers.items() if k in sources}
        
        all_data = []
        
        # Get data from each fetcher
        for fetcher_name, fetcher in fetchers.items():
            try:
                if dimension == 'environmental':
                    df = await fetcher.fetch_environmental_data(
                        region_code, metric_types, start_date, end_date
                    )
                elif dimension == 'social':
                    df = await fetcher.fetch_social_data(
                        region_code, metric_types, start_date, end_date
                    )
                elif dimension == 'governance':
                    df = await fetcher.fetch_governance_data(
                        region_code, metric_types, start_date, end_date
                    )
                elif dimension == 'infrastructure':
                    df = await fetcher.fetch_infrastructure_data(
                        region_code, metric_types, start_date, end_date
                    )
                
                if not df.empty:
                    # Add source information if not already present
                    if 'source' not in df.columns:
                        df['source'] = fetcher_name
                    
                    all_data.append(df)
            except Exception as e:
                logger.error(f"Error fetching {dimension} data from {fetcher_name}: {str(e)}")
        
        # Combine all data
        if not all_data:
            return pd.DataFrame()
        
        combined_df = pd.concat(all_data, ignore_index=True)
        
        # Reconcile and normalize the data
        normalized_df = self._reconcile_data(combined_df, dimension)
        
        return normalized_df
    
    async def get_esg_data(self, region_code, dimensions=None, metric_types=None, start_date=None, end_date=None, sources=None):
        """
        Get ESG data for multiple dimensions
        
        Args:
            region_code (str): Region code
            dimensions (list, optional): List of ESG dimensions to fetch
            metric_types (list, optional): List of specific metric types to fetch
            start_date (str, optional): Start date in YYYY-MM-DD format
            end_date (str, optional): End date in YYYY-MM-DD format
            sources (list, optional): List of specific sources to use
            
        Returns:
            dict: Dictionary with DataFrames for each dimension
        """
        if dimensions is None:
            dimensions = ['environmental', 'social', 'governance', 'infrastructure']
        
        results = {}
        
        for dimension in dimensions:
            results[dimension] = await self.get_dimension_data(
                region_code, dimension, metric_types, start_date, end_date, sources
            )
        
        return results
    
    async def calculate_esg_scores(self, region_code, date=None, sources=None):
        """
        Calculate composite ESG scores for a region
        
        Args:
            region_code (str): Region code
            date (str, optional): Date for which to calculate scores (YYYY-MM-DD format)
            sources (list, optional): List of specific sources to use
            
        Returns:
            dict: Dictionary with ESG scores
        """
        if date is None:
            date = datetime.now().strftime('%Y-%m-%d')
        
        # Get data for the specified date range
        # Use a 365-day window to ensure we have sufficient data
        start_date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=365)
        end_date = datetime.strptime(date, '%Y-%m-%d')
        
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Get data for all dimensions
        data = await self.get_esg_data(
            region_code, 
            dimensions=['environmental', 'social', 'governance', 'infrastructure'],
            start_date=start_date_str,
            end_date=end_date_str,
            sources=sources
        )
        
        # Calculate scores for each dimension
        env_score = self._calculate_dimension_score(data.get('environmental', pd.DataFrame()))
        social_score = self._calculate_dimension_score(data.get('social', pd.DataFrame()))
        gov_score = self._calculate_dimension_score(data.get('governance', pd.DataFrame()))
        infra_score = self._calculate_dimension_score(data.get('infrastructure', pd.DataFrame()))
        
        # Calculate overall score using weighted average
        weights = {
            'environmental': 0.25,
            'social': 0.25,
            'governance': 0.25,
            'infrastructure': 0.25
        }
        
        weighted_scores = []
        weighted_weights = []
        
        if env_score is not None:
            weighted_scores.append(env_score * weights['environmental'])
            weighted_weights.append(weights['environmental'])
        
        if social_score is not None:
            weighted_scores.append(social_score * weights['social'])
            weighted_weights.append(weights['social'])
        
        if gov_score is not None:
            weighted_scores.append(gov_score * weights['governance'])
            weighted_weights.append(weights['governance'])
        
        if infra_score is not None:
            weighted_scores.append(infra_score * weights['infrastructure'])
            weighted_weights.append(weights['infrastructure'])
        
        overall_score = None
        if weighted_scores:
            overall_score = sum(weighted_scores) / sum(weighted_weights)
        
        # Prepare component details
        components = {
            'environmental': self._get_score_components(data.get('environmental', pd.DataFrame())),
            'social': self._get_score_components(data.get('social', pd.DataFrame())),
            'governance': self._get_score_components(data.get('governance', pd.DataFrame())),
            'infrastructure': self._get_score_components(data.get('infrastructure', pd.DataFrame()))
        }
        
        return {
            'region_code': region_code,
            'environmental_score': env_score,
            'social_score': social_score,
            'governance_score': gov_score,
            'infrastructure_score': infra_score,
            'overall_score': overall_score,
            'date': date,
            'methodology': 'weighted_average',
            'weights': weights,
            'components': components
        }
    
    def _reconcile_data(self, df, dimension):
        """
        Reconcile and normalize data from multiple sources
        
        Args:
            df (pandas.DataFrame): Combined DataFrame from multiple sources
            dimension (str): ESG dimension
            
        Returns:
            pandas.DataFrame: Reconciled and normalized DataFrame
        """
        if df.empty:
            return df
        
        # Ensure required columns
        required_cols = ['region_code', 'metric_type', 'value', 'date']
        for col in required_cols:
            if col not in df.columns:
                logger.error(f"Required column '{col}' missing in {dimension} data")
                return pd.DataFrame()
        
        # Convert date strings to datetime
        if df['date'].dtype == 'object':
            df['date'] = pd.to_datetime(df['date'])
        
        # Group by region, metric type, and date
        # If there are multiple values for the same metric from different sources,
        # we need to reconcile them. For now, we'll take the average.
        grouped = df.groupby(['region_code', 'metric_type', 'date'])
        
        reconciled_data = []
        
        for (region, metric, date), group in grouped:
            if len(group) == 1:
                # Only one source, no need to reconcile
                reconciled_data.append(group.iloc[0].to_dict())
            else:
                # Multiple sources, reconcile
                # Calculate weighted average based on confidence
                if 'confidence' in group.columns:
                    # Use confidence as weights
                    weights = group['confidence'].fillna(0.5).values
                    weights = weights / weights.sum()
                    value = (group['value'] * weights).sum()
                    confidence = group['confidence'].max()  # Use the highest confidence
                else:
                    # Simple average if no confidence values
                    value = group['value'].mean()
                    confidence = 0.7  # Default confidence for reconciled data
                
                # Combine source information
                sources = ', '.join(sorted(group['source'].unique()))
                
                # Use the most common unit
                unit = group['unit'].mode().iloc[0] if 'unit' in group.columns else None
                
                # Get the most comprehensive description
                description = None
                if 'description' in group.columns:
                    descriptions = group['description'].dropna()
                    if not descriptions.empty:
                        description = max(descriptions, key=len)
                
                reconciled_data.append({
                    'region_code': region,
                    'metric_type': metric,
                    'value': value,
                    'date': date,
                    'unit': unit,
                    'source': sources,
                    'confidence': confidence,
                    'description': description,
                    'reconciled': True
                })
        
        return pd.DataFrame(reconciled_data)
    
    def _calculate_dimension_score(self, df):
        """
        Calculate score for a single dimension
        
        Args:
            df (pandas.DataFrame): DataFrame with metrics
            
        Returns:
            float: Score for the dimension (0-100)
        """
        if df.empty:
            return None
        
        # Basic scoring logic - can be enhanced with more sophisticated algorithms
        # We'll use a weighted average of normalized metrics
        
        # Get unique metric types
        metric_types = df['metric_type'].unique()
        if len(metric_types) == 0:
            return None
        
        # Get the latest date for each metric type
        latest_metrics = []
        for metric in metric_types:
            metric_df = df[df['metric_type'] == metric]
            if not metric_df.empty:
                latest_date = metric_df['date'].max()
                latest_row = metric_df[metric_df['date'] == latest_date].iloc[0]
                latest_metrics.append(latest_row)
        
        if not latest_metrics:
            return None
        
        # Convert to DataFrame for easier processing
        metrics_df = pd.DataFrame(latest_metrics)
        
        # Define metric weights (can be customized based on importance)
        # For now, we'll use equal weights
        weights = {metric: 1.0 / len(metric_types) for metric in metric_types}
        
        # Normalize each metric to 0-100 scale based on reference values
        normalized_values = []
        
        for _, row in metrics_df.iterrows():
            metric = row['metric_type']
            value = row['value']
            
            # Get reference values for normalization
            # These should be domain-specific minimum and maximum values
            min_val, max_val = self._get_reference_values(metric)
            
            # Normalize
            if max_val == min_val:
                normalized = 100 if value >= max_val else 0
            else:
                normalized = ((value - min_val) / (max_val - min_val)) * 100
                
                # Clip to 0-100 range
                normalized = max(0, min(100, normalized))
            
            # Apply weight
            weight = weights.get(metric, 1.0 / len(metric_types))
            normalized_values.append((normalized, weight))
        
        # Calculate weighted average
        if not normalized_values:
            return None
        
        weighted_sum = sum(norm * weight for norm, weight in normalized_values)
        total_weight = sum(weight for _, weight in normalized_values)
        
        if total_weight == 0:
            return None
        
        return weighted_sum / total_weight
    
    def _get_reference_values(self, metric_type):
        """
        Get reference values for normalizing a metric
        
        Args:
            metric_type (str): Type of metric
            
        Returns:
            tuple: (min_value, max_value) for normalization
        """
        # These reference values should be based on domain knowledge
        # and expected ranges for each metric type
        reference_values = {
            # Environmental metrics
            "electricity_access": (0, 100),  # Percentage
            "water_access": (0, 100),        # Percentage
            "waste_collection": (0, 100),    # Percentage
            
            # Social metrics
            "education_enrolment": (0, 100), # Percentage
            "literacy_rate": (0, 100),       # Percentage
            "healthcare_access": (0, 100),   # Percentage
            "grant_recipients": (0, 50),     # Percentage (higher might not be better)
            
            # Governance metrics
            "service_satisfaction": (0, 100), # Percentage
            "municipal_performance": (0, 100), # Percentage
            
            # Infrastructure metrics
            "roads_paved": (0, 100),         # Percentage
            "telecommunications": (0, 100),  # Percentage
            "schools_access": (0, 100),      # Percentage
            "clinics_access": (0, 100),      # Percentage
            "schools_count": (0, 10000),     # Count
            "healthcare_count": (0, 1000),   # Count
            "water_access_points": (0, 5000), # Count
            "power_facilities": (0, 500),    # Count
            "road_length_km": (0, 10000),    # Length in km
            
            # Derived metrics
            "schools_density": (0, 50),      # per 1000 km²
            "healthcare_density": (0, 10),   # per 1000 km²
            "schools_per_capita": (0, 500),  # per 100,000 people
            "healthcare_per_capita": (0, 50), # per 100,000 people
            "road_density": (0, 5),          # km/km²
        }
        
        # Return default values if metric not found
        return reference_values.get(metric_type, (0, 100))
    
    def _get_score_components(self, df):
        """
        Get component details for a dimension score
        
        Args:
            df (pandas.DataFrame): DataFrame with metrics
            
        Returns:
            dict: Dictionary with score components
        """
        if df.empty:
            return {}
        
        components = {}
        
        # Get unique metric types
        metric_types = df['metric_type'].unique()
        
        # Get the latest value for each metric type
        for metric in metric_types:
            metric_df = df[df['metric_type'] == metric]
            if not metric_df.empty:
                latest_date = metric_df['date'].max()
                latest_row = metric_df[metric_df['date'] == latest_date].iloc[0].to_dict()
                
                # Get reference values for normalization
                min_val, max_val = self._get_reference_values(metric)
                
                # Calculate normalized score
                value = latest_row.get('value')
                if max_val == min_val:
                    normalized_score = 100 if value >= max_val else 0
                else:
                    normalized_score = ((value - min_val) / (max_val - min_val)) * 100
                    normalized_score = max(0, min(100, normalized_score))
                
                components[metric] = {
                    'value': value,
                    'unit': latest_row.get('unit'),
                    'date': latest_row.get('date').strftime('%Y-%m-%d') if isinstance(latest_row.get('date'), (datetime, date)) else latest_row.get('date'),
                    'source': latest_row.get('source'),
                    'confidence': latest_row.get('confidence'),
                    'description': latest_row.get('description'),
                    'normalized_score': normalized_score,
                    'reference_min': min_val,
                    'reference_max': max_val
                }
        
        return components