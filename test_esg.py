"""
ESG API Testing Script

This script demonstrates how to call the ESG API endpoints and process the responses.
"""

import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Base URL for the API
BASE_URL = "http://localhost:5000/api/esg"

def test_esg_api_index():
    """Test the ESG API index endpoint"""
    try:
        response = requests.get(f"{BASE_URL}/")
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"ESG API Version: {data.get('version')}")
        logger.info(f"Available endpoints: {len(data.get('endpoints', []))} endpoints")
        
        return data
    except Exception as e:
        logger.error(f"Error testing ESG API index: {str(e)}")
        return None

def test_get_regions(region_type=None, country=None):
    """
    Test the regions endpoint
    
    Args:
        region_type (str, optional): Filter by region type (country, province, municipality)
        country (str, optional): Filter by country
    """
    try:
        # Build query parameters
        params = {}
        if region_type:
            params['region_type'] = region_type
        if country:
            params['country'] = country
            
        # Make request
        response = requests.get(f"{BASE_URL}/regions", params=params)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"Retrieved {data.get('count', 0)} regions")
        
        # Display the first few regions
        regions = data.get('regions', [])
        if regions:
            for i, region in enumerate(regions[:3]):
                logger.info(f"Region {i+1}: {region.get('name')} ({region.get('code')})")
            
            if len(regions) > 3:
                logger.info(f"... and {len(regions) - 3} more regions")
        
        return data
    except Exception as e:
        logger.error(f"Error testing regions endpoint: {str(e)}")
        return None

def test_get_environmental_metrics(region_id=None, metric_type=None, 
                                 start_date=None, end_date=None, 
                                 export_format=None):
    """
    Test the environmental metrics endpoint
    
    Args:
        region_id (int, optional): Filter by region ID
        metric_type (str, optional): Filter by metric type
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        export_format (str, optional): Export format (json, csv)
    """
    try:
        # Build query parameters
        params = {}
        if region_id:
            params['region_id'] = region_id
        if metric_type:
            params['metric_type'] = metric_type
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if export_format:
            params['format'] = export_format
            
        # Make request
        response = requests.get(f"{BASE_URL}/environmental", params=params)
        response.raise_for_status()
        
        # If export format is specified, handle file download
        if export_format:
            # Get filename from content-disposition header
            content_disposition = response.headers.get('content-disposition', '')
            filename = content_disposition.split('filename=')[1].strip('"') if 'filename=' in content_disposition else f"environmental_metrics.{export_format}"
            
            # Save to file
            with open(filename, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Exported environmental metrics to {filename}")
            return filename
        
        # Handle JSON response
        data = response.json()
        metrics = data.get('metrics', [])
        logger.info(f"Retrieved {data.get('count', 0)} environmental metrics")
        
        # Display the first few metrics
        if metrics:
            for i, metric in enumerate(metrics[:3]):
                logger.info(f"Metric {i+1}: {metric.get('metric_type')} = {metric.get('value')} {metric.get('unit')} ({metric.get('region_name')})")
            
            if len(metrics) > 3:
                logger.info(f"... and {len(metrics) - 3} more metrics")
        
        return data
    except Exception as e:
        logger.error(f"Error testing environmental metrics endpoint: {str(e)}")
        return None

def test_get_esg_scores(region_id=None, start_date=None, end_date=None, export_format=None):
    """
    Test the ESG scores endpoint
    
    Args:
        region_id (int, optional): Filter by region ID
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
        export_format (str, optional): Export format (json, csv)
    """
    try:
        # Build query parameters
        params = {}
        if region_id:
            params['region_id'] = region_id
        if start_date:
            params['start_date'] = start_date
        if end_date:
            params['end_date'] = end_date
        if export_format:
            params['format'] = export_format
            
        # Make request
        response = requests.get(f"{BASE_URL}/scores", params=params)
        response.raise_for_status()
        
        # If export format is specified, handle file download
        if export_format:
            # Get filename from content-disposition header
            content_disposition = response.headers.get('content-disposition', '')
            filename = content_disposition.split('filename=')[1].strip('"') if 'filename=' in content_disposition else f"esg_scores.{export_format}"
            
            # Save to file
            with open(filename, 'wb') as f:
                f.write(response.content)
                
            logger.info(f"Exported ESG scores to {filename}")
            return filename
        
        # Handle JSON response
        data = response.json()
        scores = data.get('scores', [])
        logger.info(f"Retrieved {data.get('count', 0)} ESG scores")
        
        # Display the scores
        if scores:
            for i, score in enumerate(scores[:3]):
                logger.info(f"Score {i+1}: {score.get('region_name')} ({score.get('region_code')})")
                logger.info(f"  Environmental: {score.get('environmental_score')}")
                logger.info(f"  Social: {score.get('social_score')}")
                logger.info(f"  Governance: {score.get('governance_score')}")
                logger.info(f"  Infrastructure: {score.get('infrastructure_score')}")
                logger.info(f"  Overall: {score.get('overall_score')}")
            
            if len(scores) > 3:
                logger.info(f"... and {len(scores) - 3} more scores")
        
        return data
    except Exception as e:
        logger.error(f"Error testing ESG scores endpoint: {str(e)}")
        return None

def run_all_tests():
    """Run all ESG API tests"""
    logger.info("=== Testing ESG API ===")
    
    # Test index endpoint
    logger.info("\n=== Testing ESG API Index ===")
    test_esg_api_index()
    
    # Test regions endpoint
    logger.info("\n=== Testing Regions Endpoint ===")
    test_get_regions()
    
    # Test regions endpoint with filters
    logger.info("\n=== Testing Regions Endpoint (Countries Only) ===")
    test_get_regions(region_type="country")
    
    # Test environmental metrics endpoint
    logger.info("\n=== Testing Environmental Metrics Endpoint ===")
    test_get_environmental_metrics()
    
    # Test ESG scores endpoint
    logger.info("\n=== Testing ESG Scores Endpoint ===")
    test_get_esg_scores()
    
    logger.info("\n=== All tests completed ===")

if __name__ == "__main__":
    run_all_tests()