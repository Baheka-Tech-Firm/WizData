"""
Test ESG Data Integration

This script tests the Africa ESG data integration components.
"""

import os
import sys
import json
import asyncio
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the current directory to the Python path so we can import the modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import required modules
from src.ingestion.esg.africa_esg_fetcher import AfricaESGFetcher
import requests

BASE_URL = "http://localhost:5000"  # Adjust if running on a different port

def test_seed_data():
    """Test the seed data endpoint"""
    logger.info("Testing seed data endpoint...")
    response = requests.post(f"{BASE_URL}/api/esg/africa/seed")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert data['status'] == 'success', f"Expected status 'success', got {data['status']}"
    
    logger.info("Seed data endpoint test: PASSED")
    return data

def test_get_regions():
    """Test the regions endpoint"""
    logger.info("Testing regions endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/regions")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['regions']) > 0, "Expected at least one region in the response"
    
    # Test filtering by country
    response = requests.get(f"{BASE_URL}/api/esg/africa/regions?country=ZA")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert all(r['country'] == 'South Africa' for r in data['regions']), "Expected all regions to be from South Africa"
    
    # Test filtering by region type
    response = requests.get(f"{BASE_URL}/api/esg/africa/regions?region_type=province")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert all(r['region_type'] == 'province' for r in data['regions']), "Expected all regions to be provinces"
    
    logger.info("Regions endpoint test: PASSED")
    return data

def test_environmental_metrics():
    """Test the environmental metrics endpoint"""
    logger.info("Testing environmental metrics endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/environmental?region_code=ZA-GT")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['metrics']) > 0, "Expected at least one metric in the response"
    
    logger.info("Environmental metrics endpoint test: PASSED")
    return data

def test_social_metrics():
    """Test the social metrics endpoint"""
    logger.info("Testing social metrics endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/social?region_code=ZA-GT")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['metrics']) > 0, "Expected at least one metric in the response"
    
    logger.info("Social metrics endpoint test: PASSED")
    return data

def test_governance_metrics():
    """Test the governance metrics endpoint"""
    logger.info("Testing governance metrics endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/governance?region_code=ZA-GT")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['metrics']) > 0, "Expected at least one metric in the response"
    
    logger.info("Governance metrics endpoint test: PASSED")
    return data

def test_infrastructure_metrics():
    """Test the infrastructure metrics endpoint"""
    logger.info("Testing infrastructure metrics endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/infrastructure?region_code=ZA-GT")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['metrics']) > 0, "Expected at least one metric in the response"
    
    logger.info("Infrastructure metrics endpoint test: PASSED")
    return data

def test_esg_scores():
    """Test the ESG scores endpoint"""
    logger.info("Testing ESG scores endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/scores?region_code=ZA-GT")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert 'environmental_score' in data, "Expected environmental score in the response"
    assert 'social_score' in data, "Expected social score in the response"
    assert 'governance_score' in data, "Expected governance score in the response"
    assert 'infrastructure_score' in data, "Expected infrastructure score in the response"
    assert 'overall_score' in data, "Expected overall score in the response"
    
    logger.info("ESG scores endpoint test: PASSED")
    return data

def test_comparative_analysis():
    """Test the comparative analysis endpoint"""
    logger.info("Testing comparative analysis endpoint...")
    region_codes = "ZA-GT,ZA-WC,ZA-EC"
    response = requests.get(f"{BASE_URL}/api/esg/africa/compare?region_codes={region_codes}&dimension=environmental")
    data = response.json()
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert len(data['regions']) == 3, "Expected 3 regions in the comparison"
    assert len(data['metrics']) > 0, "Expected at least one metric in the comparison"
    
    logger.info("Comparative analysis endpoint test: PASSED")
    return data

def test_export_data():
    """Test the export data endpoint"""
    logger.info("Testing export data endpoint...")
    response = requests.get(f"{BASE_URL}/api/esg/africa/export?region_code=ZA-GT&dimension=environmental&format=json")
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert response.headers.get('Content-Type') == 'application/json', "Expected JSON content type"
    
    response = requests.get(f"{BASE_URL}/api/esg/africa/export?region_code=ZA-GT&dimension=environmental&format=csv")
    
    assert response.status_code == 200, f"Expected status code 200, got {response.status_code}"
    assert response.headers.get('Content-Type') == 'text/csv', "Expected CSV content type"
    
    logger.info("Export data endpoint test: PASSED")
    return "Export tests passed"

def run_all_tests():
    """Run all tests"""
    logger.info("Starting ESG integration tests...")
    
    try:
        # First, ensure we have some data
        seed_result = test_seed_data()
        logger.info(f"Seed result: {json.dumps(seed_result, indent=2)}")
        
        # Test the endpoints
        regions = test_get_regions()
        env_metrics = test_environmental_metrics()
        social_metrics = test_social_metrics()
        gov_metrics = test_governance_metrics()
        infra_metrics = test_infrastructure_metrics()
        scores = test_esg_scores()
        comparison = test_comparative_analysis()
        export_result = test_export_data()
        
        logger.info("ESG integration tests: ALL TESTS PASSED")
    except AssertionError as e:
        logger.error(f"Test failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during testing: {str(e)}")

if __name__ == "__main__":
    run_all_tests()