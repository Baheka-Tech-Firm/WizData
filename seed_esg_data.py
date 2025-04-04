"""
Seed ESG Data Script

This script populates the database with sample ESG data for African regions.
It's useful for development and testing purposes.
"""

import os
import sys
import logging
import asyncio
from pathlib import Path
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the current directory to the Python path so we can import the modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

from src.ingestion.esg.africa_esg_fetcher import AfricaESGFetcher

async def seed_data():
    """Seed the database with sample ESG data"""
    logger.info("Starting ESG data seeding...")
    
    # Initialize the Africa ESG fetcher
    fetcher = AfricaESGFetcher()
    
    # Seed the database with demo data
    result = await fetcher.seed_demo_data()
    
    if result['status'] == 'success':
        logger.info(f"Successfully seeded the database with ESG data:")
        logger.info(f"  - {result['regions_added']} regions added")
        logger.info(f"  - {result['environmental_metrics_added']} environmental metrics added")
        logger.info(f"  - {result['social_metrics_added']} social metrics added")
        logger.info(f"  - {result['governance_metrics_added']} governance metrics added")
        logger.info(f"  - {result['infrastructure_metrics_added']} infrastructure metrics added")
        logger.info(f"  - {result['esg_scores_added']} ESG scores added")
    else:
        logger.error(f"Failed to seed the database: {result['message']}")

if __name__ == "__main__":
    # Create event loop
    loop = asyncio.get_event_loop()
    
    try:
        # Run the seed function
        loop.run_until_complete(seed_data())
    except Exception as e:
        logger.error(f"Error seeding ESG data: {str(e)}")
    finally:
        # Close the event loop
        loop.close()
        
    logger.info("ESG data seeding completed.")