"""
ESG Data Export Example

This script demonstrates how to export ESG data for consumption via the WizData API.
It shows how to format and export data in different formats (JSON, CSV) for various use cases.
"""

import os
import sys
import json
import asyncio
import logging
from pathlib import Path
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Add the current directory to the Python path so we can import the modules
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Import the Africa ESG fetcher
from src.ingestion.esg.africa_esg_fetcher import AfricaESGFetcher

# Define the output directory for exports
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), 'data', 'exports', 'esg')
os.makedirs(OUTPUT_DIR, exist_ok=True)

async def export_regions_to_json():
    """Export available regions to JSON for API consumption"""
    logger.info("Exporting regions to JSON...")
    
    fetcher = AfricaESGFetcher()
    regions = await fetcher.get_regions()
    
    # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"african_regions_{timestamp}.json"
    output_path = os.path.join(OUTPUT_DIR, filename)
    
    with open(output_path, 'w') as f:
        json.dump(regions, f, indent=2)
    
    logger.info(f"Exported {len(regions)} regions to {output_path}")
    return output_path

async def export_esg_data_for_region(region_code, dimension, start_date=None, end_date=None):
    """
    Export ESG data for a specific region and dimension
    
    Args:
        region_code (str): Region code (e.g., 'ZA-GT')
        dimension (str): ESG dimension ('environmental', 'social', 'governance', 'infrastructure')
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
    """
    logger.info(f"Exporting {dimension} data for region {region_code}...")
    
    fetcher = AfricaESGFetcher()
    
    # Default to last 12 months if no dates provided
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Export in both JSON and CSV formats
    result_json = await fetcher.export_esg_data(
        region_code=region_code,
        dimension=dimension,
        start_date=start_date,
        end_date=end_date,
        format="json"
    )
    
    result_csv = await fetcher.export_esg_data(
        region_code=region_code,
        dimension=dimension,
        start_date=start_date,
        end_date=end_date,
        format="csv"
    )
    
    logger.info(f"Exported {dimension} data for {region_code} to:")
    logger.info(f"  - JSON: {result_json['filename']}")
    logger.info(f"  - CSV: {result_csv['filename']}")
    
    return {
        "json_path": result_json['filename'],
        "csv_path": result_csv['filename'],
        "record_count": result_json['count']
    }

async def export_esg_scores(region_code, date=None):
    """
    Export ESG scores for a specific region
    
    Args:
        region_code (str): Region code (e.g., 'ZA-GT')
        date (str, optional): Date in YYYY-MM-DD format
    """
    logger.info(f"Exporting ESG scores for region {region_code}...")
    
    fetcher = AfricaESGFetcher()
    
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    scores = await fetcher.fetch_esg_scores(region_code=region_code, date=date)
    
    # Save to JSON file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{region_code}_esg_scores_{timestamp}.json"
    output_path = os.path.join(OUTPUT_DIR, filename)
    
    with open(output_path, 'w') as f:
        json.dump(scores, f, indent=2)
    
    logger.info(f"Exported ESG scores for {region_code} to {output_path}")
    return output_path

async def export_comparative_analysis(region_codes, dimension=None, metrics=None, date=None):
    """
    Export comparative analysis of multiple regions
    
    Args:
        region_codes (list): List of region codes to compare
        dimension (str, optional): ESG dimension to compare
        metrics (list, optional): Specific metrics to compare
        date (str, optional): Date in YYYY-MM-DD format
    """
    logger.info(f"Exporting comparative analysis for regions: {', '.join(region_codes)}...")
    
    fetcher = AfricaESGFetcher()
    
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    analysis = await fetcher.fetch_comparative_analysis(
        region_codes=region_codes,
        dimension=dimension,
        metrics=metrics,
        date=date
    )
    
    # Save to both JSON and CSV formats
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dimension_part = f"_{dimension}" if dimension else ""
    json_filename = f"comparative_analysis{dimension_part}_{timestamp}.json"
    csv_filename = f"comparative_analysis{dimension_part}_{timestamp}.csv"
    
    json_path = os.path.join(OUTPUT_DIR, json_filename)
    csv_path = os.path.join(OUTPUT_DIR, csv_filename)
    
    # Save JSON
    with open(json_path, 'w') as f:
        json.dump(analysis.to_dict(orient='records'), f, indent=2)
    
    # Save CSV
    analysis.to_csv(csv_path, index=False)
    
    logger.info(f"Exported comparative analysis to:")
    logger.info(f"  - JSON: {json_path}")
    logger.info(f"  - CSV: {csv_path}")
    
    return {
        "json_path": json_path,
        "csv_path": csv_path,
        "record_count": len(analysis)
    }

async def main():
    """Main export function"""
    logger.info("Starting ESG data export...")
    
    # Export regions
    await export_regions_to_json()
    
    # Export environmental data for Gauteng, South Africa
    await export_esg_data_for_region('ZA-GT', 'environmental')
    
    # Export social data for Western Cape, South Africa
    await export_esg_data_for_region('ZA-WC', 'social')
    
    # Export governance data for Nairobi, Kenya
    await export_esg_data_for_region('KE-110', 'governance')
    
    # Export infrastructure data for Lagos, Nigeria
    await export_esg_data_for_region('NG-LA', 'infrastructure')
    
    # Export ESG scores for multiple regions
    await export_esg_scores('ZA-GT')
    await export_esg_scores('KE-110')
    
    # Export comparative analysis of South African provinces
    await export_comparative_analysis(
        region_codes=['ZA-GT', 'ZA-WC', 'ZA-EC', 'ZA-NL'],
        dimension='environmental'
    )
    
    # Export comparative analysis of major African cities
    await export_comparative_analysis(
        region_codes=['ZA-GT', 'KE-110', 'NG-LA', 'EG-C', 'GH-AA', 'ZA-CPT'],
        dimension='overall'
    )
    
    logger.info("ESG data export completed successfully.")

if __name__ == "__main__":
    # Create event loop
    loop = asyncio.get_event_loop()
    
    try:
        # Run the main function
        loop.run_until_complete(main())
    except Exception as e:
        logger.error(f"Error exporting ESG data: {str(e)}")
    finally:
        # Close the event loop
        loop.close()