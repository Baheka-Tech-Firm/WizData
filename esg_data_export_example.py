"""
ESG Data Export Example

This script demonstrates how to export ESG data for consumption via the WizData API.
It shows how to format and export data in different formats (JSON, CSV) for various use cases.
"""

import asyncio
import json
import os
import logging
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create exports directory if it doesn't exist
EXPORT_DIR = Path("data/exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)

async def export_regions_to_json():
    """Export available regions to JSON for API consumption"""
    try:
        # Load region data
        # In a real implementation, this would fetch from the database
        # For demonstration, we'll create sample data
        
        regions = [
            {
                "id": 1,
                "name": "South Africa",
                "code": "ZA",
                "region_type": "country",
                "parent_id": None,
                "country": "South Africa",
                "population": 59308690,
                "area_km2": 1221037
            },
            {
                "id": 2,
                "name": "Gauteng",
                "code": "ZA-GT",
                "region_type": "province",
                "parent_id": 1,
                "country": "South Africa",
                "population": 15488137,
                "area_km2": 18178
            },
            {
                "id": 3,
                "name": "City of Johannesburg",
                "code": "ZA-GT-JHB",
                "region_type": "municipality",
                "parent_id": 2,
                "country": "South Africa",
                "population": 5738535,
                "area_km2": 1645
            },
            {
                "id": 4,
                "name": "Kenya",
                "code": "KE",
                "region_type": "country",
                "parent_id": None,
                "country": "Kenya",
                "population": 53771296,
                "area_km2": 580367
            },
            {
                "id": 5,
                "name": "Nairobi",
                "code": "KE-NB",
                "region_type": "province",
                "parent_id": 4,
                "country": "Kenya",
                "population": 4397073,
                "area_km2": 696
            }
        ]
        
        # Create a DataFrame
        df = pd.DataFrame(regions)
        
        # Export to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"regions_{timestamp}.json"
        file_path = EXPORT_DIR / filename
        
        df.to_json(file_path, orient='records')
        
        logger.info(f"Exported {len(regions)} regions to {file_path}")
        return file_path
        
    except Exception as e:
        logger.error(f"Error exporting regions: {str(e)}")
        return None

async def export_esg_data_for_region(region_code, dimension, start_date=None, end_date=None):
    """
    Export ESG data for a specific region and dimension
    
    Args:
        region_code (str): Region code (e.g., 'ZA-GT')
        dimension (str): ESG dimension ('environmental', 'social', 'governance', 'infrastructure')
        start_date (str, optional): Start date in YYYY-MM-DD format
        end_date (str, optional): End date in YYYY-MM-DD format
    """
    try:
        # Set default dates if not provided
        if not start_date:
            start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
            
        # Generate sample data based on dimension
        # In a real implementation, this would fetch from the database
        
        metrics = []
        
        if dimension == "environmental":
            metrics = [
                {
                    "region_code": region_code,
                    "metric_type": "electricity_access",
                    "value": 87.5,
                    "unit": "%",
                    "date": "2023-01-15",
                    "confidence": 0.95
                },
                {
                    "region_code": region_code,
                    "metric_type": "water_access",
                    "value": 92.1,
                    "unit": "%",
                    "date": "2023-02-10",
                    "confidence": 0.92
                },
                {
                    "region_code": region_code,
                    "metric_type": "air_quality_index",
                    "value": 65.4,
                    "unit": "AQI",
                    "date": "2023-03-05",
                    "confidence": 0.88
                }
            ]
        elif dimension == "social":
            metrics = [
                {
                    "region_code": region_code,
                    "metric_type": "literacy_rate",
                    "value": 91.2,
                    "unit": "%",
                    "date": "2023-01-20",
                    "confidence": 0.93
                },
                {
                    "region_code": region_code,
                    "metric_type": "healthcare_access",
                    "value": 78.5,
                    "unit": "%",
                    "date": "2023-02-15",
                    "confidence": 0.90
                },
                {
                    "region_code": region_code,
                    "metric_type": "employment_rate",
                    "value": 65.8,
                    "unit": "%",
                    "date": "2023-03-10",
                    "confidence": 0.89
                }
            ]
        elif dimension == "governance":
            metrics = [
                {
                    "region_code": region_code,
                    "metric_type": "audit_outcome",
                    "value": 82.0,
                    "status": "Unqualified",
                    "unit": "score",
                    "date": "2023-01-25",
                    "confidence": 0.94
                },
                {
                    "region_code": region_code,
                    "metric_type": "corruption_index",
                    "value": 58.3,
                    "unit": "index",
                    "date": "2023-02-20",
                    "confidence": 0.87
                },
                {
                    "region_code": region_code,
                    "metric_type": "public_participation",
                    "value": 71.5,
                    "unit": "index",
                    "date": "2023-03-15",
                    "confidence": 0.91
                }
            ]
        elif dimension == "infrastructure":
            metrics = [
                {
                    "region_code": region_code,
                    "metric_type": "roads_paved",
                    "value": 72.4,
                    "unit": "%",
                    "date": "2023-01-30",
                    "confidence": 0.92
                },
                {
                    "region_code": region_code,
                    "metric_type": "internet_connectivity",
                    "value": 81.2,
                    "unit": "%",
                    "date": "2023-02-25",
                    "confidence": 0.93
                },
                {
                    "region_code": region_code,
                    "metric_type": "public_transport_access",
                    "value": 67.8,
                    "unit": "%",
                    "date": "2023-03-20",
                    "confidence": 0.89
                }
            ]
        
        # Filter by date range
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        filtered_metrics = [
            m for m in metrics 
            if start_date_obj <= datetime.strptime(m["date"], "%Y-%m-%d").date() <= end_date_obj
        ]
        
        # Create a DataFrame
        df = pd.DataFrame(filtered_metrics)
        
        # Export to both JSON and CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"{region_code}_{dimension}_{timestamp}.json"
        csv_filename = f"{region_code}_{dimension}_{timestamp}.csv"
        
        json_path = EXPORT_DIR / json_filename
        csv_path = EXPORT_DIR / csv_filename
        
        df.to_json(json_path, orient='records')
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Exported {len(filtered_metrics)} {dimension} metrics for {region_code} to {json_path} and {csv_path}")
        
        return {
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "count": len(filtered_metrics)
        }
        
    except Exception as e:
        logger.error(f"Error exporting {dimension} data for {region_code}: {str(e)}")
        return None

async def export_esg_scores(region_code, date=None):
    """
    Export ESG scores for a specific region
    
    Args:
        region_code (str): Region code (e.g., 'ZA-GT')
        date (str, optional): Date in YYYY-MM-DD format
    """
    try:
        # Set default date if not provided
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
            
        # Generate sample ESG scores
        # In a real implementation, this would fetch from the database
        
        scores = {
            "region_code": region_code,
            "environmental_score": 72.5,
            "social_score": 68.9,
            "governance_score": 64.2,
            "infrastructure_score": 70.1,
            "overall_score": 68.9,
            "date": date,
            "methodology": "WizData ESG Composite Methodology v1.0",
            "confidence": 0.91
        }
        
        # Export to JSON
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{region_code}_esg_scores_{timestamp}.json"
        file_path = EXPORT_DIR / filename
        
        with open(file_path, 'w') as f:
            json.dump(scores, f, indent=2)
        
        logger.info(f"Exported ESG scores for {region_code} to {file_path}")
        
        return file_path
        
    except Exception as e:
        logger.error(f"Error exporting ESG scores for {region_code}: {str(e)}")
        return None

async def export_comparative_analysis(region_codes, dimension=None, metrics=None, date=None):
    """
    Export comparative analysis of multiple regions
    
    Args:
        region_codes (list): List of region codes to compare
        dimension (str, optional): ESG dimension to compare
        metrics (list, optional): Specific metrics to compare
        date (str, optional): Date in YYYY-MM-DD format
    """
    try:
        # Set defaults if not provided
        if not date:
            date = datetime.now().strftime("%Y-%m-%d")
        if not dimension:
            dimension = "overall"  # Compare overall scores by default
        if not metrics and dimension != "overall":
            # Set default metrics based on dimension
            if dimension == "environmental":
                metrics = ["electricity_access", "water_access"]
            elif dimension == "social":
                metrics = ["literacy_rate", "healthcare_access"]
            elif dimension == "governance":
                metrics = ["corruption_index", "public_participation"]
            elif dimension == "infrastructure":
                metrics = ["roads_paved", "internet_connectivity"]
        
        # Generate sample comparison data
        # In a real implementation, this would fetch from the database
        
        comparison_data = []
        
        if dimension == "overall":
            # Compare overall ESG scores
            for region_code in region_codes:
                # Generate random-ish but deterministic scores based on region code
                seed = sum(ord(c) for c in region_code)
                env_score = (seed % 20 + 60)  # Range 60-80
                soc_score = (seed % 25 + 55)  # Range 55-80
                gov_score = (seed % 30 + 50)  # Range 50-80
                inf_score = (seed % 22 + 58)  # Range 58-80
                overall = (env_score + soc_score + gov_score + inf_score) / 4
                
                comparison_data.append({
                    "region_code": region_code,
                    "environmental_score": env_score,
                    "social_score": soc_score,
                    "governance_score": gov_score,
                    "infrastructure_score": inf_score,
                    "overall_score": overall,
                    "date": date
                })
        else:
            # Compare specific metrics
            for region_code in region_codes:
                seed = sum(ord(c) for c in region_code)
                metric_values = {}
                
                for metric in metrics:
                    # Generate a deterministic but varied value for each metric and region
                    metric_seed = seed + sum(ord(c) for c in metric)
                    value = (metric_seed % 40 + 50)  # Range 50-90
                    metric_values[metric] = value
                
                comparison_data.append({
                    "region_code": region_code,
                    "dimension": dimension,
                    "metrics": metric_values,
                    "date": date
                })
        
        # Create a DataFrame for export
        if dimension == "overall":
            df = pd.DataFrame(comparison_data)
        else:
            # Reshape the data for easier comparison in the DataFrame
            rows = []
            for item in comparison_data:
                region_code = item["region_code"]
                for metric_name, value in item["metrics"].items():
                    rows.append({
                        "region_code": region_code,
                        "dimension": dimension,
                        "metric": metric_name,
                        "value": value,
                        "date": item["date"]
                    })
            df = pd.DataFrame(rows)
        
        # Export to both JSON and CSV
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        json_filename = f"comparison_{dimension}_{timestamp}.json"
        csv_filename = f"comparison_{dimension}_{timestamp}.csv"
        
        json_path = EXPORT_DIR / json_filename
        csv_path = EXPORT_DIR / csv_filename
        
        df.to_json(json_path, orient='records')
        df.to_csv(csv_path, index=False)
        
        logger.info(f"Exported comparison analysis for {len(region_codes)} regions to {json_path} and {csv_path}")
        
        return {
            "json_path": str(json_path),
            "csv_path": str(csv_path),
            "count": len(comparison_data)
        }
        
    except Exception as e:
        logger.error(f"Error exporting comparative analysis: {str(e)}")
        return None

async def main():
    """Main export function"""
    # Export list of regions
    await export_regions_to_json()
    
    # Export ESG data for specific regions
    region_codes = ["ZA-GT", "KE-NB"]
    for region_code in region_codes:
        for dimension in ["environmental", "social", "governance", "infrastructure"]:
            await export_esg_data_for_region(region_code, dimension)
    
    # Export ESG scores
    for region_code in region_codes:
        await export_esg_scores(region_code)
    
    # Export comparative analysis
    await export_comparative_analysis(region_codes)
    
    # Export comparative analysis for specific dimension and metrics
    await export_comparative_analysis(
        region_codes=region_codes, 
        dimension="environmental", 
        metrics=["electricity_access", "water_access", "air_quality_index"]
    )

if __name__ == "__main__":
    asyncio.run(main())