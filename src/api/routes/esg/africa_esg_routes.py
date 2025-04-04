"""
Africa ESG API Routes

This module provides endpoints for accessing ESG (Environmental, Social, Governance)
data for African regions.
"""

import logging
import json
import os
from datetime import datetime, timedelta
import traceback
from pathlib import Path
from flask import Blueprint, jsonify, request, current_app, send_file, g
import pandas as pd
from sqlalchemy import func, and_, or_

from models import Region, db
from src.api.auth import require_api_key
from src.ingestion.esg.africa_esg_fetcher import AfricaESGFetcher

logger = logging.getLogger(__name__)

# Create the Africa ESG blueprint
africa_esg_bp = Blueprint('africa_esg', __name__, url_prefix='/api/esg/africa')

@africa_esg_bp.route('/')
def africa_esg_index():
    """Africa ESG API index endpoint"""
    return jsonify({
        "status": "success",
        "version": "1.0.0",
        "endpoints": [
            "/api/esg/africa/regions",
            "/api/esg/africa/environmental",
            "/api/esg/africa/social",
            "/api/esg/africa/governance",
            "/api/esg/africa/infrastructure",
            "/api/esg/africa/scores",
            "/api/esg/africa/compare",
            "/api/esg/africa/export"
        ]
    })

@africa_esg_bp.route('/regions')
def get_african_regions():
    """
    Get available African regions
    
    Query parameters:
    - country: Filter by country code (e.g., 'ZA' for South Africa)
    - region_type: Filter by region type (country, province, municipality)
    
    Returns:
        JSON response with regions
    """
    try:
        # Get query parameters
        country = request.args.get('country')
        region_type = request.args.get('region_type')
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch regions
        regions = loop.run_until_complete(fetcher.get_regions(country, region_type))
        loop.close()
        
        return jsonify({
            "status": "success",
            "count": len(regions),
            "regions": regions
        })
        
    except Exception as e:
        logger.exception(f"Error in get_african_regions: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/environmental')
def get_african_environmental_metrics():
    """
    Get environmental metrics for African regions
    
    Query parameters:
    - region_code: Region code to filter by (e.g., 'ZA-GT')
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with environmental metrics or a file download
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch data
        df = loop.run_until_complete(fetcher.fetch_environmental_metrics(region_code, start_date, end_date))
        
        # If export format is specified, export data
        if export_format in ['json', 'csv']:
            export_result = loop.run_until_complete(fetcher.export_esg_data(
                region_code=region_code,
                dimension="environmental",
                start_date=start_date,
                end_date=end_date,
                format=export_format
            ))
            
            loop.close()
            
            if export_result['status'] == 'success':
                # Return file for download
                return send_file(
                    export_result['file_path'],
                    as_attachment=True,
                    download_name=os.path.basename(export_result['file_path']),
                    mimetype='application/json' if export_format == 'json' else 'text/csv'
                )
            else:
                return jsonify(export_result), 400
        
        # Convert DataFrame to records
        if not df.empty:
            metrics = df.to_dict(orient='records')
            
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(metrics),
                "metrics": metrics
            })
        else:
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "metrics": []
            })
        
    except Exception as e:
        logger.exception(f"Error in get_african_environmental_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/social')
def get_african_social_metrics():
    """
    Get social metrics for African regions
    
    Query parameters:
    - region_code: Region code to filter by (e.g., 'ZA-GT')
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with social metrics or a file download
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch data
        df = loop.run_until_complete(fetcher.fetch_social_metrics(region_code, start_date, end_date))
        
        # If export format is specified, export data
        if export_format in ['json', 'csv']:
            export_result = loop.run_until_complete(fetcher.export_esg_data(
                region_code=region_code,
                dimension="social",
                start_date=start_date,
                end_date=end_date,
                format=export_format
            ))
            
            loop.close()
            
            if export_result['status'] == 'success':
                # Return file for download
                return send_file(
                    export_result['file_path'],
                    as_attachment=True,
                    download_name=os.path.basename(export_result['file_path']),
                    mimetype='application/json' if export_format == 'json' else 'text/csv'
                )
            else:
                return jsonify(export_result), 400
        
        # Convert DataFrame to records
        if not df.empty:
            metrics = df.to_dict(orient='records')
            
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(metrics),
                "metrics": metrics
            })
        else:
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "metrics": []
            })
        
    except Exception as e:
        logger.exception(f"Error in get_african_social_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/governance')
def get_african_governance_metrics():
    """
    Get governance metrics for African regions
    
    Query parameters:
    - region_code: Region code to filter by (e.g., 'ZA-GT')
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with governance metrics or a file download
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch data
        df = loop.run_until_complete(fetcher.fetch_governance_metrics(region_code, start_date, end_date))
        
        # If export format is specified, export data
        if export_format in ['json', 'csv']:
            export_result = loop.run_until_complete(fetcher.export_esg_data(
                region_code=region_code,
                dimension="governance",
                start_date=start_date,
                end_date=end_date,
                format=export_format
            ))
            
            loop.close()
            
            if export_result['status'] == 'success':
                # Return file for download
                return send_file(
                    export_result['file_path'],
                    as_attachment=True,
                    download_name=os.path.basename(export_result['file_path']),
                    mimetype='application/json' if export_format == 'json' else 'text/csv'
                )
            else:
                return jsonify(export_result), 400
        
        # Convert DataFrame to records
        if not df.empty:
            metrics = df.to_dict(orient='records')
            
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(metrics),
                "metrics": metrics
            })
        else:
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "metrics": []
            })
        
    except Exception as e:
        logger.exception(f"Error in get_african_governance_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/infrastructure')
def get_african_infrastructure_metrics():
    """
    Get infrastructure metrics for African regions
    
    Query parameters:
    - region_code: Region code to filter by (e.g., 'ZA-GT')
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with infrastructure metrics or a file download
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch data
        df = loop.run_until_complete(fetcher.fetch_infrastructure_metrics(region_code, start_date, end_date))
        
        # If export format is specified, export data
        if export_format in ['json', 'csv']:
            export_result = loop.run_until_complete(fetcher.export_esg_data(
                region_code=region_code,
                dimension="infrastructure",
                start_date=start_date,
                end_date=end_date,
                format=export_format
            ))
            
            loop.close()
            
            if export_result['status'] == 'success':
                # Return file for download
                return send_file(
                    export_result['file_path'],
                    as_attachment=True,
                    download_name=os.path.basename(export_result['file_path']),
                    mimetype='application/json' if export_format == 'json' else 'text/csv'
                )
            else:
                return jsonify(export_result), 400
        
        # Convert DataFrame to records
        if not df.empty:
            metrics = df.to_dict(orient='records')
            
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": len(metrics),
                "metrics": metrics
            })
        else:
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "start_date": start_date,
                "end_date": end_date,
                "count": 0,
                "metrics": []
            })
        
    except Exception as e:
        logger.exception(f"Error in get_african_infrastructure_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/scores')
def get_african_esg_scores():
    """
    Get ESG scores for African regions
    
    Query parameters:
    - region_code: Region code to fetch scores for (required)
    - date: Date in YYYY-MM-DD format (optional)
    
    Returns:
        JSON response with ESG scores
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        date = request.args.get('date')
        
        # Validate required parameters
        if not region_code:
            return jsonify({
                "status": "error",
                "message": "region_code parameter is required"
            }), 400
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch scores
        scores = loop.run_until_complete(fetcher.fetch_esg_scores(region_code, date))
        
        loop.close()
        
        if scores:
            return jsonify({
                "status": "success",
                "region_code": region_code,
                "date": date,
                "scores": scores
            })
        else:
            return jsonify({
                "status": "error",
                "message": f"No ESG scores found for region {region_code}"
            }), 404
        
    except Exception as e:
        logger.exception(f"Error in get_african_esg_scores: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/compare')
def compare_african_regions():
    """
    Compare ESG data for multiple African regions
    
    Query parameters:
    - region_codes: Comma-separated list of region codes to compare
    - dimension: ESG dimension to compare ('environmental', 'social', 'governance', 'infrastructure', 'overall')
    - metrics: Comma-separated list of specific metrics to compare (optional)
    - date: Date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with comparative analysis or a file download
    """
    try:
        # Get query parameters
        region_codes_str = request.args.get('region_codes')
        dimension = request.args.get('dimension', 'overall')
        metrics_str = request.args.get('metrics')
        date = request.args.get('date')
        export_format = request.args.get('format')
        
        # Validate required parameters
        if not region_codes_str:
            return jsonify({
                "status": "error",
                "message": "region_codes parameter is required"
            }), 400
        
        # Parse region_codes and metrics
        region_codes = region_codes_str.split(',')
        metrics = metrics_str.split(',') if metrics_str else None
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Fetch comparative analysis
        df = loop.run_until_complete(fetcher.fetch_comparative_analysis(region_codes, dimension, metrics, date))
        
        # If export format is specified, export data
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"comparison_{dimension}_{timestamp}.{export_format}"
            
            export_dir = Path("data/esg/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            loop.close()
            
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
        
        # Convert DataFrame to records
        if not df.empty:
            results = df.to_dict(orient='records')
            
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_codes": region_codes,
                "dimension": dimension,
                "metrics": metrics,
                "date": date,
                "count": len(results),
                "results": results
            })
        else:
            loop.close()
            
            return jsonify({
                "status": "success",
                "region_codes": region_codes,
                "dimension": dimension,
                "metrics": metrics,
                "date": date,
                "count": 0,
                "results": []
            })
        
    except Exception as e:
        logger.exception(f"Error in compare_african_regions: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/export')
def export_african_esg_data():
    """
    Export ESG data for African regions
    
    Query parameters:
    - region_code: Region code to export data for (required)
    - dimension: ESG dimension to export ('environmental', 'social', 'governance', 'infrastructure')
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv, default: json)
    
    Returns:
        File download
    """
    try:
        # Get query parameters
        region_code = request.args.get('region_code')
        dimension = request.args.get('dimension')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format', 'json')
        
        # Validate required parameters
        if not region_code:
            return jsonify({
                "status": "error",
                "message": "region_code parameter is required"
            }), 400
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Export data
        export_result = loop.run_until_complete(fetcher.export_esg_data(
            region_code=region_code,
            dimension=dimension,
            start_date=start_date,
            end_date=end_date,
            format=export_format
        ))
        
        loop.close()
        
        if export_result['status'] == 'success':
            # Return file for download
            return send_file(
                export_result['file_path'],
                as_attachment=True,
                download_name=os.path.basename(export_result['file_path']),
                mimetype='application/json' if export_format == 'json' else 'text/csv'
            )
        else:
            return jsonify(export_result), 400
        
    except Exception as e:
        logger.exception(f"Error in export_african_esg_data: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@africa_esg_bp.route('/seed', methods=['POST'])
def seed_african_esg_data():
    """
    Seed the database with sample ESG data for African regions
    
    This endpoint is for development and testing purposes only.
    
    Returns:
        JSON response with seed results
    """
    try:
        # Check for admin access (you might want to implement proper authentication)
        is_admin = request.args.get('admin_key') == os.environ.get('ADMIN_SECRET_KEY')
        
        if not is_admin:
            return jsonify({
                "status": "error",
                "message": "Unauthorized. This endpoint requires admin privileges."
            }), 403
        
        # Initialize fetcher
        fetcher = AfricaESGFetcher()
        
        # Create event loop
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        # Seed data
        seed_result = loop.run_until_complete(fetcher.seed_demo_data())
        
        loop.close()
        
        return jsonify(seed_result)
        
    except Exception as e:
        logger.exception(f"Error in seed_african_esg_data: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500