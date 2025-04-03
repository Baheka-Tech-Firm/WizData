"""
ESG API Routes

This module provides endpoints for accessing ESG (Environmental, Social, Governance)
and infrastructure data for African regions, allowing users to get ESG scores,
metrics, and facility information.
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

from models import (Region, EnvironmentalMetric, SocialMetric, GovernanceMetric, 
                   InfrastructureMetric, InfrastructureFacility, SDGMetric, ESGCompositeScore, 
                   DataSource, db)
from src.api.auth import require_api_key

logger = logging.getLogger(__name__)

# Create the ESG blueprint
esg_bp = Blueprint('esg', __name__, url_prefix='/api/esg')

@esg_bp.route('/')
def esg_index():
    """ESG API index endpoint"""
    return jsonify({
        "status": "success",
        "version": "1.0.0",
        "endpoints": [
            "/api/esg/regions",
            "/api/esg/environmental",
            "/api/esg/social",
            "/api/esg/governance",
            "/api/esg/infrastructure",
            "/api/esg/facilities",
            "/api/esg/sdg",
            "/api/esg/scores"
        ]
    })

@esg_bp.route('/regions')
def get_regions():
    """
    Get available regions
    
    Query parameters:
    - region_type: Filter by region type (country, province, municipality)
    - country: Filter by country
    - parent_id: Filter by parent region ID
    
    Returns:
        JSON response with regions
    """
    try:
        # Get query parameters
        region_type = request.args.get('region_type')
        country = request.args.get('country')
        parent_id = request.args.get('parent_id')
        
        # Build query
        query = Region.query
        
        if region_type:
            query = query.filter(Region.region_type == region_type)
            
        if country:
            query = query.filter(Region.country == country)
            
        if parent_id:
            try:
                parent_id = int(parent_id)
                query = query.filter(Region.parent_id == parent_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid parent_id parameter"
                }), 400
                
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
            
        return jsonify({
            "status": "success",
            "count": len(result),
            "regions": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_regions: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/environmental')
def get_environmental_metrics():
    """
    Get environmental metrics for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - metric_type: Filter by metric type (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with environmental metrics or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        metric_type = request.args.get('metric_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = EnvironmentalMetric.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(EnvironmentalMetric.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if metric_type:
            query = query.filter(EnvironmentalMetric.metric_type == metric_type)
            
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(EnvironmentalMetric.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(EnvironmentalMetric.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
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
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"environmental_metrics_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "metrics": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_environmental_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/social')
def get_social_metrics():
    """
    Get social metrics for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - metric_type: Filter by metric type (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with social metrics or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        metric_type = request.args.get('metric_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = SocialMetric.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(SocialMetric.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if metric_type:
            query = query.filter(SocialMetric.metric_type == metric_type)
            
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(SocialMetric.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(SocialMetric.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
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
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"social_metrics_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "metrics": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_social_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/governance')
def get_governance_metrics():
    """
    Get governance metrics for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - metric_type: Filter by metric type (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with governance metrics or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        metric_type = request.args.get('metric_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = GovernanceMetric.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(GovernanceMetric.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if metric_type:
            query = query.filter(GovernanceMetric.metric_type == metric_type)
            
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(GovernanceMetric.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(GovernanceMetric.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
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
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"governance_metrics_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "metrics": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_governance_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/infrastructure')
def get_infrastructure_metrics():
    """
    Get infrastructure metrics for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - metric_type: Filter by metric type (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with infrastructure metrics or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        metric_type = request.args.get('metric_type')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = InfrastructureMetric.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(InfrastructureMetric.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if metric_type:
            query = query.filter(InfrastructureMetric.metric_type == metric_type)
            
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(InfrastructureMetric.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(InfrastructureMetric.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
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
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"infrastructure_metrics_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "metrics": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_infrastructure_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/facilities')
def get_infrastructure_facilities():
    """
    Get infrastructure facilities for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - facility_type: Filter by facility type (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with infrastructure facilities or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        facility_type = request.args.get('facility_type')
        export_format = request.args.get('format')
        
        # Build query
        query = InfrastructureFacility.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(InfrastructureFacility.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if facility_type:
            query = query.filter(InfrastructureFacility.facility_type == facility_type)
            
        # Execute query
        facilities = query.all()
        
        # Format results
        result = []
        for facility in facilities:
            result.append({
                "id": facility.id,
                "region_id": facility.region_id,
                "region_name": facility.region.name,
                "region_code": facility.region.code,
                "name": facility.name,
                "facility_type": facility.facility_type,
                "latitude": facility.latitude,
                "longitude": facility.longitude,
                "status": facility.status,
                "capacity": facility.capacity,
                "capacity_unit": facility.capacity_unit,
                "year_established": facility.year_established,
                "last_renovated": facility.last_renovated
            })
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"infrastructure_facilities_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "facilities": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_infrastructure_facilities: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/sdg')
def get_sdg_metrics():
    """
    Get SDG (Sustainable Development Goals) metrics for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - sdg_number: Filter by SDG number (1-17) (optional)
    - target_number: Filter by target number (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with SDG metrics or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        sdg_number = request.args.get('sdg_number')
        target_number = request.args.get('target_number')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = SDGMetric.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(SDGMetric.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if sdg_number:
            try:
                sdg_number = int(sdg_number)
                if sdg_number < 1 or sdg_number > 17:
                    return jsonify({
                        "status": "error",
                        "message": "SDG number must be between 1 and 17"
                    }), 400
                query = query.filter(SDGMetric.sdg_number == sdg_number)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid sdg_number parameter"
                }), 400
                
        if target_number:
            query = query.filter(SDGMetric.target_number == target_number)
            
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(SDGMetric.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(SDGMetric.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
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
                "sdg_number": metric.sdg_number,
                "target_number": metric.target_number,
                "indicator_code": metric.indicator_code,
                "value": metric.value,
                "unit": metric.unit,
                "date": metric.date.isoformat(),
                "confidence": metric.confidence
            })
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"sdg_metrics_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "metrics": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_sdg_metrics: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@esg_bp.route('/scores')
def get_esg_scores():
    """
    Get ESG composite scores for regions
    
    Query parameters:
    - region_id: Filter by region ID (optional)
    - start_date: Start date in YYYY-MM-DD format (optional)
    - end_date: End date in YYYY-MM-DD format (optional)
    - format: Export format (json, csv) - if provided, triggers a file download
    
    Returns:
        JSON response with ESG scores or a file download
    """
    try:
        # Get query parameters
        region_id = request.args.get('region_id')
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        export_format = request.args.get('format')
        
        # Build query
        query = ESGCompositeScore.query.join(Region)
        
        if region_id:
            try:
                region_id = int(region_id)
                query = query.filter(ESGCompositeScore.region_id == region_id)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid region_id parameter"
                }), 400
                
        if start_date:
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
                query = query.filter(ESGCompositeScore.date >= start_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid start_date format. Use YYYY-MM-DD"
                }), 400
                
        if end_date:
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
                query = query.filter(ESGCompositeScore.date <= end_date)
            except ValueError:
                return jsonify({
                    "status": "error",
                    "message": "Invalid end_date format. Use YYYY-MM-DD"
                }), 400
                
        # Execute query
        scores = query.all()
        
        # Format results
        result = []
        for score in scores:
            result.append({
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
            })
            
        # Export if requested
        if export_format in ['json', 'csv']:
            # Setup output path
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"esg_scores_{timestamp}.{export_format}"
            
            # Convert to DataFrame
            df = pd.DataFrame(result)
            
            # Save to file
            export_dir = Path("data/exports")
            export_dir.mkdir(parents=True, exist_ok=True)
            
            file_path = export_dir / filename
            
            if export_format == 'json':
                df.to_json(file_path, orient='records')
                mime_type = 'application/json'
            else:  # csv
                df.to_csv(file_path, index=False)
                mime_type = 'text/csv'
                
            # Return file for download
            return send_file(
                file_path,
                as_attachment=True,
                download_name=filename,
                mimetype=mime_type
            )
            
        # Return JSON response
        return jsonify({
            "status": "success",
            "count": len(result),
            "scores": result
        })
        
    except Exception as e:
        logger.exception(f"Error in get_esg_scores: {str(e)}")
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500