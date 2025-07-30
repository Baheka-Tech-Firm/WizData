"""
Secure Data Access API Endpoints
Provides license-controlled access to datasets with rate limiting and usage tracking
"""

from flask import Blueprint, request, jsonify, current_app, g
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import json

from models import (
    db, Dataset, Symbol, Region, EnvironmentalMetric, SocialMetric, 
    GovernanceMetric, InfrastructureMetric, SDGMetric, ESGCompositeScore,
    DatasetCategory
)
from middleware.auth_middleware import token_required, api_key_required
from middleware.license_middleware import (
    dataset_access_required, license_feature_required, 
    subscription_tier_required
)
from services.licensing_service import LicensingService
from services.usage_tracker import UsageTracker

# Create blueprint
data_access_api = Blueprint('data_access', __name__, url_prefix='/api/v1/data')

# Initialize services
licensing_service = LicensingService()
usage_tracker = UsageTracker()

@data_access_api.route('/market/<dataset_slug>/symbols', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
def get_market_symbols(current_user, dataset_slug: str):
    """
    Get available market symbols for a dataset
    """
    try:
        dataset = g.dataset
        
        # Query parameters
        exchange = request.args.get('exchange')
        sector = request.args.get('sector')
        country = request.args.get('country')
        asset_type = request.args.get('asset_type')
        search = request.args.get('search')
        limit = min(request.args.get('limit', 100, type=int), g.requested_records)
        offset = request.args.get('offset', 0, type=int)
        
        # Build query
        query = Symbol.query.filter(Symbol.is_active == True)
        
        if exchange:
            query = query.filter(Symbol.exchange.ilike(f'%{exchange}%'))
        
        if sector:
            query = query.filter(Symbol.sector.ilike(f'%{sector}%'))
        
        if country:
            query = query.filter(Symbol.country.ilike(f'%{country}%'))
        
        if asset_type:
            query = query.filter(Symbol.asset_type == asset_type)
        
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                db.or_(
                    Symbol.symbol.ilike(search_term),
                    Symbol.name.ilike(search_term)
                )
            )
        
        # Apply pagination
        symbols = query.offset(offset).limit(limit).all()
        total_count = query.count()
        
        # Format response
        result = {
            'data': [
                {
                    'symbol': symbol.symbol,
                    'name': symbol.name,
                    'exchange': symbol.exchange,
                    'asset_type': symbol.asset_type,
                    'sector': symbol.sector,
                    'country': symbol.country,
                    'last_price': symbol.last_price,
                    'last_updated': symbol.last_updated.isoformat() if symbol.last_updated else None
                }
                for symbol in symbols
            ],
            'pagination': {
                'offset': offset,
                'limit': limit,
                'total': total_count,
                'count': len(symbols)
            },
            'metadata': {
                'dataset': dataset.name,
                'category': dataset.category.value,
                'last_updated': dataset.last_updated.isoformat() if dataset.last_updated else None
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting market symbols: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/market/<dataset_slug>/historical', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
def get_historical_data(current_user, dataset_slug: str):
    """
    Get historical market data (placeholder - would integrate with actual market data)
    """
    try:
        dataset = g.dataset
        
        # Query parameters
        symbol = request.args.get('symbol', required=True)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        interval = request.args.get('interval', 'daily')  # daily, hourly, minute
        limit = min(request.args.get('limit', 100, type=int), g.requested_records)
        
        if not symbol:
            return jsonify({'error': 'Symbol parameter is required'}), 400
        
        # Validate symbol exists
        symbol_obj = Symbol.query.filter(Symbol.symbol == symbol).first()
        if not symbol_obj:
            return jsonify({'error': 'Symbol not found'}), 404
        
        # Parse dates
        try:
            if start_date:
                start_date = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            else:
                start_date = datetime.utcnow() - timedelta(days=30)
            
            if end_date:
                end_date = datetime.fromisoformat(end_date.replace('Z', '+00:00'))
            else:
                end_date = datetime.utcnow()
        except ValueError:
            return jsonify({'error': 'Invalid date format. Use ISO format (YYYY-MM-DD)'}), 400
        
        # Check historical access limits
        license_info = g.license_info
        if 'historical_access_days' in license_info and license_info['historical_access_days']:
            max_historical_date = datetime.utcnow() - timedelta(days=license_info['historical_access_days'])
            if start_date < max_historical_date:
                return jsonify({
                    'error': f'Historical data access limited to {license_info["historical_access_days"]} days',
                    'error_code': 'HISTORICAL_ACCESS_LIMITED'
                }), 403
        
        # TODO: Integrate with actual market data provider (Alpha Vantage, Yahoo Finance, etc.)
        # For now, return sample data structure
        sample_data = []
        current_date = start_date
        
        while current_date <= end_date and len(sample_data) < limit:
            sample_data.append({
                'date': current_date.isoformat(),
                'open': 100.0 + (len(sample_data) * 0.5),
                'high': 105.0 + (len(sample_data) * 0.5),
                'low': 95.0 + (len(sample_data) * 0.5),
                'close': 102.0 + (len(sample_data) * 0.5),
                'volume': 1000000 + (len(sample_data) * 1000),
                'symbol': symbol
            })
            
            if interval == 'daily':
                current_date += timedelta(days=1)
            elif interval == 'hourly':
                current_date += timedelta(hours=1)
            else:  # minute
                current_date += timedelta(minutes=1)
        
        result = {
            'data': sample_data,
            'metadata': {
                'symbol': symbol,
                'interval': interval,
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat(),
                'dataset': dataset.name,
                'records_returned': len(sample_data)
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting historical data: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/market/<dataset_slug>/realtime', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
@license_feature_required('real_time_access')
def get_realtime_data(current_user, dataset_slug: str):
    """
    Get real-time market data (requires real-time access license feature)
    """
    try:
        dataset = g.dataset
        
        symbols = request.args.getlist('symbols')  # Can request multiple symbols
        if not symbols:
            return jsonify({'error': 'At least one symbol is required'}), 400
        
        # Limit number of symbols based on license
        max_symbols = min(len(symbols), g.requested_records)
        symbols = symbols[:max_symbols]
        
        # TODO: Integrate with real-time data provider
        # For now, return sample real-time data
        realtime_data = []
        
        for symbol in symbols:
            symbol_obj = Symbol.query.filter(Symbol.symbol == symbol).first()
            if symbol_obj:
                realtime_data.append({
                    'symbol': symbol,
                    'price': symbol_obj.last_price or 100.0,
                    'change': 1.5,
                    'change_percent': 1.5,
                    'volume': 50000,
                    'bid': 99.8,
                    'ask': 100.2,
                    'timestamp': datetime.utcnow().isoformat()
                })
        
        result = {
            'data': realtime_data,
            'metadata': {
                'dataset': dataset.name,
                'timestamp': datetime.utcnow().isoformat(),
                'symbols_requested': len(symbols),
                'symbols_returned': len(realtime_data)
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting real-time data: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/esg/<dataset_slug>/regions', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
def get_esg_regions(current_user, dataset_slug: str):
    """
    Get available regions for ESG data
    """
    try:
        dataset = g.dataset
        
        # Query parameters
        country = request.args.get('country')
        region_type = request.args.get('region_type')  # country, province, municipality
        search = request.args.get('search')
        limit = min(request.args.get('limit', 100, type=int), g.requested_records)
        offset = request.args.get('offset', 0, type=int)
        
        # Build query
        query = Region.query
        
        if country:
            query = query.filter(Region.country.ilike(f'%{country}%'))
        
        if region_type:
            query = query.filter(Region.region_type == region_type)
        
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                db.or_(
                    Region.name.ilike(search_term),
                    Region.code.ilike(search_term)
                )
            )
        
        # Apply pagination
        regions = query.offset(offset).limit(limit).all()
        total_count = query.count()
        
        # Format response
        result = {
            'data': [
                {
                    'id': region.id,
                    'name': region.name,
                    'code': region.code,
                    'region_type': region.region_type,
                    'country': region.country,
                    'population': region.population,
                    'area_km2': region.area_km2,
                    'parent_id': region.parent_id
                }
                for region in regions
            ],
            'pagination': {
                'offset': offset,
                'limit': limit,
                'total': total_count,
                'count': len(regions)
            },
            'metadata': {
                'dataset': dataset.name,
                'category': dataset.category.value
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting ESG regions: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/esg/<dataset_slug>/metrics', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
def get_esg_metrics(current_user, dataset_slug: str):
    """
    Get ESG metrics for specified regions and time periods
    """
    try:
        dataset = g.dataset
        
        # Query parameters
        region_id = request.args.get('region_id', type=int)
        metric_type = request.args.get('metric_type')
        category = request.args.get('category')  # environmental, social, governance, infrastructure
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = min(request.args.get('limit', 100, type=int), g.requested_records)
        offset = request.args.get('offset', 0, type=int)
        
        # Parse dates
        if start_date:
            try:
                start_date = datetime.fromisoformat(start_date).date()
            except ValueError:
                return jsonify({'error': 'Invalid start_date format. Use YYYY-MM-DD'}), 400
        
        if end_date:
            try:
                end_date = datetime.fromisoformat(end_date).date()
            except ValueError:
                return jsonify({'error': 'Invalid end_date format. Use YYYY-MM-DD'}), 400
        
        # Determine which metrics table to query based on category
        metrics_data = []
        
        if not category or category == 'environmental':
            env_query = EnvironmentalMetric.query
            if region_id:
                env_query = env_query.filter(EnvironmentalMetric.region_id == region_id)
            if metric_type:
                env_query = env_query.filter(EnvironmentalMetric.metric_type == metric_type)
            if start_date:
                env_query = env_query.filter(EnvironmentalMetric.date >= start_date)
            if end_date:
                env_query = env_query.filter(EnvironmentalMetric.date <= end_date)
            
            env_metrics = env_query.offset(offset).limit(limit).all()
            
            for metric in env_metrics:
                metrics_data.append({
                    'category': 'environmental',
                    'region_id': metric.region_id,
                    'region_name': metric.region.name,
                    'metric_type': metric.metric_type,
                    'value': metric.value,
                    'unit': metric.unit,
                    'date': metric.date.isoformat(),
                    'confidence': metric.confidence,
                    'source': metric.source.name if metric.source else None
                })
        
        if not category or category == 'social':
            social_query = SocialMetric.query
            if region_id:
                social_query = social_query.filter(SocialMetric.region_id == region_id)
            if metric_type:
                social_query = social_query.filter(SocialMetric.metric_type == metric_type)
            if start_date:
                social_query = social_query.filter(SocialMetric.date >= start_date)
            if end_date:
                social_query = social_query.filter(SocialMetric.date <= end_date)
            
            social_metrics = social_query.offset(offset).limit(limit).all()
            
            for metric in social_metrics:
                metrics_data.append({
                    'category': 'social',
                    'region_id': metric.region_id,
                    'region_name': metric.region.name,
                    'metric_type': metric.metric_type,
                    'value': metric.value,
                    'unit': metric.unit,
                    'date': metric.date.isoformat(),
                    'confidence': metric.confidence,
                    'source': metric.source.name if metric.source else None
                })
        
        if not category or category == 'governance':
            gov_query = GovernanceMetric.query
            if region_id:
                gov_query = gov_query.filter(GovernanceMetric.region_id == region_id)
            if metric_type:
                gov_query = gov_query.filter(GovernanceMetric.metric_type == metric_type)
            if start_date:
                gov_query = gov_query.filter(GovernanceMetric.date >= start_date)
            if end_date:
                gov_query = gov_query.filter(GovernanceMetric.date <= end_date)
            
            gov_metrics = gov_query.offset(offset).limit(limit).all()
            
            for metric in gov_metrics:
                metrics_data.append({
                    'category': 'governance',
                    'region_id': metric.region_id,
                    'region_name': metric.region.name,
                    'metric_type': metric.metric_type,
                    'value': metric.value,
                    'status': metric.status,
                    'unit': metric.unit,
                    'date': metric.date.isoformat(),
                    'confidence': metric.confidence,
                    'source': metric.source.name if metric.source else None
                })
        
        # Limit total results
        metrics_data = metrics_data[:limit]
        
        result = {
            'data': metrics_data,
            'pagination': {
                'offset': offset,
                'limit': limit,
                'count': len(metrics_data)
            },
            'metadata': {
                'dataset': dataset.name,
                'category': dataset.category.value,
                'filters': {
                    'region_id': region_id,
                    'metric_type': metric_type,
                    'category': category,
                    'start_date': start_date.isoformat() if start_date else None,
                    'end_date': end_date.isoformat() if end_date else None
                }
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting ESG metrics: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/esg/<dataset_slug>/composite-scores', methods=['GET'])
@token_required
@dataset_access_required('dataset_slug')
def get_esg_composite_scores(current_user, dataset_slug: str):
    """
    Get ESG composite scores for regions
    """
    try:
        dataset = g.dataset
        
        # Query parameters
        region_id = request.args.get('region_id', type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = min(request.args.get('limit', 100, type=int), g.requested_records)
        offset = request.args.get('offset', 0, type=int)
        
        # Build query
        query = ESGCompositeScore.query
        
        if region_id:
            query = query.filter(ESGCompositeScore.region_id == region_id)
        
        if start_date:
            try:
                start_date_obj = datetime.fromisoformat(start_date).date()
                query = query.filter(ESGCompositeScore.date >= start_date_obj)
            except ValueError:
                return jsonify({'error': 'Invalid start_date format. Use YYYY-MM-DD'}), 400
        
        if end_date:
            try:
                end_date_obj = datetime.fromisoformat(end_date).date()
                query = query.filter(ESGCompositeScore.date <= end_date_obj)
            except ValueError:
                return jsonify({'error': 'Invalid end_date format. Use YYYY-MM-DD'}), 400
        
        # Apply pagination and ordering
        scores = query.order_by(ESGCompositeScore.date.desc()).offset(offset).limit(limit).all()
        total_count = query.count()
        
        # Format response
        result = {
            'data': [
                {
                    'region_id': score.region_id,
                    'region_name': score.region.name,
                    'environmental_score': score.environmental_score,
                    'social_score': score.social_score,
                    'governance_score': score.governance_score,
                    'infrastructure_score': score.infrastructure_score,
                    'overall_score': score.overall_score,
                    'date': score.date.isoformat(),
                    'methodology': score.methodology,
                    'confidence': score.confidence,
                    'components': score.components
                }
                for score in scores
            ],
            'pagination': {
                'offset': offset,
                'limit': limit,
                'total': total_count,
                'count': len(scores)
            },
            'metadata': {
                'dataset': dataset.name,
                'category': dataset.category.value
            }
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting ESG composite scores: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@data_access_api.route('/bulk/<dataset_slug>/export', methods=['POST'])
@token_required
@dataset_access_required('dataset_slug')
@license_feature_required('bulk_download')
def request_bulk_export(current_user, dataset_slug: str):
    """
    Request bulk data export (requires bulk_download license feature)
    """
    try:
        dataset = g.dataset
        
        data = request.get_json()
        export_format = data.get('format', 'json')  # json, csv, parquet
        filters = data.get('filters', {})
        
        if export_format not in ['json', 'csv', 'parquet']:
            return jsonify({'error': 'Invalid export format. Supported: json, csv, parquet'}), 400
        
        # TODO: Implement async bulk export job
        # For now, return job information
        export_job_id = f"export_{dataset.id}_{current_user.id}_{int(datetime.utcnow().timestamp())}"
        
        result = {
            'export_job_id': export_job_id,
            'status': 'queued',
            'dataset': dataset.name,
            'format': export_format,
            'estimated_completion': (datetime.utcnow() + timedelta(minutes=30)).isoformat(),
            'filters': filters,
            'message': 'Export job has been queued. You will receive an email when the export is ready.'
        }
        
        return jsonify(result), 202
        
    except Exception as e:
        current_app.logger.error(f"Error requesting bulk export: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

# Error handlers
@data_access_api.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Data not found'}), 404

@data_access_api.errorhandler(429)
def rate_limit_exceeded(error):
    return jsonify({
        'error': 'Rate limit exceeded',
        'error_code': 'RATE_LIMIT_EXCEEDED',
        'retry_after': getattr(error, 'retry_after', 60)
    }), 429
