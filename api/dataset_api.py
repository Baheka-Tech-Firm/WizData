"""
Dataset Registry and Licensing API Endpoints
Provides RESTful APIs for B2B dataset management, licensing, and access control
"""

from flask import Blueprint, request, jsonify, current_app
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from datetime import datetime, timedelta
from decimal import Decimal
import json
from typing import Dict, List, Optional, Any

from models import (
    db, Dataset, DatasetLicense, Subscription, User, UsageLog, 
    DataQualityMetric, WebhookEndpoint, LicenseType, DatasetCategory,
    SubscriptionTier
)
from middleware.auth_middleware import token_required, api_key_required
from services.licensing_service import LicensingService
from services.usage_tracker import UsageTracker

# Create blueprint
dataset_api = Blueprint('dataset_api', __name__, url_prefix='/api/v1/datasets')

# Initialize services
licensing_service = LicensingService()
usage_tracker = UsageTracker()

# Rate limiting
limiter = Limiter(
    app=current_app,
    key_func=get_remote_address,
    default_limits=["200 per day", "50 per hour"]
)

@dataset_api.route('/', methods=['GET'])
@limiter.limit("100 per hour")
def list_datasets():
    """
    List available datasets with filtering and pagination
    """
    try:
        # Query parameters
        category = request.args.get('category')
        provider = request.args.get('provider')
        min_quality_score = request.args.get('min_quality_score', type=float)
        search = request.args.get('search')
        page = request.args.get('page', 1, type=int)
        per_page = min(request.args.get('per_page', 20, type=int), 100)
        
        # Build query
        query = Dataset.query.filter(Dataset.is_public == True, Dataset.is_active == True)
        
        if category:
            try:
                category_enum = DatasetCategory(category)
                query = query.filter(Dataset.category == category_enum)
            except ValueError:
                return jsonify({'error': 'Invalid category'}), 400
        
        if provider:
            query = query.filter(Dataset.provider.ilike(f'%{provider}%'))
        
        if min_quality_score:
            query = query.filter(Dataset.data_quality_score >= min_quality_score)
        
        if search:
            search_term = f'%{search}%'
            query = query.filter(
                db.or_(
                    Dataset.name.ilike(search_term),
                    Dataset.description.ilike(search_term)
                )
            )
        
        # Apply pagination
        datasets = query.paginate(
            page=page, 
            per_page=per_page, 
            error_out=False
        )
        
        # Format response
        result = {
            'datasets': [],
            'pagination': {
                'page': page,
                'per_page': per_page,
                'total': datasets.total,
                'pages': datasets.pages,
                'has_next': datasets.has_next,
                'has_prev': datasets.has_prev
            }
        }
        
        for dataset in datasets.items:
            dataset_data = {
                'id': dataset.id,
                'name': dataset.name,
                'slug': dataset.slug,
                'description': dataset.description,
                'category': dataset.category.value,
                'provider': dataset.provider,
                'version': dataset.version,
                'update_frequency': dataset.update_frequency,
                'historical_depth': dataset.historical_depth,
                'geographic_coverage': dataset.geographic_coverage,
                'data_formats': dataset.data_formats,
                'data_quality_score': dataset.data_quality_score,
                'total_records': dataset.total_records,
                'last_updated': dataset.last_updated.isoformat() if dataset.last_updated else None,
                'documentation_url': dataset.documentation_url,
                'sample_data_url': dataset.sample_data_url,
                'licenses': [
                    {
                        'id': license.id,
                        'type': license.license_type.value,
                        'name': license.name,
                        'monthly_price': float(license.monthly_price or 0),
                        'annual_price': float(license.annual_price or 0),
                        'rate_limit_per_minute': license.rate_limit_per_minute,
                        'real_time_access': license.real_time_access,
                        'api_access': license.api_access
                    }
                    for license in dataset.licenses.filter(DatasetLicense.is_active == True)
                ]
            }
            result['datasets'].append(dataset_data)
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error listing datasets: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/<slug>', methods=['GET'])
@limiter.limit("200 per hour")
def get_dataset(slug: str):
    """
    Get detailed information about a specific dataset
    """
    try:
        dataset = Dataset.query.filter(
            Dataset.slug == slug,
            Dataset.is_active == True
        ).first_or_404()
        
        # Get latest quality metrics
        latest_quality = DataQualityMetric.query.filter(
            DataQualityMetric.dataset_id == dataset.id
        ).order_by(DataQualityMetric.measurement_date.desc()).first()
        
        # Format response
        result = {
            'id': dataset.id,
            'name': dataset.name,
            'slug': dataset.slug,
            'description': dataset.description,
            'category': dataset.category.value,
            'provider': dataset.provider,
            'version': dataset.version,
            'schema_definition': dataset.schema_definition,
            'update_frequency': dataset.update_frequency,
            'historical_depth': dataset.historical_depth,
            'geographic_coverage': dataset.geographic_coverage,
            'data_formats': dataset.data_formats,
            'api_endpoints': dataset.api_endpoints,
            'data_quality_score': dataset.data_quality_score,
            'completeness_score': dataset.completeness_score,
            'accuracy_score': dataset.accuracy_score,
            'latency_score': dataset.latency_score,
            'total_records': dataset.total_records,
            'last_updated': dataset.last_updated.isoformat() if dataset.last_updated else None,
            'documentation_url': dataset.documentation_url,
            'sample_data_url': dataset.sample_data_url,
            'created_at': dataset.created_at.isoformat(),
            'licenses': []
        }
        
        # Add license information
        for license in dataset.licenses.filter(DatasetLicense.is_active == True):
            license_data = {
                'id': license.id,
                'type': license.license_type.value,
                'name': license.name,
                'description': license.description,
                'monthly_price': float(license.monthly_price or 0),
                'annual_price': float(license.annual_price or 0),
                'price_per_record': float(license.price_per_record or 0),
                'price_per_api_call': float(license.price_per_api_call or 0),
                'rate_limit_per_minute': license.rate_limit_per_minute,
                'rate_limit_per_day': license.rate_limit_per_day,
                'rate_limit_per_month': license.rate_limit_per_month,
                'max_records_per_request': license.max_records_per_request,
                'historical_access_days': license.historical_access_days,
                'real_time_access': license.real_time_access,
                'bulk_download': license.bulk_download,
                'api_access': license.api_access,
                'webhook_support': license.webhook_support,
                'custom_queries': license.custom_queries,
                'white_label_access': license.white_label_access,
                'redistribution_allowed': license.redistribution_allowed,
                'commercial_use_allowed': license.commercial_use_allowed,
                'attribution_required': license.attribution_required,
                'terms_of_use': license.terms_of_use
            }
            result['licenses'].append(license_data)
        
        # Add quality metrics if available
        if latest_quality:
            result['quality_metrics'] = {
                'measurement_date': latest_quality.measurement_date.isoformat(),
                'completeness_score': latest_quality.completeness_score,
                'accuracy_score': latest_quality.accuracy_score,
                'consistency_score': latest_quality.consistency_score,
                'timeliness_score': latest_quality.timeliness_score,
                'validity_score': latest_quality.validity_score,
                'uniqueness_score': latest_quality.uniqueness_score,
                'overall_score': latest_quality.overall_score,
                'total_records': latest_quality.total_records,
                'null_count': latest_quality.null_count,
                'duplicate_count': latest_quality.duplicate_count,
                'invalid_format_count': latest_quality.invalid_format_count
            }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting dataset {slug}: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/<slug>/subscribe', methods=['POST'])
@token_required
def subscribe_to_dataset(current_user, slug: str):
    """
    Subscribe a user to a dataset with a specific license
    """
    try:
        data = request.get_json()
        license_id = data.get('license_id')
        subscription_type = data.get('subscription_type', 'monthly')
        
        if not license_id:
            return jsonify({'error': 'License ID is required'}), 400
        
        # Get dataset and license
        dataset = Dataset.query.filter(
            Dataset.slug == slug,
            Dataset.is_active == True
        ).first_or_404()
        
        license = DatasetLicense.query.filter(
            DatasetLicense.id == license_id,
            DatasetLicense.dataset_id == dataset.id,
            DatasetLicense.is_active == True
        ).first_or_404()
        
        # Check if user already has an active subscription
        existing_subscription = Subscription.query.filter(
            Subscription.user_id == current_user.id,
            Subscription.dataset_id == dataset.id,
            Subscription.status == 'active'
        ).first()
        
        if existing_subscription:
            return jsonify({'error': 'User already has an active subscription to this dataset'}), 400
        
        # Create subscription using licensing service
        subscription = licensing_service.create_subscription(
            user_id=current_user.id,
            dataset_id=dataset.id,
            license_id=license_id,
            subscription_type=subscription_type
        )
        
        return jsonify({
            'message': 'Successfully subscribed to dataset',
            'subscription': {
                'id': subscription.id,
                'dataset': dataset.name,
                'license': license.name,
                'subscription_type': subscription.subscription_type,
                'status': subscription.status,
                'start_date': subscription.start_date.isoformat(),
                'end_date': subscription.end_date.isoformat() if subscription.end_date else None,
                'monthly_price': float(subscription.monthly_price or 0),
                'annual_price': float(subscription.annual_price or 0)
            }
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Error subscribing to dataset {slug}: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/subscriptions', methods=['GET'])
@token_required
def get_user_subscriptions(current_user):
    """
    Get all subscriptions for the current user
    """
    try:
        subscriptions = Subscription.query.filter(
            Subscription.user_id == current_user.id
        ).all()
        
        result = []
        for subscription in subscriptions:
            subscription_data = {
                'id': subscription.id,
                'dataset': {
                    'id': subscription.dataset.id,
                    'name': subscription.dataset.name,
                    'slug': subscription.dataset.slug,
                    'category': subscription.dataset.category.value
                },
                'license': {
                    'id': subscription.license.id,
                    'name': subscription.license.name,
                    'type': subscription.license.license_type.value
                },
                'subscription_type': subscription.subscription_type,
                'status': subscription.status,
                'start_date': subscription.start_date.isoformat(),
                'end_date': subscription.end_date.isoformat() if subscription.end_date else None,
                'monthly_price': float(subscription.monthly_price or 0),
                'annual_price': float(subscription.annual_price or 0),
                'current_usage_cost': float(subscription.current_usage_cost),
                'current_month_api_calls': subscription.current_month_api_calls,
                'current_month_records_accessed': subscription.current_month_records_accessed,
                'auto_renew': subscription.auto_renew
            }
            result.append(subscription_data)
        
        return jsonify({'subscriptions': result}), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting user subscriptions: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/subscriptions/<int:subscription_id>/cancel', methods=['POST'])
@token_required
def cancel_subscription(current_user, subscription_id: int):
    """
    Cancel a subscription
    """
    try:
        subscription = Subscription.query.filter(
            Subscription.id == subscription_id,
            Subscription.user_id == current_user.id,
            Subscription.status == 'active'
        ).first_or_404()
        
        # Update subscription status
        subscription.status = 'cancelled'
        subscription.auto_renew = False
        subscription.updated_at = datetime.utcnow()
        
        db.session.commit()
        
        return jsonify({
            'message': 'Subscription cancelled successfully',
            'subscription_id': subscription_id
        }), 200
        
    except Exception as e:
        current_app.logger.error(f"Error cancelling subscription {subscription_id}: {str(e)}")
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/usage', methods=['GET'])
@token_required
def get_usage_analytics(current_user):
    """
    Get usage analytics for the current user
    """
    try:
        # Query parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        dataset_id = request.args.get('dataset_id', type=int)
        
        # Default to current month if no dates provided
        if not start_date or not end_date:
            now = datetime.utcnow()
            start_date = now.replace(day=1).isoformat()
            end_date = now.isoformat()
        
        # Build query
        query = UsageLog.query.filter(
            UsageLog.user_id == current_user.id,
            UsageLog.timestamp >= start_date,
            UsageLog.timestamp <= end_date
        )
        
        if dataset_id:
            query = query.filter(UsageLog.dataset_id == dataset_id)
        
        usage_logs = query.all()
        
        # Aggregate usage data
        total_api_calls = len(usage_logs)
        total_records = sum(log.records_returned for log in usage_logs)
        total_cost = sum(log.cost_amount for log in usage_logs)
        
        # Group by dataset
        dataset_usage = {}
        for log in usage_logs:
            dataset_id = log.dataset_id
            if dataset_id not in dataset_usage:
                dataset_usage[dataset_id] = {
                    'dataset': {
                        'id': log.dataset.id,
                        'name': log.dataset.name,
                        'slug': log.dataset.slug
                    },
                    'api_calls': 0,
                    'records_accessed': 0,
                    'cost': Decimal('0.00')
                }
            
            dataset_usage[dataset_id]['api_calls'] += 1
            dataset_usage[dataset_id]['records_accessed'] += log.records_returned
            dataset_usage[dataset_id]['cost'] += log.cost_amount
        
        # Convert to list and format decimals
        dataset_usage_list = []
        for usage in dataset_usage.values():
            usage['cost'] = float(usage['cost'])
            dataset_usage_list.append(usage)
        
        result = {
            'period': {
                'start_date': start_date,
                'end_date': end_date
            },
            'summary': {
                'total_api_calls': total_api_calls,
                'total_records_accessed': total_records,
                'total_cost': float(total_cost)
            },
            'dataset_usage': dataset_usage_list
        }
        
        return jsonify(result), 200
        
    except Exception as e:
        current_app.logger.error(f"Error getting usage analytics: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/webhooks', methods=['GET'])
@token_required
def list_webhooks(current_user):
    """
    List webhook endpoints for the current user
    """
    try:
        webhooks = WebhookEndpoint.query.filter(
            WebhookEndpoint.user_id == current_user.id
        ).all()
        
        result = []
        for webhook in webhooks:
            webhook_data = {
                'id': webhook.id,
                'dataset': {
                    'id': webhook.dataset.id,
                    'name': webhook.dataset.name,
                    'slug': webhook.dataset.slug
                },
                'url': webhook.url,
                'is_active': webhook.is_active,
                'event_types': webhook.event_types,
                'filters': webhook.filters,
                'last_delivery_attempt': webhook.last_delivery_attempt.isoformat() if webhook.last_delivery_attempt else None,
                'last_successful_delivery': webhook.last_successful_delivery.isoformat() if webhook.last_successful_delivery else None,
                'consecutive_failures': webhook.consecutive_failures,
                'total_deliveries': webhook.total_deliveries,
                'total_failures': webhook.total_failures,
                'created_at': webhook.created_at.isoformat()
            }
            result.append(webhook_data)
        
        return jsonify({'webhooks': result}), 200
        
    except Exception as e:
        current_app.logger.error(f"Error listing webhooks: {str(e)}")
        return jsonify({'error': 'Internal server error'}), 500

@dataset_api.route('/webhooks', methods=['POST'])
@token_required
def create_webhook(current_user):
    """
    Create a new webhook endpoint
    """
    try:
        data = request.get_json()
        
        required_fields = ['dataset_id', 'url']
        if not all(field in data for field in required_fields):
            return jsonify({'error': 'Missing required fields'}), 400
        
        # Verify user has access to the dataset
        subscription = Subscription.query.filter(
            Subscription.user_id == current_user.id,
            Subscription.dataset_id == data['dataset_id'],
            Subscription.status == 'active'
        ).first()
        
        if not subscription:
            return jsonify({'error': 'No active subscription to this dataset'}), 403
        
        # Check if license supports webhooks
        if not subscription.license.webhook_support:
            return jsonify({'error': 'Current license does not support webhooks'}), 403
        
        # Create webhook
        webhook = WebhookEndpoint(
            user_id=current_user.id,
            dataset_id=data['dataset_id'],
            url=data['url'],
            secret_key=data.get('secret_key'),
            event_types=data.get('event_types'),
            filters=data.get('filters'),
            retry_count=data.get('retry_count', 3),
            timeout_seconds=data.get('timeout_seconds', 30)
        )
        
        db.session.add(webhook)
        db.session.commit()
        
        return jsonify({
            'message': 'Webhook created successfully',
            'webhook': {
                'id': webhook.id,
                'url': webhook.url,
                'is_active': webhook.is_active,
                'created_at': webhook.created_at.isoformat()
            }
        }), 201
        
    except Exception as e:
        current_app.logger.error(f"Error creating webhook: {str(e)}")
        db.session.rollback()
        return jsonify({'error': 'Internal server error'}), 500

# Error handlers
@dataset_api.errorhandler(404)
def not_found(error):
    return jsonify({'error': 'Resource not found'}), 404

@dataset_api.errorhandler(429)
def rate_limit_exceeded(error):
    return jsonify({'error': 'Rate limit exceeded', 'retry_after': error.retry_after}), 429
