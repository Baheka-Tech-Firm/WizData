"""
Usage Tracker Service for API Call Monitoring and Billing
Tracks dataset API usage, calculates costs, and provides analytics
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Any
import logging
from collections import defaultdict

from models import (
    db, User, Dataset, Subscription, UsageLog, DatasetLicense,
    APIKey, APIRequest
)

logger = logging.getLogger(__name__)

class UsageTracker:
    """Service for tracking and analyzing API usage"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def record_usage(
        self,
        user_id: int,
        dataset_id: int,
        endpoint: str,
        method: str,
        records_returned: int,
        response_size_bytes: int,
        response_time_ms: int,
        status_code: int,
        subscription_id: Optional[int] = None,
        api_key_id: Optional[int] = None,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
        query_parameters: Optional[Dict] = None
    ) -> Optional[UsageLog]:
        """
        Record a single API usage event
        
        Args:
            user_id: ID of the user making the request
            dataset_id: ID of the dataset being accessed
            endpoint: API endpoint called
            method: HTTP method (GET, POST, etc.)
            records_returned: Number of records returned
            response_size_bytes: Size of response in bytes
            response_time_ms: Response time in milliseconds
            status_code: HTTP status code
            subscription_id: ID of the subscription used (optional)
            api_key_id: ID of the API key used (optional)
            ip_address: IP address of requester (optional)
            user_agent: User agent string (optional)
            query_parameters: Query parameters as dict (optional)
            
        Returns:
            Created UsageLog object or None if error
        """
        try:
            # Get subscription if not provided
            if not subscription_id:
                subscription = Subscription.query.filter(
                    Subscription.user_id == user_id,
                    Subscription.dataset_id == dataset_id,
                    Subscription.status == 'active'
                ).first()
                subscription_id = subscription.id if subscription else None
            
            # Calculate cost
            cost_amount, cost_basis = self._calculate_usage_cost(
                subscription_id, records_returned
            )
            
            # Create usage log entry
            usage_log = UsageLog(
                user_id=user_id,
                dataset_id=dataset_id,
                subscription_id=subscription_id,
                api_key_id=api_key_id,
                endpoint=endpoint,
                method=method,
                records_returned=records_returned,
                response_size_bytes=response_size_bytes,
                response_time_ms=response_time_ms,
                status_code=status_code,
                cost_amount=cost_amount,
                cost_basis=cost_basis,
                ip_address=ip_address,
                user_agent=user_agent,
                query_parameters=query_parameters,
                timestamp=datetime.utcnow()
            )
            
            db.session.add(usage_log)
            
            # Update subscription usage counters
            if subscription_id:
                subscription = Subscription.query.get(subscription_id)
                if subscription:
                    subscription.current_month_api_calls += 1
                    subscription.current_month_records_accessed += records_returned
                    subscription.current_usage_cost += cost_amount
                    subscription.total_api_calls += 1
                    subscription.total_records_accessed += records_returned
                    subscription.updated_at = datetime.utcnow()
            
            # Update user's current monthly spend
            user = User.query.get(user_id)
            if user:
                user.current_monthly_spend += cost_amount
                user.last_active = datetime.utcnow()
            
            db.session.commit()
            
            self.logger.debug(f"Recorded usage: user={user_id}, dataset={dataset_id}, cost={cost_amount}")
            return usage_log
            
        except Exception as e:
            db.session.rollback()
            self.logger.error(f"Error recording usage: {str(e)}")
            return None
    
    def _calculate_usage_cost(
        self, 
        subscription_id: Optional[int], 
        records_returned: int
    ) -> tuple[Decimal, Optional[str]]:
        """Calculate the cost for a usage event"""
        if not subscription_id:
            return Decimal('0.00'), None
        
        try:
            subscription = Subscription.query.get(subscription_id)
            if not subscription or not subscription.license:
                return Decimal('0.00'), None
            
            license = subscription.license
            cost = Decimal('0.00')
            cost_basis = None
            
            # For subscription-based licenses, cost is typically 0 per request
            if subscription.subscription_type in ['monthly', 'annual']:
                if license.price_per_record and license.price_per_record > 0:
                    cost = license.price_per_record * records_returned
                    cost_basis = 'per_record'
                elif license.price_per_api_call and license.price_per_api_call > 0:
                    cost = license.price_per_api_call
                    cost_basis = 'per_api_call'
                else:
                    cost_basis = 'subscription'
            
            # For pay-per-use, always charge per record or per call
            elif subscription.subscription_type == 'pay-per-use':
                if license.price_per_record:
                    cost = license.price_per_record * records_returned
                    cost_basis = 'per_record'
                elif license.price_per_api_call:
                    cost = license.price_per_api_call
                    cost_basis = 'per_api_call'
            
            return cost, cost_basis
            
        except Exception as e:
            self.logger.error(f"Error calculating usage cost: {str(e)}")
            return Decimal('0.00'), None
    
    def get_user_usage_summary(
        self, 
        user_id: int, 
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get usage summary for a user over a specified period
        
        Args:
            user_id: ID of the user
            start_date: Start of period (defaults to current month start)
            end_date: End of period (defaults to now)
            
        Returns:
            Dictionary with usage summary
        """
        try:
            # Default to current month if no dates provided
            if not start_date:
                now = datetime.utcnow()
                start_date = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            
            if not end_date:
                end_date = datetime.utcnow()
            
            # Get usage logs for the period
            usage_logs = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.timestamp >= start_date,
                UsageLog.timestamp <= end_date
            ).all()
            
            # Aggregate data
            total_api_calls = len(usage_logs)
            total_records = sum(log.records_returned for log in usage_logs)
            total_cost = sum(log.cost_amount for log in usage_logs)
            total_response_time = sum(log.response_time_ms or 0 for log in usage_logs)
            avg_response_time = total_response_time / total_api_calls if total_api_calls > 0 else 0
            
            # Group by dataset
            dataset_usage = defaultdict(lambda: {
                'api_calls': 0,
                'records_accessed': 0,
                'cost': Decimal('0.00'),
                'avg_response_time': 0,
                'error_rate': 0
            })
            
            for log in usage_logs:
                dataset_id = log.dataset_id
                dataset_usage[dataset_id]['api_calls'] += 1
                dataset_usage[dataset_id]['records_accessed'] += log.records_returned
                dataset_usage[dataset_id]['cost'] += log.cost_amount
                
                if log.response_time_ms:
                    current_avg = dataset_usage[dataset_id]['avg_response_time']
                    call_count = dataset_usage[dataset_id]['api_calls']
                    dataset_usage[dataset_id]['avg_response_time'] = (
                        (current_avg * (call_count - 1) + log.response_time_ms) / call_count
                    )
                
                if log.status_code >= 400:
                    dataset_usage[dataset_id]['error_rate'] += 1
            
            # Calculate error rates and add dataset info
            dataset_usage_list = []
            for dataset_id, usage in dataset_usage.items():
                dataset = Dataset.query.get(dataset_id)
                if dataset:
                    usage['error_rate'] = (usage['error_rate'] / usage['api_calls']) * 100
                    usage['cost'] = float(usage['cost'])
                    
                    dataset_usage_list.append({
                        'dataset': {
                            'id': dataset.id,
                            'name': dataset.name,
                            'slug': dataset.slug,
                            'category': dataset.category.value
                        },
                        'usage': usage
                    })
            
            # Get top endpoints
            endpoint_usage = defaultdict(int)
            for log in usage_logs:
                endpoint_usage[log.endpoint] += 1
            
            top_endpoints = sorted(
                endpoint_usage.items(), 
                key=lambda x: x[1], 
                reverse=True
            )[:10]
            
            return {
                'period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'summary': {
                    'total_api_calls': total_api_calls,
                    'total_records_accessed': total_records,
                    'total_cost': float(total_cost),
                    'average_response_time_ms': round(avg_response_time, 2)
                },
                'datasets': dataset_usage_list,
                'top_endpoints': [
                    {'endpoint': endpoint, 'calls': calls}
                    for endpoint, calls in top_endpoints
                ]
            }
            
        except Exception as e:
            self.logger.error(f"Error getting user usage summary: {str(e)}")
            return {}
    
    def get_dataset_usage_analytics(
        self, 
        dataset_id: int,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get usage analytics for a specific dataset
        
        Args:
            dataset_id: ID of the dataset
            start_date: Start of period (defaults to last 30 days)
            end_date: End of period (defaults to now)
            
        Returns:
            Dictionary with dataset usage analytics
        """
        try:
            # Default to last 30 days if no dates provided
            if not end_date:
                end_date = datetime.utcnow()
            
            if not start_date:
                start_date = end_date - timedelta(days=30)
            
            # Get usage logs for the dataset
            usage_logs = UsageLog.query.filter(
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= start_date,
                UsageLog.timestamp <= end_date
            ).all()
            
            if not usage_logs:
                return {
                    'period': {
                        'start_date': start_date.isoformat(),
                        'end_date': end_date.isoformat()
                    },
                    'summary': {
                        'total_api_calls': 0,
                        'unique_users': 0,
                        'total_records_served': 0,
                        'total_revenue': 0.0
                    }
                }
            
            # Aggregate data
            total_api_calls = len(usage_logs)
            unique_users = len(set(log.user_id for log in usage_logs))
            total_records = sum(log.records_returned for log in usage_logs)
            total_revenue = sum(log.cost_amount for log in usage_logs)
            
            # Group by user
            user_usage = defaultdict(lambda: {
                'api_calls': 0,
                'records_accessed': 0,
                'revenue': Decimal('0.00')
            })
            
            for log in usage_logs:
                user_id = log.user_id
                user_usage[user_id]['api_calls'] += 1
                user_usage[user_id]['records_accessed'] += log.records_returned
                user_usage[user_id]['revenue'] += log.cost_amount
            
            # Get top users
            top_users = sorted(
                user_usage.items(),
                key=lambda x: x[1]['revenue'],
                reverse=True
            )[:10]
            
            top_users_list = []
            for user_id, usage in top_users:
                user = User.query.get(user_id)
                if user:
                    top_users_list.append({
                        'user': {
                            'id': user.id,
                            'email': user.email,
                            'company_name': user.company_name
                        },
                        'usage': {
                            'api_calls': usage['api_calls'],
                            'records_accessed': usage['records_accessed'],
                            'revenue': float(usage['revenue'])
                        }
                    })
            
            # Daily usage trend
            daily_usage = defaultdict(lambda: {
                'api_calls': 0,
                'records': 0,
                'revenue': Decimal('0.00')
            })
            
            for log in usage_logs:
                date_key = log.timestamp.date().isoformat()
                daily_usage[date_key]['api_calls'] += 1
                daily_usage[date_key]['records'] += log.records_returned
                daily_usage[date_key]['revenue'] += log.cost_amount
            
            daily_trend = []
            for date_str, usage in sorted(daily_usage.items()):
                daily_trend.append({
                    'date': date_str,
                    'api_calls': usage['api_calls'],
                    'records': usage['records'],
                    'revenue': float(usage['revenue'])
                })
            
            return {
                'period': {
                    'start_date': start_date.isoformat(),
                    'end_date': end_date.isoformat()
                },
                'summary': {
                    'total_api_calls': total_api_calls,
                    'unique_users': unique_users,
                    'total_records_served': total_records,
                    'total_revenue': float(total_revenue)
                },
                'top_users': top_users_list,
                'daily_trend': daily_trend
            }
            
        except Exception as e:
            self.logger.error(f"Error getting dataset usage analytics: {str(e)}")
            return {}
    
    def get_rate_limit_status(self, user_id: int, dataset_id: int) -> Dict[str, Any]:
        """
        Get current rate limit status for a user and dataset
        
        Args:
            user_id: ID of the user
            dataset_id: ID of the dataset
            
        Returns:
            Dictionary with rate limit status
        """
        try:
            # Get active subscription
            subscription = Subscription.query.filter(
                Subscription.user_id == user_id,
                Subscription.dataset_id == dataset_id,
                Subscription.status == 'active'
            ).first()
            
            if not subscription:
                return {'error': 'No active subscription found'}
            
            license = subscription.license
            now = datetime.utcnow()
            
            # Check current usage for different time windows
            # Last minute
            minute_ago = now - timedelta(minutes=1)
            minute_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= minute_ago
            ).count()
            
            # Today
            day_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            daily_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= day_start
            ).count()
            
            # This month
            month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= month_start
            ).count()
            
            return {
                'rate_limits': {
                    'per_minute': {
                        'limit': license.rate_limit_per_minute,
                        'used': minute_usage,
                        'remaining': max(0, license.rate_limit_per_minute - minute_usage),
                        'reset_time': (minute_ago + timedelta(minutes=1)).isoformat()
                    },
                    'per_day': {
                        'limit': license.rate_limit_per_day,
                        'used': daily_usage,
                        'remaining': max(0, license.rate_limit_per_day - daily_usage),
                        'reset_time': (day_start + timedelta(days=1)).isoformat()
                    },
                    'per_month': {
                        'limit': license.rate_limit_per_month,
                        'used': monthly_usage,
                        'remaining': max(0, license.rate_limit_per_month - monthly_usage),
                        'reset_time': (month_start + timedelta(days=32)).replace(day=1).isoformat()
                    }
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting rate limit status: {str(e)}")
            return {'error': 'Internal error'}
    
    def reset_monthly_usage(self, user_id: Optional[int] = None) -> int:
        """
        Reset monthly usage counters (typically called on the 1st of each month)
        
        Args:
            user_id: Optional user ID to reset (if None, resets for all users)
            
        Returns:
            Number of subscriptions reset
        """
        try:
            query = Subscription.query.filter(Subscription.status == 'active')
            
            if user_id:
                query = query.filter(Subscription.user_id == user_id)
            
            subscriptions = query.all()
            reset_count = 0
            
            for subscription in subscriptions:
                subscription.current_month_api_calls = 0
                subscription.current_month_records_accessed = 0
                subscription.current_usage_cost = Decimal('0.00')
                subscription.last_usage_reset = datetime.utcnow()
                subscription.updated_at = datetime.utcnow()
                reset_count += 1
            
            # Also reset user monthly spend
            if user_id:
                user = User.query.get(user_id)
                if user:
                    user.current_monthly_spend = Decimal('0.00')
            else:
                # Reset all users
                users = User.query.filter(User.is_active == True).all()
                for user in users:
                    user.current_monthly_spend = Decimal('0.00')
            
            db.session.commit()
            
            self.logger.info(f"Reset monthly usage for {reset_count} subscriptions")
            return reset_count
            
        except Exception as e:
            db.session.rollback()
            self.logger.error(f"Error resetting monthly usage: {str(e)}")
            return 0
