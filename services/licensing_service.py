"""
Licensing Service for Dataset Access Control and Subscription Management
Handles license validation, subscription creation, and access control logic
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional, Dict, List, Any
import logging

from models import (
    db, User, Dataset, DatasetLicense, Subscription, UsageLog,
    LicenseType, SubscriptionTier
)

logger = logging.getLogger(__name__)

class LicensingService:
    """Service for managing dataset licenses and subscriptions"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def create_subscription(
        self, 
        user_id: int, 
        dataset_id: int, 
        license_id: int,
        subscription_type: str = "monthly"
    ) -> Subscription:
        """
        Create a new subscription for a user to a dataset
        
        Args:
            user_id: ID of the user
            dataset_id: ID of the dataset
            license_id: ID of the license
            subscription_type: Type of subscription (monthly, annual, pay-per-use)
            
        Returns:
            Created subscription object
            
        Raises:
            ValueError: If invalid parameters or business rules violated
        """
        try:
            # Validate inputs
            user = User.query.get(user_id)
            if not user or not user.is_active:
                raise ValueError("Invalid or inactive user")
            
            dataset = Dataset.query.get(dataset_id)
            if not dataset or not dataset.is_active:
                raise ValueError("Invalid or inactive dataset")
            
            license = DatasetLicense.query.get(license_id)
            if not license or not license.is_active or license.dataset_id != dataset_id:
                raise ValueError("Invalid license for this dataset")
            
            # Check for existing active subscription
            existing = Subscription.query.filter(
                Subscription.user_id == user_id,
                Subscription.dataset_id == dataset_id,
                Subscription.status == 'active'
            ).first()
            
            if existing:
                raise ValueError("User already has an active subscription to this dataset")
            
            # Calculate pricing and dates
            start_date = datetime.utcnow()
            end_date = None
            monthly_price = None
            annual_price = None
            
            if subscription_type == "monthly":
                end_date = start_date + timedelta(days=30)
                monthly_price = license.monthly_price
            elif subscription_type == "annual":
                end_date = start_date + timedelta(days=365)
                annual_price = license.annual_price
            elif subscription_type == "pay-per-use":
                # No end date for pay-per-use
                pass
            else:
                raise ValueError("Invalid subscription type")
            
            # Check user's spending limits
            if user.subscription_tier != SubscriptionTier.ENTERPRISE:
                total_price = monthly_price or annual_price or Decimal('0')
                if user.current_monthly_spend + total_price > user.monthly_spend_limit:
                    raise ValueError("Subscription would exceed user's monthly spending limit")
            
            # Create subscription
            subscription = Subscription(
                user_id=user_id,
                dataset_id=dataset_id,
                license_id=license_id,
                subscription_type=subscription_type,
                status='active',
                start_date=start_date,
                end_date=end_date,
                monthly_price=monthly_price,
                annual_price=annual_price,
                auto_renew=True
            )
            
            db.session.add(subscription)
            
            # Update user's monthly spend if applicable
            if monthly_price:
                user.current_monthly_spend += monthly_price
            elif annual_price:
                # For annual subscriptions, add monthly equivalent to current spend
                user.current_monthly_spend += annual_price / 12
            
            db.session.commit()
            
            self.logger.info(f"Created subscription {subscription.id} for user {user_id} to dataset {dataset_id}")
            return subscription
            
        except Exception as e:
            db.session.rollback()
            self.logger.error(f"Error creating subscription: {str(e)}")
            raise
    
    def validate_access(
        self, 
        user_id: int, 
        dataset_id: int, 
        requested_records: int = 1
    ) -> Dict[str, Any]:
        """
        Validate if a user has access to a dataset and check rate limits
        
        Args:
            user_id: ID of the user
            dataset_id: ID of the dataset
            requested_records: Number of records being requested
            
        Returns:
            Dictionary with access validation results
        """
        try:
            # Get active subscription
            subscription = Subscription.query.filter(
                Subscription.user_id == user_id,
                Subscription.dataset_id == dataset_id,
                Subscription.status == 'active'
            ).first()
            
            if not subscription:
                return {
                    'allowed': False,
                    'reason': 'No active subscription to this dataset',
                    'error_code': 'NO_SUBSCRIPTION'
                }
            
            # Check subscription expiry
            if subscription.end_date and subscription.end_date < datetime.utcnow():
                return {
                    'allowed': False,
                    'reason': 'Subscription has expired',
                    'error_code': 'SUBSCRIPTION_EXPIRED'
                }
            
            license = subscription.license
            
            # Check record limit per request
            if requested_records > license.max_records_per_request:
                return {
                    'allowed': False,
                    'reason': f'Requested records ({requested_records}) exceeds limit ({license.max_records_per_request})',
                    'error_code': 'RECORD_LIMIT_EXCEEDED'
                }
            
            # Check rate limits
            now = datetime.utcnow()
            
            # Check daily limit
            daily_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            daily_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= daily_start
            ).count()
            
            if daily_usage >= license.rate_limit_per_day:
                return {
                    'allowed': False,
                    'reason': 'Daily API call limit exceeded',
                    'error_code': 'DAILY_LIMIT_EXCEEDED',
                    'reset_time': (daily_start + timedelta(days=1)).isoformat()
                }
            
            # Check monthly limit
            monthly_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            monthly_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= monthly_start
            ).count()
            
            if monthly_usage >= license.rate_limit_per_month:
                # Calculate next month start
                if now.month == 12:
                    next_month = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    next_month = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
                
                return {
                    'allowed': False,
                    'reason': 'Monthly API call limit exceeded',
                    'error_code': 'MONTHLY_LIMIT_EXCEEDED',
                    'reset_time': next_month.isoformat()
                }
            
            # Check minute-based rate limit (sliding window)
            minute_ago = now - timedelta(minutes=1)
            minute_usage = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= minute_ago
            ).count()
            
            if minute_usage >= license.rate_limit_per_minute:
                return {
                    'allowed': False,
                    'reason': 'Rate limit exceeded (requests per minute)',
                    'error_code': 'RATE_LIMIT_EXCEEDED',
                    'reset_time': (minute_ago + timedelta(minutes=1)).isoformat()
                }
            
            # Calculate cost for this request
            cost = self._calculate_request_cost(license, requested_records)
            
            return {
                'allowed': True,
                'subscription_id': subscription.id,
                'license': {
                    'id': license.id,
                    'type': license.license_type.value,
                    'real_time_access': license.real_time_access,
                    'bulk_download': license.bulk_download,
                    'webhook_support': license.webhook_support
                },
                'cost': float(cost),
                'remaining_calls': {
                    'daily': license.rate_limit_per_day - daily_usage,
                    'monthly': license.rate_limit_per_month - monthly_usage,
                    'per_minute': license.rate_limit_per_minute - minute_usage
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error validating access: {str(e)}")
            return {
                'allowed': False,
                'reason': 'Internal error validating access',
                'error_code': 'INTERNAL_ERROR'
            }
    
    def _calculate_request_cost(self, license: DatasetLicense, records_count: int) -> Decimal:
        """Calculate the cost for a request based on license pricing"""
        cost = Decimal('0.00')
        
        # Add per-API-call cost
        if license.price_per_api_call:
            cost += license.price_per_api_call
        
        # Add per-record cost
        if license.price_per_record:
            cost += license.price_per_record * records_count
        
        return cost
    
    def get_subscription_status(self, user_id: int, dataset_id: int) -> Optional[Dict[str, Any]]:
        """Get detailed subscription status for a user and dataset"""
        try:
            subscription = Subscription.query.filter(
                Subscription.user_id == user_id,
                Subscription.dataset_id == dataset_id
            ).order_by(Subscription.created_at.desc()).first()
            
            if not subscription:
                return None
            
            # Calculate usage for current period
            now = datetime.utcnow()
            period_start = subscription.last_usage_reset
            
            usage_logs = UsageLog.query.filter(
                UsageLog.user_id == user_id,
                UsageLog.dataset_id == dataset_id,
                UsageLog.timestamp >= period_start
            ).all()
            
            current_api_calls = len(usage_logs)
            current_records = sum(log.records_returned for log in usage_logs)
            current_cost = sum(log.cost_amount for log in usage_logs)
            
            return {
                'subscription_id': subscription.id,
                'status': subscription.status,
                'subscription_type': subscription.subscription_type,
                'start_date': subscription.start_date.isoformat(),
                'end_date': subscription.end_date.isoformat() if subscription.end_date else None,
                'auto_renew': subscription.auto_renew,
                'license': {
                    'id': subscription.license.id,
                    'name': subscription.license.name,
                    'type': subscription.license.license_type.value
                },
                'current_period': {
                    'start': period_start.isoformat(),
                    'api_calls': current_api_calls,
                    'records_accessed': current_records,
                    'cost': float(current_cost)
                },
                'limits': {
                    'rate_per_minute': subscription.license.rate_limit_per_minute,
                    'rate_per_day': subscription.license.rate_limit_per_day,
                    'rate_per_month': subscription.license.rate_limit_per_month,
                    'max_records_per_request': subscription.license.max_records_per_request
                }
            }
            
        except Exception as e:
            self.logger.error(f"Error getting subscription status: {str(e)}")
            return None
    
    def renew_subscription(self, subscription_id: int) -> bool:
        """Renew an expiring subscription"""
        try:
            subscription = Subscription.query.get(subscription_id)
            if not subscription:
                return False
            
            if subscription.status != 'active':
                return False
            
            # Extend subscription based on type
            if subscription.subscription_type == "monthly":
                subscription.end_date = subscription.end_date + timedelta(days=30)
            elif subscription.subscription_type == "annual":
                subscription.end_date = subscription.end_date + timedelta(days=365)
            
            # Reset usage counters
            subscription.current_month_api_calls = 0
            subscription.current_month_records_accessed = 0
            subscription.current_usage_cost = Decimal('0.00')
            subscription.last_usage_reset = datetime.utcnow()
            subscription.updated_at = datetime.utcnow()
            
            db.session.commit()
            
            self.logger.info(f"Renewed subscription {subscription_id}")
            return True
            
        except Exception as e:
            db.session.rollback()
            self.logger.error(f"Error renewing subscription {subscription_id}: {str(e)}")
            return False
    
    def suspend_subscription(self, subscription_id: int, reason: str = None) -> bool:
        """Suspend a subscription (e.g., for non-payment)"""
        try:
            subscription = Subscription.query.get(subscription_id)
            if not subscription:
                return False
            
            subscription.status = 'suspended'
            subscription.updated_at = datetime.utcnow()
            
            db.session.commit()
            
            self.logger.info(f"Suspended subscription {subscription_id}: {reason}")
            return True
            
        except Exception as e:
            db.session.rollback()
            self.logger.error(f"Error suspending subscription {subscription_id}: {str(e)}")
            return False
    
    def get_expiring_subscriptions(self, days_ahead: int = 7) -> List[Subscription]:
        """Get subscriptions that will expire within the specified number of days"""
        try:
            cutoff_date = datetime.utcnow() + timedelta(days=days_ahead)
            
            subscriptions = Subscription.query.filter(
                Subscription.status == 'active',
                Subscription.end_date.isnot(None),
                Subscription.end_date <= cutoff_date,
                Subscription.auto_renew == True
            ).all()
            
            return subscriptions
            
        except Exception as e:
            self.logger.error(f"Error getting expiring subscriptions: {str(e)}")
            return []
