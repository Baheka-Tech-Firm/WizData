#!/usr/bin/env python3
"""
WizData B2B Platform Management Script
Provides administrative tools for dataset registry, user management, and system maintenance
"""

import sys
import argparse
from datetime import datetime, timedelta
from decimal import Decimal
from tabulate import tabulate

from app import create_app, db
from models import (
    Dataset, DatasetLicense, User, Subscription, UsageLog, DataQualityMetric,
    LicenseType, SubscriptionTier, DatasetCategory
)
from services.licensing_service import LicensingService
from services.usage_tracker import UsageTracker

def setup_argparse():
    """Setup command line argument parsing"""
    parser = argparse.ArgumentParser(description='WizData B2B Platform Management')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Dataset management
    dataset_parser = subparsers.add_parser('datasets', help='Dataset management')
    dataset_subparsers = dataset_parser.add_subparsers(dest='dataset_action')
    
    dataset_subparsers.add_parser('list', help='List all datasets')
    dataset_subparsers.add_parser('status', help='Dataset status overview')
    
    create_dataset_parser = dataset_subparsers.add_parser('create', help='Create new dataset')
    create_dataset_parser.add_argument('--name', required=True, help='Dataset name')
    create_dataset_parser.add_argument('--slug', required=True, help='Dataset slug')
    create_dataset_parser.add_argument('--category', required=True, help='Dataset category')
    create_dataset_parser.add_argument('--provider', required=True, help='Data provider')
    create_dataset_parser.add_argument('--description', required=True, help='Dataset description')
    
    # User management
    user_parser = subparsers.add_parser('users', help='User management')
    user_subparsers = user_parser.add_subparsers(dest='user_action')
    
    user_subparsers.add_parser('list', help='List all users')
    user_subparsers.add_parser('stats', help='User statistics')
    
    create_user_parser = user_subparsers.add_parser('create', help='Create new user')
    create_user_parser.add_argument('--email', required=True, help='User email')
    create_user_parser.add_argument('--company', required=True, help='Company name')
    create_user_parser.add_argument('--contact', required=True, help='Contact name')
    create_user_parser.add_argument('--tier', default='free', help='Subscription tier')
    
    # Subscription management
    subscription_parser = subparsers.add_parser('subscriptions', help='Subscription management')
    subscription_subparsers = subscription_parser.add_subparsers(dest='subscription_action')
    
    subscription_subparsers.add_parser('list', help='List all subscriptions')
    subscription_subparsers.add_parser('expiring', help='List expiring subscriptions')
    subscription_subparsers.add_parser('renew-all', help='Renew eligible subscriptions')
    
    # Usage analytics
    usage_parser = subparsers.add_parser('usage', help='Usage analytics')
    usage_subparsers = usage_parser.add_subparsers(dest='usage_action')
    
    usage_subparsers.add_parser('summary', help='Usage summary')
    usage_subparsers.add_parser('top-users', help='Top users by usage')
    usage_subparsers.add_parser('revenue', help='Revenue analytics')
    
    reset_usage_parser = usage_subparsers.add_parser('reset-monthly', help='Reset monthly usage counters')
    reset_usage_parser.add_argument('--user-id', type=int, help='Reset for specific user only')
    
    # System maintenance
    system_parser = subparsers.add_parser('system', help='System maintenance')
    system_subparsers = system_parser.add_subparsers(dest='system_action')
    
    system_subparsers.add_parser('health', help='System health check')
    system_subparsers.add_parser('cleanup', help='Clean up old data')
    system_subparsers.add_parser('init', help='Initialize database')
    
    return parser

def list_datasets():
    """List all datasets with basic information"""
    datasets = Dataset.query.all()
    
    table_data = []
    for dataset in datasets:
        table_data.append([
            dataset.id,
            dataset.name,
            dataset.slug,
            dataset.category.value,
            dataset.provider,
            'Active' if dataset.is_active else 'Inactive',
            f"${dataset.monthly_subscription_price or 0}",
            dataset.total_records,
            dataset.last_updated.strftime('%Y-%m-%d') if dataset.last_updated else 'Never'
        ])
    
    headers = ['ID', 'Name', 'Slug', 'Category', 'Provider', 'Status', 'Price/Month', 'Records', 'Last Updated']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    print(f"\nTotal datasets: {len(datasets)}")

def dataset_status():
    """Show dataset status overview"""
    datasets = Dataset.query.all()
    
    # Category breakdown
    category_counts = {}
    for dataset in datasets:
        category = dataset.category.value
        category_counts[category] = category_counts.get(category, 0) + 1
    
    # Status breakdown
    active_count = Dataset.query.filter(Dataset.is_active == True).count()
    inactive_count = Dataset.query.filter(Dataset.is_active == False).count()
    
    # Quality metrics
    avg_quality = db.session.query(db.func.avg(Dataset.data_quality_score)).scalar() or 0
    
    print("Dataset Status Overview")
    print("=" * 50)
    print(f"Total Datasets: {len(datasets)}")
    print(f"Active: {active_count}")
    print(f"Inactive: {inactive_count}")
    print(f"Average Quality Score: {avg_quality:.1f}")
    print()
    
    print("By Category:")
    for category, count in category_counts.items():
        print(f"  {category}: {count}")

def create_dataset(args):
    """Create a new dataset"""
    try:
        category_enum = DatasetCategory(args.category)
    except ValueError:
        print(f"Invalid category. Valid options: {[c.value for c in DatasetCategory]}")
        return
    
    dataset = Dataset(
        name=args.name,
        slug=args.slug,
        description=args.description,
        category=category_enum,
        provider=args.provider,
        version="1.0.0",
        is_active=True,
        is_public=True,
        created_at=datetime.utcnow()
    )
    
    db.session.add(dataset)
    db.session.commit()
    
    print(f"✅ Created dataset: {dataset.name} (ID: {dataset.id})")

def list_users():
    """List all users with basic information"""
    users = User.query.all()
    
    table_data = []
    for user in users:
        subscription_count = Subscription.query.filter(
            Subscription.user_id == user.id,
            Subscription.status == 'active'
        ).count()
        
        table_data.append([
            user.id,
            user.email,
            user.company_name or 'N/A',
            user.subscription_tier.value,
            'Active' if user.is_active else 'Inactive',
            subscription_count,
            f"${user.current_monthly_spend}",
            user.created_at.strftime('%Y-%m-%d')
        ])
    
    headers = ['ID', 'Email', 'Company', 'Tier', 'Status', 'Subscriptions', 'Monthly Spend', 'Created']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    print(f"\nTotal users: {len(users)}")

def user_stats():
    """Show user statistics"""
    total_users = User.query.count()
    active_users = User.query.filter(User.is_active == True).count()
    verified_users = User.query.filter(User.is_verified == True).count()
    
    # Tier breakdown
    tier_counts = {}
    for tier in SubscriptionTier:
        count = User.query.filter(User.subscription_tier == tier).count()
        tier_counts[tier.value] = count
    
    # Recent signups
    week_ago = datetime.utcnow() - timedelta(days=7)
    recent_signups = User.query.filter(User.created_at >= week_ago).count()
    
    print("User Statistics")
    print("=" * 30)
    print(f"Total Users: {total_users}")
    print(f"Active: {active_users}")
    print(f"Verified: {verified_users}")
    print(f"Recent Signups (7 days): {recent_signups}")
    print()
    
    print("By Subscription Tier:")
    for tier, count in tier_counts.items():
        print(f"  {tier}: {count}")

def create_user(args):
    """Create a new user"""
    try:
        tier_enum = SubscriptionTier(args.tier)
    except ValueError:
        print(f"Invalid tier. Valid options: {[t.value for t in SubscriptionTier]}")
        return
    
    # Check if email already exists
    existing_user = User.query.filter(User.email == args.email).first()
    if existing_user:
        print(f"❌ User with email {args.email} already exists")
        return
    
    user = User(
        email=args.email,
        company_name=args.company,
        contact_name=args.contact,
        subscription_tier=tier_enum,
        is_active=True,
        is_verified=True,
        created_at=datetime.utcnow()
    )
    
    db.session.add(user)
    db.session.commit()
    
    print(f"✅ Created user: {user.email} (ID: {user.id})")

def list_subscriptions():
    """List all subscriptions"""
    subscriptions = Subscription.query.all()
    
    table_data = []
    for sub in subscriptions:
        table_data.append([
            sub.id,
            sub.user.email,
            sub.dataset.name,
            sub.license.license_type.value,
            sub.status,
            f"${sub.monthly_price or sub.annual_price or 0}",
            sub.start_date.strftime('%Y-%m-%d'),
            sub.end_date.strftime('%Y-%m-%d') if sub.end_date else 'Ongoing',
            sub.current_month_api_calls
        ])
    
    headers = ['ID', 'User', 'Dataset', 'License', 'Status', 'Price', 'Start', 'End', 'API Calls']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    print(f"\nTotal subscriptions: {len(subscriptions)}")

def expiring_subscriptions():
    """List subscriptions expiring soon"""
    licensing_service = LicensingService()
    expiring = licensing_service.get_expiring_subscriptions(days_ahead=30)
    
    if not expiring:
        print("No subscriptions expiring in the next 30 days")
        return
    
    table_data = []
    for sub in expiring:
        days_remaining = (sub.end_date - datetime.utcnow()).days
        table_data.append([
            sub.id,
            sub.user.email,
            sub.dataset.name,
            sub.end_date.strftime('%Y-%m-%d'),
            days_remaining,
            'Yes' if sub.auto_renew else 'No'
        ])
    
    headers = ['ID', 'User', 'Dataset', 'Expires', 'Days Left', 'Auto Renew']
    print(tabulate(table_data, headers=headers, tablefmt='grid'))
    print(f"\nExpiring subscriptions: {len(expiring)}")

def renew_all_subscriptions():
    """Renew all eligible subscriptions"""
    licensing_service = LicensingService()
    expiring = licensing_service.get_expiring_subscriptions(days_ahead=7)
    
    renewed_count = 0
    for subscription in expiring:
        if subscription.auto_renew:
            if licensing_service.renew_subscription(subscription.id):
                renewed_count += 1
                print(f"✅ Renewed subscription {subscription.id} for {subscription.user.email}")
            else:
                print(f"❌ Failed to renew subscription {subscription.id}")
    
    print(f"\n✅ Renewed {renewed_count} subscriptions")

def usage_summary():
    """Show usage summary"""
    usage_tracker = UsageTracker()
    
    # Current month
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    total_calls = UsageLog.query.filter(UsageLog.timestamp >= month_start).count()
    total_records = db.session.query(db.func.sum(UsageLog.records_returned)).filter(
        UsageLog.timestamp >= month_start
    ).scalar() or 0
    total_revenue = db.session.query(db.func.sum(UsageLog.cost_amount)).filter(
        UsageLog.timestamp >= month_start
    ).scalar() or 0
    
    unique_users = db.session.query(db.func.count(db.distinct(UsageLog.user_id))).filter(
        UsageLog.timestamp >= month_start
    ).scalar() or 0
    
    print("Usage Summary (Current Month)")
    print("=" * 40)
    print(f"Total API Calls: {total_calls:,}")
    print(f"Total Records Served: {total_records:,}")
    print(f"Total Revenue: ${total_revenue:.2f}")
    print(f"Active Users: {unique_users}")
    
    # Average per user
    if unique_users > 0:
        avg_calls = total_calls / unique_users
        avg_revenue = float(total_revenue) / unique_users
        print(f"Average Calls per User: {avg_calls:.1f}")
        print(f"Average Revenue per User: ${avg_revenue:.2f}")

def top_users_by_usage():
    """Show top users by usage"""
    # Current month
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Query top users by API calls
    top_users = db.session.query(
        UsageLog.user_id,
        db.func.count(UsageLog.id).label('api_calls'),
        db.func.sum(UsageLog.records_returned).label('records'),
        db.func.sum(UsageLog.cost_amount).label('revenue')
    ).filter(
        UsageLog.timestamp >= month_start
    ).group_by(
        UsageLog.user_id
    ).order_by(
        db.func.count(UsageLog.id).desc()
    ).limit(10).all()
    
    table_data = []
    for user_stats in top_users:
        user = User.query.get(user_stats.user_id)
        table_data.append([
            user.email,
            user.company_name or 'N/A',
            user_stats.api_calls,
            user_stats.records or 0,
            f"${user_stats.revenue or 0:.2f}"
        ])
    
    headers = ['Email', 'Company', 'API Calls', 'Records', 'Revenue']
    print("Top Users by Usage (Current Month)")
    print("=" * 50)
    print(tabulate(table_data, headers=headers, tablefmt='grid'))

def revenue_analytics():
    """Show revenue analytics"""
    # Current month
    now = datetime.utcnow()
    month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    
    # Revenue by dataset
    dataset_revenue = db.session.query(
        Dataset.name,
        db.func.sum(UsageLog.cost_amount).label('revenue'),
        db.func.count(UsageLog.id).label('api_calls')
    ).join(
        UsageLog, Dataset.id == UsageLog.dataset_id
    ).filter(
        UsageLog.timestamp >= month_start
    ).group_by(
        Dataset.id, Dataset.name
    ).order_by(
        db.func.sum(UsageLog.cost_amount).desc()
    ).all()
    
    # Subscription revenue
    subscription_revenue = db.session.query(
        db.func.sum(Subscription.monthly_price).label('monthly_subscriptions'),
        db.func.sum(Subscription.annual_price).label('annual_subscriptions')
    ).filter(
        Subscription.status == 'active'
    ).first()
    
    print("Revenue Analytics")
    print("=" * 30)
    
    if dataset_revenue:
        print("\nRevenue by Dataset (Usage-based):")
        table_data = []
        for dataset_name, revenue, calls in dataset_revenue:
            table_data.append([dataset_name, f"${revenue:.2f}", calls])
        
        headers = ['Dataset', 'Revenue', 'API Calls']
        print(tabulate(table_data, headers=headers, tablefmt='grid'))
    
    if subscription_revenue:
        monthly_rev = subscription_revenue.monthly_subscriptions or 0
        annual_rev = subscription_revenue.annual_subscriptions or 0
        print(f"\nRecurring Revenue:")
        print(f"Monthly Subscriptions: ${monthly_rev:.2f}")
        print(f"Annual Subscriptions: ${annual_rev:.2f}")
        print(f"Annualized Revenue: ${(monthly_rev * 12) + annual_rev:.2f}")

def reset_monthly_usage(user_id=None):
    """Reset monthly usage counters"""
    usage_tracker = UsageTracker()
    reset_count = usage_tracker.reset_monthly_usage(user_id)
    
    if user_id:
        user = User.query.get(user_id)
        print(f"✅ Reset monthly usage for user: {user.email if user else 'Unknown'}")
    else:
        print(f"✅ Reset monthly usage for {reset_count} subscriptions")

def system_health():
    """Check system health"""
    print("System Health Check")
    print("=" * 30)
    
    # Database connectivity
    try:
        db.session.execute(db.text('SELECT 1'))
        print("✅ Database: Connected")
    except Exception as e:
        print(f"❌ Database: Error - {e}")
    
    # Data counts
    dataset_count = Dataset.query.count()
    user_count = User.query.count()
    subscription_count = Subscription.query.filter(Subscription.status == 'active').count()
    
    print(f"📊 Datasets: {dataset_count}")
    print(f"👥 Users: {user_count}")
    print(f"📋 Active Subscriptions: {subscription_count}")
    
    # Recent activity
    day_ago = datetime.utcnow() - timedelta(days=1)
    recent_usage = UsageLog.query.filter(UsageLog.timestamp >= day_ago).count()
    print(f"🚀 API Calls (24h): {recent_usage}")

def cleanup_old_data():
    """Clean up old data"""
    # Remove usage logs older than 2 years
    cutoff_date = datetime.utcnow() - timedelta(days=730)
    
    old_logs = UsageLog.query.filter(UsageLog.timestamp < cutoff_date)
    count = old_logs.count()
    
    if count > 0:
        old_logs.delete()
        db.session.commit()
        print(f"✅ Cleaned up {count} old usage logs")
    else:
        print("No old data to clean up")

def init_database():
    """Initialize database with tables"""
    db.create_all()
    print("✅ Database tables created")

def main():
    """Main function"""
    parser = setup_argparse()
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    app, _ = create_app()
    
    with app.app_context():
        if args.command == 'datasets':
            if args.dataset_action == 'list':
                list_datasets()
            elif args.dataset_action == 'status':
                dataset_status()
            elif args.dataset_action == 'create':
                create_dataset(args)
        
        elif args.command == 'users':
            if args.user_action == 'list':
                list_users()
            elif args.user_action == 'stats':
                user_stats()
            elif args.user_action == 'create':
                create_user(args)
        
        elif args.command == 'subscriptions':
            if args.subscription_action == 'list':
                list_subscriptions()
            elif args.subscription_action == 'expiring':
                expiring_subscriptions()
            elif args.subscription_action == 'renew-all':
                renew_all_subscriptions()
        
        elif args.command == 'usage':
            if args.usage_action == 'summary':
                usage_summary()
            elif args.usage_action == 'top-users':
                top_users_by_usage()
            elif args.usage_action == 'revenue':
                revenue_analytics()
            elif args.usage_action == 'reset-monthly':
                reset_monthly_usage(args.user_id if hasattr(args, 'user_id') else None)
        
        elif args.command == 'system':
            if args.system_action == 'health':
                system_health()
            elif args.system_action == 'cleanup':
                cleanup_old_data()
            elif args.system_action == 'init':
                init_database()

if __name__ == '__main__':
    main()
