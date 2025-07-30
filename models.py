from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from sqlalchemy import JSON
from enum import Enum
from decimal import Decimal
# from sqlalchemy.dialects.postgresql import ARRAY, JSON

# Initialize db here to avoid circular imports
db = SQLAlchemy()

class APIKey(db.Model):
    """API Key model for authentication"""
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_used = db.Column(db.DateTime, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    permissions = db.Column(db.String(256), nullable=True)  # JSON string of permissions

class DataSource(db.Model):
    """Model for tracking data sources"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    type = db.Column(db.String(50), nullable=False)  # 'jse', 'crypto', 'forex', etc.
    url = db.Column(db.String(256), nullable=True)
    description = db.Column(db.Text, nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    last_fetch = db.Column(db.DateTime, nullable=True)
    fetch_frequency = db.Column(db.Integer, default=60)  # minutes
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class SchedulerJob(db.Model):
    """Model for tracking scheduler jobs"""
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(100), nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    status = db.Column(db.String(20), nullable=False, default='pending')  # pending, running, completed, failed
    start_time = db.Column(db.DateTime, nullable=True)
    end_time = db.Column(db.DateTime, nullable=True)
    records_processed = db.Column(db.Integer, default=0)
    error_message = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class Symbol(db.Model):
    """Model for financial symbols/tickers"""
    id = db.Column(db.Integer, primary_key=True)
    symbol = db.Column(db.String(20), nullable=False, unique=True)
    name = db.Column(db.String(200), nullable=True)
    exchange = db.Column(db.String(20), nullable=True)
    asset_type = db.Column(db.String(20), nullable=False)  # stock, crypto, forex, etc.
    sector = db.Column(db.String(100), nullable=True)
    country = db.Column(db.String(50), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    last_price = db.Column(db.Float, nullable=True)
    last_updated = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class APIRequest(db.Model):
    """Model for tracking API requests"""
    id = db.Column(db.Integer, primary_key=True)
    endpoint = db.Column(db.String(100), nullable=False)
    method = db.Column(db.String(10), nullable=False)
    user_id = db.Column(db.Integer, nullable=True)
    api_key_id = db.Column(db.Integer, db.ForeignKey('api_key.id'), nullable=True)
    ip_address = db.Column(db.String(50), nullable=True)
    status_code = db.Column(db.Integer, nullable=False)
    response_time = db.Column(db.Float, nullable=True)  # in milliseconds
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    parameters = db.Column(db.Text, nullable=True)  # JSON string of request parameters

# ESG Data Models
class Region(db.Model):
    """Model for geographic regions in Africa (country, province/state, municipality)"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    code = db.Column(db.String(20), nullable=False, unique=True)
    region_type = db.Column(db.String(20), nullable=False)  # country, province, municipality
    parent_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=True)
    country = db.Column(db.String(50), nullable=False)
    geometry = db.Column(db.Text, nullable=True)  # GeoJSON representation
    population = db.Column(db.Integer, nullable=True)
    area_km2 = db.Column(db.Float, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to parent region
    children = db.relationship('Region', 
                              backref=db.backref('parent', remote_side=[id]),
                              lazy='dynamic')
    
    # Define relationships to ESG data
    environmental_metrics = db.relationship('EnvironmentalMetric', backref='region', lazy='dynamic')
    social_metrics = db.relationship('SocialMetric', backref='region', lazy='dynamic')
    governance_metrics = db.relationship('GovernanceMetric', backref='region', lazy='dynamic')
    infrastructure_metrics = db.relationship('InfrastructureMetric', backref='region', lazy='dynamic')

class EnvironmentalMetric(db.Model):
    """Model for environmental ESG metrics"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    metric_type = db.Column(db.String(50), nullable=False)  # electricity_access, load_shedding_index, water_access, etc.
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(20), nullable=True)  # %, index, etc.
    date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    confidence = db.Column(db.Float, nullable=True)  # Confidence score (0-1)
    raw_data = db.Column(JSON, nullable=True)  # Original data in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to data source
    source = db.relationship('DataSource')

class SocialMetric(db.Model):
    """Model for social ESG metrics"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    metric_type = db.Column(db.String(50), nullable=False)  # literacy_rate, school_coverage, healthcare_access, etc.
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(20), nullable=True)  # %, index, ratio, etc.
    date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    confidence = db.Column(db.Float, nullable=True)  # Confidence score (0-1)
    raw_data = db.Column(JSON, nullable=True)  # Original data in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to data source
    source = db.relationship('DataSource')

class GovernanceMetric(db.Model):
    """Model for governance ESG metrics"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    metric_type = db.Column(db.String(50), nullable=False)  # audit_outcome, protests, corruption_index, etc.
    value = db.Column(db.Float, nullable=True)  # Numeric value if applicable
    status = db.Column(db.String(50), nullable=True)  # String status (e.g., "Clean", "Qualified", etc.)
    unit = db.Column(db.String(20), nullable=True)  # %, index, count, etc.
    date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    confidence = db.Column(db.Float, nullable=True)  # Confidence score (0-1)
    raw_data = db.Column(JSON, nullable=True)  # Original data in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to data source
    source = db.relationship('DataSource')

class InfrastructureMetric(db.Model):
    """Model for infrastructure metrics"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    metric_type = db.Column(db.String(50), nullable=False)  # roads_paved_km, schools_count, clinics_count, etc.
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(20), nullable=True)  # km, count, ratio, %, etc.
    date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    confidence = db.Column(db.Float, nullable=True)  # Confidence score (0-1)
    raw_data = db.Column(JSON, nullable=True)  # Original data in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to data source
    source = db.relationship('DataSource')

class InfrastructureFacility(db.Model):
    """Model for specific infrastructure facilities"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    name = db.Column(db.String(200), nullable=False)
    facility_type = db.Column(db.String(50), nullable=False)  # school, clinic, hospital, power_station, etc.
    latitude = db.Column(db.Float, nullable=True)
    longitude = db.Column(db.Float, nullable=True)
    status = db.Column(db.String(20), nullable=True)  # operational, construction, closed, etc.
    capacity = db.Column(db.Float, nullable=True)  # Capacity where applicable
    capacity_unit = db.Column(db.String(20), nullable=True)  # students, beds, MW, etc.
    year_established = db.Column(db.Integer, nullable=True)
    last_renovated = db.Column(db.Integer, nullable=True)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    properties = db.Column(JSON, nullable=True)  # Additional properties in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to region and data source
    region = db.relationship('Region')
    source = db.relationship('DataSource')

class SDGMetric(db.Model):
    """Model for Sustainable Development Goals metrics"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    sdg_number = db.Column(db.Integer, nullable=False)  # 1-17 for the 17 SDGs
    target_number = db.Column(db.String(10), nullable=True)  # E.g., "1.1", "3.2", etc.
    indicator_code = db.Column(db.String(20), nullable=True)  # Official SDG indicator code
    value = db.Column(db.Float, nullable=False)
    unit = db.Column(db.String(20), nullable=True)
    date = db.Column(db.Date, nullable=False)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    confidence = db.Column(db.Float, nullable=True)  # Confidence score (0-1)
    raw_data = db.Column(JSON, nullable=True)  # Original data in JSON format
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to region and data source
    region = db.relationship('Region')
    source = db.relationship('DataSource')

class ESGCompositeScore(db.Model):
    """Model for composite ESG scores by region"""
    id = db.Column(db.Integer, primary_key=True)
    region_id = db.Column(db.Integer, db.ForeignKey('region.id'), nullable=False)
    environmental_score = db.Column(db.Float, nullable=True)
    social_score = db.Column(db.Float, nullable=True)
    governance_score = db.Column(db.Float, nullable=True)
    infrastructure_score = db.Column(db.Float, nullable=True)
    overall_score = db.Column(db.Float, nullable=True)
    date = db.Column(db.Date, nullable=False)
    methodology = db.Column(db.String(100), nullable=True)  # Scoring methodology used
    components = db.Column(JSON, nullable=True)  # Detailed breakdown of score components
    confidence = db.Column(db.Float, nullable=True)  # Overall confidence in the score (0-1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to region
    region = db.relationship('Region')

# B2B Dataset Registry and Licensing System

class LicenseType(Enum):
    """Enumeration for license types"""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"

class DatasetCategory(Enum):
    """Enumeration for dataset categories"""
    MARKET_DATA = "market_data"
    ESG_DATA = "esg_data"
    NEWS_DATA = "news_data"
    ECONOMIC_DATA = "economic_data"
    INFRASTRUCTURE_DATA = "infrastructure_data"
    SDG_DATA = "sdg_data"
    GEOSPATIAL_DATA = "geospatial_data"
    ALTERNATIVE_DATA = "alternative_data"

class SubscriptionTier(Enum):
    """Enumeration for subscription tiers"""
    FREE = "free"
    STARTER = "starter"
    PROFESSIONAL = "professional"
    ENTERPRISE = "enterprise"
    CUSTOM = "custom"

class User(db.Model):
    """Enhanced user model for B2B clients"""
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False)
    company_name = db.Column(db.String(255), nullable=True)
    contact_name = db.Column(db.String(255), nullable=False)
    password_hash = db.Column(db.String(255), nullable=True)
    phone = db.Column(db.String(50), nullable=True)
    country = db.Column(db.String(100), nullable=True)
    industry = db.Column(db.String(100), nullable=True)
    company_size = db.Column(db.String(50), nullable=True)  # startup, sme, enterprise
    use_case = db.Column(db.Text, nullable=True)
    subscription_tier = db.Column(db.Enum(SubscriptionTier), default=SubscriptionTier.FREE)
    monthly_spend_limit = db.Column(db.Numeric(10, 2), default=0)
    current_monthly_spend = db.Column(db.Numeric(10, 2), default=0)
    is_active = db.Column(db.Boolean, default=True)
    is_verified = db.Column(db.Boolean, default=False)
    onboarded_at = db.Column(db.DateTime, nullable=True)
    last_active = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    api_keys = db.relationship('APIKey', backref='user', lazy='dynamic')
    subscriptions = db.relationship('Subscription', backref='user', lazy='dynamic')
    usage_logs = db.relationship('UsageLog', backref='user', lazy='dynamic')

class Dataset(db.Model):
    """Dataset registry for B2B data products"""
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(255), nullable=False)
    slug = db.Column(db.String(255), unique=True, nullable=False)
    description = db.Column(db.Text, nullable=False)
    category = db.Column(db.Enum(DatasetCategory), nullable=False)
    provider = db.Column(db.String(255), nullable=False)  # internal, jse, alpha_vantage, etc.
    version = db.Column(db.String(50), default="1.0.0")
    
    # Technical specifications
    schema_definition = db.Column(JSON, nullable=True)  # JSON schema for dataset structure
    update_frequency = db.Column(db.String(50), nullable=True)  # real-time, daily, weekly, monthly
    historical_depth = db.Column(db.String(100), nullable=True)  # "10 years", "since 2010", etc.
    geographic_coverage = db.Column(JSON, nullable=True)  # List of covered regions/countries
    data_formats = db.Column(JSON, nullable=True)  # json, csv, parquet, etc.
    api_endpoints = db.Column(JSON, nullable=True)  # Available API endpoints
    
    # Business information
    price_per_record = db.Column(db.Numeric(10, 4), nullable=True)
    price_per_api_call = db.Column(db.Numeric(10, 4), nullable=True)
    monthly_subscription_price = db.Column(db.Numeric(10, 2), nullable=True)
    annual_subscription_price = db.Column(db.Numeric(10, 2), nullable=True)
    
    # Quality metrics
    data_quality_score = db.Column(db.Float, nullable=True)  # 0-100
    completeness_score = db.Column(db.Float, nullable=True)  # 0-100
    accuracy_score = db.Column(db.Float, nullable=True)  # 0-100
    latency_score = db.Column(db.Float, nullable=True)  # 0-100
    
    # Status and metadata
    is_active = db.Column(db.Boolean, default=True)
    is_public = db.Column(db.Boolean, default=True)
    documentation_url = db.Column(db.String(255), nullable=True)
    sample_data_url = db.Column(db.String(255), nullable=True)
    last_updated = db.Column(db.DateTime, nullable=True)
    total_records = db.Column(db.BigInteger, default=0)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    licenses = db.relationship('DatasetLicense', backref='dataset', lazy='dynamic')
    subscriptions = db.relationship('Subscription', backref='dataset', lazy='dynamic')
    usage_logs = db.relationship('UsageLog', backref='dataset', lazy='dynamic')

class DatasetLicense(db.Model):
    """License configurations for datasets"""
    id = db.Column(db.Integer, primary_key=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=False)
    license_type = db.Column(db.Enum(LicenseType), nullable=False)
    name = db.Column(db.String(255), nullable=False)
    description = db.Column(db.Text, nullable=True)
    
    # Access controls
    rate_limit_per_minute = db.Column(db.Integer, default=60)
    rate_limit_per_day = db.Column(db.Integer, default=10000)
    rate_limit_per_month = db.Column(db.Integer, default=100000)
    max_records_per_request = db.Column(db.Integer, default=1000)
    historical_access_days = db.Column(db.Integer, nullable=True)  # None = unlimited
    
    # Pricing
    monthly_price = db.Column(db.Numeric(10, 2), nullable=True)
    annual_price = db.Column(db.Numeric(10, 2), nullable=True)
    price_per_record = db.Column(db.Numeric(10, 4), nullable=True)
    price_per_api_call = db.Column(db.Numeric(10, 4), nullable=True)
    
    # Features
    real_time_access = db.Column(db.Boolean, default=False)
    bulk_download = db.Column(db.Boolean, default=False)
    api_access = db.Column(db.Boolean, default=True)
    webhook_support = db.Column(db.Boolean, default=False)
    custom_queries = db.Column(db.Boolean, default=False)
    white_label_access = db.Column(db.Boolean, default=False)
    
    # Terms and conditions
    terms_of_use = db.Column(db.Text, nullable=True)
    redistribution_allowed = db.Column(db.Boolean, default=False)
    commercial_use_allowed = db.Column(db.Boolean, default=True)
    attribution_required = db.Column(db.Boolean, default=True)
    
    is_active = db.Column(db.Boolean, default=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class Subscription(db.Model):
    """User subscriptions to datasets"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=False)
    license_id = db.Column(db.Integer, db.ForeignKey('dataset_license.id'), nullable=False)
    
    # Subscription details
    subscription_type = db.Column(db.String(50), default="monthly")  # monthly, annual, pay-per-use
    status = db.Column(db.String(50), default="active")  # active, suspended, cancelled, expired
    
    # Billing
    monthly_price = db.Column(db.Numeric(10, 2), nullable=True)
    annual_price = db.Column(db.Numeric(10, 2), nullable=True)
    current_usage_cost = db.Column(db.Numeric(10, 2), default=0)
    
    # Subscription period
    start_date = db.Column(db.DateTime, nullable=False)
    end_date = db.Column(db.DateTime, nullable=True)
    auto_renew = db.Column(db.Boolean, default=True)
    
    # Usage tracking
    current_month_api_calls = db.Column(db.Integer, default=0)
    current_month_records_accessed = db.Column(db.BigInteger, default=0)
    total_api_calls = db.Column(db.BigInteger, default=0)
    total_records_accessed = db.Column(db.BigInteger, default=0)
    
    # Reset tracking
    last_usage_reset = db.Column(db.DateTime, default=datetime.utcnow)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    license = db.relationship('DatasetLicense', backref='subscriptions')

class UsageLog(db.Model):
    """Detailed usage tracking for billing and analytics"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=False)
    subscription_id = db.Column(db.Integer, db.ForeignKey('subscription.id'), nullable=True)
    api_key_id = db.Column(db.Integer, db.ForeignKey('api_key.id'), nullable=True)
    
    # Request details
    endpoint = db.Column(db.String(255), nullable=False)
    method = db.Column(db.String(10), nullable=False)
    records_returned = db.Column(db.Integer, default=0)
    response_size_bytes = db.Column(db.BigInteger, default=0)
    response_time_ms = db.Column(db.Integer, nullable=True)
    status_code = db.Column(db.Integer, nullable=False)
    
    # Cost calculation
    cost_amount = db.Column(db.Numeric(10, 4), default=0)
    cost_basis = db.Column(db.String(50), nullable=True)  # "per_record", "per_api_call", "subscription"
    
    # Request metadata
    ip_address = db.Column(db.String(50), nullable=True)
    user_agent = db.Column(db.String(500), nullable=True)
    query_parameters = db.Column(JSON, nullable=True)
    
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    subscription = db.relationship('Subscription', backref='usage_logs_rel')

class DataIngestionJob(db.Model):
    """Enhanced data ingestion job tracking"""
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(255), nullable=False)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=True)
    source_id = db.Column(db.Integer, db.ForeignKey('data_source.id'), nullable=True)
    
    # Job configuration
    job_type = db.Column(db.String(50), nullable=False)  # full_refresh, incremental, real_time
    schedule_cron = db.Column(db.String(100), nullable=True)
    is_active = db.Column(db.Boolean, default=True)
    
    # Execution tracking
    status = db.Column(db.String(50), default="pending")  # pending, running, completed, failed, cancelled
    start_time = db.Column(db.DateTime, nullable=True)
    end_time = db.Column(db.DateTime, nullable=True)
    next_run_time = db.Column(db.DateTime, nullable=True)
    
    # Data metrics
    records_processed = db.Column(db.BigInteger, default=0)
    records_inserted = db.Column(db.BigInteger, default=0)
    records_updated = db.Column(db.BigInteger, default=0)
    records_failed = db.Column(db.BigInteger, default=0)
    data_quality_score = db.Column(db.Float, nullable=True)
    
    # Error handling
    error_message = db.Column(db.Text, nullable=True)
    error_details = db.Column(JSON, nullable=True)
    retry_count = db.Column(db.Integer, default=0)
    max_retries = db.Column(db.Integer, default=3)
    
    # Performance metrics
    cpu_usage_percent = db.Column(db.Float, nullable=True)
    memory_usage_mb = db.Column(db.Float, nullable=True)
    execution_time_seconds = db.Column(db.Float, nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    dataset = db.relationship('Dataset', backref='ingestion_jobs')
    source = db.relationship('DataSource', backref='ingestion_jobs')

class DataQualityMetric(db.Model):
    """Data quality tracking for datasets"""
    id = db.Column(db.Integer, primary_key=True)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=False)
    
    # Quality dimensions
    completeness_score = db.Column(db.Float, nullable=True)  # % of non-null values
    accuracy_score = db.Column(db.Float, nullable=True)  # % of accurate values
    consistency_score = db.Column(db.Float, nullable=True)  # % of consistent values
    timeliness_score = db.Column(db.Float, nullable=True)  # freshness of data
    validity_score = db.Column(db.Float, nullable=True)  # % of valid format values
    uniqueness_score = db.Column(db.Float, nullable=True)  # % of unique values where expected
    
    # Aggregated scores
    overall_score = db.Column(db.Float, nullable=True)  # Weighted average
    
    # Detailed metrics
    total_records = db.Column(db.BigInteger, default=0)
    null_count = db.Column(db.BigInteger, default=0)
    duplicate_count = db.Column(db.BigInteger, default=0)
    invalid_format_count = db.Column(db.BigInteger, default=0)
    
    # Metadata
    measurement_date = db.Column(db.DateTime, nullable=False)
    methodology = db.Column(db.String(255), nullable=True)
    quality_rules = db.Column(JSON, nullable=True)  # Applied quality rules
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    dataset = db.relationship('Dataset', backref='quality_metrics')

class ClientOnboarding(db.Model):
    """Client onboarding process tracking"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    
    # Onboarding stages
    stage = db.Column(db.String(50), default="initial_contact")  # initial_contact, demo_scheduled, demo_completed, trial_started, converted, churned
    
    # Contact information
    lead_source = db.Column(db.String(100), nullable=True)  # website, referral, sales_call, etc.
    sales_rep = db.Column(db.String(255), nullable=True)
    demo_scheduled_date = db.Column(db.DateTime, nullable=True)
    demo_completed_date = db.Column(db.DateTime, nullable=True)
    
    # Requirements and notes
    requirements = db.Column(db.Text, nullable=True)
    use_cases = db.Column(JSON, nullable=True)
    budget_range = db.Column(db.String(100), nullable=True)
    decision_timeline = db.Column(db.String(100), nullable=True)
    
    # Follow-up tracking
    last_contact_date = db.Column(db.DateTime, nullable=True)
    next_follow_up_date = db.Column(db.DateTime, nullable=True)
    contact_attempts = db.Column(db.Integer, default=0)
    
    # Notes and communication log
    notes = db.Column(db.Text, nullable=True)
    communication_log = db.Column(JSON, nullable=True)
    
    # Conversion tracking
    trial_start_date = db.Column(db.DateTime, nullable=True)
    trial_end_date = db.Column(db.DateTime, nullable=True)
    conversion_date = db.Column(db.DateTime, nullable=True)
    churn_date = db.Column(db.DateTime, nullable=True)
    churn_reason = db.Column(db.String(255), nullable=True)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship('User', backref='onboarding_records')

class WebhookEndpoint(db.Model):
    """Webhook endpoints for real-time data delivery"""
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)
    dataset_id = db.Column(db.Integer, db.ForeignKey('dataset.id'), nullable=False)
    
    # Endpoint configuration
    url = db.Column(db.String(500), nullable=False)
    secret_key = db.Column(db.String(255), nullable=True)  # For signature verification
    is_active = db.Column(db.Boolean, default=True)
    
    # Event filters
    event_types = db.Column(JSON, nullable=True)  # Types of events to send
    filters = db.Column(JSON, nullable=True)  # Data filters (symbols, regions, etc.)
    
    # Delivery settings
    retry_count = db.Column(db.Integer, default=3)
    timeout_seconds = db.Column(db.Integer, default=30)
    
    # Status tracking
    last_delivery_attempt = db.Column(db.DateTime, nullable=True)
    last_successful_delivery = db.Column(db.DateTime, nullable=True)
    consecutive_failures = db.Column(db.Integer, default=0)
    total_deliveries = db.Column(db.BigInteger, default=0)
    total_failures = db.Column(db.BigInteger, default=0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    user = db.relationship('User', backref='webhook_endpoints')
    dataset = db.relationship('Dataset', backref='webhook_endpoints')