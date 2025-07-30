from app import db
from datetime import datetime
from sqlalchemy.dialects.postgresql import ARRAY, JSONB

class APIKey(db.Model):
    """API Key model for authentication"""
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    name = db.Column(db.String(100), nullable=False)
    user_id = db.Column(db.Integer, nullable=False)
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
    raw_data = db.Column(JSONB, nullable=True)  # Original data in JSON format
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
    raw_data = db.Column(JSONB, nullable=True)  # Original data in JSON format
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
    raw_data = db.Column(JSONB, nullable=True)  # Original data in JSON format
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
    raw_data = db.Column(JSONB, nullable=True)  # Original data in JSON format
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
    properties = db.Column(JSONB, nullable=True)  # Additional properties in JSON format
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
    raw_data = db.Column(JSONB, nullable=True)  # Original data in JSON format
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
    components = db.Column(JSONB, nullable=True)  # Detailed breakdown of score components
    confidence = db.Column(db.Float, nullable=True)  # Overall confidence in the score (0-1)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Define relationship to region
    region = db.relationship('Region')