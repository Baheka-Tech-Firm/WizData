from main import db
from datetime import datetime

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