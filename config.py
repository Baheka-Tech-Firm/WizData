"""
Configuration management for WizData platform
Implements 12-Factor App principles for environment-based configuration
"""

import os
from typing import Optional, Dict, Any
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

@dataclass
class DatabaseConfig:
    """Database configuration settings"""
    url: str
    pool_size: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 300
    pool_pre_ping: bool = True
    echo: bool = False

@dataclass
class RedisConfig:
    """Redis configuration for caching and rate limiting"""
    url: str
    decode_responses: bool = True
    socket_connect_timeout: int = 5
    socket_timeout: int = 5
    retry_on_timeout: bool = True
    health_check_interval: int = 30

@dataclass
class APIConfig:
    """API client configurations"""
    openai_api_key: Optional[str] = None
    alpha_vantage_api_key: Optional[str] = None
    bloomberg_api_key: Optional[str] = None
    refinitiv_api_key: Optional[str] = None
    refinitiv_api_secret: Optional[str] = None
    sp_global_api_key: Optional[str] = None

@dataclass
class RateLimitConfig:
    """Rate limiting configuration"""
    default_requests_per_minute: int = 60
    authenticated_requests_per_minute: int = 200
    burst_requests: int = 10
    window_size: int = 60  # seconds

@dataclass
class CacheConfig:
    """Caching configuration"""
    default_ttl: int = 300  # 5 minutes
    market_data_ttl: int = 60  # 1 minute for market data
    esg_data_ttl: int = 3600  # 1 hour for ESG data
    static_data_ttl: int = 86400  # 24 hours for static data

@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration"""
    metrics_enabled: bool = True
    tracing_enabled: bool = True
    log_level: str = "INFO"
    metrics_port: int = 9090
    health_check_interval: int = 30

class Config:
    """Main configuration class that loads settings from environment variables"""
    
    def __init__(self):
        self.environment = os.getenv("ENVIRONMENT", "development")
        self.debug = os.getenv("DEBUG", "false").lower() == "true"
        self.secret_key = os.getenv("SESSION_SECRET", self._generate_secret_key())
        
        # Database configuration
        self.database = DatabaseConfig(
            url=os.getenv("DATABASE_URL", "postgresql://localhost:5432/wizdata"),
            pool_size=int(os.getenv("DB_POOL_SIZE", "20")),
            pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
            pool_recycle=int(os.getenv("DB_POOL_RECYCLE", "300")),
            echo=os.getenv("DB_ECHO", "false").lower() == "true"
        )
        
        # Redis configuration for caching and rate limiting
        self.redis = RedisConfig(
            url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            socket_connect_timeout=int(os.getenv("REDIS_CONNECT_TIMEOUT", "5")),
            socket_timeout=int(os.getenv("REDIS_TIMEOUT", "5")),
            health_check_interval=int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
        )
        
        # API keys and external service configuration
        self.api = APIConfig(
            openai_api_key=os.getenv("OPENAI_API_KEY"),
            alpha_vantage_api_key=os.getenv("ALPHA_VANTAGE_API_KEY"),
            bloomberg_api_key=os.getenv("BLOOMBERG_API_KEY"),
            refinitiv_api_key=os.getenv("REFINITIV_API_KEY"),
            refinitiv_api_secret=os.getenv("REFINITIV_API_SECRET"),
            sp_global_api_key=os.getenv("SP_GLOBAL_API_KEY")
        )
        
        # Rate limiting configuration
        self.rate_limit = RateLimitConfig(
            default_requests_per_minute=int(os.getenv("RATE_LIMIT_RPM", "60")),
            authenticated_requests_per_minute=int(os.getenv("RATE_LIMIT_AUTH_RPM", "200")),
            burst_requests=int(os.getenv("RATE_LIMIT_BURST", "10")),
            window_size=int(os.getenv("RATE_LIMIT_WINDOW", "60"))
        )
        
        # Caching configuration
        self.cache = CacheConfig(
            default_ttl=int(os.getenv("CACHE_DEFAULT_TTL", "300")),
            market_data_ttl=int(os.getenv("CACHE_MARKET_TTL", "60")),
            esg_data_ttl=int(os.getenv("CACHE_ESG_TTL", "3600")),
            static_data_ttl=int(os.getenv("CACHE_STATIC_TTL", "86400"))
        )
        
        # Monitoring configuration
        self.monitoring = MonitoringConfig(
            metrics_enabled=os.getenv("METRICS_ENABLED", "true").lower() == "true",
            tracing_enabled=os.getenv("TRACING_ENABLED", "true").lower() == "true",
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            metrics_port=int(os.getenv("METRICS_PORT", "9090")),
            health_check_interval=int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))
        )
        
        # Data export directory
        self.data_export_dir = os.getenv("DATA_EXPORT_DIR", "data/exports")
        
        # Ensure data directories exist
        os.makedirs(self.data_export_dir, exist_ok=True)
        os.makedirs("data/cache", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
    
    def _generate_secret_key(self) -> str:
        """Generate a secret key for development environments"""
        if self.environment == "development":
            return "dev_secret_key_for_development_only_do_not_use_in_production"
        else:
            raise ValueError("SESSION_SECRET environment variable must be set for non-development environments")
    
    def get_api_key_status(self) -> Dict[str, bool]:
        """Get the status of all API key configurations"""
        return {
            "openai": bool(self.api.openai_api_key),
            "alpha_vantage": bool(self.api.alpha_vantage_api_key),
            "bloomberg": bool(self.api.bloomberg_api_key),
            "refinitiv": bool(self.api.refinitiv_api_key and self.api.refinitiv_api_secret),
            "sp_global": bool(self.api.sp_global_api_key)
        }
    
    @property
    def is_production(self) -> bool:
        """Check if running in production environment"""
        return self.environment.lower() == "production"
    
    @property
    def is_development(self) -> bool:
        """Check if running in development environment"""
        return self.environment.lower() == "development"

# Global configuration instance
config = Config()