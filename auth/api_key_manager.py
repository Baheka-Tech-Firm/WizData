"""
API Key Management System for Inter-Product Integration
Manages scoped API keys for secure access between WizData and client products
"""

from sqlalchemy import create_engine, Column, String, DateTime, Boolean, Text, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from datetime import datetime, timedelta
import secrets
import hashlib
import json
import os
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass
from enum import Enum

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class ProductType(Enum):
    """WizData ecosystem products"""
    VUEON = "vueon"      # Charts platform
    TRADER = "trader"    # Strada trading platform  
    PULSE = "pulse"      # Market overview
    WEALTH = "wealth"    # Novia wealth management
    CONNECT = "connect"  # Communication & alerts

class AccessScope(Enum):
    """API access scopes"""
    MARKET_DATA = "market_data"
    CHARTING = "charting"
    INDICATORS = "indicators"
    NEWS = "news"
    EVENTS = "events"
    STREAMING = "streaming"
    SCREENER = "screener"
    PROFILES = "profiles"
    ADMIN = "admin"

@dataclass
class EndpointPermission:
    """Endpoint permission definition"""
    path: str
    methods: List[str]
    description: str

class APIKey(Base):
    """API Key database model"""
    __tablename__ = "api_keys"
    
    key_id = Column(String, primary_key=True)
    key_hash = Column(String, nullable=False, unique=True)
    product = Column(String, nullable=False)  # ProductType enum value
    name = Column(String, nullable=False)
    description = Column(Text)
    scopes = Column(Text, nullable=False)  # JSON array of scopes
    rate_limit = Column(Integer, default=1000)  # requests per hour
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used = Column(DateTime)
    expires_at = Column(DateTime)
    usage_count = Column(Integer, default=0)
    
class APIKeyUsage(Base):
    """API Key usage tracking"""
    __tablename__ = "api_key_usage"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    key_id = Column(String, nullable=False)
    endpoint = Column(String, nullable=False)
    method = Column(String, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    response_code = Column(Integer)
    response_time_ms = Column(Integer)
    ip_address = Column(String)

# Create tables
Base.metadata.create_all(bind=engine)

class APIKeyManager:
    """Centralized API key management system"""
    
    def __init__(self):
        self.scope_permissions = self._define_scope_permissions()
        self.product_default_scopes = self._define_product_scopes()
    
    def _define_scope_permissions(self) -> Dict[AccessScope, List[EndpointPermission]]:
        """Define what endpoints each scope can access"""
        return {
            AccessScope.MARKET_DATA: [
                EndpointPermission("/api/v1/charting/market-data/*", ["GET"], "Current market quotes"),
                EndpointPermission("/api/v1/data-services/market-data", ["GET"], "Multi-symbol market data"),
                EndpointPermission("/api/scrapers/sources", ["GET"], "Data sources status"),
            ],
            AccessScope.CHARTING: [
                EndpointPermission("/api/v1/charting/symbols", ["GET"], "Available symbols"),
                EndpointPermission("/api/v1/charting/ohlcv/*", ["GET"], "OHLCV candlestick data"),
                EndpointPermission("/api/v1/charting/screener", ["GET"], "Market screener"),
                EndpointPermission("/api/v1/charting/sectors", ["GET"], "Sector performance"),
                EndpointPermission("/api/v1/charting/currency-rates", ["GET"], "Currency rates"),
            ],
            AccessScope.INDICATORS: [
                EndpointPermission("/api/v1/data-services/indicators/*", ["GET"], "Technical indicators"),
                EndpointPermission("/api/v1/charting/ohlcv/*", ["GET"], "OHLCV data for indicators"),
            ],
            AccessScope.NEWS: [
                EndpointPermission("/api/v1/charting/news/*", ["GET"], "Financial news"),
                EndpointPermission("/api/v1/data-services/news", ["GET"], "News feed"),
            ],
            AccessScope.EVENTS: [
                EndpointPermission("/api/v1/charting/events/*", ["GET"], "Corporate events"),
                EndpointPermission("/api/v1/data-services/events", ["GET"], "Events calendar"),
            ],
            AccessScope.STREAMING: [
                EndpointPermission("/ws", ["GET"], "WebSocket streaming"),
                EndpointPermission("/api/websocket/*", ["GET"], "Streaming API"),
            ],
            AccessScope.SCREENER: [
                EndpointPermission("/api/v1/charting/screener", ["GET"], "Market screener"),
                EndpointPermission("/api/v1/data-services/screener", ["GET"], "Advanced screener"),
            ],
            AccessScope.PROFILES: [
                EndpointPermission("/api/v1/data-services/profile/*", ["GET"], "Company profiles"),
                EndpointPermission("/api/v1/charting/symbols", ["GET"], "Symbol metadata"),
            ],
            AccessScope.ADMIN: [
                EndpointPermission("/api/*", ["GET", "POST", "PUT", "DELETE"], "Full API access"),
                EndpointPermission("/api-services", ["GET"], "API documentation"),
            ]
        }
    
    def _define_product_scopes(self) -> Dict[ProductType, List[AccessScope]]:
        """Define default scopes for each product"""
        return {
            ProductType.VUEON: [
                AccessScope.CHARTING,
                AccessScope.INDICATORS,
                AccessScope.EVENTS,
                AccessScope.NEWS
            ],
            ProductType.TRADER: [
                AccessScope.MARKET_DATA,
                AccessScope.SCREENER,
                AccessScope.STREAMING,
                AccessScope.INDICATORS
            ],
            ProductType.PULSE: [
                AccessScope.CHARTING,
                AccessScope.SCREENER,
                AccessScope.MARKET_DATA
            ],
            ProductType.WEALTH: [
                AccessScope.PROFILES,
                AccessScope.EVENTS,
                AccessScope.INDICATORS,
                AccessScope.MARKET_DATA
            ],
            ProductType.CONNECT: [
                AccessScope.NEWS,
                AccessScope.EVENTS,
                AccessScope.MARKET_DATA
            ]
        }
    
    def generate_api_key(
        self,
        product: ProductType,
        name: str,
        description: str = "",
        custom_scopes: Optional[List[AccessScope]] = None,
        rate_limit: int = 1000,
        expires_days: Optional[int] = None
    ) -> Dict[str, Any]:
        """Generate new API key for a product"""
        
        # Generate secure key
        key_value = f"wiz_{product.value}_{secrets.token_urlsafe(32)}"
        key_id = f"key_{secrets.token_hex(8)}"
        
        # Hash the key for storage
        key_hash = hashlib.sha256(key_value.encode()).hexdigest()
        
        # Determine scopes
        scopes = custom_scopes or self.product_default_scopes.get(product, [])
        scopes_json = json.dumps([scope.value for scope in scopes])
        
        # Calculate expiration
        expires_at = None
        if expires_days:
            expires_at = datetime.utcnow() + timedelta(days=expires_days)
        
        # Create database entry
        db = SessionLocal()
        try:
            api_key = APIKey(
                key_id=key_id,
                key_hash=key_hash,
                product=product.value,
                name=name,
                description=description,
                scopes=scopes_json,
                rate_limit=rate_limit,
                expires_at=expires_at
            )
            
            db.add(api_key)
            db.commit()
            
            return {
                "key_id": key_id,
                "api_key": key_value,  # Only returned once!
                "product": product.value,
                "name": name,
                "scopes": [scope.value for scope in scopes],
                "rate_limit": rate_limit,
                "expires_at": expires_at.isoformat() if expires_at else None,
                "created_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            db.rollback()
            raise e
        finally:
            db.close()
    
    def validate_api_key(self, api_key: str, endpoint: str, method: str) -> Dict[str, Any]:
        """Validate API key and check permissions"""
        
        # Hash the provided key
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        db = SessionLocal()
        try:
            # Find the API key
            key_record = db.query(APIKey).filter(
                APIKey.key_hash == key_hash,
                APIKey.is_active == True
            ).first()
            
            if not key_record:
                return {
                    "valid": False,
                    "error": "Invalid API key",
                    "code": "INVALID_KEY"
                }
            
            # Check expiration
            if key_record.expires_at and datetime.utcnow() > key_record.expires_at:
                return {
                    "valid": False,
                    "error": "API key expired",
                    "code": "EXPIRED_KEY"
                }
            
            # Parse scopes
            scopes = [AccessScope(scope) for scope in json.loads(key_record.scopes)]
            
            # Check endpoint permission
            if not self._check_endpoint_permission(scopes, endpoint, method):
                return {
                    "valid": False,
                    "error": "Insufficient permissions for this endpoint",
                    "code": "INSUFFICIENT_PERMISSIONS",
                    "required_scopes": self._get_required_scopes(endpoint, method)
                }
            
            # Update usage
            key_record.last_used = datetime.utcnow()
            key_record.usage_count += 1
            db.commit()
            
            return {
                "valid": True,
                "key_id": key_record.key_id,
                "product": key_record.product,
                "scopes": [scope.value for scope in scopes],
                "rate_limit": key_record.rate_limit
            }
            
        except Exception as e:
            db.rollback()
            return {
                "valid": False,
                "error": f"Validation error: {str(e)}",
                "code": "VALIDATION_ERROR"
            }
        finally:
            db.close()
    
    def _check_endpoint_permission(self, scopes: List[AccessScope], endpoint: str, method: str) -> bool:
        """Check if scopes allow access to endpoint"""
        
        for scope in scopes:
            if scope == AccessScope.ADMIN:
                return True  # Admin has access to everything
            
            permissions = self.scope_permissions.get(scope, [])
            for permission in permissions:
                if self._match_endpoint_pattern(permission.path, endpoint) and method in permission.methods:
                    return True
        
        return False
    
    def _match_endpoint_pattern(self, pattern: str, endpoint: str) -> bool:
        """Match endpoint against pattern with wildcards"""
        if pattern.endswith("/*"):
            return endpoint.startswith(pattern[:-2])
        elif pattern.endswith("*"):
            return endpoint.startswith(pattern[:-1])
        else:
            return pattern == endpoint
    
    def _get_required_scopes(self, endpoint: str, method: str) -> List[str]:
        """Get required scopes for an endpoint"""
        required = []
        
        for scope, permissions in self.scope_permissions.items():
            for permission in permissions:
                if self._match_endpoint_pattern(permission.path, endpoint) and method in permission.methods:
                    required.append(scope.value)
        
        return required
    
    def revoke_api_key(self, key_id: str) -> bool:
        """Revoke an API key"""
        db = SessionLocal()
        try:
            key_record = db.query(APIKey).filter(APIKey.key_id == key_id).first()
            if key_record:
                key_record.is_active = False
                db.commit()
                return True
            return False
        except:
            db.rollback()
            return False
        finally:
            db.close()
    
    def list_api_keys(self, product: Optional[ProductType] = None) -> List[Dict[str, Any]]:
        """List API keys (without actual key values)"""
        db = SessionLocal()
        try:
            query = db.query(APIKey)
            if product:
                query = query.filter(APIKey.product == product.value)
            
            keys = query.all()
            
            result = []
            for key in keys:
                result.append({
                    "key_id": key.key_id,
                    "product": key.product,
                    "name": key.name,
                    "description": key.description,
                    "scopes": json.loads(key.scopes),
                    "rate_limit": key.rate_limit,
                    "is_active": key.is_active,
                    "created_at": key.created_at.isoformat(),
                    "last_used": key.last_used.isoformat() if key.last_used else None,
                    "expires_at": key.expires_at.isoformat() if key.expires_at else None,
                    "usage_count": key.usage_count
                })
            
            return result
            
        finally:
            db.close()
    
    def get_usage_stats(self, key_id: str, days: int = 7) -> Dict[str, Any]:
        """Get usage statistics for an API key"""
        db = SessionLocal()
        try:
            since_date = datetime.utcnow() - timedelta(days=days)
            
            usage_records = db.query(APIKeyUsage).filter(
                APIKeyUsage.key_id == key_id,
                APIKeyUsage.timestamp >= since_date
            ).all()
            
            # Aggregate statistics
            total_requests = len(usage_records)
            unique_endpoints = len(set(record.endpoint for record in usage_records))
            avg_response_time = sum(record.response_time_ms for record in usage_records if record.response_time_ms) / max(1, total_requests)
            
            # Error rate
            error_count = sum(1 for record in usage_records if record.response_code >= 400)
            error_rate = (error_count / max(1, total_requests)) * 100
            
            # Hourly breakdown
            hourly_usage = {}
            for record in usage_records:
                hour_key = record.timestamp.strftime("%Y-%m-%d %H:00")
                hourly_usage[hour_key] = hourly_usage.get(hour_key, 0) + 1
            
            return {
                "key_id": key_id,
                "period_days": days,
                "total_requests": total_requests,
                "unique_endpoints": unique_endpoints,
                "avg_response_time_ms": round(avg_response_time, 2),
                "error_rate_percent": round(error_rate, 2),
                "hourly_usage": hourly_usage
            }
            
        finally:
            db.close()
    
    def track_usage(self, key_id: str, endpoint: str, method: str, response_code: int, response_time_ms: int, ip_address: str):
        """Track API key usage"""
        db = SessionLocal()
        try:
            usage = APIKeyUsage(
                key_id=key_id,
                endpoint=endpoint,
                method=method,
                response_code=response_code,
                response_time_ms=response_time_ms,
                ip_address=ip_address
            )
            
            db.add(usage)
            db.commit()
            
        except:
            db.rollback()
        finally:
            db.close()

# Initialize the manager
api_key_manager = APIKeyManager()

def setup_default_api_keys():
    """Set up default API keys for all products"""
    
    default_keys = [
        {
            "product": ProductType.VUEON,
            "name": "VueOn Charts Platform",
            "description": "Primary API key for VueOn charting platform access"
        },
        {
            "product": ProductType.TRADER,
            "name": "Trader (Strada) Platform",
            "description": "Real-time trading platform API access"
        },
        {
            "product": ProductType.PULSE,
            "name": "Pulse Market Overview",
            "description": "Market overview and sector analysis access"
        },
        {
            "product": ProductType.WEALTH,
            "name": "Wealth (Novia) Management",
            "description": "Wealth management platform API access"
        },
        {
            "product": ProductType.CONNECT,
            "name": "Connect Communication Platform",
            "description": "News and alerts communication platform access"
        }
    ]
    
    generated_keys = []
    
    for key_config in default_keys:
        try:
            key_info = api_key_manager.generate_api_key(
                product=key_config["product"],
                name=key_config["name"],
                description=key_config["description"],
                rate_limit=5000,  # Higher limit for internal products
                expires_days=365  # 1 year expiration
            )
            generated_keys.append(key_info)
            
        except Exception as e:
            print(f"Error generating key for {key_config['product'].value}: {e}")
    
    return generated_keys

if __name__ == "__main__":
    # Generate default keys
    print("Setting up default API keys for WizData ecosystem...")
    keys = setup_default_api_keys()
    
    print(f"\n✅ Generated {len(keys)} API keys:")
    for key in keys:
        print(f"\n🔑 {key['name']}")
        print(f"   Product: {key['product']}")
        print(f"   Key: {key['api_key']}")
        print(f"   Scopes: {', '.join(key['scopes'])}")
        print(f"   Rate Limit: {key['rate_limit']} req/hour")