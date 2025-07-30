"""
Symbol Registry Service - Central Metadata Management
Manages symbol definitions, metadata, and real-time availability
"""

from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import create_engine, Column, String, Integer, Boolean, DateTime, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, List, Any, Optional
import json
import uvicorn
import os

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://user:pass@localhost/wizdata_db')
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Database Models
class SymbolMetadata(Base):
    """Symbol metadata table"""
    __tablename__ = "symbol_metadata"
    
    symbol = Column(String, primary_key=True, index=True)
    name = Column(String, nullable=False)
    type = Column(String, nullable=False)  # stock, crypto, forex, commodity, index
    sector = Column(String)
    industry = Column(String)
    currency = Column(String)
    country = Column(String)
    primary_exchange = Column(String)
    market_cap = Column(Float)
    shares_outstanding = Column(Float)
    description = Column(Text)
    website = Column(String)
    ceo = Column(String)
    employees = Column(Integer)
    founded = Column(Integer)
    headquarters = Column(String)
    streaming_available = Column(Boolean, default=True)
    data_sources = Column(Text)  # JSON array of available sources
    last_updated = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)

# Create tables
Base.metadata.create_all(bind=engine)

# Pydantic Models
class SymbolInfo(BaseModel):
    symbol: str
    name: str
    type: str
    sector: Optional[str] = None
    industry: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    primary_exchange: Optional[str] = None
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    description: Optional[str] = None
    website: Optional[str] = None
    ceo: Optional[str] = None
    employees: Optional[int] = None
    founded: Optional[int] = None
    headquarters: Optional[str] = None
    streaming_available: bool = True
    data_sources: List[str] = []
    is_active: bool = True

class SymbolRegistryResponse(BaseModel):
    success: bool
    data: Any
    timestamp: str
    count: Optional[int] = None

# FastAPI app
app = FastAPI(
    title="WizData Symbol Registry Service",
    description="Central symbol metadata and registry management",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class SymbolRegistryService:
    """Central symbol registry and metadata service"""
    
    def __init__(self):
        self.initialize_default_symbols()
    
    def initialize_default_symbols(self):
        """Initialize default symbol definitions"""
        db = SessionLocal()
        try:
            # Check if symbols already exist
            existing_count = db.query(SymbolMetadata).count()
            if existing_count > 0:
                return
            
            # JSE symbols
            jse_symbols = [
                {
                    'symbol': 'JSE:NPN',
                    'name': 'Naspers Limited',
                    'type': 'stock',
                    'sector': 'Technology',
                    'industry': 'Internet Services',
                    'currency': 'ZAR',
                    'country': 'South Africa',
                    'primary_exchange': 'JSE',
                    'market_cap': 1500000000000,
                    'shares_outstanding': 430000000,
                    'description': 'Naspers is a global consumer internet group and technology investor.',
                    'website': 'https://www.naspers.com',
                    'ceo': 'Fabricio Bloisi',
                    'employees': 28000,
                    'founded': 1915,
                    'headquarters': 'Cape Town, South Africa',
                    'data_sources': ['jse_direct', 'alpha_vantage', 'yahoo_finance']
                },
                {
                    'symbol': 'JSE:PRX',
                    'name': 'Prosus N.V.',
                    'type': 'stock',
                    'sector': 'Technology',
                    'industry': 'Internet Services',
                    'currency': 'ZAR',
                    'country': 'Netherlands',
                    'primary_exchange': 'JSE',
                    'market_cap': 2000000000000,
                    'shares_outstanding': 1600000000,
                    'description': 'Prosus is a global consumer internet group spun off from Naspers.',
                    'website': 'https://www.prosus.com',
                    'data_sources': ['jse_direct', 'alpha_vantage']
                },
                {
                    'symbol': 'JSE:BHP',
                    'name': 'BHP Group Limited',
                    'type': 'stock',
                    'sector': 'Mining',
                    'industry': 'Diversified Mining',
                    'currency': 'ZAR',
                    'country': 'Australia',
                    'primary_exchange': 'JSE',
                    'market_cap': 850000000000,
                    'description': 'BHP is a leading global resources company.',
                    'website': 'https://www.bhp.com',
                    'data_sources': ['jse_direct', 'alpha_vantage', 'bloomberg']
                },
                {
                    'symbol': 'JSE:SOL',
                    'name': 'Sasol Limited',
                    'type': 'stock',
                    'sector': 'Energy',
                    'industry': 'Chemicals',
                    'currency': 'ZAR',
                    'country': 'South Africa',
                    'primary_exchange': 'JSE',
                    'market_cap': 320000000000,
                    'description': 'Sasol is an integrated chemicals and energy company.',
                    'website': 'https://www.sasol.com',
                    'data_sources': ['jse_direct', 'alpha_vantage']
                }
            ]
            
            # Cryptocurrency symbols
            crypto_symbols = [
                {
                    'symbol': 'BTC/USDT',
                    'name': 'Bitcoin',
                    'type': 'cryptocurrency',
                    'sector': 'Digital Assets',
                    'industry': 'Cryptocurrency',
                    'currency': 'USD',
                    'country': 'Global',
                    'primary_exchange': 'Binance',
                    'description': 'Bitcoin is the first and largest cryptocurrency by market capitalization.',
                    'streaming_available': True,
                    'data_sources': ['coingecko', 'binance', 'coinbase']
                },
                {
                    'symbol': 'ETH/USDT',
                    'name': 'Ethereum',
                    'type': 'cryptocurrency',
                    'sector': 'Digital Assets',
                    'industry': 'Cryptocurrency',
                    'currency': 'USD',
                    'country': 'Global',
                    'primary_exchange': 'Binance',
                    'description': 'Ethereum is a decentralized platform for smart contracts.',
                    'streaming_available': True,
                    'data_sources': ['coingecko', 'binance', 'coinbase']
                }
            ]
            
            # US stocks
            us_stocks = [
                {
                    'symbol': 'AAPL',
                    'name': 'Apple Inc.',
                    'type': 'stock',
                    'sector': 'Technology',
                    'industry': 'Consumer Electronics',
                    'currency': 'USD',
                    'country': 'United States',
                    'primary_exchange': 'NASDAQ',
                    'market_cap': 3000000000000,
                    'description': 'Apple designs, manufactures, and markets smartphones, personal computers, tablets, wearables, and accessories.',
                    'website': 'https://www.apple.com',
                    'ceo': 'Tim Cook',
                    'data_sources': ['alpha_vantage', 'yahoo_finance', 'polygon']
                },
                {
                    'symbol': 'GOOGL',
                    'name': 'Alphabet Inc.',
                    'type': 'stock',
                    'sector': 'Technology',
                    'industry': 'Internet Services',
                    'currency': 'USD',
                    'country': 'United States',
                    'primary_exchange': 'NASDAQ',
                    'market_cap': 1700000000000,
                    'description': 'Alphabet is the parent company of Google and other subsidiaries.',
                    'website': 'https://abc.xyz',
                    'ceo': 'Sundar Pichai',
                    'data_sources': ['alpha_vantage', 'yahoo_finance', 'polygon']
                }
            ]
            
            # Forex pairs
            forex_pairs = [
                {
                    'symbol': 'USD/ZAR',
                    'name': 'US Dollar / South African Rand',
                    'type': 'forex',
                    'sector': 'Currency',
                    'industry': 'Foreign Exchange',
                    'currency': 'ZAR',
                    'country': 'Global',
                    'primary_exchange': 'FX',
                    'description': 'USD/ZAR currency pair',
                    'streaming_available': True,
                    'data_sources': ['alpha_vantage', 'oanda', 'fxcm']
                },
                {
                    'symbol': 'EUR/USD',
                    'name': 'Euro / US Dollar',
                    'type': 'forex',
                    'sector': 'Currency',
                    'industry': 'Foreign Exchange',
                    'currency': 'USD',
                    'country': 'Global',
                    'primary_exchange': 'FX',
                    'description': 'EUR/USD currency pair',
                    'streaming_available': True,
                    'data_sources': ['alpha_vantage', 'oanda', 'fxcm']
                }
            ]
            
            # Insert all symbols
            all_symbols = jse_symbols + crypto_symbols + us_stocks + forex_pairs
            
            for symbol_data in all_symbols:
                symbol_data['data_sources'] = json.dumps(symbol_data.get('data_sources', []))
                db_symbol = SymbolMetadata(**symbol_data)
                db.add(db_symbol)
            
            db.commit()
            print(f"Initialized {len(all_symbols)} symbols in registry")
            
        except Exception as e:
            print(f"Error initializing symbols: {e}")
            db.rollback()
        finally:
            db.close()
    
    def get_symbol_info(self, db: Session, symbol: str) -> Optional[SymbolMetadata]:
        """Get symbol metadata"""
        return db.query(SymbolMetadata).filter(SymbolMetadata.symbol == symbol).first()
    
    def get_symbols_by_type(self, db: Session, symbol_type: str) -> List[SymbolMetadata]:
        """Get symbols by type"""
        return db.query(SymbolMetadata).filter(
            SymbolMetadata.type == symbol_type,
            SymbolMetadata.is_active == True
        ).all()
    
    def get_symbols_by_exchange(self, db: Session, exchange: str) -> List[SymbolMetadata]:
        """Get symbols by exchange"""
        return db.query(SymbolMetadata).filter(
            SymbolMetadata.primary_exchange == exchange,
            SymbolMetadata.is_active == True
        ).all()
    
    def search_symbols(self, db: Session, query: str, limit: int = 20) -> List[SymbolMetadata]:
        """Search symbols by name or symbol"""
        return db.query(SymbolMetadata).filter(
            db.or_(
                SymbolMetadata.symbol.ilike(f"%{query}%"),
                SymbolMetadata.name.ilike(f"%{query}%")
            ),
            SymbolMetadata.is_active == True
        ).limit(limit).all()
    
    def update_streaming_status(self, db: Session, symbol: str, available: bool):
        """Update streaming availability for symbol"""
        symbol_obj = self.get_symbol_info(db, symbol)
        if symbol_obj:
            symbol_obj.streaming_available = available
            symbol_obj.last_updated = datetime.utcnow()
            db.commit()
    
    def add_symbol(self, db: Session, symbol_data: SymbolInfo) -> SymbolMetadata:
        """Add new symbol to registry"""
        data_dict = symbol_data.dict()
        data_dict['data_sources'] = json.dumps(data_dict.get('data_sources', []))
        
        db_symbol = SymbolMetadata(**data_dict)
        db.add(db_symbol)
        db.commit()
        db.refresh(db_symbol)
        return db_symbol

# Service instance
registry_service = SymbolRegistryService()

# API Endpoints
@app.get("/symbols/{symbol}", response_model=SymbolRegistryResponse)
async def get_symbol(symbol: str, db: Session = Depends(get_db)):
    """Get detailed symbol information"""
    symbol_info = registry_service.get_symbol_info(db, symbol)
    if not symbol_info:
        raise HTTPException(status_code=404, detail="Symbol not found")
    
    # Convert data_sources back to list
    if symbol_info.data_sources:
        symbol_info.data_sources = json.loads(symbol_info.data_sources)
    
    return SymbolRegistryResponse(
        success=True,
        data=symbol_info.__dict__,
        timestamp=datetime.now().isoformat()
    )

@app.get("/symbols", response_model=SymbolRegistryResponse)
async def get_symbols(
    type: Optional[str] = None,
    exchange: Optional[str] = None,
    search: Optional[str] = None,
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db)
):
    """Get symbols with optional filtering"""
    if search:
        symbols = registry_service.search_symbols(db, search, limit)
    elif type:
        symbols = registry_service.get_symbols_by_type(db, type)
    elif exchange:
        symbols = registry_service.get_symbols_by_exchange(db, exchange)
    else:
        symbols = db.query(SymbolMetadata).filter(SymbolMetadata.is_active == True).limit(limit).all()
    
    # Convert to dict and parse data_sources
    result = []
    for symbol in symbols:
        symbol_dict = symbol.__dict__.copy()
        symbol_dict.pop('_sa_instance_state', None)
        if symbol_dict.get('data_sources'):
            symbol_dict['data_sources'] = json.loads(symbol_dict['data_sources'])
        result.append(symbol_dict)
    
    return SymbolRegistryResponse(
        success=True,
        data=result,
        timestamp=datetime.now().isoformat(),
        count=len(result)
    )

@app.get("/symbols/streaming/available", response_model=SymbolRegistryResponse)
async def get_streaming_symbols(db: Session = Depends(get_db)):
    """Get symbols available for real-time streaming"""
    symbols = db.query(SymbolMetadata).filter(
        SymbolMetadata.streaming_available == True,
        SymbolMetadata.is_active == True
    ).all()
    
    result = []
    for symbol in symbols:
        result.append({
            'symbol': symbol.symbol,
            'name': symbol.name,
            'type': symbol.type,
            'exchange': symbol.primary_exchange,
            'data_sources': json.loads(symbol.data_sources) if symbol.data_sources else []
        })
    
    return SymbolRegistryResponse(
        success=True,
        data=result,
        timestamp=datetime.now().isoformat(),
        count=len(result)
    )

@app.post("/symbols", response_model=SymbolRegistryResponse)
async def add_symbol(symbol_data: SymbolInfo, db: Session = Depends(get_db)):
    """Add new symbol to registry"""
    # Check if symbol already exists
    existing = registry_service.get_symbol_info(db, symbol_data.symbol)
    if existing:
        raise HTTPException(status_code=400, detail="Symbol already exists")
    
    new_symbol = registry_service.add_symbol(db, symbol_data)
    
    return SymbolRegistryResponse(
        success=True,
        data={"symbol": new_symbol.symbol, "status": "created"},
        timestamp=datetime.now().isoformat()
    )

@app.put("/symbols/{symbol}/streaming")
async def update_streaming_status(
    symbol: str,
    available: bool,
    db: Session = Depends(get_db)
):
    """Update streaming availability for symbol"""
    registry_service.update_streaming_status(db, symbol, available)
    
    return SymbolRegistryResponse(
        success=True,
        data={"symbol": symbol, "streaming_available": available},
        timestamp=datetime.now().isoformat()
    )

@app.get("/exchanges", response_model=SymbolRegistryResponse)
async def get_exchanges(db: Session = Depends(get_db)):
    """Get list of available exchanges"""
    exchanges = db.query(SymbolMetadata.primary_exchange).distinct().all()
    exchange_list = [ex[0] for ex in exchanges if ex[0]]
    
    return SymbolRegistryResponse(
        success=True,
        data=exchange_list,
        timestamp=datetime.now().isoformat(),
        count=len(exchange_list)
    )

@app.get("/types", response_model=SymbolRegistryResponse)
async def get_symbol_types(db: Session = Depends(get_db)):
    """Get list of available symbol types"""
    types = db.query(SymbolMetadata.type).distinct().all()
    type_list = [t[0] for t in types if t[0]]
    
    return SymbolRegistryResponse(
        success=True,
        data=type_list,
        timestamp=datetime.now().isoformat(),
        count=len(type_list)
    )

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {
        "status": "healthy",
        "service": "symbol-registry",
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    uvicorn.run(
        "symbol_registry_service:app",
        host="0.0.0.0",
        port=5002,
        reload=True,
        log_level="info"
    )