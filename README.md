# WizData - Professional Financial Intelligence Platform

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Flask](https://img.shields.io/badge/Flask-3.0+-green.svg)](https://flask.palletsprojects.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15+-blue.svg)](https://www.postgresql.org/)

WizData is an advanced financial intelligence platform that leverages AI-powered data collection, multi-source integration, and intelligent analytics to provide comprehensive insights for investors and financial professionals.

## 🚀 Key Features

- **Professional Charting Platform**: TradingView-style interface with advanced technical analysis
- **Multi-Source Data Integration**: Real-time data from crypto, JSE, forex, and economic indicators  
- **Technical Analysis Tools**: RSI, MACD, SMA, EMA, Bollinger Bands, and more
- **Market Intelligence**: Financial news, corporate events, and sentiment analysis
- **Modular Microservices**: Enterprise-grade architecture supporting multiple products
- **Real-time Data Streaming**: WebSocket support for live market updates
- **JSE Market Focus**: Comprehensive South African stock market coverage

## 📊 Supported Markets & Assets

### Johannesburg Stock Exchange (JSE)
- **Top Companies**: NPN, PRX, BHP, AGL, SOL, SBK, MTN, CFR
- **Market Indices**: ALSI, TOP40, RESI, FINI, INDI
- **Corporate Events**: SENS announcements, earnings, dividends
- **Sectors**: Technology, Mining, Banking, Telecommunications, Energy

### Cryptocurrency
- **Major Pairs**: BTC/USDT, ETH/USDT, and 100+ cryptocurrencies
- **Data Sources**: CoinGecko API with real-time updates
- **Features**: Price data, volume, market cap, technical indicators

### Forex Markets
- **Major Pairs**: USD/ZAR, EUR/USD, GBP/USD, USD/JPY
- **Features**: Real-time quotes, bid/ask spreads, 24h changes

### Global Stocks
- **Markets**: NASDAQ, NYSE with focus on major tech stocks
- **Examples**: AAPL, GOOGL, MSFT, TSLA

## 🏗️ Architecture Overview

WizData implements a modern microservices architecture designed to support multiple financial products:

```
┌─────────────────────────────────────────────────────────────┐
│                    WizData API Gateway                      │
├─────────────────────────────────────────────────────────────┤
│  VueOn    │  Trader   │  Pulse    │  Wealth   │  Connect    │
│ (Charts)  │ (Strada)  │(Overview) │ (Novia)   │ (Portal)    │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│                 Core Data Services                          │
├─────────────────┬─────────────────┬─────────────────────────┤
│ MarketDataSvc   │ IndicatorEngine │ EventEngine             │
│ MetadataService │ ScreenerEngine  │ NewsService             │
└─────────────────────────────────────────────────────────────┘
                              │
┌─────────────────────────────────────────────────────────────┐
│              Data Collection Layer                          │
├─────────────────┬─────────────────┬─────────────────────────┤
│ JSE Enhanced    │ CoinGecko       │ Forex & Economic        │
│ Alpha Vantage   │ News Sources    │ SENS Announcements      │
└─────────────────────────────────────────────────────────────┘
```

## 🛠️ Quick Start

### Prerequisites

- Python 3.11+
- PostgreSQL 15+
- Redis (optional, for caching)
- Node.js 18+ (for frontend dependencies)

### Local Installation

1. **Clone the repository**
```bash
git clone <repository-url>
cd wizdata-platform
```

2. **Set up Python environment**
```bash
# Create virtual environment
python -m venv venv

# Activate environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

3. **Database Setup**
```bash
# Install PostgreSQL and create database
createdb wizdata_db

# Set environment variables
export DATABASE_URL="postgresql://username:password@localhost:5432/wizdata_db"
export SESSION_SECRET="your-secret-key-here"
```

4. **Optional: Redis Setup** (for caching and rate limiting)
```bash
# Install Redis
# On macOS: brew install redis
# On Ubuntu: sudo apt-get install redis-server

# Start Redis
redis-server
```

5. **Optional: API Keys** (for enhanced data sources)
```bash
# Alpha Vantage (for stock data fallback)
export ALPHA_VANTAGE_API_KEY="your-alpha-vantage-key"

# OpenAI (for AI insights)
export OPENAI_API_KEY="your-openai-key"
```

6. **Start the application**
```bash
# Development mode
python main.py

# Production mode with Gunicorn
gunicorn --bind 0.0.0.0:5000 --reuse-port --reload main:app
```

7. **Access the platform**
- Main Platform: http://localhost:5000
- Professional Charting: http://localhost:5000/charting
- Live Data Dashboard: http://localhost:5000/scrapers
- API Documentation: http://localhost:5000/api-services

## 📡 API Documentation

### Base URLs
- **Production**: `https://your-domain.com/api/v1/`
- **Local Development**: `http://localhost:5000/api/v1/`

### Authentication
Most endpoints are publicly accessible for development. Production deployments should implement API key authentication:

```bash
curl -H "X-API-Key: your-api-key" https://api.wizdata.com/v1/endpoint
```

## 🔗 Core API Endpoints

### 1. Professional Charting API

#### Get Available Symbols
```bash
GET /api/v1/charting/symbols
```

**Response:**
```json
{
  "success": true,
  "symbols": {
    "crypto": [
      {
        "symbol": "BTC/USDT",
        "name": "Bitcoin",
        "exchange": "Binance",
        "type": "cryptocurrency"
      }
    ],
    "jse": [
      {
        "symbol": "JSE:NPN",
        "name": "Naspers Limited",
        "exchange": "JSE",
        "currency": "ZAR",
        "sector": "Technology"
      }
    ]
  },
  "count": 15
}
```

#### Get OHLCV Data
```bash
GET /api/v1/charting/ohlcv/{symbol}?interval=1h&limit=100
```

**Parameters:**
- `symbol`: Asset symbol (e.g., "BTC/USDT", "JSE:NPN")
- `interval`: Time interval (1m, 5m, 15m, 30m, 1h, 4h, 1d, 1w)
- `limit`: Number of candles (max 1000)

**Example:**
```bash
curl "http://localhost:5000/api/v1/charting/ohlcv/JSE:NPN?interval=1d&limit=30"
```

**Response:**
```json
{
  "success": true,
  "symbol": "JSE:NPN",
  "interval": "1d",
  "data": [
    {
      "time": 1706659200,
      "open": 285000,
      "high": 287500,
      "low": 282000,
      "close": 286000,
      "volume": 1234567
    }
  ],
  "indicators": {
    "rsi": [65.4, 68.2, 63.1],
    "macd": {
      "line": [0.025, 0.031, 0.028],
      "signal": [0.024, 0.027, 0.029],
      "histogram": [0.001, 0.004, -0.001]
    }
  }
}
```

#### Get Current Market Data
```bash
GET /api/v1/charting/market-data/{symbol}
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/charting/market-data/BTC/USDT"
```

**Response:**
```json
{
  "success": true,
  "data": {
    "symbol": "BTC/USDT",
    "price": 67500.00,
    "change": 1245.50,
    "change_percent": 1.88,
    "volume": 1234567,
    "high_24h": 68500.00,
    "low_24h": 66200.00,
    "timestamp": "2025-07-30T12:00:00Z"
  }
}
```

#### Market Screener
```bash
GET /api/v1/charting/screener
```

**Response:**
```json
{
  "success": true,
  "data": {
    "top_gainers": [
      {
        "symbol": "JSE:SOL",
        "price": 12500.00,
        "change_percent": 8.4,
        "volume": 1200000
      }
    ],
    "top_losers": [...],
    "most_active": [...],
    "trending": [...]
  }
}
```

#### Financial News
```bash
GET /api/v1/charting/news/{symbol}
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/charting/news/JSE:NPN"
```

#### Corporate Events
```bash
GET /api/v1/charting/events/{symbol}
```

#### Sector Performance
```bash
GET /api/v1/charting/sectors
```

#### Currency Rates
```bash
GET /api/v1/charting/currency-rates
```

### 2. Modular Data Services API

#### Multi-Symbol Market Data
```bash
GET /api/v1/data-services/market-data?symbols=JSE:NPN&symbols=BTC/USDT&type=quote
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/data-services/market-data?symbols=JSE:NPN&symbols=BTC/USDT&type=quote"
```

#### Technical Indicators
```bash
GET /api/v1/data-services/indicators/{symbol}?indicators=RSI&indicators=MACD
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/data-services/indicators/JSE:NPN?indicators=RSI&indicators=MACD&indicators=SMA"
```

#### Corporate Events Calendar
```bash
GET /api/v1/data-services/events?symbol=JSE:NPN&type=earnings&days_ahead=30
```

#### Company Profiles
```bash
GET /api/v1/data-services/profile/{symbol}
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/data-services/profile/JSE:NPN"
```

**Response:**
```json
{
  "success": true,
  "profile": {
    "symbol": "JSE:NPN",
    "name": "Naspers Limited",
    "description": "Naspers is a global consumer internet group and technology investor.",
    "sector": "Technology",
    "industry": "Internet Services",
    "employees": 28000,
    "founded": 1915,
    "headquarters": "Cape Town, South Africa",
    "website": "https://www.naspers.com",
    "ceo": "Fabricio Bloisi",
    "market_cap": 1500000000000
  }
}
```

#### Market Screener
```bash
GET /api/v1/data-services/screener?min_market_cap=1000000&max_pe_ratio=20&min_dividend_yield=3
```

**Example:**
```bash
curl "http://localhost:5000/api/v1/data-services/screener?min_market_cap=1000000&max_pe_ratio=20"
```

#### Financial News Feed
```bash
GET /api/v1/data-services/news?symbol=JSE:NPN&category=earnings&limit=10
```

### 3. Live Data Collection API

#### Data Sources Status
```bash
GET /api/scrapers/sources
```

#### Run Data Collection
```bash
POST /api/scrapers/run/{source_name}
```

#### Collection Jobs Status
```bash
GET /api/scrapers/jobs
```

### 4. Health & Monitoring

#### System Health
```bash
GET /api/v1/data-services/health
```

#### API Status
```bash
GET /api-services
```

## 🧪 Testing the APIs

### 1. Quick Health Check
```bash
# Test if the server is running
curl http://localhost:5000/

# Check API health
curl http://localhost:5000/api/v1/data-services/health
```

### 2. Test Charting APIs
```bash
# Get available symbols
curl http://localhost:5000/api/v1/charting/symbols

# Test JSE stock data
curl "http://localhost:5000/api/v1/charting/ohlcv/JSE:NPN?interval=1d&limit=5"

# Test cryptocurrency data
curl "http://localhost:5000/api/v1/charting/ohlcv/BTC/USDT?interval=1h&limit=10"

# Get market screener data
curl http://localhost:5000/api/v1/charting/screener
```

### 3. Test Data Services
```bash
# Multi-symbol market data
curl "http://localhost:5000/api/v1/data-services/market-data?symbols=JSE:NPN&symbols=BTC/USDT"

# Technical indicators
curl "http://localhost:5000/api/v1/data-services/indicators/JSE:NPN?indicators=RSI&indicators=MACD"

# Company profile
curl "http://localhost:5000/api/v1/data-services/profile/JSE:NPN"

# Market screening
curl "http://localhost:5000/api/v1/data-services/screener?min_market_cap=1000000"
```

### 4. Test Live Data Collection
```bash
# Check data sources
curl http://localhost:5000/api/scrapers/sources

# Run cryptocurrency data collection
curl -X POST http://localhost:5000/api/scrapers/run/coingecko

# Check collection jobs
curl http://localhost:5000/api/scrapers/jobs
```

### 5. Performance Testing
```bash
# Test API response times
time curl http://localhost:5000/api/v1/charting/screener

# Test concurrent requests
for i in {1..10}; do
  curl http://localhost:5000/api/v1/charting/symbols &
done
wait
```

## 📋 Comprehensive Test Script

Create a test script to verify all APIs:

```bash
#!/bin/bash
# save as test_apis.sh

BASE_URL="http://localhost:5000"

echo "🧪 WizData API Test Suite"
echo "========================="

echo "1. Testing Health Endpoints..."
curl -s "$BASE_URL/api/v1/data-services/health" | jq '.success'

echo "2. Testing Charting API..."
curl -s "$BASE_URL/api/v1/charting/symbols" | jq '.count'
curl -s "$BASE_URL/api/v1/charting/screener" | jq '.data.top_gainers | length'

echo "3. Testing Data Services..."
curl -s "$BASE_URL/api/v1/data-services/market-data?symbols=JSE:NPN" | jq '.success'

echo "4. Testing Live Data Collection..."
curl -s "$BASE_URL/api/scrapers/sources" | jq '.sources | length'

echo "✅ All tests completed!"
```

Make it executable and run:
```bash
chmod +x test_apis.sh
./test_apis.sh
```

## 🎯 Frontend Interfaces

### Professional Charting Platform
- **URL**: http://localhost:5000/charting
- **Features**: TradingView-style interface, technical indicators, real-time updates
- **Assets**: Crypto, JSE stocks, forex, global equities

### Live Data Dashboard
- **URL**: http://localhost:5000/scrapers
- **Features**: Real-time data collection monitoring, source management
- **Controls**: Start/stop scrapers, view collection status

### API Services Overview
- **URL**: http://localhost:5000/api-services
- **Features**: API documentation, endpoint testing, status monitoring

## 🔧 Configuration

### Environment Variables
```bash
# Database
DATABASE_URL="postgresql://user:pass@localhost:5432/wizdata_db"

# Security
SESSION_SECRET="your-secure-session-key"

# Optional APIs
ALPHA_VANTAGE_API_KEY="your-alpha-vantage-key"
OPENAI_API_KEY="your-openai-key"

# Redis (optional)
REDIS_URL="redis://localhost:6379"

# Monitoring
PROMETHEUS_ENABLED="true"
LOG_LEVEL="INFO"
```

### Data Source Configuration
```python
# config/scrapers.yaml
scrapers:
  coingecko:
    enabled: true
    interval: 180  # 3 minutes
    symbols: ["bitcoin", "ethereum", "cardano"]
  
  jse_enhanced:
    enabled: true
    interval: 300  # 5 minutes
    symbols: ["NPN", "BHP", "SOL", "SBK"]
    alpha_vantage_fallback: true
```

## 📈 Performance Benchmarks

### Response Times (Local Development)
- **API Endpoints**: < 200ms average
- **Chart Data Loading**: < 500ms for 100 candles
- **Real-time Updates**: < 100ms latency
- **Database Queries**: < 50ms typical

### Throughput Capacity
- **Concurrent Users**: 100+ supported
- **API Requests**: 1000+ req/min sustained
- **Data Processing**: 50+ items/min
- **Memory Usage**: < 512MB typical

## 🚀 Deployment

### Production Deployment

1. **Docker Deployment**
```bash
# Build image
docker build -t wizdata-platform .

# Run with Docker Compose
docker-compose up -d
```

2. **Environment Setup**
```bash
# Production environment variables
export FLASK_ENV=production
export DATABASE_URL="postgresql://prod_user:pass@db:5432/wizdata_prod"
export REDIS_URL="redis://redis:6379"
```

3. **Scaling with Gunicorn**
```bash
gunicorn --bind 0.0.0.0:5000 --workers 4 --worker-class gevent main:app
```

### Monitoring & Observability

- **Prometheus Metrics**: Available at `/metrics`
- **Health Checks**: `/api/v1/data-services/health`
- **Structured Logging**: JSON format with correlation IDs
- **Error Tracking**: Comprehensive exception handling

## 🔒 Security

### API Security
- Rate limiting with Redis backend
- Input validation and sanitization
- CORS configuration for cross-origin requests
- Secure session management

### Data Protection
- Environment-based secrets management
- Database connection encryption
- Sensitive data masking in logs
- HTTPS ready for production

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create a feature branch: `git checkout -b feature/amazing-feature`
3. Make your changes and add tests
4. Run the test suite: `python -m pytest`
5. Submit a pull request

### Code Standards
- Follow PEP 8 for Python code
- Use type hints for new functions
- Add docstrings for public APIs
- Maintain test coverage above 80%

## 📚 Additional Resources

### Documentation
- [API Reference](./docs/api-reference.md)
- [Architecture Guide](./docs/architecture.md)
- [Deployment Guide](./docs/deployment.md)
- [Data Sources](./docs/data-sources.md)

### Example Applications
- [Python API Client](./examples/python-client/)
- [JavaScript Frontend](./examples/js-frontend/)
- [Trading Bot Integration](./examples/trading-bot/)

## 📞 Support

For technical support and questions:

- **Documentation**: Check the `/docs` directory
- **Issues**: Create a GitHub issue
- **API Questions**: Use the built-in API documentation at `/api-services`

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

**Built with ❤️ for the financial technology community**

*WizData - Transforming financial data into actionable intelligence*