# WizData B2B Dataset Registry & Licensing System

WizData has been transformed into a comprehensive B2B data intelligence platform for African finance, ESG metrics, and market intelligence. This system provides enterprise-grade data access with robust licensing, usage tracking, and subscription management.

## 🚀 Key Features

### Dataset Registry
- **Comprehensive Catalog**: Market data, ESG metrics, news intelligence, economic indicators
- **Flexible Licensing**: Free, Basic, Premium, and Enterprise tiers
- **Quality Metrics**: Data completeness, accuracy, and freshness scores
- **Multi-format Support**: JSON, CSV, Parquet, GeoJSON

### B2B Platform Features
- **Subscription Management**: Monthly, annual, and pay-per-use models
- **Rate Limiting**: Per-minute, daily, and monthly limits based on license tier
- **Usage Tracking**: Detailed analytics and cost calculation
- **API Access Control**: Token-based authentication with license validation
- **Real-time Data**: WebSocket streaming for premium subscribers
- **Bulk Export**: Large dataset downloads for enterprise clients
- **Webhook Support**: Real-time data delivery to client endpoints

### Available Datasets

#### 1. JSE Market Data (`jse-market-data`)
- **Description**: Real-time and historical data from Johannesburg Stock Exchange
- **Coverage**: South African equities, indices, corporate actions
- **Update Frequency**: Real-time (Premium+), 15-minute delay (Basic)
- **Historical Depth**: 20 years
- **Price**: From R0.001/record (Free: R0/month, Premium: R299/month)

#### 2. SADC ESG Intelligence (`sadc-esg-intelligence`)
- **Description**: Environmental, Social, Governance metrics for 16 SADC countries
- **Coverage**: Infrastructure, SDG indicators, governance scores
- **Update Frequency**: Monthly
- **Historical Depth**: 10 years
- **Price**: From R499/month (Basic), R999/month (Premium)

#### 3. African News Intelligence (`african-news-intelligence`)
- **Description**: Real-time news aggregation with sentiment analysis
- **Coverage**: Pan-African news sources with topic classification
- **Update Frequency**: Real-time
- **Historical Depth**: 5 years
- **Price**: R0.005/record or R199/month subscription

#### 4. African Economic Indicators (`african-economic-indicators`)
- **Description**: Key economic metrics from IMF, World Bank, central banks
- **Coverage**: 54 African countries
- **Update Frequency**: Quarterly
- **Historical Depth**: 30 years
- **Price**: R399/month (Basic), R999/month (Premium)

## 🔧 API Endpoints

### Dataset Discovery
```http
GET /api/v1/datasets/
GET /api/v1/datasets/{slug}
```

### Subscription Management
```http
POST /api/v1/datasets/{slug}/subscribe
GET /api/v1/datasets/subscriptions
POST /api/v1/datasets/subscriptions/{id}/cancel
```

### Secure Data Access
```http
GET /api/v1/data/market/{dataset_slug}/symbols
GET /api/v1/data/market/{dataset_slug}/historical
GET /api/v1/data/market/{dataset_slug}/realtime
GET /api/v1/data/esg/{dataset_slug}/regions
GET /api/v1/data/esg/{dataset_slug}/metrics
GET /api/v1/data/esg/{dataset_slug}/composite-scores
```

### Usage Analytics
```http
GET /api/v1/datasets/usage
GET /api/v1/datasets/webhooks
POST /api/v1/datasets/webhooks
```

## 🛡️ License Tiers

### Free Tier
- **Rate Limits**: 10/min, 1,000/day, 10,000/month
- **Features**: API access, 30-day historical data
- **Price**: R0/month
- **Use Case**: Evaluation and development

### Basic Tier
- **Rate Limits**: 60/min, 10,000/day, 100,000/month
- **Features**: API access, 1-year historical data, custom queries
- **Price**: 50% of dataset price
- **Use Case**: Small businesses and startups

### Premium Tier
- **Rate Limits**: 200/min, 50,000/day, 500,000/month
- **Features**: Real-time access, bulk downloads, webhooks, unlimited history
- **Price**: Full dataset price
- **Use Case**: Professional organizations

### Enterprise Tier
- **Rate Limits**: 1,000/min, 1M/day, 10M/month
- **Features**: All Premium + white-label access, redistribution rights
- **Price**: 3x dataset price
- **Use Case**: Large corporations and resellers

## 🚀 Quick Start

### 1. Initialize the System
```bash
# Create database tables and seed with sample data
python scripts/init_dataset_registry.py
```

### 2. Start the Application
```bash
# Using Docker
docker-compose up -d

# Or directly with Python
pip install -r requirements.txt
python app.py
```

### 3. Explore the API
```bash
# Get available datasets
curl http://localhost:5000/api/v1/datasets/

# Get specific dataset details
curl http://localhost:5000/api/v1/datasets/jse-market-data

# Access data (requires authentication)
curl -H "Authorization: Bearer YOUR_TOKEN" \
     "http://localhost:5000/api/v1/data/market/jse-market-data/symbols"
```

## 📊 Business Model

### Revenue Streams
1. **Subscription Fees**: Monthly/annual dataset subscriptions
2. **Usage-based Pricing**: Pay-per-record or pay-per-API-call
3. **Enterprise Licensing**: Custom contracts with volume discounts
4. **White-label Solutions**: Data reseller partnerships
5. **Professional Services**: Custom data integration and analytics

### Target Markets
- **FinTech Companies**: Robo-advisors, trading platforms, risk management
- **Investment Firms**: Portfolio management, market research, due diligence
- **Consulting Firms**: Market entry analysis, competitive intelligence
- **Academic Institutions**: Economic research, development studies
- **NGOs and Development Organizations**: SDG tracking, impact assessment
- **Mining and Resource Companies**: ESG compliance, risk monitoring

## 🔒 Security & Compliance

### Authentication
- **JWT Tokens**: Secure API access with expiration
- **API Keys**: Long-term authentication for automated systems
- **User Management**: Role-based access control

### Data Protection
- **Encryption**: All data encrypted in transit and at rest
- **Rate Limiting**: Prevents API abuse and ensures fair usage
- **Audit Logging**: Complete usage tracking for compliance
- **Access Control**: License-based feature restrictions

### Compliance
- **POPIA**: South African data protection compliance
- **GDPR**: European data protection standards
- **SOC 2**: Security and availability controls
- **ISO 27001**: Information security management

## 📈 Analytics & Monitoring

### Usage Analytics
- **Real-time Dashboards**: Monitor API usage, revenue, and performance
- **User Insights**: Track subscription patterns and feature adoption
- **Data Quality Metrics**: Monitor data freshness, completeness, and accuracy
- **Cost Analysis**: Track infrastructure costs and profit margins

### Performance Monitoring
- **API Response Times**: Monitor latency across all endpoints
- **Error Rates**: Track and alert on API failures
- **System Health**: Database, Redis, and service status monitoring
- **Capacity Planning**: Scale resources based on usage patterns

## 🚀 Scaling Strategy

### Technical Scaling
1. **Database Optimization**: PostgreSQL with read replicas and partitioning
2. **Caching**: Redis for API responses and rate limiting
3. **Load Balancing**: Multiple API servers behind a load balancer
4. **Data Pipeline**: Kafka for real-time data ingestion
5. **Microservices**: Split data sources into independent services

### Business Scaling
1. **Geographic Expansion**: Add datasets for other African regions
2. **Data Source Integration**: Partner with more data providers
3. **Vertical Integration**: Add sector-specific datasets (agriculture, healthcare)
4. **Platform Partnerships**: Integrate with popular analytics platforms
5. **Developer Ecosystem**: SDKs, documentation, and community support

## 🔧 Configuration

### Environment Variables
```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/wizdata

# Redis
REDIS_URL=redis://localhost:6379/0

# API Keys
ALPHA_VANTAGE_API_KEY=your_alpha_vantage_key
OPENAI_API_KEY=your_openai_key

# Application
ENVIRONMENT=production
DEBUG=false
SESSION_SECRET=your_secret_key
```

### Docker Deployment
```yaml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "5000:5000"
    environment:
      - DATABASE_URL=postgresql://postgres:password@db:5432/wizdata
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - db
      - redis
  
  db:
    image: postgres:15
    environment:
      POSTGRES_DB: wizdata
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: password
    volumes:
      - postgres_data:/var/lib/postgresql/data
  
  redis:
    image: redis:7-alpine
    volumes:
      - redis_data:/data

volumes:
  postgres_data:
  redis_data:
```

## 📚 Integration Examples

### Python SDK Example
```python
import wizdata

# Initialize client
client = wizdata.Client(api_key="your_api_key")

# Get market data
symbols = client.market.get_symbols("jse-market-data", exchange="JSE")
historical = client.market.get_historical("jse-market-data", 
                                         symbol="SBK", 
                                         start_date="2024-01-01")

# Get ESG metrics
regions = client.esg.get_regions("sadc-esg-intelligence", country="South Africa")
metrics = client.esg.get_metrics("sadc-esg-intelligence", 
                                region_id=1, 
                                category="environmental")
```

### JavaScript SDK Example
```javascript
import WizData from 'wizdata-js';

const client = new WizData({ apiKey: 'your_api_key' });

// Get real-time market data
const realtimeData = await client.market.getRealtime('jse-market-data', {
  symbols: ['SBK', 'FSR', 'NED']
});

// Subscribe to webhooks
await client.webhooks.create({
  dataset_id: 1,
  url: 'https://your-app.com/webhooks/market-data',
  event_types: ['price_update', 'volume_alert']
});
```

## 🤝 Support & Contact

### Documentation
- **API Docs**: https://docs.wizdata.co/
- **Developer Portal**: https://developers.wizdata.co/
- **GitHub**: https://github.com/wizdata/platform

### Business Inquiries
- **Sales**: sales@wizdata.co
- **Partnerships**: partnerships@wizdata.co
- **Enterprise**: enterprise@wizdata.co

### Technical Support
- **Support Portal**: https://support.wizdata.co/
- **Email**: support@wizdata.co
- **Slack Community**: https://slack.wizdata.co/

---

**WizData** - Powering African Finance with Intelligent Data Solutions 🌍📊
