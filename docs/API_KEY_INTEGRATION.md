# WizData API Key Integration Guide

## Overview

WizData implements a comprehensive scoped API key system for secure inter-product integration across the financial platform ecosystem. Each product receives tailored access permissions based on their specific data requirements.

## Products & Integration Map

| Product | Purpose | Primary Endpoints | Key Features |
|---------|---------|-------------------|--------------|
| **VueOn** | Professional charting platform | `/charting/ohlcv`, `/indicators`, `/events` | OHLCV data, technical indicators, corporate events |
| **Trader (Strada)** | Real-time trading platform | `/market-data`, `/screener`, WebSocket | Live quotes, market screening, streaming feeds |
| **Pulse** | Market overview & analytics | `/sectors`, `/screener`, `/currency-rates` | Sector performance, market overview, FX rates |
| **Wealth (Novia)** | Investment management | `/profile`, `/events`, `/indicators` | Company profiles, fundamentals, corporate events |
| **Connect** | Communication & alerts | `/news`, `/events`, `/market-data` | Financial news, calendar events, market alerts |

## Authentication Methods

### 1. Bearer Token (Recommended)
```http
Authorization: Bearer wiz_vueon_[32-char-token]
```

### 2. API Key Header
```http
X-API-Key: wiz_trader_[32-char-token]
```

### 3. Query Parameter (Development Only)
```http
GET /api/v1/charting/ohlcv/BTC/USDT?api_key=wiz_pulse_[token]
```

## Scope-Based Access Control

### Access Scopes

| Scope | Description | Endpoints Covered |
|-------|-------------|-------------------|
| `market_data` | Real-time market quotes | `/api/v1/charting/market-data/*`, `/api/v1/data-services/market-data` |
| `charting` | OHLCV and chart data | `/api/v1/charting/ohlcv/*`, `/api/v1/charting/symbols`, `/api/v1/charting/screener` |
| `indicators` | Technical analysis | `/api/v1/data-services/indicators/*`, `/api/v1/charting/ohlcv/*` |
| `news` | Financial news | `/api/v1/charting/news/*`, `/api/v1/data-services/news` |
| `events` | Corporate events | `/api/v1/charting/events/*`, `/api/v1/data-services/events` |
| `streaming` | WebSocket feeds | `/ws`, `/api/websocket/*` |
| `screener` | Market screening | `/api/v1/charting/screener`, `/api/v1/data-services/screener` |
| `profiles` | Company profiles | `/api/v1/data-services/profile/*`, `/api/v1/charting/symbols` |
| `admin` | Full access | `/api/*` (all endpoints) |

### Product Default Scopes

```python
VueOn: [charting, indicators, events, news]
Trader: [market_data, screener, streaming, indicators]  
Pulse: [charting, screener, market_data]
Wealth: [profiles, events, indicators, market_data]
Connect: [news, events, market_data]
```

## Integration Examples

### VueOn Charts Integration

```javascript
// React/Vue component for fetching OHLCV data
const fetchOHLCVData = async (symbol, interval = '1h') => {
  const response = await fetch(`/api/v1/charting/ohlcv/${symbol}`, {
    headers: {
      'Authorization': `Bearer ${process.env.VUEON_API_KEY}`,
      'Content-Type': 'application/json'
    },
    params: { interval, limit: 100 }
  });
  
  return response.json();
};

// Technical indicators
const fetchIndicators = async (symbol, indicators = ['RSI', 'MACD']) => {
  const params = indicators.map(ind => `indicators=${ind}`).join('&');
  const response = await fetch(`/api/v1/data-services/indicators/${symbol}?${params}`, {
    headers: {
      'Authorization': `Bearer ${process.env.VUEON_API_KEY}`
    }
  });
  
  return response.json();
};
```

### Trader (Strada) Real-time Integration

```javascript
// WebSocket streaming for real-time data
class TradingDataStream {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.ws = null;
  }
  
  connect() {
    this.ws = new WebSocket(`wss://wizdata.app/ws`);
    
    this.ws.onopen = () => {
      // Authenticate with API key
      this.ws.send(JSON.stringify({
        action: 'authenticate',
        api_key: this.apiKey
      }));
      
      // Subscribe to symbols
      this.ws.send(JSON.stringify({
        action: 'subscribe',
        symbols: ['BTC/USDT', 'JSE:NPN', 'AAPL']
      }));
    };
    
    this.ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      this.handleMarketData(data);
    };
  }
  
  handleMarketData(data) {
    if (data.type === 'market_data') {
      // Update trading interface with real-time data
      this.updateTradingView(data.symbol, data.data);
    }
  }
}

// Market screener
const fetchTopMovers = async () => {
  const response = await fetch('/api/v1/charting/screener', {
    headers: {
      'Authorization': `Bearer ${process.env.TRADER_API_KEY}`
    },
    params: {
      sort_by: 'change_percent',
      order: 'desc',
      limit: 20
    }
  });
  
  return response.json();
};
```

### Pulse Market Overview

```javascript
// Sector performance dashboard
const fetchSectorPerformance = async () => {
  const response = await fetch('/api/v1/charting/sectors', {
    headers: {
      'Authorization': `Bearer ${process.env.PULSE_API_KEY}`
    }
  });
  
  return response.json();
};

// Currency rates for global overview
const fetchCurrencyRates = async () => {
  const response = await fetch('/api/v1/charting/currency-rates', {
    headers: {
      'Authorization': `Bearer ${process.env.PULSE_API_KEY}`
    }
  });
  
  return response.json();
};
```

### Wealth (Novia) Portfolio Integration

```javascript
// Company profile and fundamentals
const fetchCompanyProfile = async (symbol) => {
  const response = await fetch(`/api/v1/data-services/profile/${symbol}`, {
    headers: {
      'Authorization': `Bearer ${process.env.WEALTH_API_KEY}`
    }
  });
  
  return response.json();
};

// Corporate events for portfolio monitoring
const fetchCorporateEvents = async (symbols, days = 30) => {
  const symbolParams = symbols.map(s => `symbols=${s}`).join('&');
  const response = await fetch(`/api/v1/charting/events/corporate?${symbolParams}&days=${days}`, {
    headers: {
      'Authorization': `Bearer ${process.env.WEALTH_API_KEY}`
    }
  });
  
  return response.json();
};
```

### Connect News & Alerts

```javascript
// Financial news feed
const fetchFinancialNews = async (categories = ['market', 'earnings']) => {
  const categoryParams = categories.map(c => `categories=${c}`).join('&');
  const response = await fetch(`/api/v1/charting/news/financial?${categoryParams}`, {
    headers: {
      'Authorization': `Bearer ${process.env.CONNECT_API_KEY}`
    }
  });
  
  return response.json();
};

// Event calendar
const fetchEventCalendar = async (startDate, endDate) => {
  const response = await fetch('/api/v1/charting/events/calendar', {
    headers: {
      'Authorization': `Bearer ${process.env.CONNECT_API_KEY}`
    },
    params: {
      start_date: startDate,
      end_date: endDate
    }
  });
  
  return response.json();
};
```

## Rate Limiting

### Default Limits
- **Internal Products**: 5,000 requests/hour
- **External Integrations**: 1,000 requests/hour
- **WebSocket Connections**: 100 concurrent per key

### Rate Limit Headers
```http
X-Rate-Limit: 5000
X-Rate-Limit-Remaining: 4856
X-Rate-Limit-Reset: 1640995200
Retry-After: 3600
```

### Handling Rate Limits
```javascript
const apiCall = async (url, options) => {
  const response = await fetch(url, options);
  
  if (response.status === 429) {
    const retryAfter = response.headers.get('Retry-After');
    console.warn(`Rate limited. Retry after ${retryAfter} seconds`);
    
    // Implement exponential backoff
    await new Promise(resolve => setTimeout(resolve, retryAfter * 1000));
    return apiCall(url, options);
  }
  
  return response;
};
```

## Error Handling

### Standard Error Response
```json
{
  "success": false,
  "error": "Insufficient permissions for this endpoint",
  "code": "INSUFFICIENT_PERMISSIONS",
  "message": "Your API key does not have access to this endpoint",
  "required_scopes": ["market_data", "charting"],
  "timestamp": "2025-07-30T13:20:00Z",
  "endpoint": "/api/v1/charting/market-data/quotes",
  "method": "GET"
}
```

### Error Codes
- `MISSING_API_KEY`: No API key provided
- `INVALID_KEY`: API key not found or invalid
- `EXPIRED_KEY`: API key has expired
- `INSUFFICIENT_PERMISSIONS`: Endpoint requires higher scope
- `RATE_LIMITED`: Rate limit exceeded
- `VALIDATION_ERROR`: Request validation failed

## Security Best Practices

### Key Storage
```bash
# Environment variables (recommended)
export VUEON_API_KEY="wiz_vueon_[32-char-token]"
export TRADER_API_KEY="wiz_trader_[32-char-token]"

# Docker secrets
echo "wiz_vueon_[token]" | docker secret create vueon_api_key -

# Kubernetes secrets
kubectl create secret generic api-keys \
  --from-literal=vueon-key="wiz_vueon_[token]" \
  --from-literal=trader-key="wiz_trader_[token]"
```

### Rotation Strategy
```python
# Python script for key rotation
import os
from auth.api_key_manager import api_key_manager, ProductType

def rotate_api_key(product: ProductType, old_key_id: str):
    # Generate new key
    new_key = api_key_manager.generate_api_key(
        product=product,
        name=f"{product.value} - Rotated Key",
        description="Rotated for security"
    )
    
    # Update application configuration
    update_environment_variable(f"{product.value.upper()}_API_KEY", new_key['api_key'])
    
    # Revoke old key after grace period
    schedule_key_revocation(old_key_id, hours=24)
    
    return new_key
```

## Monitoring & Analytics

### Usage Tracking
All API calls are automatically tracked with:
- Request timestamp and duration
- Endpoint and HTTP method
- Response code and size
- Client IP address
- API key identification

### Admin Dashboard
Access the management interface at `/admin/api-keys` to:
- View all API keys and their status
- Monitor usage statistics and patterns
- Revoke compromised keys
- Generate new keys with custom scopes
- Set up alerts for unusual activity

### Metrics & Alerting
```python
# Custom alerts for monitoring
def setup_api_key_alerts():
    alerts = [
        {
            "name": "High Error Rate",
            "condition": "error_rate > 10%",
            "action": "notify_admin"
        },
        {
            "name": "Rate Limit Exceeded",
            "condition": "rate_limit_hits > 100/hour",
            "action": "auto_scale_limits"
        },
        {
            "name": "Unusual Usage Pattern",
            "condition": "requests_spike > 300%",
            "action": "security_review"
        }
    ]
    
    return alerts
```

## Testing & Validation

### API Key Validation
```bash
# Test API key authentication
curl -H "Authorization: Bearer wiz_vueon_[token]" \
     https://wizdata.app/api/v1/charting/ohlcv/BTC/USDT

# Expected response (success)
{
  "success": true,
  "data": [...],
  "timestamp": "2025-07-30T13:20:00Z"
}

# Expected response (invalid key)
{
  "success": false,
  "error": "Invalid API key",
  "code": "INVALID_KEY",
  "timestamp": "2025-07-30T13:20:00Z"
}
```

### Integration Testing
```python
import pytest
import requests

class TestAPIKeyIntegration:
    
    @pytest.mark.parametrize("product,endpoint", [
        ("vueon", "/api/v1/charting/ohlcv/BTC/USDT"),
        ("trader", "/api/v1/charting/market-data/quotes"),
        ("pulse", "/api/v1/charting/sectors"),
        ("wealth", "/api/v1/data-services/profile/AAPL"),
        ("connect", "/api/v1/charting/news/financial")
    ])
    def test_product_endpoint_access(self, product, endpoint):
        api_key = os.getenv(f"{product.upper()}_API_KEY")
        
        response = requests.get(
            f"https://wizdata.app{endpoint}",
            headers={"Authorization": f"Bearer {api_key}"}
        )
        
        assert response.status_code == 200
        assert response.json()["success"] is True
```

---

**Security Status**: ✅ Production Ready  
**Integration Coverage**: 5/5 Products Supported  
**API Scope Management**: Granular permissions implemented  
**Rate Limiting**: Active with Redis backend  
**Monitoring**: Comprehensive usage tracking enabled