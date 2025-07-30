"""
API Documentation and Status Endpoints
Provides comprehensive API documentation and system status
"""

from flask import Blueprint, jsonify, render_template_string
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

api_docs_bp = Blueprint('api_docs', __name__, url_prefix='/api/v2')

@api_docs_bp.route('/docs')
def api_documentation():
    """API documentation homepage"""
    
    docs_html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>WizData API Documentation</title>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 0; padding: 20px; background: #f8f9fa; }
            .container { max-width: 1200px; margin: 0 auto; }
            .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 40px; border-radius: 10px; margin-bottom: 30px; }
            .section { background: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }
            .endpoint { border: 1px solid #e9ecef; border-radius: 8px; padding: 20px; margin-bottom: 15px; }
            .method { display: inline-block; padding: 4px 12px; border-radius: 4px; font-weight: bold; font-size: 12px; margin-right: 10px; }
            .get { background: #28a745; color: white; }
            .post { background: #007bff; color: white; }
            .delete { background: #dc3545; color: white; }
            .url { font-family: 'Monaco', 'Menlo', monospace; background: #f8f9fa; padding: 8px; border-radius: 4px; }
            .param { background: #e9ecef; padding: 8px; border-radius: 4px; margin: 5px 0; }
            .response { background: #f8f9fa; padding: 15px; border-radius: 6px; font-family: 'Monaco', 'Menlo', monospace; font-size: 12px; }
            .auth-required { color: #856404; background: #fff3cd; padding: 4px 8px; border-radius: 4px; font-size: 12px; }
            .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(400px, 1fr)); gap: 20px; }
            .nav { background: white; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
            .nav a { color: #007bff; text-decoration: none; margin-right: 20px; font-weight: 500; }
            .nav a:hover { text-decoration: underline; }
            .feature-badge { background: #17a2b8; color: white; padding: 2px 6px; border-radius: 3px; font-size: 11px; }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>WizData API Documentation</h1>
                <p>Comprehensive financial and market data API with real-time quotes, historical data, news, and ESG information.</p>
                <p><strong>Version:</strong> 2.0 | <strong>Last Updated:</strong> {{ timestamp }}</p>
            </div>
            
            <div class="nav">
                <a href="#authentication">Authentication</a>
                <a href="#rate-limits">Rate Limits</a>
                <a href="#market-data">Market Data</a>
                <a href="#alerts">Alerts</a>
                <a href="#jobs">Background Jobs</a>
                <a href="#status">Status</a>
            </div>
            
            <div id="authentication" class="section">
                <h2>🔐 Authentication</h2>
                <p>WizData API supports two authentication methods:</p>
                
                <div class="endpoint">
                    <h4>API Key Authentication</h4>
                    <p>Include your API key in the request header:</p>
                    <div class="param"><strong>Header:</strong> <code>X-API-Key: your_api_key_here</code></div>
                    <p>Or use Bearer token format:</p>
                    <div class="param"><strong>Header:</strong> <code>Authorization: Bearer your_api_key_here</code></div>
                </div>
                
                <div class="endpoint">
                    <h4>JWT Token Authentication</h4>
                    <div class="param"><strong>Header:</strong> <code>Authorization: Bearer your_jwt_token_here</code></div>
                </div>
                
                <div class="endpoint">
                    <span class="method post">POST</span>
                    <div class="url">/api/v2/auth/api-key</div>
                    <p>Generate a new API key</p>
                    <div class="param"><strong>Body:</strong> {"user_id": "string", "permissions": ["market_data:read"], "expires_days": 365}</div>
                </div>
                
                <div class="endpoint">
                    <span class="method post">POST</span>
                    <div class="url">/api/v2/auth/token</div>
                    <p>Generate a JWT token</p>
                    <div class="param"><strong>Body:</strong> {"user_id": "string", "permissions": ["market_data:read"], "expires_hours": 24}</div>
                </div>
            </div>
            
            <div id="rate-limits" class="section">
                <h2>⏱️ Rate Limits</h2>
                <p>API requests are rate limited to ensure fair usage:</p>
                <ul>
                    <li><strong>Authenticated users:</strong> 200 requests per minute, 20 burst</li>
                    <li><strong>Anonymous users:</strong> 60 requests per minute, 10 burst</li>
                    <li><strong>Historical data:</strong> 60 requests per minute, 10 burst</li>
                    <li><strong>Stock screener:</strong> 20 requests per minute, 5 burst</li>
                </ul>
                <p>Rate limit headers are included in responses:</p>
                <div class="response">X-RateLimit-Limit: 200
X-RateLimit-Remaining: 195
X-RateLimit-Reset: 1643723400</div>
            </div>
            
            <div id="market-data" class="section">
                <h2>📈 Market Data Endpoints</h2>
                
                <div class="grid">
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/quotes</div>
                        <p>Get real-time quotes for multiple symbols</p>
                        <div class="param"><strong>symbols:</strong> Comma-separated list of symbols (required)</div>
                        <div class="param"><strong>market:</strong> Market identifier (optional)</div>
                        <div class="param"><strong>include_fundamentals:</strong> Include fundamental data (optional)</div>
                        <div class="response">
{
  "status": "success",
  "data": {
    "JSE:NPN": {
      "symbol": "NPN",
      "price": 285000,
      "change": 2500,
      "change_percent": 0.88,
      "volume": 1250000
    }
  },
  "metadata": {
    "count": 1,
    "timestamp": "2025-07-30T10:30:15Z"
  }
}
                        </div>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/historical</div>
                        <p>Get historical OHLCV data</p>
                        <div class="param"><strong>symbols:</strong> Comma-separated symbols (required)</div>
                        <div class="param"><strong>period:</strong> 1d, 5d, 1mo, 3mo, 6mo, 1y, 2y, 5y, 10y, ytd, max</div>
                        <div class="param"><strong>interval:</strong> 1m, 5m, 15m, 30m, 1h, 1d, 1wk, 1mo</div>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/fundamentals</div>
                        <p>Get fundamental data for symbols</p>
                        <div class="param"><strong>symbols:</strong> Comma-separated symbols (required)</div>
                        <div class="param"><strong>metrics:</strong> Specific metrics to fetch (optional)</div>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/news</div>
                        <p>Get market news</p>
                        <div class="param"><strong>symbols:</strong> Filter by symbols (optional)</div>
                        <div class="param"><strong>category:</strong> earnings, mergers, analyst, general</div>
                        <div class="param"><strong>limit:</strong> Number of articles (max 100)</div>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method post">POST</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/screener</div>
                        <p>Screen stocks based on criteria</p>
                        <div class="response">
{
  "filters": {
    "market_cap": {"min": 1000000000, "max": 50000000000},
    "pe_ratio": {"min": 5, "max": 25},
    "dividend_yield": {"min": 0.02}
  },
  "sort_by": "market_cap",
  "sort_order": "desc",
  "limit": 50
}
                        </div>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/markets/status</div>
                        <p>Get market status and trading hours</p>
                        <div class="param"><strong>markets:</strong> Comma-separated market codes (optional)</div>
                    </div>
                </div>
            </div>
            
            <div id="alerts" class="section">
                <h2>🔔 Alerts & Watchlists</h2>
                
                <div class="grid">
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="method post">POST</span> <span class="method delete">DELETE</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/watchlist</div>
                        <p>Manage user watchlists</p>
                        <p><strong>GET:</strong> Retrieve watchlist | <strong>POST:</strong> Add/update symbols | <strong>DELETE:</strong> Remove symbols</p>
                    </div>
                    
                    <div class="endpoint">
                        <span class="method get">GET</span> <span class="method post">POST</span> <span class="method delete">DELETE</span> <span class="auth-required">🔐 Auth Required</span>
                        <div class="url">/api/v2/market-data/alerts</div>
                        <p>Manage price alerts</p>
                        <p><strong>GET:</strong> Get alerts | <strong>POST:</strong> Create alert | <strong>DELETE:</strong> Remove alert</p>
                        <div class="response">
POST Body:
{
  "symbol": "JSE:NPN",
  "condition": "above",
  "value": 300000,
  "message": "NPN hit target price",
  "notification_method": "email"
}
                        </div>
                    </div>
                </div>
            </div>
            
            <div id="jobs" class="section">
                <h2>⚙️ Background Jobs <span class="feature-badge">Enhanced</span></h2>
                <p>WizData runs automated background jobs for data collection and processing:</p>
                
                <div class="grid">
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                        <h4>Market Data Collection</h4>
                        <ul>
                            <li>JSE quotes (every 2 minutes)</li>
                            <li>Crypto quotes (every 1 minute)</li>
                            <li>Forex quotes (every 5 minutes)</li>
                            <li>Historical data (daily at 18:00)</li>
                        </ul>
                    </div>
                    
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                        <h4>News & Intelligence</h4>
                        <ul>
                            <li>Market news (every 15 minutes)</li>
                            <li>Sentiment analysis (every 30 minutes)</li>
                            <li>ESG data collection (daily at 02:00)</li>
                        </ul>
                    </div>
                    
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                        <h4>Data Processing</h4>
                        <ul>
                            <li>Data cleaning (daily at 01:00)</li>
                            <li>Data validation (daily at 03:00)</li>
                            <li>Quality reporting (daily at 23:00)</li>
                        </ul>
                    </div>
                    
                    <div style="background: #f8f9fa; padding: 15px; border-radius: 8px;">
                        <h4>Monitoring & Alerts</h4>
                        <ul>
                            <li>Price alerts (every 5 minutes)</li>
                            <li>System health (every 1 minute)</li>
                            <li>Cache cleanup (daily at 04:00)</li>
                        </ul>
                    </div>
                </div>
            </div>
            
            <div id="status" class="section">
                <h2>📊 System Status</h2>
                
                <div class="endpoint">
                    <span class="method get">GET</span>
                    <div class="url">/api/v2/health</div>
                    <p>Enhanced health check with service status</p>
                    <div class="response">
{
  "status": "healthy",
  "timestamp": "2025-07-30T10:30:15Z",
  "services": {
    "database": "healthy",
    "redis": "healthy",
    "authentication": "healthy",
    "job_scheduler": "healthy",
    "rate_limiter": "healthy"
  },
  "features": {
    "enhanced_api": true,
    "background_jobs": true,
    "caching": true,
    "rate_limiting": true
  }
}
                    </div>
                </div>
            </div>
            
            <div class="section">
                <h2>🚀 Getting Started</h2>
                <ol>
                    <li><strong>Get API Key:</strong> Generate an API key using <code>POST /api/v2/auth/api-key</code></li>
                    <li><strong>Test Connection:</strong> Check system status with <code>GET /api/v2/health</code></li>
                    <li><strong>Fetch Data:</strong> Get market quotes with <code>GET /api/v2/market-data/quotes?symbols=JSE:NPN,JSE:BHP</code></li>
                    <li><strong>Set Alerts:</strong> Create price alerts using <code>POST /api/v2/market-data/alerts</code></li>
                </ol>
                
                <div style="background: #d1ecf1; padding: 15px; border-radius: 8px; border-left: 4px solid #17a2b8;">
                    <strong>💡 Pro Tip:</strong> Use the enhanced features like watchlists, alerts, and background job monitoring for a complete market data solution.
                </div>
            </div>
            
            <div class="section" style="text-align: center; color: #6c757d;">
                <p>WizData API v2.0 - Built with advanced rate limiting, authentication, and background job processing</p>
                <p>For support, visit our documentation or contact the development team.</p>
            </div>
        </div>
    </body>
    </html>
    """
    
    return render_template_string(docs_html, timestamp=datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

@api_docs_bp.route('/status')
def api_status():
    """Comprehensive API status endpoint"""
    try:
        # Get Flask app instance
        from flask import current_app
        
        status = {
            'api_version': '2.0',
            'timestamp': datetime.now().isoformat(),
            'status': 'operational',
            'services': {
                'database': 'healthy',
                'redis': 'healthy' if hasattr(current_app, 'cache_manager') and current_app.cache_manager else 'unavailable',
                'authentication': 'healthy',
                'rate_limiter': 'healthy' if hasattr(current_app, 'rate_limiter') and current_app.rate_limiter else 'unavailable',
                'job_scheduler': 'healthy' if hasattr(current_app, 'job_scheduler') and current_app.job_scheduler else 'unavailable'
            },
            'features': {
                'enhanced_api': True,
                'background_jobs': hasattr(current_app, 'job_scheduler') and current_app.job_scheduler is not None,
                'caching': hasattr(current_app, 'cache_manager') and current_app.cache_manager is not None,
                'rate_limiting': hasattr(current_app, 'rate_limiter') and current_app.rate_limiter is not None,
                'authentication': True,
                'alerts': True,
                'watchlists': True,
                'market_data': True,
                'news_feed': True,
                'esg_data': True
            },
            'endpoints': {
                'market_data': [
                    'GET /api/v2/market-data/quotes',
                    'GET /api/v2/market-data/historical',
                    'GET /api/v2/market-data/fundamentals',
                    'GET /api/v2/market-data/news',
                    'POST /api/v2/market-data/screener',
                    'GET /api/v2/market-data/markets/status'
                ],
                'user_features': [
                    'GET|POST|DELETE /api/v2/market-data/watchlist',
                    'GET|POST|DELETE /api/v2/market-data/alerts'
                ],
                'authentication': [
                    'POST /api/v2/auth/api-key',
                    'POST /api/v2/auth/token',
                    'POST /api/v2/auth/validate'
                ],
                'system': [
                    'GET /api/v2/health',
                    'GET /api/v2/status',
                    'GET /api/v2/docs'
                ]
            },
            'rate_limits': {
                'authenticated_users': '200 requests/minute, 20 burst',
                'anonymous_users': '60 requests/minute, 10 burst',
                'historical_data': '60 requests/minute, 10 burst',
                'stock_screener': '20 requests/minute, 5 burst'
            },
            'data_sources': {
                'jse': 'Johannesburg Stock Exchange',
                'crypto': 'Cryptocurrency markets',
                'forex': 'Foreign exchange markets',
                'news': 'Financial news aggregation',
                'esg': 'Environmental, Social, Governance data'
            },
            'background_jobs': {
                'data_collection': [
                    'JSE quotes (every 2 minutes)',
                    'Crypto quotes (every 1 minute)',
                    'Forex quotes (every 5 minutes)',
                    'Historical data (daily)',
                    'Market news (every 15 minutes)',
                    'ESG data (daily)'
                ],
                'data_processing': [
                    'Data cleaning (daily)',
                    'Data validation (daily)',
                    'Quality reporting (daily)',
                    'Cache maintenance (daily)'
                ],
                'monitoring': [
                    'Price alerts (every 5 minutes)',
                    'System health monitoring',
                    'Usage analytics'
                ]
            }
        }
        
        # Check if any critical services are down
        critical_services = ['database', 'authentication']
        if any(status['services'][service] != 'healthy' for service in critical_services):
            status['status'] = 'degraded'
        
        return jsonify(status)
        
    except Exception as e:
        logger.error(f"Error generating API status: {str(e)}")
        return jsonify({
            'status': 'error',
            'message': 'Failed to generate status report',
            'timestamp': datetime.now().isoformat()
        }), 500

def register_api_docs(app):
    """Register API documentation blueprint"""
    app.register_blueprint(api_docs_bp)
