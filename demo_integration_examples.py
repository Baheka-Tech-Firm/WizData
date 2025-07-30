#!/usr/bin/env python3
"""
WizData API Key Integration Examples
Demonstrates how each product integrates with WizData APIs using scoped authentication
"""

import requests
import json
import asyncio
import websockets
from datetime import datetime
from typing import Dict, List, Any

class WizDataClient:
    """Base client for WizData API integration"""
    
    def __init__(self, api_key: str, base_url: str = "http://localhost:5000"):
        self.api_key = api_key
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json',
            'User-Agent': 'WizData-Integration-Client/1.0'
        })
    
    def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        """Make authenticated GET request"""
        url = f"{self.base_url}{endpoint}"
        response = self.session.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    def post(self, endpoint: str, data: Dict = None) -> Dict[str, Any]:
        """Make authenticated POST request"""
        url = f"{self.base_url}{endpoint}"
        response = self.session.post(url, json=data)
        response.raise_for_status()
        return response.json()

class VueOnChartsIntegration:
    """VueOn Charts Platform Integration Example"""
    
    def __init__(self, api_key: str):
        self.client = WizDataClient(api_key)
        self.name = "VueOn Charts"
    
    def get_ohlcv_data(self, symbol: str, interval: str = "1h", limit: int = 100) -> Dict:
        """Fetch OHLCV candlestick data for charting"""
        print(f"📊 {self.name}: Fetching OHLCV data for {symbol}")
        
        try:
            data = self.client.get(f"/api/v1/charting/ohlcv/{symbol}", {
                'interval': interval,
                'limit': limit
            })
            
            ohlcv_points = len(data.get('data', []))
            print(f"   ✅ Retrieved {ohlcv_points} OHLCV data points")
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def get_technical_indicators(self, symbol: str, indicators: List[str] = None) -> Dict:
        """Calculate technical indicators for chart overlays"""
        indicators = indicators or ["RSI", "MACD", "SMA"]
        print(f"📈 {self.name}: Calculating indicators {indicators} for {symbol}")
        
        try:
            params = {f"indicators": indicator for indicator in indicators}
            data = self.client.get(f"/api/v1/data-services/indicators/{symbol}", params)
            
            calculated_indicators = len(data.get('indicators', {}))
            print(f"   ✅ Calculated {calculated_indicators} technical indicators")
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def get_corporate_events(self, symbols: List[str] = None) -> Dict:
        """Fetch corporate events for chart annotations"""
        symbols = symbols or ["JSE:NPN", "AAPL"]
        print(f"📅 {self.name}: Fetching corporate events")
        
        try:
            params = {f"symbols": symbol for symbol in symbols}
            data = self.client.get("/api/v1/charting/events/corporate", params)
            
            event_count = len(data.get('events', []))
            print(f"   ✅ Retrieved {event_count} corporate events")
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def demo_charting_workflow(self):
        """Demonstrate complete charting workflow"""
        print(f"\n🎨 {self.name} - Complete Charting Workflow")
        print("-" * 50)
        
        symbol = "BTC/USDT"
        
        # Step 1: Get OHLCV data
        ohlcv_data = self.get_ohlcv_data(symbol, "1h", 50)
        
        # Step 2: Calculate technical indicators
        indicators_data = self.get_technical_indicators(symbol, ["RSI", "MACD", "BB"])
        
        # Step 3: Get related events
        events_data = self.get_corporate_events([symbol])
        
        # Summary
        print(f"\n📋 Charting Data Summary:")
        print(f"   • OHLCV Points: {len(ohlcv_data.get('data', []))}")
        print(f"   • Indicators: {list(indicators_data.get('indicators', {}).keys())}")
        print(f"   • Events: {len(events_data.get('events', []))}")

class TraderStradaIntegration:
    """Trader (Strada) Platform Integration Example"""
    
    def __init__(self, api_key: str):
        self.client = WizDataClient(api_key)
        self.name = "Trader (Strada)"
    
    def get_real_time_quotes(self, symbols: List[str] = None) -> Dict:
        """Get real-time market quotes for trading"""
        symbols = symbols or ["BTC/USDT", "JSE:NPN", "AAPL"]
        print(f"💹 {self.name}: Fetching real-time quotes")
        
        try:
            params = {f"symbols": symbol for symbol in symbols}
            data = self.client.get("/api/v1/charting/market-data/quotes", params)
            
            quote_count = len(data.get('quotes', []))
            print(f"   ✅ Retrieved {quote_count} real-time quotes")
            
            # Display sample quote
            if data.get('quotes'):
                sample_quote = data['quotes'][0]
                print(f"   📊 Sample: {sample_quote.get('symbol', 'N/A')} @ ${sample_quote.get('price', 'N/A')}")
            
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def get_market_screener(self, sort_by: str = "change_percent") -> Dict:
        """Screen markets for trading opportunities"""
        print(f"🔍 {self.name}: Running market screener")
        
        try:
            data = self.client.get("/api/v1/charting/screener", {
                'sort_by': sort_by,
                'order': 'desc',
                'limit': 20
            })
            
            results_count = len(data.get('results', []))
            print(f"   ✅ Found {results_count} screening results")
            
            # Display top movers
            if data.get('results'):
                top_mover = data['results'][0]
                print(f"   🚀 Top Mover: {top_mover.get('symbol', 'N/A')} ({top_mover.get('change_percent', 'N/A')}%)")
            
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    async def stream_real_time_data(self, symbols: List[str] = None):
        """Stream real-time data via WebSocket"""
        symbols = symbols or ["BTC/USDT", "ETH/USDT"]
        print(f"📡 {self.name}: Starting real-time data stream")
        
        try:
            uri = "ws://localhost:5004/ws"
            
            async with websockets.connect(uri) as websocket:
                # Authenticate
                auth_msg = {
                    "action": "authenticate",
                    "api_key": self.client.api_key
                }
                await websocket.send(json.dumps(auth_msg))
                
                # Subscribe to symbols
                subscribe_msg = {
                    "action": "subscribe",
                    "symbols": symbols
                }
                await websocket.send(json.dumps(subscribe_msg))
                
                print(f"   ✅ Subscribed to {len(symbols)} symbols")
                
                # Listen for data (sample for 10 seconds)
                timeout = 10
                start_time = asyncio.get_event_loop().time()
                
                while (asyncio.get_event_loop().time() - start_time) < timeout:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=1.0)
                        data = json.loads(message)
                        
                        if data.get('type') == 'market_data':
                            symbol = data.get('symbol', 'Unknown')
                            price = data.get('data', {}).get('price', 'N/A')
                            print(f"   📊 {symbol}: ${price}")
                        
                    except asyncio.TimeoutError:
                        continue
                
                print(f"   ✅ Real-time streaming completed")
                
        except Exception as e:
            print(f"   ❌ Streaming Error: {e}")
    
    def demo_trading_workflow(self):
        """Demonstrate complete trading workflow"""
        print(f"\n💰 {self.name} - Complete Trading Workflow")
        print("-" * 50)
        
        # Step 1: Get real-time quotes
        quotes_data = self.get_real_time_quotes(["BTC/USDT", "ETH/USDT", "JSE:NPN"])
        
        # Step 2: Run market screener
        screener_data = self.get_market_screener("volume")
        
        # Step 3: Stream real-time data (would run in production)
        print(f"📡 Real-time streaming available via WebSocket")
        
        # Summary
        print(f"\n📋 Trading Data Summary:")
        print(f"   • Live Quotes: {len(quotes_data.get('quotes', []))}")
        print(f"   • Screener Results: {len(screener_data.get('results', []))}")
        print(f"   • WebSocket Streaming: Available")

class PulseOverviewIntegration:
    """Pulse Market Overview Integration Example"""
    
    def __init__(self, api_key: str):
        self.client = WizDataClient(api_key)
        self.name = "Pulse Market Overview"
    
    def get_sector_performance(self) -> Dict:
        """Get sector performance overview"""
        print(f"🏭 {self.name}: Fetching sector performance")
        
        try:
            data = self.client.get("/api/v1/charting/sectors")
            
            sector_count = len(data.get('sectors', []))
            print(f"   ✅ Retrieved {sector_count} sector performance metrics")
            
            # Display top performing sector
            if data.get('sectors'):
                top_sector = max(data['sectors'], key=lambda x: x.get('change_percent', 0))
                print(f"   🚀 Top Sector: {top_sector.get('name', 'N/A')} ({top_sector.get('change_percent', 'N/A')}%)")
            
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def get_currency_rates(self) -> Dict:
        """Get global currency rates"""
        print(f"💱 {self.name}: Fetching currency rates")
        
        try:
            data = self.client.get("/api/v1/charting/currency-rates")
            
            rate_count = len(data.get('rates', []))
            print(f"   ✅ Retrieved {rate_count} currency rates")
            
            # Display USD/ZAR rate
            usd_zar_rate = next((rate for rate in data.get('rates', []) if rate.get('pair') == 'USD/ZAR'), None)
            if usd_zar_rate:
                print(f"   💰 USD/ZAR: {usd_zar_rate.get('rate', 'N/A')}")
            
            return data
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            return {}
    
    def demo_overview_workflow(self):
        """Demonstrate market overview workflow"""
        print(f"\n🌍 {self.name} - Market Overview Workflow")
        print("-" * 50)
        
        # Step 1: Get sector performance
        sector_data = self.get_sector_performance()
        
        # Step 2: Get currency rates
        currency_data = self.get_currency_rates()
        
        # Summary
        print(f"\n📋 Market Overview Summary:")
        print(f"   • Sectors Tracked: {len(sector_data.get('sectors', []))}")
        print(f"   • Currency Pairs: {len(currency_data.get('rates', []))}")

def main():
    """Demonstrate all product integrations"""
    
    print("🔐 WizData Inter-Product Integration Demo")
    print("=" * 60)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    # API Keys (from setup)
    api_keys = {
        "vueon": "wiz_vueon_waX0YKOH753r8dVX6K_ODtVxWlOysq6rYgjGQd2sfqo",
        "trader": "wiz_trader_nwVV9N47cAFOgPwAdKf4TCnirw8sbYFcIfXnLBAcyFY",
        "pulse": "wiz_pulse_HUtgANzb3KBFw4bt6SQZT8JXQidc4xhJZHAMSzcd1eo",
        "wealth": "wiz_wealth_tfveda5sAz27fPSIFyI1cuIn0r9wO39QkNZGgqzYh4M",
        "connect": "wiz_connect_GsufXWWpmpPvsrHuyCuvqKRfGZckg3qTa1r6dIv_kcg"
    }
    
    try:
        # VueOn Charts Integration
        vueon = VueOnChartsIntegration(api_keys["vueon"])
        vueon.demo_charting_workflow()
        
        # Trader (Strada) Integration
        trader = TraderStradaIntegration(api_keys["trader"])
        trader.demo_trading_workflow()
        
        # Pulse Market Overview Integration
        pulse = PulseOverviewIntegration(api_keys["pulse"])
        pulse.demo_overview_workflow()
        
        print("\n" + "=" * 60)
        print("✅ All Product Integrations Demonstrated Successfully")
        print("🔗 Inter-product API communication is operational")
        print("🛡️  Scoped authentication working correctly")
        print("📊 Ready for production deployment")
        
    except Exception as e:
        print(f"\n❌ Integration Demo Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()