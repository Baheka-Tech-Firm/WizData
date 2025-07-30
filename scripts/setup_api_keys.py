#!/usr/bin/env python3
"""
Setup script for generating API keys for inter-product integration
Creates default API keys for all WizData ecosystem products
"""

import sys
import os
sys.path.append('/home/runner/workspace')

from auth.api_key_manager import api_key_manager, ProductType, setup_default_api_keys
from datetime import datetime

def main():
    """Generate and display API keys for all products"""
    
    print("🔐 WizData Inter-Product API Key Setup")
    print("=" * 60)
    print("Generating scoped API keys for ecosystem integration...")
    print()
    
    try:
        # Generate default API keys
        keys = setup_default_api_keys()
        
        if not keys:
            print("❌ No keys were generated. They may already exist.")
            
            # List existing keys
            existing_keys = api_key_manager.list_api_keys()
            if existing_keys:
                print("\n📋 Existing API Keys:")
                for key in existing_keys:
                    status = "✅ Active" if key['is_active'] else "❌ Inactive"
                    print(f"\n🔑 {key['name']}")
                    print(f"   Product: {key['product']}")
                    print(f"   Key ID: {key['key_id']}")
                    print(f"   Status: {status}")
                    print(f"   Scopes: {', '.join(key['scopes'])}")
                    print(f"   Usage: {key['usage_count']} requests")
            return
        
        print(f"✅ Successfully generated {len(keys)} API keys:\n")
        
        # Display keys organized by product
        for key_info in keys:
            print(f"🔑 {key_info['name']}")
            print(f"   Product: {key_info['product'].upper()}")
            print(f"   API Key: {key_info['api_key']}")
            print(f"   Key ID: {key_info['key_id']}")
            print(f"   Scopes: {', '.join(key_info['scopes'])}")
            print(f"   Rate Limit: {key_info['rate_limit']} requests/hour")
            print(f"   Expires: {key_info['expires_at'] or 'Never'}")
            print()
        
        # Generate integration examples
        print("🔗 Integration Examples:")
        print("-" * 40)
        
        integration_examples = {
            "VueOn Charts": {
                "key": next((k for k in keys if k['product'] == 'vueon'), None),
                "endpoints": [
                    "/api/v1/charting/ohlcv/BTC/USDT",
                    "/api/v1/data-services/indicators/RSI",
                    "/api/v1/charting/events/corporate"
                ]
            },
            "Trader (Strada)": {
                "key": next((k for k in keys if k['product'] == 'trader'), None),
                "endpoints": [
                    "/api/v1/charting/market-data/quotes",
                    "/api/v1/charting/screener",
                    "/ws (WebSocket streaming)"
                ]
            },
            "Pulse": {
                "key": next((k for k in keys if k['product'] == 'pulse'), None),
                "endpoints": [
                    "/api/v1/charting/sectors",
                    "/api/v1/charting/screener",
                    "/api/v1/charting/currency-rates"
                ]
            },
            "Wealth (Novia)": {
                "key": next((k for k in keys if k['product'] == 'wealth'), None),
                "endpoints": [
                    "/api/v1/data-services/profile/company",
                    "/api/v1/charting/events/corporate",
                    "/api/v1/data-services/indicators/fundamentals"
                ]
            },
            "Connect": {
                "key": next((k for k in keys if k['product'] == 'connect'), None),
                "endpoints": [
                    "/api/v1/charting/news/financial",
                    "/api/v1/charting/events/calendar",
                    "/api/v1/charting/market-data/alerts"
                ]
            }
        }
        
        for product_name, info in integration_examples.items():
            key = info['key']
            if key:
                print(f"\n📱 {product_name}:")
                print(f"   Authorization: Bearer {key['api_key'][:20]}...")
                print(f"   Accessible endpoints:")
                for endpoint in info['endpoints']:
                    print(f"     • {endpoint}")
        
        # Usage instructions
        print("\n" + "=" * 60)
        print("📖 USAGE INSTRUCTIONS")
        print("=" * 60)
        
        print("""
🔧 HTTP Header Authentication:
   Authorization: Bearer wiz_vueon_[KEY]
   # OR
   X-API-Key: wiz_vueon_[KEY]

📱 cURL Example:
   curl -H "Authorization: Bearer wiz_vueon_[KEY]" \\
        https://wizdata.app/api/v1/charting/ohlcv/BTC/USDT

⚙️  JavaScript/React Example:
   fetch('/api/v1/charting/market-data/quotes', {
     headers: {
       'Authorization': 'Bearer wiz_trader_[KEY]',
       'Content-Type': 'application/json'
     }
   })

🔍 Management Interface:
   Visit: /admin/api-keys for key management
   Monitor usage, revoke keys, create new ones

🛡️  Security Notes:
   • Store keys securely in environment variables
   • Each key has product-specific scope limitations
   • Rate limits: 1000-5000 requests/hour per key
   • Keys expire in 1 year (renewable)
""")
        
        print("🎯 Next Steps:")
        print("   1. Copy the API keys to your product configurations")
        print("   2. Test integration with the /admin/api-keys dashboard")
        print("   3. Monitor usage and performance via the admin interface")
        print("   4. Set up alerts for rate limit or error thresholds")
        
    except Exception as e:
        print(f"❌ Error setting up API keys: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()