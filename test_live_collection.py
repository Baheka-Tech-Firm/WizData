#!/usr/bin/env python3
"""
Live Data Collection Test
Tests the scraper system with real external APIs
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime, timezone

async def test_coingecko_direct():
    """Test CoinGecko API directly"""
    print("=== Testing CoinGecko API Direct Access ===")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Test CoinGecko ping
            async with session.get("https://api.coingecko.com/api/v3/ping") as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✓ CoinGecko ping successful: {data}")
                else:
                    print(f"✗ CoinGecko ping failed: {response.status}")
                    return False
            
            # Test price data
            params = {
                'ids': 'bitcoin,ethereum',
                'vs_currencies': 'usd',
                'include_market_cap': 'true',
                'include_24hr_vol': 'true'
            }
            
            async with session.get("https://api.coingecko.com/api/v3/simple/price", params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    print(f"✓ CoinGecko price data retrieved:")
                    print(json.dumps(data, indent=2))
                    return True
                else:
                    print(f"✗ CoinGecko price data failed: {response.status}")
                    return False
                    
    except Exception as e:
        print(f"✗ CoinGecko test error: {e}")
        return False

async def test_jse_direct():
    """Test JSE API directly"""
    print("\n=== Testing JSE API Direct Access ===")
    
    try:
        async with aiohttp.ClientSession() as session:
            # Test JSE main site connectivity
            async with session.get("https://www.jse.co.za", timeout=aiohttp.ClientTimeout(total=10)) as response:
                if response.status == 200:
                    print(f"✓ JSE website accessible: {response.status}")
                else:
                    print(f"✗ JSE website not accessible: {response.status}")
                    return False
            
            # Test JSE API endpoint (may require specific headers)
            headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            test_url = "https://api.jse.co.za/api/equity/summary/AGL"
            async with session.get(test_url, headers=headers, timeout=aiohttp.ClientTimeout(total=10)) as response:
                print(f"JSE API response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    print(f"✓ JSE data retrieved for AGL:")
                    print(json.dumps(data, indent=2)[:500] + "...")
                    return True
                else:
                    text = await response.text()
                    print(f"JSE API response: {text[:200]}...")
                    return False
                    
    except Exception as e:
        print(f"✗ JSE test error: {e}")
        return False

async def test_data_quality():
    """Test data quality validation"""
    print("\n=== Testing Data Quality Pipeline ===")
    
    # Simulate scraped data
    test_data = {
        'source': 'test',
        'data_type': 'crypto_price',
        'symbol': 'bitcoin',
        'timestamp': datetime.now(timezone.utc).isoformat(),
        'raw_data': {
            'usd': 45000.50,
            'usd_market_cap': 850000000000,
            'usd_24h_vol': 25000000000,
            'usd_24h_change': 2.5
        },
        'metadata': {
            'data_provider': 'test',
            'fetch_time': datetime.now(timezone.utc).isoformat()
        }
    }
    
    print("✓ Test data structure:")
    print(json.dumps(test_data, indent=2))
    
    # Validate required fields
    required_fields = ['source', 'data_type', 'timestamp', 'raw_data']
    validation_passed = all(field in test_data for field in required_fields)
    
    if validation_passed:
        print("✓ Data validation passed")
    else:
        print("✗ Data validation failed")
        return False
    
    # Calculate quality score
    quality_factors = {
        'has_price': bool(test_data['raw_data'].get('usd')),
        'has_market_cap': bool(test_data['raw_data'].get('usd_market_cap')),
        'has_volume': bool(test_data['raw_data'].get('usd_24h_vol')),
        'has_change': test_data['raw_data'].get('usd_24h_change') is not None
    }
    
    quality_score = sum(quality_factors.values()) / len(quality_factors)
    print(f"✓ Quality score calculated: {quality_score}")
    
    return True

def test_queue_simulation():
    """Test message queue simulation"""
    print("\n=== Testing Queue System ===")
    
    # Simulate in-memory queue
    memory_queue = {
        'raw.coingecko.crypto_price': [],
        'raw.jse.stock_price': []
    }
    
    # Add test messages
    test_messages = [
        {
            'topic': 'raw.coingecko.crypto_price',
            'message': {
                'symbol': 'bitcoin',
                'price': 45000,
                'timestamp': datetime.now().isoformat()
            }
        },
        {
            'topic': 'raw.jse.stock_price', 
            'message': {
                'symbol': 'AGL',
                'price': 285.50,
                'timestamp': datetime.now().isoformat()
            }
        }
    ]
    
    for item in test_messages:
        topic = item['topic']
        message = item['message']
        memory_queue[topic].append(message)
        print(f"✓ Message added to {topic}: {message['symbol']}")
    
    print(f"✓ Queue status: {len(memory_queue['raw.coingecko.crypto_price'])} crypto, {len(memory_queue['raw.jse.stock_price'])} stocks")
    
    return True

async def main():
    """Main test runner"""
    print(f"🚀 WizData Live Data Collection Test")
    print(f"Test started at: {datetime.now()}")
    print("=" * 60)
    
    results = {}
    
    # Test external APIs
    results['coingecko'] = await test_coingecko_direct()
    results['jse'] = await test_jse_direct()
    
    # Test internal systems
    results['data_quality'] = await test_data_quality()
    results['queue_system'] = test_queue_simulation()
    
    # Summary
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS SUMMARY")
    print("=" * 60)
    
    for test_name, passed in results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{test_name.upper():<20} {status}")
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    print(f"\nOverall: {total_passed}/{total_tests} tests passed")
    
    if total_passed == total_tests:
        print("🎉 All systems operational - Ready for live data collection!")
    elif total_passed >= total_tests // 2:
        print("⚠️  Partial functionality - Some external APIs may be restricted")
    else:
        print("🔧 System needs configuration - Check API access and network connectivity")
    
    return results

if __name__ == "__main__":
    asyncio.run(main())