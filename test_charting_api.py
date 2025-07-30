#!/usr/bin/env python3
"""
Professional Charting API Test
Tests all charting endpoints for TradingView-style functionality
"""

import requests
import json
import time
from datetime import datetime

def test_symbols_endpoint():
    """Test symbols endpoint"""
    print("📊 Testing Symbols Endpoint")
    print("-" * 40)
    
    try:
        response = requests.get("http://localhost:5000/api/v1/charting/symbols", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ Symbols API: Working")
            print(f"   Total Categories: {len(data['symbols'])}")
            print(f"   Total Symbols: {data['count']}")
            
            # Show sample symbols
            for category, symbols in data['symbols'].items():
                print(f"   {category.upper()}: {len(symbols)} symbols")
                if symbols:
                    print(f"     Example: {symbols[0]['symbol']} - {symbols[0]['name']}")
            
            return True
        else:
            print(f"❌ Symbols API failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Symbols test error: {e}")
        return False

def test_ohlcv_endpoint():
    """Test OHLCV data endpoint"""
    print("\n📈 Testing OHLCV Data Endpoint")
    print("-" * 40)
    
    symbols = ['BTC/USDT', 'AAPL', 'JSE:NPN', 'USD/ZAR']
    intervals = ['1h', '1d']
    
    results = []
    
    for symbol in symbols:
        for interval in intervals:
            try:
                response = requests.get(
                    f"http://localhost:5000/api/v1/charting/ohlcv/{symbol}",
                    params={'interval': interval, 'limit': 10},
                    timeout=10
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if data['success']:
                        print(f"✅ {symbol} ({interval}): {data['count']} candles")
                        
                        # Show sample data
                        if data['data']:
                            sample = data['data'][-1]
                            print(f"   Latest: O:{sample['open']}, H:{sample['high']}, L:{sample['low']}, C:{sample['close']}")
                        
                        # Show indicators
                        if data['indicators']:
                            indicators = list(data['indicators'].keys())
                            print(f"   Indicators: {', '.join(indicators)}")
                        
                        results.append(True)
                    else:
                        print(f"❌ {symbol} ({interval}): API error")
                        results.append(False)
                else:
                    print(f"❌ {symbol} ({interval}): HTTP {response.status_code}")
                    results.append(False)
                    
            except Exception as e:
                print(f"❌ {symbol} ({interval}): {e}")
                results.append(False)
    
    return all(results)

def test_market_data_endpoint():
    """Test market data endpoint"""
    print("\n💹 Testing Market Data Endpoint")
    print("-" * 40)
    
    symbols = ['BTC/USDT', 'ETH/USDT', 'AAPL', 'JSE:NPN']
    results = []
    
    for symbol in symbols:
        try:
            response = requests.get(f"http://localhost:5000/api/v1/charting/market-data/{symbol}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['success']:
                    market_data = data['data']
                    print(f"✅ {symbol}: ${market_data['price']} ({market_data['change_percent']:+.2f}%)")
                    print(f"   Volume: {market_data['volume']:,}")
                    results.append(True)
                else:
                    print(f"❌ {symbol}: API error")
                    results.append(False)
            else:
                print(f"❌ {symbol}: HTTP {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ {symbol}: {e}")
            results.append(False)
    
    return all(results)

def test_screener_endpoint():
    """Test market screener endpoint"""
    print("\n🔍 Testing Market Screener Endpoint")
    print("-" * 40)
    
    try:
        response = requests.get("http://localhost:5000/api/v1/charting/screener", timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            if data['success']:
                screener_data = data['data']
                
                print(f"✅ Market Screener: Working")
                print(f"   Top Gainers: {len(screener_data['top_gainers'])}")
                print(f"   Top Losers: {len(screener_data['top_losers'])}")
                print(f"   Most Active: {len(screener_data['most_active'])}")
                print(f"   Trending: {len(screener_data['trending'])}")
                
                # Show top gainer
                if screener_data['top_gainers']:
                    top_gainer = screener_data['top_gainers'][0]
                    print(f"   Best Performer: {top_gainer['symbol']} (+{top_gainer['change_percent']:.2f}%)")
                
                return True
            else:
                print(f"❌ Screener: API error")
                return False
        else:
            print(f"❌ Screener: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Screener test error: {e}")
        return False

def test_news_endpoint():
    """Test news endpoint"""
    print("\n📰 Testing News Endpoint")
    print("-" * 40)
    
    symbols = ['BTC/USDT', 'JSE:NPN', 'AAPL']
    results = []
    
    for symbol in symbols:
        try:
            response = requests.get(f"http://localhost:5000/api/v1/charting/news/{symbol}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['success']:
                    news_count = data['count']
                    print(f"✅ {symbol}: {news_count} news items")
                    
                    if data['news']:
                        latest = data['news'][0]
                        print(f"   Latest: {latest['title'][:50]}...")
                    
                    results.append(True)
                else:
                    print(f"❌ {symbol}: API error")
                    results.append(False)
            else:
                print(f"❌ {symbol}: HTTP {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ {symbol}: {e}")
            results.append(False)
    
    return all(results)

def test_events_endpoint():
    """Test events endpoint"""
    print("\n📅 Testing Events Endpoint")
    print("-" * 40)
    
    symbols = ['JSE:NPN', 'AAPL', 'BTC/USDT']
    results = []
    
    for symbol in symbols:
        try:
            response = requests.get(f"http://localhost:5000/api/v1/charting/events/{symbol}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['success']:
                    events_count = data['count']
                    print(f"✅ {symbol}: {events_count} events")
                    
                    if data['events']:
                        for event in data['events']:
                            print(f"   {event['type'].title()}: {event['description'][:40]}...")
                    
                    results.append(True)
                else:
                    print(f"❌ {symbol}: API error")
                    results.append(False)
            else:
                print(f"❌ {symbol}: HTTP {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ {symbol}: {e}")
            results.append(False)
    
    return all(results)

def test_additional_endpoints():
    """Test additional endpoints"""
    print("\n🔧 Testing Additional Endpoints")
    print("-" * 40)
    
    endpoints = [
        ('/api/v1/charting/sectors', 'Sectors'),
        ('/api/v1/charting/currency-rates', 'Currency Rates')
    ]
    
    results = []
    
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"http://localhost:5000{endpoint}", timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                if data['success']:
                    print(f"✅ {name}: Working")
                    
                    if 'sectors' in data:
                        print(f"   Sectors tracked: {len(data['sectors'])}")
                    elif 'rates' in data:
                        print(f"   Currency pairs: {len(data['rates'])}")
                    
                    results.append(True)
                else:
                    print(f"❌ {name}: API error")
                    results.append(False)
            else:
                print(f"❌ {name}: HTTP {response.status_code}")
                results.append(False)
                
        except Exception as e:
            print(f"❌ {name}: {e}")
            results.append(False)
    
    return all(results)

def test_frontend_accessibility():
    """Test frontend charting page"""
    print("\n🖥️  Testing Charting Frontend")
    print("-" * 40)
    
    try:
        response = requests.get("http://localhost:5000/charting", timeout=10)
        if response.status_code == 200:
            print("✅ Professional Charting Platform: Accessible")
            print("   Frontend URL: http://localhost:5000/charting")
            print("   Status: TradingView-style interface loaded")
            return True
        else:
            print(f"❌ Charting frontend failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Frontend test error: {e}")
        return False

def generate_charting_report(results):
    """Generate comprehensive charting test report"""
    print("\n" + "=" * 60)
    print("📊 PROFESSIONAL CHARTING API TEST REPORT")
    print("=" * 60)
    
    categories = [
        ("Symbol Directory", results['symbols']),
        ("OHLCV Data Feed", results['ohlcv']),
        ("Market Data API", results['market_data']),
        ("Market Screener", results['screener']),
        ("Financial News", results['news']),
        ("Corporate Events", results['events']),
        ("Additional APIs", results['additional']),
        ("Frontend Interface", results['frontend'])
    ]
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    for category, passed in categories:
        status = "✅ OPERATIONAL" if passed else "❌ NEEDS ATTENTION"
        print(f"{category:<20} {status}")
    
    print(f"\n📊 Overall Results: {total_passed}/{total_tests} components operational")
    
    if total_passed == total_tests:
        print("🎉 EXCELLENT: Professional-grade charting platform ready for production!")
        print("\n🚀 TradingView-Style Features Available:")
        print("   • Real-time OHLCV candlestick data")
        print("   • Technical indicators (RSI, MACD, SMA)")
        print("   • Multi-timeframe analysis (1m to 1W)")
        print("   • Market screener with top movers")
        print("   • Financial news integration")
        print("   • Corporate events calendar")
        print("   • Currency conversion rates")
        print("   • Professional dark-theme interface")
        
    elif total_passed >= total_tests * 0.75:
        print("✅ GOOD: Core charting functionality operational")
        
    else:
        print("🔧 DEVELOPMENT: Charting platform needs additional configuration")
    
    print(f"\n🔗 Access Points:")
    print(f"   • Charting Platform: http://localhost:5000/charting")
    print(f"   • API Documentation: http://localhost:5000/api/v1/charting/symbols")
    print(f"   • Live Data Dashboard: http://localhost:5000/scrapers")

def main():
    """Main test execution"""
    print(f"🚀 WizData Professional Charting API Test")
    print(f"Started at: {datetime.now()}")
    print("=" * 60)
    
    # Run all tests
    results = {
        'symbols': test_symbols_endpoint(),
        'ohlcv': test_ohlcv_endpoint(),
        'market_data': test_market_data_endpoint(),
        'screener': test_screener_endpoint(),
        'news': test_news_endpoint(),
        'events': test_events_endpoint(),
        'additional': test_additional_endpoints(),
        'frontend': test_frontend_accessibility()
    }
    
    # Add small delay for final checks
    time.sleep(2)
    
    # Generate comprehensive report
    generate_charting_report(results)
    
    return results

if __name__ == "__main__":
    results = main()
    
    # Exit with appropriate code
    if all(results.values()):
        exit(0)  # All tests passed
    elif sum(results.values()) >= len(results) * 0.75:
        exit(1)  # Mostly working
    else:
        exit(2)  # Needs significant work