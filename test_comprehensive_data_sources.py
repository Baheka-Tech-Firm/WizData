#!/usr/bin/env python3
"""
Comprehensive Data Sources Test
Tests all implemented data sources and frontend exposure
"""

import requests
import json
import time
from datetime import datetime

def test_scraper_status():
    """Test scraper system status"""
    print("🔧 Testing WizData Comprehensive Data Sources")
    print("=" * 60)
    
    try:
        response = requests.get("http://localhost:5000/api/scrapers/status", timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"✅ System Status: {data['status']}")
            print(f"   Available Scrapers: {data['jobs_summary']['available_scrapers']}")
            print(f"   Enabled Jobs: {data['jobs_summary']['enabled']}/{data['jobs_summary']['total']}")
            return True
        else:
            print(f"❌ Status check failed: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ Error checking status: {e}")
        return False

def test_crypto_data():
    """Test cryptocurrency data collection"""
    print("\n📊 Testing Crypto Data Collection")
    print("-" * 40)
    
    try:
        response = requests.post(
            "http://localhost:5000/api/scrapers/jobs/crypto_prices/run",
            headers={'Content-Type': 'application/json'},
            json={},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"✅ Crypto Scraper: {result['success']}")
            if result['success']:
                print(f"   Duration: {result['duration_seconds']:.2f}s")
                print(f"   Items: {result['items_processed']}")
                print(f"   Quality: {result['quality_score']:.2f}")
            return result['success']
        else:
            print(f"❌ Crypto scraper failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Crypto test error: {e}")
        return False

def test_news_data():
    """Test financial news data collection"""
    print("\n📰 Testing News Data Collection")
    print("-" * 40)
    
    try:
        response = requests.post(
            "http://localhost:5000/api/scrapers/jobs/financial_news/run",
            headers={'Content-Type': 'application/json'},
            json={},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"📰 News Scraper: {result['success']}")
            if result['success']:
                print(f"   Duration: {result['duration_seconds']:.2f}s")
                print(f"   Items: {result['items_processed']}")
                print(f"   Quality: {result['quality_score']:.2f}")
            else:
                print(f"   Errors: {result.get('errors', [])}")
            return result['success']
        else:
            print(f"❌ News scraper failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ News test error: {e}")
        return False

def test_forex_data():
    """Test forex data collection"""
    print("\n💱 Testing Forex Data Collection")
    print("-" * 40)
    
    try:
        response = requests.post(
            "http://localhost:5000/api/scrapers/jobs/forex_rates/run",
            headers={'Content-Type': 'application/json'},
            json={},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"💱 Forex Scraper: {result['success']}")
            if result['success']:
                print(f"   Duration: {result['duration_seconds']:.2f}s")
                print(f"   Items: {result['items_processed']}")
                print(f"   Quality: {result['quality_score']:.2f}")
            else:
                print(f"   Errors: {result.get('errors', [])}")
            return result['success']
        else:
            print(f"❌ Forex scraper failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Forex test error: {e}")
        return False

def test_economic_data():
    """Test economic data collection"""
    print("\n📈 Testing Economic Data Collection")
    print("-" * 40)
    
    try:
        response = requests.post(
            "http://localhost:5000/api/scrapers/jobs/economic_data/run",
            headers={'Content-Type': 'application/json'},
            json={},
            timeout=30
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"📈 Economic Scraper: {result['success']}")
            if result['success']:
                print(f"   Duration: {result['duration_seconds']:.2f}s")
                print(f"   Items: {result['items_processed']}")
                print(f"   Quality: {result['quality_score']:.2f}")
            else:
                print(f"   Errors: {result.get('errors', [])}")
            return result['success']
        else:
            print(f"❌ Economic scraper failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Economic test error: {e}")
        return False

def test_frontend_exposure():
    """Test frontend dashboard access"""
    print("\n🖥️  Testing Frontend Exposure")
    print("-" * 40)
    
    try:
        response = requests.get("http://localhost:5000/scrapers", timeout=10)
        if response.status_code == 200:
            print("✅ Live Data Dashboard: Accessible")
            print("   Frontend URL: http://localhost:5000/scrapers")
            print("   Status: Loaded successfully")
            return True
        else:
            print(f"❌ Frontend failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Frontend test error: {e}")
        return False

def test_api_endpoints():
    """Test all API endpoints"""
    print("\n🔌 Testing API Endpoints")
    print("-" * 40)
    
    endpoints = [
        ("/api/scrapers/status", "System Status"),
        ("/api/scrapers/jobs", "Jobs Listing"),
        ("/api/scrapers/health", "Health Check"),
        ("/api/status", "General API Status")
    ]
    
    results = []
    for endpoint, name in endpoints:
        try:
            response = requests.get(f"http://localhost:5000{endpoint}", timeout=10)
            if response.status_code == 200:
                print(f"✅ {name}: Working")
                results.append(True)
            else:
                print(f"❌ {name}: Failed ({response.status_code})")
                results.append(False)
        except Exception as e:
            print(f"❌ {name}: Error ({e})")
            results.append(False)
    
    return all(results)

def generate_test_report(results):
    """Generate comprehensive test report"""
    print("\n" + "=" * 60)
    print("📋 COMPREHENSIVE DATA SOURCES TEST REPORT")
    print("=" * 60)
    
    categories = [
        ("System Status", results['status']),
        ("Cryptocurrency Data", results['crypto']),
        ("Financial News", results['news']),
        ("Forex Rates", results['forex']),
        ("Economic Indicators", results['economic']),
        ("Frontend Dashboard", results['frontend']),
        ("API Endpoints", results['api'])
    ]
    
    total_passed = sum(results.values())
    total_tests = len(results)
    
    for category, passed in categories:
        status = "✅ OPERATIONAL" if passed else "❌ NEEDS ATTENTION"
        print(f"{category:<25} {status}")
    
    print(f"\n📊 Overall Results: {total_passed}/{total_tests} components operational")
    
    if total_passed == total_tests:
        print("🎉 EXCELLENT: All data sources and frontend components working!")
        print("\n🚀 Ready for Production:")
        print("   • Live cryptocurrency data collection")
        print("   • Real-time financial news aggregation")
        print("   • Comprehensive forex rate tracking")
        print("   • Economic indicators monitoring")
        print("   • Interactive frontend dashboard")
        print("   • Full API management interface")
        
    elif total_passed >= total_tests * 0.75:
        print("✅ GOOD: Most components operational, minor issues to resolve")
        
    elif total_passed >= total_tests * 0.5:
        print("⚠️  PARTIAL: Significant functionality available, some components need work")
        
    else:
        print("🔧 DEVELOPMENT: System needs configuration and troubleshooting")
    
    print(f"\n🔗 Access Points:")
    print(f"   • Live Dashboard: http://localhost:5000/scrapers")
    print(f"   • API Status: http://localhost:5000/api/status")
    print(f"   • Main Platform: http://localhost:5000/")

def main():
    """Main test execution"""
    print(f"🚀 WizData Comprehensive Data Sources Test")
    print(f"Started at: {datetime.now()}")
    print("=" * 60)
    
    # Run all tests
    results = {
        'status': test_scraper_status(),
        'crypto': test_crypto_data(),
        'news': test_news_data(),
        'forex': test_forex_data(),
        'economic': test_economic_data(),
        'frontend': test_frontend_exposure(),
        'api': test_api_endpoints()
    }
    
    # Add small delay for final checks
    time.sleep(2)
    
    # Generate comprehensive report
    generate_test_report(results)
    
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