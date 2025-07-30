#!/usr/bin/env python3
"""
WizData Comprehensive API Test Suite
Tests all endpoints, performance, and functionality
"""

import requests
import json
import time
import asyncio
import aiohttp
from datetime import datetime
from typing import Dict, List, Any
import concurrent.futures
from threading import Thread

class WizDataAPITester:
    """Comprehensive API testing suite for WizData platform"""
    
    def __init__(self, base_url: str = "http://localhost:5000"):
        self.base_url = base_url
        self.session = requests.Session()
        self.results = {
            'passed': 0,
            'failed': 0,
            'errors': [],
            'performance': {},
            'endpoints_tested': []
        }
    
    def log_result(self, test_name: str, passed: bool, duration: float = None, error: str = None):
        """Log test result"""
        if passed:
            self.results['passed'] += 1
            status = "✅ PASS"
        else:
            self.results['failed'] += 1
            status = "❌ FAIL"
            if error:
                self.results['errors'].append(f"{test_name}: {error}")
        
        duration_str = f" ({duration:.3f}s)" if duration else ""
        print(f"{status} {test_name}{duration_str}")
        
        if duration:
            self.results['performance'][test_name] = duration
        
        self.results['endpoints_tested'].append(test_name)
    
    def test_health_endpoints(self):
        """Test health and status endpoints"""
        print("\n🏥 Testing Health Endpoints")
        print("-" * 40)
        
        endpoints = [
            ("/", "Main page"),
            ("/api/v1/data-services/health", "Data services health"),
            ("/api-services", "API services page"),
        ]
        
        for endpoint, name in endpoints:
            start_time = time.time()
            try:
                response = self.session.get(f"{self.base_url}{endpoint}", timeout=10)
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    self.log_result(name, True, duration)
                else:
                    self.log_result(name, False, duration, f"HTTP {response.status_code}")
                    
            except Exception as e:
                duration = time.time() - start_time
                self.log_result(name, False, duration, str(e))
    
    def test_charting_api(self):
        """Test professional charting API endpoints"""
        print("\n📊 Testing Charting API")
        print("-" * 40)
        
        # Test symbols endpoint
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/v1/charting/symbols", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('symbols'):
                    self.log_result("Get available symbols", True, duration)
                    
                    # Test with actual symbols from response
                    symbols_to_test = []
                    for category, symbol_list in data['symbols'].items():
                        if symbol_list:
                            symbols_to_test.append(symbol_list[0]['symbol'])
                    
                    # Test OHLCV for each symbol category
                    for symbol in symbols_to_test[:3]:  # Test first 3 symbols
                        self.test_ohlcv_data(symbol)
                        self.test_market_data(symbol)
                        
                else:
                    self.log_result("Get available symbols", False, duration, "Invalid response format")
            else:
                self.log_result("Get available symbols", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Get available symbols", False, duration, str(e))
        
        # Test other charting endpoints
        self.test_market_screener()
        self.test_sector_performance()
        self.test_currency_rates()
    
    def test_ohlcv_data(self, symbol: str):
        """Test OHLCV data endpoint for a specific symbol"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/charting/ohlcv/{symbol}"
            params = {'interval': '1h', 'limit': 10}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    self.log_result(f"OHLCV data for {symbol}", True, duration)
                else:
                    self.log_result(f"OHLCV data for {symbol}", False, duration, "No data returned")
            else:
                self.log_result(f"OHLCV data for {symbol}", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result(f"OHLCV data for {symbol}", False, duration, str(e))
    
    def test_market_data(self, symbol: str):
        """Test current market data endpoint"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/charting/market-data/{symbol}"
            response = self.session.get(url, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    self.log_result(f"Market data for {symbol}", True, duration)
                else:
                    self.log_result(f"Market data for {symbol}", False, duration, "No data returned")
            else:
                self.log_result(f"Market data for {symbol}", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result(f"Market data for {symbol}", False, duration, str(e))
    
    def test_market_screener(self):
        """Test market screener endpoint"""
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/v1/charting/screener", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    self.log_result("Market screener", True, duration)
                else:
                    self.log_result("Market screener", False, duration, "No data returned")
            else:
                self.log_result("Market screener", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Market screener", False, duration, str(e))
    
    def test_sector_performance(self):
        """Test sector performance endpoint"""
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/v1/charting/sectors", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('sectors'):
                    self.log_result("Sector performance", True, duration)
                else:
                    self.log_result("Sector performance", False, duration, "No data returned")
            else:
                self.log_result("Sector performance", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Sector performance", False, duration, str(e))
    
    def test_currency_rates(self):
        """Test currency rates endpoint"""
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/v1/charting/currency-rates", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('rates'):
                    self.log_result("Currency rates", True, duration)
                else:
                    self.log_result("Currency rates", False, duration, "No data returned")
            else:
                self.log_result("Currency rates", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Currency rates", False, duration, str(e))
    
    def test_data_services_api(self):
        """Test modular data services API"""
        print("\n🔧 Testing Data Services API")
        print("-" * 40)
        
        # Test multi-symbol market data
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/market-data"
            params = {'symbols': ['JSE:NPN', 'BTC/USDT'], 'type': 'quote'}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('data'):
                    self.log_result("Multi-symbol market data", True, duration)
                else:
                    self.log_result("Multi-symbol market data", False, duration, "No data returned")
            else:
                self.log_result("Multi-symbol market data", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Multi-symbol market data", False, duration, str(e))
        
        # Test technical indicators
        self.test_technical_indicators("JSE:NPN")
        
        # Test company profile
        self.test_company_profile("JSE:NPN")
        
        # Test market screener with criteria
        self.test_data_services_screener()
        
        # Test events calendar
        self.test_events_calendar()
        
        # Test news feed
        self.test_news_feed()
    
    def test_technical_indicators(self, symbol: str):
        """Test technical indicators endpoint"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/indicators/{symbol}"
            params = {'indicators': ['RSI', 'MACD', 'SMA']}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('indicators'):
                    self.log_result(f"Technical indicators for {symbol}", True, duration)
                else:
                    self.log_result(f"Technical indicators for {symbol}", False, duration, "No indicators returned")
            else:
                self.log_result(f"Technical indicators for {symbol}", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result(f"Technical indicators for {symbol}", False, duration, str(e))
    
    def test_company_profile(self, symbol: str):
        """Test company profile endpoint"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/profile/{symbol}"
            response = self.session.get(url, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success') and data.get('profile'):
                    self.log_result(f"Company profile for {symbol}", True, duration)
                else:
                    self.log_result(f"Company profile for {symbol}", False, duration, "No profile returned")
            else:
                self.log_result(f"Company profile for {symbol}", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result(f"Company profile for {symbol}", False, duration, str(e))
    
    def test_data_services_screener(self):
        """Test data services screener with criteria"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/screener"
            params = {'min_market_cap': 1000000, 'max_pe_ratio': 20}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    self.log_result("Market screener with criteria", True, duration)
                else:
                    self.log_result("Market screener with criteria", False, duration, "No results returned")
            else:
                self.log_result("Market screener with criteria", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Market screener with criteria", False, duration, str(e))
    
    def test_events_calendar(self):
        """Test events calendar endpoint"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/events"
            params = {'symbol': 'JSE:NPN', 'type': 'earnings'}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    self.log_result("Events calendar", True, duration)
                else:
                    self.log_result("Events calendar", False, duration, "No events returned")
            else:
                self.log_result("Events calendar", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Events calendar", False, duration, str(e))
    
    def test_news_feed(self):
        """Test news feed endpoint"""
        start_time = time.time()
        try:
            url = f"{self.base_url}/api/v1/data-services/news"
            params = {'symbol': 'JSE:NPN', 'limit': 5}
            response = self.session.get(url, params=params, timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    self.log_result("News feed", True, duration)
                else:
                    self.log_result("News feed", False, duration, "No news returned")
            else:
                self.log_result("News feed", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("News feed", False, duration, str(e))
    
    def test_scrapers_api(self):
        """Test live data collection API"""
        print("\n🤖 Testing Scrapers API")
        print("-" * 40)
        
        # Test sources endpoint
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/scrapers/sources", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                if data.get('sources'):
                    self.log_result("Data sources status", True, duration)
                else:
                    self.log_result("Data sources status", False, duration, "No sources returned")
            else:
                self.log_result("Data sources status", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Data sources status", False, duration, str(e))
        
        # Test jobs endpoint
        start_time = time.time()
        try:
            response = self.session.get(f"{self.base_url}/api/scrapers/jobs", timeout=10)
            duration = time.time() - start_time
            
            if response.status_code == 200:
                self.log_result("Collection jobs status", True, duration)
            else:
                self.log_result("Collection jobs status", False, duration, f"HTTP {response.status_code}")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Collection jobs status", False, duration, str(e))
    
    def test_frontend_interfaces(self):
        """Test frontend interface accessibility"""
        print("\n🖥️  Testing Frontend Interfaces")
        print("-" * 40)
        
        interfaces = [
            ("/", "Main platform"),
            ("/charting", "Professional charting"),
            ("/scrapers", "Live data dashboard"),
            ("/api-services", "API services page")
        ]
        
        for endpoint, name in interfaces:
            start_time = time.time()
            try:
                response = self.session.get(f"{self.base_url}{endpoint}", timeout=10)
                duration = time.time() - start_time
                
                if response.status_code == 200:
                    # Check if it's HTML content
                    if 'text/html' in response.headers.get('content-type', ''):
                        self.log_result(f"{name} interface", True, duration)
                    else:
                        self.log_result(f"{name} interface", False, duration, "Not HTML content")
                else:
                    self.log_result(f"{name} interface", False, duration, f"HTTP {response.status_code}")
                    
            except Exception as e:
                duration = time.time() - start_time
                self.log_result(f"{name} interface", False, duration, str(e))
    
    def test_performance(self):
        """Test API performance with concurrent requests"""
        print("\n⚡ Testing Performance")
        print("-" * 40)
        
        def make_request(url):
            start = time.time()
            try:
                response = self.session.get(url, timeout=5)
                return time.time() - start, response.status_code == 200
            except:
                return time.time() - start, False
        
        # Test concurrent requests to symbols endpoint
        url = f"{self.base_url}/api/v1/charting/symbols"
        num_requests = 10
        
        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_requests) as executor:
            futures = [executor.submit(make_request, url) for _ in range(num_requests)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        total_time = time.time() - start_time
        successful_requests = sum(1 for _, success in results if success)
        avg_response_time = sum(duration for duration, _ in results) / num_requests
        
        success_rate = (successful_requests / num_requests) * 100
        
        print(f"   Concurrent requests: {num_requests}")
        print(f"   Total time: {total_time:.3f}s")
        print(f"   Success rate: {success_rate:.1f}%")
        print(f"   Average response time: {avg_response_time:.3f}s")
        
        self.log_result("Concurrent performance test", success_rate >= 80, total_time)
    
    def run_all_tests(self):
        """Run complete test suite"""
        print("🚀 WizData API Test Suite")
        print("=" * 60)
        print(f"Testing server: {self.base_url}")
        print(f"Started at: {datetime.now()}")
        print()
        
        start_time = time.time()
        
        # Run all test categories
        self.test_health_endpoints()
        self.test_charting_api()
        self.test_data_services_api()
        self.test_scrapers_api()
        self.test_frontend_interfaces()
        self.test_performance()
        
        total_time = time.time() - start_time
        
        # Generate final report
        self.generate_report(total_time)
    
    def generate_report(self, total_time: float):
        """Generate comprehensive test report"""
        print("\n" + "=" * 60)
        print("📊 COMPREHENSIVE API TEST REPORT")
        print("=" * 60)
        
        total_tests = self.results['passed'] + self.results['failed']
        success_rate = (self.results['passed'] / total_tests * 100) if total_tests > 0 else 0
        
        print(f"Total Tests: {total_tests}")
        print(f"Passed: {self.results['passed']} ✅")
        print(f"Failed: {self.results['failed']} ❌")
        print(f"Success Rate: {success_rate:.1f}%")
        print(f"Total Time: {total_time:.2f}s")
        
        # Performance summary
        if self.results['performance']:
            avg_response = sum(self.results['performance'].values()) / len(self.results['performance'])
            fastest = min(self.results['performance'].values())
            slowest = max(self.results['performance'].values())
            
            print(f"\n📈 Performance Summary:")
            print(f"Average Response Time: {avg_response:.3f}s")
            print(f"Fastest Response: {fastest:.3f}s")
            print(f"Slowest Response: {slowest:.3f}s")
        
        # Error summary
        if self.results['errors']:
            print(f"\n❌ Errors Encountered:")
            for error in self.results['errors']:
                print(f"   • {error}")
        
        # Overall assessment
        print(f"\n🎯 Overall Assessment:")
        if success_rate >= 95:
            print("🎉 EXCELLENT: API platform fully operational")
        elif success_rate >= 80:
            print("✅ GOOD: Core functionality working with minor issues")
        elif success_rate >= 60:
            print("⚠️  FAIR: Basic functionality working, needs attention")
        else:
            print("❌ POOR: Significant issues detected, requires immediate attention")
        
        print(f"\n🔗 Access Points:")
        print(f"   • Main Platform: {self.base_url}")
        print(f"   • Professional Charting: {self.base_url}/charting")
        print(f"   • Live Data Dashboard: {self.base_url}/scrapers")
        print(f"   • API Documentation: {self.base_url}/api-services")

def main():
    """Main test execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='WizData API Test Suite')
    parser.add_argument('--url', default='http://localhost:5000', 
                       help='Base URL of WizData instance (default: http://localhost:5000)')
    parser.add_argument('--quick', action='store_true', 
                       help='Run quick tests only (skip performance tests)')
    
    args = parser.parse_args()
    
    tester = WizDataAPITester(args.url)
    
    if args.quick:
        print("Running quick test suite...")
        tester.test_health_endpoints()
        tester.test_charting_api()
        tester.generate_report(0)
    else:
        tester.run_all_tests()

if __name__ == "__main__":
    main()