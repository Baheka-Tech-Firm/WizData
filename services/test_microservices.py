#!/usr/bin/env python3
"""
Comprehensive Microservices Test Suite
Tests all WizData microservices functionality
"""

import asyncio
import aiohttp
import json
import time
from datetime import datetime
from typing import Dict, List, Any
import websockets

class MicroservicesTestSuite:
    """Comprehensive test suite for WizData microservices"""
    
    def __init__(self):
        self.base_urls = {
            'market-data': 'http://localhost:5001',
            'symbol-registry': 'http://localhost:5002',
            'indicator-engine': 'http://localhost:5003',
            'streaming': 'http://localhost:5004'
        }
        
        self.results = {
            'passed': 0,
            'failed': 0,
            'errors': [],
            'performance': {},
            'services_tested': []
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
        
        self.results['services_tested'].append(test_name)
    
    async def test_market_data_service(self):
        """Test Market Data Service endpoints"""
        print("\n💹 Testing Market Data Service")
        print("-" * 40)
        
        base_url = self.base_urls['market-data']
        
        async with aiohttp.ClientSession() as session:
            # Test health endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/health") as response:
                    duration = time.time() - start_time
                    self.log_result("Market Data Health Check", response.status == 200, duration)
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Market Data Health Check", False, duration, str(e))
            
            # Test single quote
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/quotes/BTC/USDT") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'data' in data
                        self.log_result("Get Single Quote", success, duration)
                    else:
                        self.log_result("Get Single Quote", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Single Quote", False, duration, str(e))
            
            # Test multiple quotes
            start_time = time.time()
            try:
                params = [('symbols', 'BTC/USDT'), ('symbols', 'JSE:NPN')]
                async with session.get(f"{base_url}/quotes", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'data' in data
                        self.log_result("Get Multiple Quotes", success, duration)
                    else:
                        self.log_result("Get Multiple Quotes", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Multiple Quotes", False, duration, str(e))
            
            # Test OHLCV data
            start_time = time.time()
            try:
                params = {'interval': '1h', 'limit': 10}
                async with session.get(f"{base_url}/ohlcv/BTC/USDT", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and isinstance(data.get('data'), list)
                        self.log_result("Get OHLCV Data", success, duration)
                    else:
                        self.log_result("Get OHLCV Data", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get OHLCV Data", False, duration, str(e))
    
    async def test_symbol_registry_service(self):
        """Test Symbol Registry Service endpoints"""
        print("\n📊 Testing Symbol Registry Service")
        print("-" * 40)
        
        base_url = self.base_urls['symbol-registry']
        
        async with aiohttp.ClientSession() as session:
            # Test health endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/health") as response:
                    duration = time.time() - start_time
                    self.log_result("Symbol Registry Health Check", response.status == 200, duration)
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Symbol Registry Health Check", False, duration, str(e))
            
            # Test get all symbols
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/symbols") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and isinstance(data.get('data'), list)
                        self.log_result("Get All Symbols", success, duration)
                    else:
                        self.log_result("Get All Symbols", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get All Symbols", False, duration, str(e))
            
            # Test get specific symbol
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/symbols/JSE:NPN") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'data' in data
                        self.log_result("Get Specific Symbol", success, duration)
                    else:
                        self.log_result("Get Specific Symbol", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Specific Symbol", False, duration, str(e))
            
            # Test symbol search
            start_time = time.time()
            try:
                params = {'search': 'BTC'}
                async with session.get(f"{base_url}/symbols", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and isinstance(data.get('data'), list)
                        self.log_result("Symbol Search", success, duration)
                    else:
                        self.log_result("Symbol Search", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Symbol Search", False, duration, str(e))
            
            # Test streaming symbols
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/symbols/streaming/available") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and isinstance(data.get('data'), list)
                        self.log_result("Get Streaming Symbols", success, duration)
                    else:
                        self.log_result("Get Streaming Symbols", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Streaming Symbols", False, duration, str(e))
            
            # Test exchanges list
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/exchanges") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and isinstance(data.get('data'), list)
                        self.log_result("Get Exchanges", success, duration)
                    else:
                        self.log_result("Get Exchanges", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Exchanges", False, duration, str(e))
    
    async def test_indicator_engine_service(self):
        """Test Indicator Engine Service endpoints"""
        print("\n📈 Testing Indicator Engine Service")
        print("-" * 40)
        
        base_url = self.base_urls['indicator-engine']
        
        async with aiohttp.ClientSession() as session:
            # Test health endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/health") as response:
                    duration = time.time() - start_time
                    self.log_result("Indicator Engine Health Check", response.status == 200, duration)
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Indicator Engine Health Check", False, duration, str(e))
            
            # Test available indicators
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/indicators/available") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'indicators' in data
                        self.log_result("Get Available Indicators", success, duration)
                    else:
                        self.log_result("Get Available Indicators", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Available Indicators", False, duration, str(e))
            
            # Test RSI calculation
            start_time = time.time()
            try:
                params = [('indicators', 'RSI')]
                async with session.get(f"{base_url}/indicators/BTC/USDT", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'indicators' in data and 'rsi' in data['indicators']
                        self.log_result("Calculate RSI", success, duration)
                    else:
                        self.log_result("Calculate RSI", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Calculate RSI", False, duration, str(e))
            
            # Test multiple indicators
            start_time = time.time()
            try:
                params = [('indicators', 'RSI'), ('indicators', 'MACD'), ('indicators', 'SMA')]
                async with session.get(f"{base_url}/indicators/JSE:NPN", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        indicators = data.get('indicators', {})
                        success = all(ind in indicators for ind in ['rsi', 'macd', 'sma'])
                        self.log_result("Calculate Multiple Indicators", success, duration)
                    else:
                        self.log_result("Calculate Multiple Indicators", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Calculate Multiple Indicators", False, duration, str(e))
            
            # Test POST endpoint
            start_time = time.time()
            try:
                payload = {
                    'symbol': 'BTC/USDT',
                    'indicators': ['BOLLINGER', 'STOCHASTIC'],
                    'data_points': 50
                }
                async with session.post(f"{base_url}/calculate", json=payload) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'indicators' in data
                        self.log_result("POST Calculate Indicators", success, duration)
                    else:
                        self.log_result("POST Calculate Indicators", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("POST Calculate Indicators", False, duration, str(e))
    
    async def test_streaming_service(self):
        """Test Streaming Service endpoints"""
        print("\n🔄 Testing Streaming Service")
        print("-" * 40)
        
        base_url = self.base_urls['streaming']
        
        async with aiohttp.ClientSession() as session:
            # Test health endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/health") as response:
                    duration = time.time() - start_time
                    self.log_result("Streaming Service Health Check", response.status == 200, duration)
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Streaming Service Health Check", False, duration, str(e))
            
            # Test status endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/status") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'connections' in data
                        self.log_result("Get Streaming Status", success, duration)
                    else:
                        self.log_result("Get Streaming Status", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Streaming Status", False, duration, str(e))
            
            # Test connections endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/connections") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'connections' in data
                        self.log_result("Get Active Connections", success, duration)
                    else:
                        self.log_result("Get Active Connections", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Active Connections", False, duration, str(e))
            
            # Test subscriptions endpoint
            start_time = time.time()
            try:
                async with session.get(f"{base_url}/subscriptions") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'subscriptions' in data
                        self.log_result("Get Subscriptions", success, duration)
                    else:
                        self.log_result("Get Subscriptions", False, duration, f"HTTP {response.status}")
            except Exception as e:
                duration = time.time() - start_time
                self.log_result("Get Subscriptions", False, duration, str(e))
    
    async def test_websocket_connection(self):
        """Test WebSocket connection and messaging"""
        print("\n🔌 Testing WebSocket Connection")
        print("-" * 40)
        
        start_time = time.time()
        try:
            uri = "ws://localhost:5004/ws"
            
            async with websockets.connect(uri) as websocket:
                # Wait for welcome message
                welcome_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                welcome_data = json.loads(welcome_msg)
                
                if welcome_data.get('type') == 'welcome':
                    duration = time.time() - start_time
                    self.log_result("WebSocket Connection", True, duration)
                    
                    # Test subscription
                    subscribe_msg = {
                        'action': 'subscribe',
                        'symbols': ['BTC/USDT', 'JSE:NPN']
                    }
                    
                    await websocket.send(json.dumps(subscribe_msg))
                    
                    # Wait for subscription confirmation
                    confirm_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                    confirm_data = json.loads(confirm_msg)
                    
                    if confirm_data.get('type') == 'subscription_confirmed':
                        self.log_result("WebSocket Subscription", True)
                        
                        # Test ping/pong
                        ping_msg = {'action': 'ping'}
                        await websocket.send(json.dumps(ping_msg))
                        
                        pong_msg = await asyncio.wait_for(websocket.recv(), timeout=5)
                        pong_data = json.loads(pong_msg)
                        
                        if pong_data.get('type') == 'pong':
                            self.log_result("WebSocket Ping/Pong", True)
                        else:
                            self.log_result("WebSocket Ping/Pong", False, error="No pong response")
                    else:
                        self.log_result("WebSocket Subscription", False, error="No subscription confirmation")
                else:
                    duration = time.time() - start_time
                    self.log_result("WebSocket Connection", False, duration, "No welcome message")
                    
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("WebSocket Connection", False, duration, str(e))
    
    async def test_service_integration(self):
        """Test integration between services"""
        print("\n🔗 Testing Service Integration")
        print("-" * 40)
        
        # Test that indicator engine can get data from market data service
        start_time = time.time()
        try:
            async with aiohttp.ClientSession() as session:
                params = [('indicators', 'RSI')]
                async with session.get(f"{self.base_urls['indicator-engine']}/indicators/BTC/USDT", params=params) as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        data = await response.json()
                        success = data.get('success') and 'indicators' in data
                        self.log_result("Market Data → Indicator Engine", success, duration)
                    else:
                        self.log_result("Market Data → Indicator Engine", False, duration, f"HTTP {response.status}")
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Market Data → Indicator Engine", False, duration, str(e))
        
        # Test that streaming service can access symbol registry
        start_time = time.time()
        try:
            # This is tested indirectly through the streaming service status
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.base_urls['streaming']}/status") as response:
                    duration = time.time() - start_time
                    if response.status == 200:
                        self.log_result("Symbol Registry → Streaming", True, duration)
                    else:
                        self.log_result("Symbol Registry → Streaming", False, duration, f"HTTP {response.status}")
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Symbol Registry → Streaming", False, duration, str(e))
    
    async def run_performance_tests(self):
        """Run performance tests"""
        print("\n⚡ Running Performance Tests")
        print("-" * 40)
        
        # Concurrent requests test
        start_time = time.time()
        try:
            async with aiohttp.ClientSession() as session:
                tasks = []
                for _ in range(10):
                    task = session.get(f"{self.base_urls['market-data']}/quotes/BTC/USDT")
                    tasks.append(task)
                
                responses = await asyncio.gather(*tasks)
                duration = time.time() - start_time
                
                success_count = sum(1 for resp in responses if resp.status == 200)
                success_rate = (success_count / len(responses)) * 100
                
                # Close responses
                for resp in responses:
                    resp.close()
                
                self.log_result(f"Concurrent Requests (10x)", success_rate >= 80, duration)
                print(f"   Success rate: {success_rate:.1f}%")
                
        except Exception as e:
            duration = time.time() - start_time
            self.log_result("Concurrent Requests (10x)", False, duration, str(e))
    
    async def run_all_tests(self):
        """Run complete test suite"""
        print("🧪 WizData Microservices Test Suite")
        print("=" * 60)
        print(f"Started at: {datetime.now()}")
        print()
        
        start_time = time.time()
        
        # Run all test categories
        await self.test_market_data_service()
        await self.test_symbol_registry_service()
        await self.test_indicator_engine_service()
        await self.test_streaming_service()
        await self.test_websocket_connection()
        await self.test_service_integration()
        await self.run_performance_tests()
        
        total_time = time.time() - start_time
        
        # Generate final report
        self.generate_report(total_time)
    
    def generate_report(self, total_time: float):
        """Generate comprehensive test report"""
        print("\n" + "=" * 60)
        print("📊 MICROSERVICES TEST REPORT")
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
            for error in self.results['errors'][:5]:  # Show first 5 errors
                print(f"   • {error}")
            if len(self.results['errors']) > 5:
                print(f"   ... and {len(self.results['errors']) - 5} more")
        
        # Overall assessment
        print(f"\n🎯 Overall Assessment:")
        if success_rate >= 95:
            print("🎉 EXCELLENT: All microservices fully operational")
        elif success_rate >= 85:
            print("✅ GOOD: Core microservices working with minor issues")
        elif success_rate >= 70:
            print("⚠️  FAIR: Basic functionality working, some services need attention")
        else:
            print("❌ POOR: Significant issues detected, requires immediate attention")
        
        print(f"\n🔗 Service Endpoints:")
        for service, url in self.base_urls.items():
            print(f"   • {service.title()}: {url}")

async def main():
    """Main test execution"""
    import argparse
    
    parser = argparse.ArgumentParser(description='WizData Microservices Test Suite')
    parser.add_argument('--service', choices=['market-data', 'symbol-registry', 'indicator-engine', 'streaming', 'all'], 
                       default='all', help='Service to test (default: all)')
    parser.add_argument('--performance', action='store_true', help='Include performance tests')
    
    args = parser.parse_args()
    
    tester = MicroservicesTestSuite()
    
    if args.service == 'all':
        await tester.run_all_tests()
    elif args.service == 'market-data':
        await tester.test_market_data_service()
        tester.generate_report(0)
    elif args.service == 'symbol-registry':
        await tester.test_symbol_registry_service()
        tester.generate_report(0)
    elif args.service == 'indicator-engine':
        await tester.test_indicator_engine_service()
        tester.generate_report(0)
    elif args.service == 'streaming':
        await tester.test_streaming_service()
        await tester.test_websocket_connection()
        tester.generate_report(0)
    
    if args.performance:
        await tester.run_performance_tests()

if __name__ == "__main__":
    asyncio.run(main())