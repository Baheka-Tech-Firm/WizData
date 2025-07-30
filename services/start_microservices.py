#!/usr/bin/env python3
"""
WizData Microservices Launcher
Starts all microservices with proper dependency management
"""

import subprocess
import time
import sys
import os
import signal
import threading
from typing import List, Dict
import requests
import json
from datetime import datetime

class MicroservicesManager:
    """Manages WizData microservices startup and monitoring"""
    
    def __init__(self):
        self.services = {
            'market-data': {
                'port': 5001,
                'script': 'market_data_service.py',
                'dependencies': [],
                'health_endpoint': '/health'
            },
            'symbol-registry': {
                'port': 5002,
                'script': 'symbol_registry_service.py',
                'dependencies': [],
                'health_endpoint': '/health'
            },
            'indicator-engine': {
                'port': 5003,
                'script': 'indicator_engine_service.py',
                'dependencies': ['market-data'],
                'health_endpoint': '/health'
            },
            'streaming': {
                'port': 5004,
                'script': 'streaming_service.py',
                'dependencies': ['market-data', 'symbol-registry'],
                'health_endpoint': '/health'
            }
        }
        
        self.processes: Dict[str, subprocess.Popen] = {}
        self.startup_timeout = 30  # seconds
        self.shutdown_timeout = 10  # seconds
    
    def check_port_available(self, port: int) -> bool:
        """Check if port is available"""
        import socket
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(1)
            result = sock.connect_ex(('localhost', port))
            sock.close()
            return result != 0  # Port is available if connection fails
        except:
            return True
    
    def wait_for_service_health(self, service_name: str, timeout: int = 30) -> bool:
        """Wait for service to become healthy"""
        service_config = self.services[service_name]
        port = service_config['port']
        endpoint = service_config['health_endpoint']
        
        url = f"http://localhost:{port}{endpoint}"
        
        print(f"  Waiting for {service_name} health check...")
        
        for attempt in range(timeout):
            try:
                response = requests.get(url, timeout=2)
                if response.status_code == 200:
                    print(f"  ✅ {service_name} is healthy")
                    return True
            except:
                pass
            
            time.sleep(1)
            if attempt % 5 == 0 and attempt > 0:
                print(f"  ⏱️  Still waiting for {service_name}... ({attempt}s)")
        
        print(f"  ❌ {service_name} health check failed after {timeout}s")
        return False
    
    def start_service(self, service_name: str) -> bool:
        """Start a single microservice"""
        if service_name in self.processes:
            print(f"  ⚠️  {service_name} already running")
            return True
        
        service_config = self.services[service_name]
        port = service_config['port']
        script = service_config['script']
        
        # Check if port is available
        if not self.check_port_available(port):
            print(f"  ❌ Port {port} is already in use for {service_name}")
            return False
        
        print(f"  🚀 Starting {service_name} on port {port}...")
        
        try:
            # Set environment variables for service URLs
            env = os.environ.copy()
            env['MARKET_DATA_SERVICE_URL'] = 'http://localhost:5001'
            env['SYMBOL_REGISTRY_SERVICE_URL'] = 'http://localhost:5002'
            env['INDICATOR_ENGINE_SERVICE_URL'] = 'http://localhost:5003'
            env['STREAMING_SERVICE_URL'] = 'http://localhost:5004'
            
            # Start the service
            process = subprocess.Popen(
                [sys.executable, script],
                cwd='/home/runner/workspace/services',
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                preexec_fn=os.setsid  # Create new process group
            )
            
            self.processes[service_name] = process
            
            # Wait a moment for startup
            time.sleep(2)
            
            # Check if process is still running
            if process.poll() is not None:
                stdout, stderr = process.communicate()
                print(f"  ❌ {service_name} failed to start")
                print(f"     stdout: {stdout.decode()}")
                print(f"     stderr: {stderr.decode()}")
                return False
            
            # Wait for health check
            if self.wait_for_service_health(service_name, self.startup_timeout):
                print(f"  ✅ {service_name} started successfully")
                return True
            else:
                print(f"  ❌ {service_name} failed health check")
                self.stop_service(service_name)
                return False
                
        except Exception as e:
            print(f"  ❌ Error starting {service_name}: {e}")
            return False
    
    def stop_service(self, service_name: str):
        """Stop a single microservice"""
        if service_name not in self.processes:
            return
        
        print(f"  🛑 Stopping {service_name}...")
        
        process = self.processes[service_name]
        
        try:
            # Send SIGTERM to process group
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            
            # Wait for graceful shutdown
            try:
                process.wait(timeout=self.shutdown_timeout)
            except subprocess.TimeoutExpired:
                # Force kill if not gracefully stopped
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                process.wait()
            
            print(f"  ✅ {service_name} stopped")
            
        except Exception as e:
            print(f"  ⚠️  Error stopping {service_name}: {e}")
        
        finally:
            del self.processes[service_name]
    
    def start_all_services(self) -> bool:
        """Start all microservices in dependency order"""
        print("🏗️  Starting WizData Microservices...")
        print("=" * 50)
        
        # Build dependency graph and start in order
        started_services = set()
        remaining_services = set(self.services.keys())
        
        while remaining_services:
            # Find services with satisfied dependencies
            ready_services = []
            
            for service_name in remaining_services:
                dependencies = self.services[service_name]['dependencies']
                if all(dep in started_services for dep in dependencies):
                    ready_services.append(service_name)
            
            if not ready_services:
                print("❌ Circular dependency detected or all remaining services have unsatisfied dependencies")
                return False
            
            # Start ready services
            for service_name in ready_services:
                if self.start_service(service_name):
                    started_services.add(service_name)
                    remaining_services.remove(service_name)
                else:
                    print(f"❌ Failed to start {service_name}")
                    return False
        
        print("\n🎉 All microservices started successfully!")
        self.print_service_summary()
        return True
    
    def stop_all_services(self):
        """Stop all microservices"""
        print("\n🛑 Stopping all microservices...")
        
        # Stop in reverse dependency order
        service_names = list(self.processes.keys())
        service_names.reverse()
        
        for service_name in service_names:
            self.stop_service(service_name)
        
        print("✅ All microservices stopped")
    
    def print_service_summary(self):
        """Print summary of running services"""
        print("\n📊 Service Status Summary")
        print("-" * 50)
        
        for service_name, config in self.services.items():
            port = config['port']
            status = "🟢 Running" if service_name in self.processes else "🔴 Stopped"
            print(f"{service_name:20} {status:12} http://localhost:{port}")
        
        print("\n🔗 API Endpoints:")
        print("Market Data:     http://localhost:5001/docs")
        print("Symbol Registry: http://localhost:5002/docs")
        print("Indicator Engine: http://localhost:5003/docs")
        print("Streaming:       http://localhost:5004/status")
        print("WebSocket:       ws://localhost:5004/ws")
    
    def monitor_services(self):
        """Monitor running services and restart if needed"""
        print("\n👁️  Monitoring services (Ctrl+C to stop)...")
        
        try:
            while True:
                for service_name in list(self.processes.keys()):
                    process = self.processes[service_name]
                    
                    # Check if process is still running
                    if process.poll() is not None:
                        print(f"⚠️  {service_name} stopped unexpectedly, restarting...")
                        del self.processes[service_name]
                        
                        # Restart the service
                        if not self.start_service(service_name):
                            print(f"❌ Failed to restart {service_name}")
                
                time.sleep(5)  # Check every 5 seconds
                
        except KeyboardInterrupt:
            print("\n📡 Stopping monitoring...")
            self.stop_all_services()
    
    def test_services(self):
        """Test all running services"""
        print("\n🧪 Testing microservices...")
        print("-" * 40)
        
        test_results = {}
        
        # Test Market Data Service
        try:
            response = requests.get('http://localhost:5001/quotes/BTC/USDT', timeout=5)
            test_results['market-data'] = response.status_code == 200
            print(f"Market Data Service: {'✅ Pass' if test_results['market-data'] else '❌ Fail'}")
        except:
            test_results['market-data'] = False
            print("Market Data Service: ❌ Fail")
        
        # Test Symbol Registry Service
        try:
            response = requests.get('http://localhost:5002/symbols/JSE:NPN', timeout=5)
            test_results['symbol-registry'] = response.status_code == 200
            print(f"Symbol Registry Service: {'✅ Pass' if test_results['symbol-registry'] else '❌ Fail'}")
        except:
            test_results['symbol-registry'] = False
            print("Symbol Registry Service: ❌ Fail")
        
        # Test Indicator Engine Service
        try:
            response = requests.get('http://localhost:5003/indicators/available', timeout=5)
            test_results['indicator-engine'] = response.status_code == 200
            print(f"Indicator Engine Service: {'✅ Pass' if test_results['indicator-engine'] else '❌ Fail'}")
        except:
            test_results['indicator-engine'] = False
            print("Indicator Engine Service: ❌ Fail")
        
        # Test Streaming Service
        try:
            response = requests.get('http://localhost:5004/status', timeout=5)
            test_results['streaming'] = response.status_code == 200
            print(f"Streaming Service: {'✅ Pass' if test_results['streaming'] else '❌ Fail'}")
        except:
            test_results['streaming'] = False
            print("Streaming Service: ❌ Fail")
        
        all_passed = all(test_results.values())
        print(f"\nOverall Test Result: {'✅ All Pass' if all_passed else '❌ Some Failed'}")
        
        return test_results

def main():
    """Main function"""
    manager = MicroservicesManager()
    
    def signal_handler(signum, frame):
        print("\n🛑 Received shutdown signal...")
        manager.stop_all_services()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='WizData Microservices Manager')
    parser.add_argument('action', choices=['start', 'stop', 'restart', 'test', 'monitor'], 
                       help='Action to perform')
    
    args = parser.parse_args()
    
    if args.action == 'start':
        success = manager.start_all_services()
        if success:
            print("\n🎯 Use 'python start_microservices.py monitor' to monitor services")
            print("🧪 Use 'python start_microservices.py test' to test services")
    
    elif args.action == 'stop':
        manager.stop_all_services()
    
    elif args.action == 'restart':
        manager.stop_all_services()
        time.sleep(2)
        manager.start_all_services()
    
    elif args.action == 'test':
        manager.test_services()
    
    elif args.action == 'monitor':
        manager.monitor_services()

if __name__ == "__main__":
    main()