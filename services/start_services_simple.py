#!/usr/bin/env python3
"""
Simple WizData Microservices Starter
Starts all microservices in background processes
"""

import subprocess
import sys
import os
import time
import signal
from typing import Dict, List

class SimpleServiceManager:
    """Simple microservices manager for development"""
    
    def __init__(self):
        self.services = {
            'market-data': {
                'port': 5001,
                'command': [sys.executable, 'market_data_service.py']
            },
            'symbol-registry': {
                'port': 5002,
                'command': [sys.executable, 'symbol_registry_service.py']
            },
            'indicator-engine': {
                'port': 5003,
                'command': [sys.executable, 'indicator_engine_service.py']
            }
        }
        
        self.processes: Dict[str, subprocess.Popen] = {}
    
    def start_service(self, service_name: str) -> bool:
        """Start a service"""
        if service_name in self.processes:
            print(f"⚠️  {service_name} already running")
            return True
        
        service_config = self.services[service_name]
        print(f"🚀 Starting {service_name} on port {service_config['port']}...")
        
        try:
            # Set environment variables
            env = os.environ.copy()
            env['PYTHONPATH'] = '/home/runner/workspace/services'
            
            process = subprocess.Popen(
                service_config['command'],
                cwd='/home/runner/workspace/services',
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE
            )
            
            self.processes[service_name] = process
            time.sleep(2)  # Give service time to start
            
            # Check if still running
            if process.poll() is None:
                print(f"✅ {service_name} started successfully")
                return True
            else:
                stdout, stderr = process.communicate()
                print(f"❌ {service_name} failed to start")
                print(f"   Error: {stderr.decode()}")
                return False
                
        except Exception as e:
            print(f"❌ Error starting {service_name}: {e}")
            return False
    
    def stop_service(self, service_name: str):
        """Stop a service"""
        if service_name not in self.processes:
            return
        
        print(f"🛑 Stopping {service_name}...")
        process = self.processes[service_name]
        
        try:
            process.terminate()
            process.wait(timeout=5)
            print(f"✅ {service_name} stopped")
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            print(f"✅ {service_name} force stopped")
        except Exception as e:
            print(f"⚠️  Error stopping {service_name}: {e}")
        
        del self.processes[service_name]
    
    def start_all(self):
        """Start all services"""
        print("🏗️  Starting WizData Microservices (Simple Mode)")
        print("=" * 50)
        
        for service_name in self.services.keys():
            self.start_service(service_name)
        
        print("\n🎉 Microservices started!")
        self.print_status()
    
    def stop_all(self):
        """Stop all services"""
        print("\n🛑 Stopping all microservices...")
        
        for service_name in list(self.processes.keys()):
            self.stop_service(service_name)
        
        print("✅ All microservices stopped")
    
    def print_status(self):
        """Print service status"""
        print("\n📊 Service Status:")
        print("-" * 30)
        
        for service_name, config in self.services.items():
            port = config['port']
            status = "🟢 Running" if service_name in self.processes else "🔴 Stopped"
            print(f"{service_name:15} {status:12} :{port}")
        
        print("\n🔗 FastAPI Documentation:")
        for service_name, config in self.services.items():
            if service_name in self.processes:
                port = config['port']
                print(f"{service_name:15} http://localhost:{port}/docs")
    
    def monitor(self):
        """Simple monitoring"""
        print("\n👁️  Monitoring services (Ctrl+C to stop)...")
        
        try:
            while True:
                time.sleep(5)
                
                for service_name in list(self.processes.keys()):
                    process = self.processes[service_name]
                    if process.poll() is not None:
                        print(f"⚠️  {service_name} stopped unexpectedly")
                        del self.processes[service_name]
        
        except KeyboardInterrupt:
            print("\n📡 Stopping monitoring...")
            self.stop_all()

def main():
    manager = SimpleServiceManager()
    
    def signal_handler(signum, frame):
        print("\n🛑 Received shutdown signal...")
        manager.stop_all()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    import argparse
    parser = argparse.ArgumentParser(description='Simple WizData Microservices Manager')
    parser.add_argument('action', choices=['start', 'stop', 'monitor'], help='Action to perform')
    
    args = parser.parse_args()
    
    if args.action == 'start':
        manager.start_all()
        print("\n🎯 Use 'python start_services_simple.py monitor' to monitor services")
    elif args.action == 'stop':
        manager.stop_all()
    elif args.action == 'monitor':
        manager.monitor()

if __name__ == "__main__":
    main()