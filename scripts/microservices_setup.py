#!/usr/bin/env python3
"""
Microservices Architecture Setup Script for WizData Platform

This script prepares the monolithic WizData application for microservices transformation.
It creates the necessary directory structure, configuration files, and service stubs.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MicroservicesSetup:
    """
    Sets up the foundation for microservices architecture
    """
    
    SERVICES = {
        'data-ingestion-service': {
            'description': 'Handles real-time data ingestion from external sources',
            'port': 5001,
            'dependencies': ['kafka', 'redis', 'postgresql'],
            'responsibilities': [
                'Real-time market data ingestion',
                'ESG data collection',
                'News and sentiment data gathering',
                'Data validation and preprocessing',
                'Message queue publishing'
            ]
        },
        'data-cleaning-service': {
            'description': 'Processes and cleans raw data',
            'port': 5002,
            'dependencies': ['kafka', 'postgresql', 'minio'],
            'responsibilities': [
                'Data quality validation',
                'Outlier detection and handling',
                'Data standardization',
                'Historical data processing',
                'Archive management'
            ]
        },
        'indicator-engine': {
            'description': 'Computes financial and ESG indicators',
            'port': 5003,
            'dependencies': ['postgresql', 'redis'],
            'responsibilities': [
                'Technical indicator calculations',
                'ESG score computations',
                'Risk metrics calculation',
                'Performance analytics',
                'Continuous aggregates management'
            ]
        },
        'api-gateway': {
            'description': 'Central API gateway with authentication and routing',
            'port': 5000,
            'dependencies': ['redis', 'all-services'],
            'responsibilities': [
                'Request routing and load balancing',
                'Authentication and authorization',
                'Rate limiting and throttling',
                'API versioning',
                'Response caching and aggregation'
            ]
        },
        'auth-service': {
            'description': 'Handles authentication and user management',
            'port': 5004,
            'dependencies': ['postgresql', 'redis'],
            'responsibilities': [
                'User authentication',
                'JWT token management',
                'Role-based access control',
                'API key management',
                'Session management'
            ]
        },
        'notification-service': {
            'description': 'Manages alerts and notifications',
            'port': 5005,
            'dependencies': ['kafka', 'redis'],
            'responsibilities': [
                'Price alerts',
                'ESG threshold notifications',
                'System health alerts',
                'Email and SMS delivery',
                'Webhook notifications'
            ]
        }
    }
    
    def __init__(self, base_path: str = "."):
        self.base_path = Path(base_path)
        self.services_dir = self.base_path / "services"
        self.infrastructure_dir = self.base_path / "infrastructure"
        self.config_dir = self.base_path / "config" / "microservices"
    
    def create_directory_structure(self):
        """Create the microservices directory structure"""
        logger.info("Creating microservices directory structure...")
        
        # Create main directories
        directories = [
            self.services_dir,
            self.infrastructure_dir,
            self.config_dir,
            self.base_path / "docker",
            self.base_path / "k8s",
            self.base_path / "scripts" / "deployment"
        ]
        
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        
        # Create service-specific directories
        for service_name in self.SERVICES.keys():
            service_dir = self.services_dir / service_name
            subdirs = ['src', 'tests', 'config', 'docker']
            
            for subdir in subdirs:
                (service_dir / subdir).mkdir(parents=True, exist_ok=True)
            
            logger.info(f"Created service structure for: {service_name}")
    
    def generate_service_configs(self):
        """Generate configuration files for each service"""
        logger.info("Generating service configuration files...")
        
        for service_name, service_info in self.SERVICES.items():
            config_file = self.config_dir / f"{service_name}.json"
            
            config = {
                "service": {
                    "name": service_name,
                    "description": service_info['description'],
                    "version": "1.0.0",
                    "port": service_info['port']
                },
                "dependencies": service_info['dependencies'],
                "responsibilities": service_info['responsibilities'],
                "environment": {
                    "development": {
                        "database_url": "${DATABASE_URL}",
                        "redis_url": "${REDIS_URL}",
                        "kafka_brokers": "${KAFKA_BROKERS}",
                        "log_level": "DEBUG"
                    },
                    "production": {
                        "database_url": "${DATABASE_URL}",
                        "redis_url": "${REDIS_URL}",
                        "kafka_brokers": "${KAFKA_BROKERS}",
                        "log_level": "INFO"
                    }
                },
                "health_check": {
                    "endpoint": "/health",
                    "interval": 30,
                    "timeout": 10,
                    "retries": 3
                },
                "monitoring": {
                    "metrics_enabled": True,
                    "tracing_enabled": True,
                    "logging_structured": True
                }
            }
            
            with open(config_file, 'w') as f:
                json.dump(config, f, indent=2)
            
            logger.info(f"Generated config for: {service_name}")
    
    def create_docker_files(self):
        """Create Docker files for each service"""
        logger.info("Creating Docker files...")
        
        # Main docker-compose.yml
        docker_compose = {
            "version": "3.8",
            "services": {},
            "networks": {
                "wizdata-network": {
                    "driver": "bridge"
                }
            },
            "volumes": {
                "postgres-data": {},
                "redis-data": {},
                "kafka-data": {},
                "minio-data": {}
            }
        }
        
        # Add infrastructure services
        docker_compose["services"].update({
            "postgres": {
                "image": "timescale/timescaledb:latest-pg14",
                "environment": [
                    "POSTGRES_DB=wizdata",
                    "POSTGRES_USER=wizdata",
                    "POSTGRES_PASSWORD=wizdata_password"
                ],
                "ports": ["5432:5432"],
                "volumes": ["postgres-data:/var/lib/postgresql/data"],
                "networks": ["wizdata-network"]
            },
            "redis": {
                "image": "redis:7-alpine",
                "ports": ["6379:6379"],
                "volumes": ["redis-data:/data"],
                "networks": ["wizdata-network"]
            },
            "kafka": {
                "image": "confluentinc/cp-kafka:latest",
                "environment": [
                    "KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181",
                    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092",
                    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1"
                ],
                "depends_on": ["zookeeper"],
                "ports": ["9092:9092"],
                "volumes": ["kafka-data:/var/lib/kafka/data"],
                "networks": ["wizdata-network"]
            },
            "zookeeper": {
                "image": "confluentinc/cp-zookeeper:latest",
                "environment": ["ZOOKEEPER_CLIENT_PORT=2181"],
                "networks": ["wizdata-network"]
            },
            "minio": {
                "image": "minio/minio:latest",
                "command": "server /data --console-address ':9001'",
                "environment": [
                    "MINIO_ROOT_USER=wizdata",
                    "MINIO_ROOT_PASSWORD=wizdata_password"
                ],
                "ports": ["9000:9000", "9001:9001"],
                "volumes": ["minio-data:/data"],
                "networks": ["wizdata-network"]
            }
        })
        
        # Add application services
        for service_name, service_info in self.SERVICES.items():
            docker_compose["services"][service_name.replace('-', '_')] = {
                "build": {
                    "context": f"./services/{service_name}",
                    "dockerfile": "docker/Dockerfile"
                },
                "ports": [f"{service_info['port']}:{service_info['port']}"],
                "environment": [
                    f"SERVICE_PORT={service_info['port']}",
                    "DATABASE_URL=postgresql://wizdata:wizdata_password@postgres:5432/wizdata",
                    "REDIS_URL=redis://redis:6379/0",
                    "KAFKA_BROKERS=kafka:9092"
                ],
                "depends_on": service_info['dependencies'][:3],  # Limit to actual infrastructure
                "networks": ["wizdata-network"],
                "restart": "unless-stopped"
            }
        
        # Write docker-compose.yml
        docker_compose_file = self.base_path / "docker" / "docker-compose.yml"
        with open(docker_compose_file, 'w') as f:
            import yaml
            yaml.dump(docker_compose, f, default_flow_style=False, indent=2)
        
        logger.info("Created docker-compose.yml")
        
        # Create individual Dockerfiles for each service
        for service_name in self.SERVICES.keys():
            dockerfile_path = self.services_dir / service_name / "docker" / "Dockerfile"
            
            dockerfile_content = f"""FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \\
    gcc \\
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/

# Set environment variables
ENV PYTHONPATH=/app
ENV SERVICE_NAME={service_name}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \\
    CMD curl -f http://localhost:${{SERVICE_PORT}}/health || exit 1

# Run the service
CMD ["python", "-m", "src.main"]
"""
            
            with open(dockerfile_path, 'w') as f:
                f.write(dockerfile_content)
            
            logger.info(f"Created Dockerfile for: {service_name}")
    
    def create_kubernetes_files(self):
        """Create Kubernetes deployment files"""
        logger.info("Creating Kubernetes files...")
        
        k8s_dir = self.base_path / "k8s"
        
        # Namespace
        namespace_yaml = """apiVersion: v1
kind: Namespace
metadata:
  name: wizdata
  labels:
    name: wizdata
"""
        
        with open(k8s_dir / "namespace.yaml", 'w') as f:
            f.write(namespace_yaml)
        
        # ConfigMap for shared configuration
        configmap_yaml = """apiVersion: v1
kind: ConfigMap
metadata:
  name: wizdata-config
  namespace: wizdata
data:
  DATABASE_URL: "postgresql://wizdata-user:password@postgres-service:5432/wizdata"
  REDIS_URL: "redis://redis-service:6379/0"
  KAFKA_BROKERS: "kafka-service:9092"
  LOG_LEVEL: "INFO"
"""
        
        with open(k8s_dir / "configmap.yaml", 'w') as f:
            f.write(configmap_yaml)
        
        logger.info("Created Kubernetes configuration files")
    
    def create_monitoring_config(self):
        """Create monitoring and observability configuration"""
        logger.info("Creating monitoring configuration...")
        
        # Prometheus configuration
        prometheus_config = {
            "global": {
                "scrape_interval": "15s",
                "evaluation_interval": "15s"
            },
            "scrape_configs": []
        }
        
        # Add scrape configs for each service
        for service_name, service_info in self.SERVICES.items():
            prometheus_config["scrape_configs"].append({
                "job_name": service_name,
                "static_configs": [{
                    "targets": [f"localhost:{service_info['port']}"]
                }],
                "metrics_path": "/metrics",
                "scrape_interval": "15s"
            })
        
        prometheus_file = self.infrastructure_dir / "prometheus.yml"
        with open(prometheus_file, 'w') as f:
            import yaml
            yaml.dump(prometheus_config, f, default_flow_style=False)
        
        # Grafana dashboard configuration
        grafana_config = {
            "dashboard": {
                "title": "WizData Microservices Dashboard",
                "panels": [
                    {
                        "title": "Service Health",
                        "type": "stat",
                        "targets": [{"expr": "up{job=~'.*-service'}"}]
                    },
                    {
                        "title": "Request Rate",
                        "type": "graph",
                        "targets": [{"expr": "rate(http_requests_total[5m])"}]
                    },
                    {
                        "title": "Response Time",
                        "type": "graph",
                        "targets": [{"expr": "http_request_duration_seconds"}]
                    },
                    {
                        "title": "Error Rate",
                        "type": "graph",
                        "targets": [{"expr": "rate(http_requests_total{status=~'5..'}[5m])"}]
                    }
                ]
            }
        }
        
        grafana_file = self.infrastructure_dir / "grafana-dashboard.json"
        with open(grafana_file, 'w') as f:
            json.dump(grafana_config, f, indent=2)
        
        logger.info("Created monitoring configuration files")
    
    def create_migration_guide(self):
        """Create a step-by-step migration guide"""
        logger.info("Creating migration guide...")
        
        migration_guide = """# WizData Microservices Migration Guide

## Overview
This guide outlines the step-by-step process to migrate the WizData monolithic application to a microservices architecture.

## Phase 1: Foundation Setup (Current)
✅ **COMPLETED**
- [x] Environment-based configuration management
- [x] API rate limiting and caching infrastructure
- [x] Monitoring and observability setup
- [x] PostgreSQL database with TimescaleDB extensions
- [x] Graceful API key handling for external services

## Phase 2: Data Layer Preparation
- [ ] Implement database schema for multi-tenant access
- [ ] Set up TimescaleDB continuous aggregates
- [ ] Configure data compression policies
- [ ] Implement database connection pooling
- [ ] Create data access layer abstractions

## Phase 3: Message Queue Infrastructure
- [ ] Deploy Kafka cluster
- [ ] Create topics for different data types:
  - `market-data-raw`
  - `market-data-processed`
  - `esg-data-raw`
  - `esg-data-processed`
  - `news-sentiment`
  - `alerts-notifications`
- [ ] Implement message producers and consumers
- [ ] Set up dead letter queues for error handling

## Phase 4: Service Extraction
### 4.1 Data Ingestion Service
- [ ] Extract data collection logic from monolith
- [ ] Implement Kafka producers for real-time data
- [ ] Add data validation and preprocessing
- [ ] Set up API clients for external data sources
- [ ] Implement retry mechanisms and error handling

### 4.2 Data Cleaning Service
- [ ] Extract data quality and cleaning logic
- [ ] Implement Kafka consumers for raw data
- [ ] Add outlier detection algorithms
- [ ] Create data standardization workflows
- [ ] Set up data archival to MinIO/S3

### 4.3 Indicator Engine
- [ ] Extract technical indicator calculations
- [ ] Implement real-time computation pipelines
- [ ] Set up caching for computed indicators
- [ ] Add support for custom indicators
- [ ] Implement batch processing for historical data

### 4.4 API Gateway
- [ ] Set up Kong or similar API gateway
- [ ] Implement service discovery
- [ ] Add load balancing and circuit breakers
- [ ] Configure authentication and authorization
- [ ] Set up request/response transformation

### 4.5 Auth Service
- [ ] Extract user management from monolith
- [ ] Implement JWT token management
- [ ] Add role-based access control
- [ ] Set up API key management
- [ ] Implement SSO integration

## Phase 5: Infrastructure
- [ ] Set up Docker containers for all services
- [ ] Create Kubernetes deployments
- [ ] Implement service mesh (Istio/Linkerd)
- [ ] Set up centralized logging (ELK stack)
- [ ] Configure distributed tracing (Jaeger)
- [ ] Implement backup and disaster recovery

## Phase 6: Migration and Testing
- [ ] Implement feature flags for gradual migration
- [ ] Set up A/B testing infrastructure
- [ ] Create comprehensive integration tests
- [ ] Implement chaos engineering tests
- [ ] Set up performance benchmarks
- [ ] Plan rollback procedures

## Phase 7: Production Deployment
- [ ] Blue-green deployment setup
- [ ] Database migration procedures
- [ ] Traffic routing configuration
- [ ] Monitoring and alerting setup
- [ ] Documentation and runbooks
- [ ] Team training and handover

## Key Decisions Made
1. **Technology Stack**: Python/Flask for all services (consistency)
2. **Message Queue**: Kafka for high-throughput real-time data
3. **Database**: PostgreSQL with TimescaleDB for time-series data
4. **Caching**: Redis for distributed caching and session storage
5. **Monitoring**: Prometheus + Grafana + structured logging
6. **Container Orchestration**: Docker + Kubernetes
7. **API Gateway**: Kong for centralized routing and policies

## Success Metrics
- **Performance**: 99.9% uptime, <200ms API response time
- **Scalability**: Independent scaling of each service
- **Development**: Reduced deployment time by 80%
- **Reliability**: Zero-downtime deployments
- **Monitoring**: Full observability across all services

## Risk Mitigation
- **Data Consistency**: Implement saga pattern for distributed transactions
- **Service Communication**: Use circuit breakers and retries
- **Dependencies**: Graceful degradation when services are unavailable
- **Security**: End-to-end encryption and service authentication
- **Performance**: Comprehensive load testing before production
"""
        
        migration_file = self.base_path / "MICROSERVICES_MIGRATION.md"
        with open(migration_file, 'w') as f:
            f.write(migration_guide)
        
        logger.info("Created migration guide")
    
    def run_setup(self):
        """Run the complete microservices setup"""
        logger.info("Starting microservices architecture setup...")
        
        try:
            self.create_directory_structure()
            self.generate_service_configs()
            self.create_docker_files()
            self.create_kubernetes_files()
            self.create_monitoring_config()
            self.create_migration_guide()
            
            logger.info("✅ Microservices setup completed successfully!")
            logger.info(f"📁 Services directory: {self.services_dir}")
            logger.info(f"🐳 Docker files: {self.base_path / 'docker'}")
            logger.info(f"☸️  Kubernetes files: {self.base_path / 'k8s'}")
            logger.info(f"📊 Infrastructure config: {self.infrastructure_dir}")
            logger.info(f"📋 Migration guide: {self.base_path / 'MICROSERVICES_MIGRATION.md'}")
            
        except Exception as e:
            logger.error(f"❌ Setup failed: {str(e)}")
            raise

if __name__ == "__main__":
    setup = MicroservicesSetup()
    setup.run_setup()