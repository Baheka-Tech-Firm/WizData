# WizData Docker Setup

This guide will help you run WizData using Docker and Docker Compose, ensuring all dependencies are properly installed and configured.

## Prerequisites

- Docker (20.10+)
- Docker Compose (1.29+)
- At least 4GB of available RAM
- At least 2GB of available disk space

## Quick Start

### 1. Setup Environment

```bash
# Copy environment template
cp .env.docker .env

# Edit the environment file with your configuration
nano .env
```

### 2. Start Services

```bash
# Option 1: Use the management script (recommended)
./scripts/docker-manage.sh start

# Option 2: Use docker-compose directly
docker-compose up -d
```

### 3. Access the Application

- **WizData Dashboard**: http://localhost:5000
- **API Documentation**: http://localhost:5000/api-services
- **Health Check**: http://localhost:5000/health

## Services

The Docker Compose setup includes:

### Core Services (Always Running)
- **wizdata**: Main Flask application (Port 5000)
- **redis**: Redis cache and session store (Port 6379)

### Optional Services
- **postgres**: PostgreSQL database (Port 5432) - Optional, SQLite is used by default
- **nginx**: Reverse proxy with SSL (Ports 80/443) - Production profile only

## Management Script

Use the provided management script for easy operations:

```bash
# Start all services
./scripts/docker-manage.sh start

# Stop all services
./scripts/docker-manage.sh stop

# Restart services
./scripts/docker-manage.sh restart

# View logs
./scripts/docker-manage.sh logs

# View specific service logs
./scripts/docker-manage.sh logs wizdata

# Check service status
./scripts/docker-manage.sh status

# Clean up resources
./scripts/docker-manage.sh cleanup
```

## Configuration

### Environment Variables

Edit the `.env` file to configure the application:

```bash
# Database (SQLite by default, PostgreSQL optional)
DATABASE_URL=sqlite:///./wizdata.db
# DATABASE_URL=postgresql://wizdata:wizdata_password@postgres:5432/wizdata

# Redis
REDIS_URL=redis://redis:6379/0

# Application
FLASK_ENV=production
SECRET_KEY=your-secret-key-here
DEBUG=false

# Optional API Keys
OPENAI_API_KEY=your-openai-key
ALPHA_VANTAGE_API_KEY=your-alpha-vantage-key
# ... other API keys
```

### Using PostgreSQL (Optional)

To use PostgreSQL instead of SQLite:

1. Edit `.env` file:
   ```bash
   DATABASE_URL=postgresql://wizdata:wizdata_password@postgres:5432/wizdata
   ```

2. Uncomment PostgreSQL service in `docker-compose.yml`

3. Update the depends_on section for the wizdata service

## Production Deployment

### With Nginx Reverse Proxy

```bash
# Start with production profile
docker-compose --profile production up -d

# This starts: wizdata, redis, postgres, nginx
```

### SSL Configuration

1. Place SSL certificates in `nginx/ssl/`
2. Update `nginx/nginx.conf` with SSL configuration
3. Restart nginx service

## Monitoring and Logging

### Health Checks

- Application health: http://localhost:5000/health
- Redis health: Automatic Docker health checks
- PostgreSQL health: Automatic Docker health checks

### Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f wizdata

# With timestamps
docker-compose logs -f -t wizdata
```

### Log Files

Application logs are also available in the mounted `logs/` directory.

## Data Persistence

Data is persisted using Docker volumes:

- **wizdata_db**: SQLite database (if using SQLite)
- **postgres_data**: PostgreSQL data (if using PostgreSQL)
- **redis_data**: Redis data
- **./data**: Application data directory
- **./logs**: Application logs
- **./static**: Static files

## Troubleshooting

### Services Won't Start

1. Check Docker is running:
   ```bash
   docker info
   ```

2. Check service logs:
   ```bash
   docker-compose logs wizdata
   ```

3. Verify environment configuration:
   ```bash
   docker-compose config
   ```

### Database Issues

1. Reset database (SQLite):
   ```bash
   docker-compose down -v
   rm -f wizdata.db
   docker-compose up -d
   ```

2. Reset database (PostgreSQL):
   ```bash
   docker-compose down -v
   docker volume rm wizdata_postgres_data
   docker-compose up -d
   ```

### Memory Issues

1. Check Docker resource limits:
   ```bash
   docker stats
   ```

2. Increase Docker memory allocation in Docker Desktop settings

### Port Conflicts

If ports are already in use:

1. Edit `docker-compose.yml` to use different ports:
   ```yaml
   ports:
     - "5001:5000"  # Use port 5001 instead of 5000
   ```

## Development Mode

For development with live code reloading:

```bash
# Create development override
cat > docker-compose.override.yml << EOF
version: '3.8'
services:
  wizdata:
    volumes:
      - .:/app
    environment:
      FLASK_ENV: development
      DEBUG: "true"
    command: python main.py
EOF

# Start in development mode
docker-compose up -d
```

## API Keys Setup

For full functionality, add these API keys to your `.env` file:

```bash
# AI Features
OPENAI_API_KEY=sk-...

# Financial Data Providers
ALPHA_VANTAGE_API_KEY=...
SP_GLOBAL_API_KEY=...
BLOOMBERG_API_KEY=...
REFINITIV_API_KEY=...
REFINITIV_API_SECRET=...
```

## Support

If you encounter issues:

1. Check the troubleshooting section above
2. Review service logs: `./scripts/docker-manage.sh logs`
3. Verify configuration: `docker-compose config`
4. Check Docker system status: `docker system info`

## Performance Tuning

### For Production

1. **Resource Allocation**:
   ```yaml
   services:
     wizdata:
       deploy:
         resources:
           limits:
             memory: 2G
             cpus: '1.0'
   ```

2. **Environment Optimization**:
   ```bash
   FLASK_ENV=production
   DEBUG=false
   WORKERS=4
   ```

3. **Database Optimization**:
   - Use PostgreSQL for production
   - Configure connection pooling
   - Set up database backups
