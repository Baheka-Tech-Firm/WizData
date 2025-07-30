#!/bin/bash

# WizData Docker Setup and Management Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info >/dev/null 2>&1; then
        print_error "Docker is not running. Please start Docker and try again."
        exit 1
    fi
    print_success "Docker is running"
}

# Function to setup environment
setup_env() {
    if [ ! -f .env ]; then
        print_status "Creating .env file from template..."
        cp .env.docker .env
        print_warning "Please edit .env file with your configuration before running the application"
    else
        print_success ".env file already exists"
    fi
}

# Function to build and start services
start_services() {
    print_status "Building and starting WizData services..."
    
    # Build the application
    docker-compose build
    
    # Start services
    docker-compose up -d redis
    
    # Wait for Redis to be ready
    print_status "Waiting for Redis to be ready..."
    sleep 5
    
    # Start the main application
    docker-compose up -d wizdata
    
    # Wait for application to be ready
    print_status "Waiting for WizData to be ready..."
    sleep 10
    
    # Check if services are running
    if docker-compose ps | grep -q "Up"; then
        print_success "Services started successfully!"
        print_status "WizData is running at: http://localhost:5000"
        print_status "Redis is running at: localhost:6379"
    else
        print_error "Failed to start services"
        docker-compose logs
        exit 1
    fi
}

# Function to stop services
stop_services() {
    print_status "Stopping WizData services..."
    docker-compose down
    print_success "Services stopped"
}

# Function to show logs
show_logs() {
    docker-compose logs -f "$@"
}

# Function to show status
show_status() {
    print_status "WizData Services Status:"
    docker-compose ps
}

# Function to clean up
cleanup() {
    print_status "Cleaning up Docker resources..."
    docker-compose down -v
    docker system prune -f
    print_success "Cleanup completed"
}

# Function to restart services
restart_services() {
    print_status "Restarting WizData services..."
    docker-compose restart
    print_success "Services restarted"
}

# Main script logic
case "$1" in
    "start")
        check_docker
        setup_env
        start_services
        ;;
    "stop")
        check_docker
        stop_services
        ;;
    "restart")
        check_docker
        restart_services
        ;;
    "logs")
        check_docker
        show_logs "${@:2}"
        ;;
    "status")
        check_docker
        show_status
        ;;
    "cleanup")
        check_docker
        cleanup
        ;;
    "setup")
        check_docker
        setup_env
        ;;
    *)
        echo "WizData Docker Management Script"
        echo ""
        echo "Usage: $0 {start|stop|restart|logs|status|cleanup|setup}"
        echo ""
        echo "Commands:"
        echo "  start    - Build and start all services"
        echo "  stop     - Stop all services"
        echo "  restart  - Restart all services"
        echo "  logs     - Show service logs (add service name for specific logs)"
        echo "  status   - Show service status"
        echo "  cleanup  - Stop services and clean up resources"
        echo "  setup    - Setup environment files"
        echo ""
        echo "Examples:"
        echo "  $0 start                 # Start all services"
        echo "  $0 logs wizdata         # Show WizData application logs"
        echo "  $0 logs                 # Show all logs"
        exit 1
        ;;
esac
