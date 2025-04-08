import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DB_USER = os.getenv("PGUSER")
DB_PASSWORD = os.getenv("PGPASSWORD")
DB_HOST = os.getenv("PGHOST")
DB_PORT = os.getenv("PGPORT")
DB_NAME = os.getenv("PGDATABASE")
DATABASE_URL = os.getenv("DATABASE_URL", f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# API Configuration
API_HOST = "0.0.0.0"
API_PORT = 8000
DEBUG_MODE = True

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = os.getenv("REDIS_PORT", 6379)
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# API keys for external services
COINMARKETCAP_API_KEY = os.getenv("COINMARKETCAP_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
NEWS_API_KEY = os.getenv("NEWS_API_KEY", "")

# Rate limiting
RATE_LIMIT_DEFAULT = 100  # requests per hour
RATE_LIMIT_PREMIUM = 1000  # requests per hour

# Scheduler settings
SCHEDULER_INTERVAL_MINUTES = {
    "jse_data": 60,         # Fetch JSE data every hour
    "crypto_data": 5,       # Fetch crypto data every 5 minutes
    "forex_data": 15,       # Fetch forex data every 15 minutes
    "news_data": 120,       # Fetch news data every 2 hours
}

# Application paths
LOG_DIR = "logs"

# Create log directory if it doesn't exist
os.makedirs(LOG_DIR, exist_ok=True)
