import asyncio
import aioschedule
import logging
import signal
import time
from datetime import datetime
import os
import sys

from ingestion.jse_scraper import JSEScraper
from ingestion.crypto_fetcher import CryptoFetcher
from ingestion.forex_fetcher import ForexFetcher
from processing.cleaner import DataCleaner

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join('logs', 'scheduler.log'), mode='a')
    ]
)
logger = logging.getLogger(__name__)

# Ensure logs directory exists
os.makedirs('logs', exist_ok=True)

# Initialize data fetchers
jse_scraper = None
crypto_fetcher = None
forex_fetcher = None
data_cleaner = None

async def fetch_jse_data():
    """Fetch JSE (Johannesburg Stock Exchange) data"""
    global jse_scraper, data_cleaner
    
    if jse_scraper is None:
        jse_scraper = JSEScraper()
    
    if data_cleaner is None:
        data_cleaner = DataCleaner()
    
    start_time = datetime.now()
    logger.info(f"Starting JSE data fetch at {start_time}")
    
    try:
        # Get available symbols
        symbols = await jse_scraper.get_symbols()
        logger.info(f"Retrieved {len(symbols)} JSE symbols")
        
        # Fetch data for all symbols
        df = await jse_scraper.fetch_data()
        
        if not df.empty:
            # Clean the data
            df_clean = data_cleaner.clean_ohlcv(df)
            
            # In a production environment, we would store the data in a database here
            logger.info(f"Successfully fetched and cleaned data for {len(df_clean)} JSE records")
        else:
            logger.warning("No JSE data retrieved")
    
    except Exception as e:
        logger.error(f"Error fetching JSE data: {str(e)}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed JSE data fetch in {duration:.2f} seconds")

async def fetch_crypto_data():
    """Fetch cryptocurrency data"""
    global crypto_fetcher, data_cleaner
    
    if crypto_fetcher is None:
        crypto_fetcher = CryptoFetcher()
    
    if data_cleaner is None:
        data_cleaner = DataCleaner()
    
    start_time = datetime.now()
    logger.info(f"Starting cryptocurrency data fetch at {start_time}")
    
    try:
        # Get available symbols
        symbols = await crypto_fetcher.get_symbols()
        logger.info(f"Retrieved {len(symbols)} cryptocurrency symbols")
        
        # Fetch data for all symbols
        df = await crypto_fetcher.fetch_data(source="coingecko")
        
        if not df.empty:
            # Clean the data
            df_clean = data_cleaner.clean_ohlcv(df)
            
            # In a production environment, we would store the data in a database here
            logger.info(f"Successfully fetched and cleaned data for {len(df_clean)} cryptocurrency records")
        else:
            logger.warning("No cryptocurrency data retrieved")
    
    except Exception as e:
        logger.error(f"Error fetching cryptocurrency data: {str(e)}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed cryptocurrency data fetch in {duration:.2f} seconds")

async def fetch_forex_data():
    """Fetch forex (foreign exchange) data"""
    global forex_fetcher, data_cleaner
    
    if forex_fetcher is None:
        forex_fetcher = ForexFetcher()
    
    if data_cleaner is None:
        data_cleaner = DataCleaner()
    
    start_time = datetime.now()
    logger.info(f"Starting forex data fetch at {start_time}")
    
    try:
        # Get available symbols
        symbols = await forex_fetcher.get_symbols()
        logger.info(f"Retrieved {len(symbols)} forex symbols")
        
        # Fetch data for all symbols
        df = await forex_fetcher.fetch_data()
        
        if not df.empty:
            # Clean the data
            df_clean = data_cleaner.clean_ohlcv(df)
            
            # In a production environment, we would store the data in a database here
            logger.info(f"Successfully fetched and cleaned data for {len(df_clean)} forex records")
        else:
            logger.warning("No forex data retrieved")
    
    except Exception as e:
        logger.error(f"Error fetching forex data: {str(e)}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Completed forex data fetch in {duration:.2f} seconds")

async def setup_schedules():
    """Setup all scheduled jobs"""
    # JSE data collection (daily at 18:00 South African time)
    aioschedule.every().day.at("18:00").do(fetch_jse_data)
    logger.info("Scheduled JSE data collection for 18:00 daily")
    
    # Cryptocurrency data collection (every 6 hours)
    aioschedule.every(6).hours.do(fetch_crypto_data)
    logger.info("Scheduled cryptocurrency data collection every 6 hours")
    
    # Forex data collection (every 12 hours)
    aioschedule.every(12).hours.do(fetch_forex_data)
    logger.info("Scheduled forex data collection every 12 hours")
    
    # Run initial data collection
    logger.info("Running initial data collection...")
    await fetch_jse_data()
    await fetch_crypto_data()
    await fetch_forex_data()

async def shutdown():
    """Shutdown the scheduler gracefully"""
    logger.info("Shutting down scheduler...")
    # Clean up resources if needed
    logger.info("Scheduler shutdown complete")

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, initiating shutdown")
    asyncio.create_task(shutdown())
    sys.exit(0)

async def main():
    """Main scheduler function"""
    logger.info("Starting WizData scheduler")
    
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Setup scheduled jobs
        await setup_schedules()
        
        # Run the scheduler loop
        while True:
            await aioschedule.run_pending()
            await asyncio.sleep(1)
    
    except Exception as e:
        logger.error(f"Error in scheduler main loop: {str(e)}")
        await shutdown()
    
    finally:
        logger.info("Scheduler stopped")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())