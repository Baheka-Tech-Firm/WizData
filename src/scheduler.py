import asyncio
import aioschedule as schedule
from datetime import datetime, timedelta
import time
import signal
import sys
import traceback

from src.ingestion.jse_scraper import JSEScraper
from src.ingestion.crypto_fetcher import CryptoFetcher
from src.ingestion.forex_fetcher import ForexFetcher
from src.processing.normalizer import DataNormalizer
from src.processing.cleaner import DataCleaner
from src.storage.timescale_db import TimescaleDBClient
from src.utils.config import SCHEDULER_INTERVAL_MINUTES
from src.utils.logger import get_scheduler_logger

# Initialize components
jse_scraper = JSEScraper()
crypto_fetcher = CryptoFetcher()
forex_fetcher = ForexFetcher()
data_normalizer = DataNormalizer()
data_cleaner = DataCleaner()
db_client = TimescaleDBClient()
logger = get_scheduler_logger()

# Flag to control the main loop
running = True

async def fetch_jse_data():
    """Fetch JSE (Johannesburg Stock Exchange) data"""
    try:
        logger.info("Starting JSE data fetch")
        
        # Get symbols from JSE
        symbols = await jse_scraper.get_symbols()
        
        # Take a sample of symbols to process (5 by default)
        # In production, you'd process all symbols
        sample_symbols = symbols[:5] if symbols else []
        
        if not sample_symbols:
            logger.warning("No JSE symbols found")
            return
        
        logger.info(f"Processing {len(sample_symbols)} JSE symbols")
        
        # Calculate date range (last 7 days)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
        
        # Process each symbol
        for symbol in sample_symbols:
            try:
                # Fetch data for symbol
                df = await jse_scraper.fetch_data(symbol, start_date, end_date)
                
                if df.empty:
                    logger.warning(f"No data found for JSE symbol {symbol}")
                    continue
                
                # Normalize and clean data
                df = data_normalizer.normalize_ohlcv(df, "jse")
                df = data_cleaner.clean_ohlcv(df)
                
                # Handle missing values
                df = data_cleaner.handle_missing_values(df, method="interpolate")
                
                # Store in database
                inserted = await db_client.insert_ohlcv(df)
                logger.info(f"Inserted {inserted} rows for JSE symbol {symbol}")
                
            except Exception as e:
                logger.error(f"Error processing JSE symbol {symbol}: {str(e)}")
                continue
        
        logger.info("Completed JSE data fetch")
        
    except Exception as e:
        logger.error(f"Error in JSE data fetch job: {str(e)}")
        traceback.print_exc()

async def fetch_crypto_data():
    """Fetch cryptocurrency data"""
    try:
        logger.info("Starting crypto data fetch")
        
        # Define top cryptocurrencies to fetch
        top_coins = ["BTC", "ETH", "SOL", "BNB", "XRP", "ADA", "DOGE", "DOT"]
        
        # Calculate date range (last 30 days)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Process each cryptocurrency
        for coin in top_coins:
            try:
                # Fetch data from CoinGecko
                df = await crypto_fetcher.fetch_data(
                    symbol=coin,
                    source="coingecko",
                    interval="daily",
                    start_date=start_date,
                    end_date=end_date
                )
                
                if df.empty:
                    logger.warning(f"No data found for crypto {coin}")
                    continue
                
                # Normalize and clean data
                df = data_normalizer.normalize_ohlcv(df, "coingecko")
                df = data_cleaner.clean_ohlcv(df)
                
                # Handle missing values
                df = data_cleaner.handle_missing_values(df, method="interpolate")
                
                # Store in database
                inserted = await db_client.insert_ohlcv(df)
                logger.info(f"Inserted {inserted} rows for crypto {coin}")
                
            except Exception as e:
                logger.error(f"Error processing crypto {coin}: {str(e)}")
                continue
        
        logger.info("Completed crypto data fetch")
        
    except Exception as e:
        logger.error(f"Error in crypto data fetch job: {str(e)}")
        traceback.print_exc()

async def fetch_forex_data():
    """Fetch forex (foreign exchange) data"""
    try:
        logger.info("Starting forex data fetch")
        
        # Get available forex pairs
        forex_pairs = await forex_fetcher.get_symbols()
        
        if not forex_pairs:
            logger.warning("No forex pairs available")
            return
        
        # Calculate date range (last 30 days)
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Process each forex pair
        for pair in forex_pairs:
            try:
                # Fetch data from Alpha Vantage
                df = await forex_fetcher.fetch_data(
                    symbol=pair,
                    interval="daily",
                    start_date=start_date,
                    end_date=end_date
                )
                
                if df.empty:
                    logger.warning(f"No data found for forex pair {pair}")
                    continue
                
                # Normalize and clean data
                df = data_normalizer.normalize_ohlcv(df, "alpha_vantage")
                df = data_cleaner.clean_ohlcv(df)
                
                # Handle missing values
                df = data_cleaner.handle_missing_values(df, method="interpolate")
                
                # Store in database
                inserted = await db_client.insert_ohlcv(df)
                logger.info(f"Inserted {inserted} rows for forex pair {pair}")
                
            except Exception as e:
                logger.error(f"Error processing forex pair {pair}: {str(e)}")
                continue
        
        logger.info("Completed forex data fetch")
        
    except Exception as e:
        logger.error(f"Error in forex data fetch job: {str(e)}")
        traceback.print_exc()

async def setup_schedules():
    """Setup all scheduled jobs"""
    # Initialize database connection
    await db_client.init_pool()
    
    # JSE data fetch
    jse_interval = SCHEDULER_INTERVAL_MINUTES.get("jse_data", 60)
    schedule.every(jse_interval).minutes.do(fetch_jse_data)
    logger.info(f"Scheduled JSE data fetch every {jse_interval} minutes")
    
    # Crypto data fetch
    crypto_interval = SCHEDULER_INTERVAL_MINUTES.get("crypto_data", 5)
    schedule.every(crypto_interval).minutes.do(fetch_crypto_data)
    logger.info(f"Scheduled crypto data fetch every {crypto_interval} minutes")
    
    # Forex data fetch
    forex_interval = SCHEDULER_INTERVAL_MINUTES.get("forex_data", 15)
    schedule.every(forex_interval).minutes.do(fetch_forex_data)
    logger.info(f"Scheduled forex data fetch every {forex_interval} minutes")
    
    # Additional schedules can be added here

async def shutdown():
    """Shutdown the scheduler gracefully"""
    logger.info("Shutting down scheduler...")
    global running
    running = False
    
    # Close database connection
    await db_client.close_pool()
    logger.info("Scheduler shutdown complete")

def signal_handler(sig, frame):
    """Handle termination signals"""
    logger.info(f"Received signal {sig}, initiating shutdown")
    asyncio.create_task(shutdown())

async def main():
    """Main scheduler function"""
    logger.info("Starting WizData scheduler")
    
    # Setup signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Setup all scheduled jobs
    await setup_schedules()
    
    # Run an initial data fetch immediately
    await fetch_jse_data()
    await fetch_crypto_data()
    await fetch_forex_data()
    
    logger.info("Scheduler is running, press Ctrl+C to exit")
    
    # Main scheduling loop
    while running:
        await schedule.run_pending()
        await asyncio.sleep(1)
    
    logger.info("Scheduler has stopped")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scheduler stopped by user")
    except Exception as e:
        logger.error(f"Unhandled exception in scheduler: {str(e)}")
        traceback.print_exc()
        sys.exit(1)
