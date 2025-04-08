#!/usr/bin/env python3
"""
WizData Global Markets Data Export Example

This script demonstrates how to use the GlobalMarketsFetcher, DividendFetcher, and
EarningsFetcher to export financial data from various global stock markets to
JSON and CSV formats.
"""
import asyncio
import os
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create export directory
EXPORT_DIR = "data/exports"
os.makedirs(EXPORT_DIR, exist_ok=True)


async def export_global_market_data():
    """
    Export data from global markets to both JSON and CSV formats
    """
    from src.ingestion.global_markets_fetcher import GlobalMarketsFetcher
    
    # Initialize fetcher
    fetcher = GlobalMarketsFetcher()
    
    # Get available markets
    markets = await fetcher.get_markets()
    logger.info(f"Found {len(markets)} global markets: {', '.join([m['code'] for m in markets])}")
    
    # Set date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    # Format dates
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Export data for each market
    for market_info in markets:
        market = market_info["code"]
        
        # Get available symbols for the market
        symbols = await fetcher.get_symbols(market)
        logger.info(f"Found {len(symbols)} symbols for {market.upper()}")
        
        if symbols:
            # Export market index data (first symbol)
            symbol = symbols[0]["symbol"]
            logger.info(f"Exporting data for {symbol} in {market.upper()}...")
            
            # Export as JSON
            json_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="json",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported JSON data to {json_path}")
            
            # Export as CSV
            csv_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="csv",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported CSV data to {csv_path}")


async def export_dividend_data():
    """
    Export dividend data from various markets
    """
    from src.ingestion.dividend_fetcher import DividendFetcher
    
    # Initialize fetcher
    fetcher = DividendFetcher()
    
    # Markets to export dividend data for
    markets = ["nasdaq", "nyse", "lse", "asx", "jse"]
    
    # Set date range (1 year for dividends)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Format dates
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Export dividend data for each market
    for market in markets:
        # Get available symbols for the market
        symbols = await fetcher.get_symbols(market)
        logger.info(f"Found {len(symbols)} symbols for {market.upper()} dividend data")
        
        if symbols:
            # Export dividend data for first symbol
            symbol = symbols[0]["symbol"]
            logger.info(f"Exporting dividend data for {symbol} in {market.upper()}...")
            
            # Export as JSON
            json_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="json",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported dividend JSON data to {json_path}")
            
            # Export as CSV
            csv_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="csv",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported dividend CSV data to {csv_path}")


async def export_earnings_data():
    """
    Export earnings data from various markets
    """
    from src.ingestion.earnings_fetcher import EarningsFetcher
    
    # Initialize fetcher
    fetcher = EarningsFetcher()
    
    # Markets to export earnings data for
    markets = ["nasdaq", "nyse", "lse", "asx", "jse"]
    
    # Set date range (2 years for earnings - 8 quarters)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=730)
    
    # Format dates
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    # Export earnings data for each market
    for market in markets:
        # Get available symbols for the market
        symbols = await fetcher.get_symbols(market)
        logger.info(f"Found {len(symbols)} symbols for {market.upper()} earnings data")
        
        if symbols:
            # Export earnings data for first symbol
            symbol = symbols[0]["symbol"]
            logger.info(f"Exporting earnings data for {symbol} in {market.upper()}...")
            
            # Export as JSON
            json_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="json",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported earnings JSON data to {json_path}")
            
            # Export as CSV
            csv_path = await fetcher.export_data(
                market=market,
                symbol=symbol,
                start_date=start_date_str,
                end_date=end_date_str,
                format="csv",
                output_path=EXPORT_DIR
            )
            logger.info(f"Exported earnings CSV data to {csv_path}")


async def export_all_data():
    """
    Export all types of financial data 
    (global market prices, dividends, and earnings)
    """
    logger.info("Starting export of global financial data...")
    
    # Export global market data
    await export_global_market_data()
    
    # Export dividend data
    await export_dividend_data()
    
    # Export earnings data
    await export_earnings_data()
    
    logger.info("Finished exporting all global financial data")


if __name__ == "__main__":
    # Create new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        # Run the export function
        loop.run_until_complete(export_all_data())
    except KeyboardInterrupt:
        logger.info("Export process interrupted")
    except Exception as e:
        logger.error(f"Error during export: {str(e)}", exc_info=True)
    finally:
        # Close the event loop
        loop.close()