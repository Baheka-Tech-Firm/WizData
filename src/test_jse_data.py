import asyncio
import pandas as pd
from datetime import datetime, timedelta

from src.ingestion.jse_scraper import JSEScraper

async def test_jse_scraper():
    """Test the JSE (Johannesburg Stock Exchange) scraper"""
    print("Testing JSE Data Scraper...")
    
    # Create the scraper
    scraper = JSEScraper()
    
    # Get available JSE symbols
    jse_symbols = await scraper.get_symbols()
    print(f"Available JSE symbols: {jse_symbols}")
    
    # Test data fetching for Sasol (SOL)
    symbol = "SOL"
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    
    print(f"Fetching JSE data for {symbol} from {start_date} to {end_date}...")
    df = await scraper.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date
    )
    
    if not df.empty:
        print(f"Successfully fetched {len(df)} rows of data")
        print("\nData Sample:")
        print(df.head())
    else:
        print("Failed to fetch data")
    
    # Test data fetching for MTN
    symbol = "MTN"
    print(f"\nFetching JSE data for {symbol} from {start_date} to {end_date}...")
    df = await scraper.fetch_data(
        symbol=symbol,
        start_date=start_date,
        end_date=end_date
    )
    
    if not df.empty:
        print(f"Successfully fetched {len(df)} rows of data")
        print("\nData Sample:")
        print(df.head())
    else:
        print("Failed to fetch data")
    
if __name__ == "__main__":
    asyncio.run(test_jse_scraper())