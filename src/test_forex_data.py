import asyncio
import pandas as pd
from datetime import datetime, timedelta

from src.ingestion.forex_fetcher import ForexFetcher

async def test_forex_fetcher():
    """Test the forex data fetcher with Alpha Vantage"""
    print("Testing Forex Data Fetcher...")
    
    # Create the fetcher
    fetcher = ForexFetcher()
    
    # Get available currency pairs
    currency_pairs = await fetcher.get_symbols()
    print(f"Available currency pairs: {currency_pairs}")
    
    # Test data fetching for EURUSD
    symbol = "EURUSD"
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"Fetching forex data for {symbol} from {start_date} to {end_date}...")
    df = await fetcher.fetch_data(
        symbol=symbol,
        interval="daily",
        start_date=start_date,
        end_date=end_date
    )
    
    if not df.empty:
        print(f"Successfully fetched {len(df)} rows of data")
        print("\nData Sample:")
        print(df.head())
    else:
        print("Failed to fetch data")
        
    # Test for ZAR currency pair (South African Rand)
    symbol = "USDZAR"
    print(f"\nFetching forex data for {symbol} from {start_date} to {end_date}...")
    df = await fetcher.fetch_data(
        symbol=symbol,
        interval="daily",
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
    asyncio.run(test_forex_fetcher())