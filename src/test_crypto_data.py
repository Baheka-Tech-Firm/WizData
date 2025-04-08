import asyncio
import pandas as pd
from datetime import datetime, timedelta

from src.ingestion.crypto_fetcher import CryptoFetcher

async def test_crypto_fetcher():
    """Test the cryptocurrency data fetcher with CoinGecko"""
    print("Testing Cryptocurrency Data Fetcher...")
    
    # Create the fetcher
    fetcher = CryptoFetcher()
    
    # Get available cryptocurrency symbols
    try:
        crypto_symbols = await fetcher.get_symbols()
        print(f"Available cryptocurrency symbols (first 10): {crypto_symbols[:10]}")
    except Exception as e:
        print(f"Error getting symbols: {str(e)}")
    
    # Test data fetching for Bitcoin
    symbol = "BTC"
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    print(f"Fetching cryptocurrency data for {symbol} from {start_date} to {end_date}...")
    df = await fetcher.fetch_data(
        symbol=symbol,
        source="coingecko",
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
    
    # Test data fetching for Ethereum
    symbol = "ETH"
    print(f"\nFetching cryptocurrency data for {symbol} from {start_date} to {end_date}...")
    df = await fetcher.fetch_data(
        symbol=symbol,
        source="coingecko",
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
    asyncio.run(test_crypto_fetcher())