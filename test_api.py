import requests
import json
from datetime import datetime, timedelta

# Base URL for our API
BASE_URL = "http://localhost:5000/api"

def test_health_check():
    """Test the health check endpoint"""
    url = f"{BASE_URL}/health"
    response = requests.get(url)
    
    print(f"Health Check Status: {response.status_code}")
    print(json.dumps(response.json(), indent=4))
    print("\n" + "-"*50 + "\n")

def test_symbols_endpoint(asset_type):
    """Test the symbols endpoint for a specific asset type"""
    url = f"{BASE_URL}/symbols?asset_type={asset_type}"
    response = requests.get(url)
    
    print(f"Symbols for {asset_type.upper()} - Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data.get('count', 0)} symbols")
        print("Sample symbols:", data.get('symbols', [])[:5])
    else:
        print("Error:", response.json())
    print("\n" + "-"*50 + "\n")

def test_prices_endpoint(asset_type, symbol):
    """Test the prices endpoint for a specific asset type and symbol"""
    # Calculate date range for the last 7 days
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}/prices?asset_type={asset_type}&symbol={symbol}&interval=daily&start_date={start_date}&end_date={end_date}"
    response = requests.get(url)
    
    print(f"Prices for {symbol} ({asset_type.upper()}) - Status: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"Found {data.get('count', 0)} data points")
        print("Sample data:")
        for item in data.get('data', [])[:3]:
            print(f"  {item.get('date')}: Open=${item.get('open')}, Close=${item.get('close')}")
    else:
        print("Error:", response.json())
    print("\n" + "-"*50 + "\n")

if __name__ == "__main__":
    print("\n" + "="*50)
    print("WizData API Test")
    print("="*50 + "\n")
    
    # Test health check
    test_health_check()
    
    # Test symbols endpoint for each asset type
    for asset_type in ["jse", "crypto", "forex"]:
        test_symbols_endpoint(asset_type)
    
    # Test prices endpoint with sample symbols
    test_prices_endpoint("jse", "SOL")  # JSE: Sasol
    test_prices_endpoint("crypto", "BTC")  # Crypto: Bitcoin
    test_prices_endpoint("forex", "EURUSD")  # Forex: EUR/USD
    
    print("Tests completed!")