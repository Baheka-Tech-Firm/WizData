"""
WizData African Markets Data Export Example

This script demonstrates how to use the AfricanMarketsFetcher to export financial data
from various African stock markets to JSON and CSV formats.
"""

import asyncio
import os
from datetime import datetime, timedelta
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Make sure data exports directory exists
os.makedirs('data/exports', exist_ok=True)

async def export_african_market_data():
    """
    Export data from African markets to both JSON and CSV formats
    """
    try:
        from src.ingestion.africa_markets_fetcher import AfricanMarketsFetcher
        
        # Initialize the fetcher
        fetcher = AfricanMarketsFetcher()
        
        # Get list of available markets
        markets = await fetcher.get_markets()
        logger.info(f"Found {len(markets)} African markets")
        
        # Set date range for data
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        
        # Export data for each market
        for market_info in markets:
            market = market_info["code"]
            name = market_info["name"]
            
            logger.info(f"Exporting data for {name} ({market.upper()})")
            
            # Get symbols for this market
            symbols = await fetcher.get_symbols(market)
            if not symbols:
                logger.warning(f"No symbols found for {market}")
                continue
                
            logger.info(f"Found {len(symbols)} symbols for {market}")
            
            # Export all symbols to JSON
            logger.info(f"Exporting all symbols for {market} to JSON")
            json_path = await fetcher.export_data(
                market=market,
                start_date=start_date,
                end_date=end_date,
                format='json',
                output_path='data/exports'
            )
            if json_path:
                logger.info(f"Exported to {json_path}")
            
            # Export all symbols to CSV
            logger.info(f"Exporting all symbols for {market} to CSV")
            csv_path = await fetcher.export_data(
                market=market,
                start_date=start_date,
                end_date=end_date,
                format='csv',
                output_path='data/exports'
            )
            if csv_path:
                logger.info(f"Exported to {csv_path}")
            
            # Export first individual symbol to both formats
            if symbols:
                symbol = symbols[0]["symbol"]
                symbol_name = symbols[0]["name"]
                logger.info(f"Exporting individual symbol {symbol} ({symbol_name}) to JSON and CSV")
                
                # JSON export
                json_path = await fetcher.export_data(
                    market=market,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    format='json',
                    output_path='data/exports'
                )
                if json_path:
                    logger.info(f"Exported to {json_path}")
                
                # CSV export
                csv_path = await fetcher.export_data(
                    market=market,
                    symbol=symbol,
                    start_date=start_date,
                    end_date=end_date,
                    format='csv',
                    output_path='data/exports'
                )
                if csv_path:
                    logger.info(f"Exported to {csv_path}")
    
    except Exception as e:
        logger.error(f"Error exporting data: {str(e)}", exc_info=True)

if __name__ == "__main__":
    # Run the export function
    logger.info("Starting African market data export")
    asyncio.run(export_african_market_data())
    logger.info("Export complete")