"""
Test script for the Data Reconciliation module
"""

import asyncio
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging

from processing.reconciliation import DataReconciliation

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_price_reconciliation():
    """Test reconciliation of price data from multiple sources"""
    logger.info("Testing price data reconciliation...")
    
    # Create test data - primary source
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(5)]
    primary_data = {
        'date': dates,
        'symbol': ['AAPL'] * 5,
        'open': [150.25, 152.30, 153.45, 151.20, 150.75],
        'high': [153.10, 154.25, 155.40, 152.90, 153.20],
        'low': [149.80, 151.40, 152.60, 150.30, 149.50],
        'close': [152.80, 153.75, 154.20, 151.80, 152.40],
        'volume': [1000000, 1200000, 1100000, 950000, 1050000]
    }
    primary_df = pd.DataFrame(primary_data)
    
    # Create test data - secondary source (with some discrepancies)
    secondary_data = {
        'date': dates,
        'symbol': ['AAPL'] * 5,
        'open': [150.20, 152.35, 153.40, 151.25, 150.70],  # minor differences
        'high': [153.15, 154.20, 155.45, 152.95, 153.25],  # minor differences
        'low': [149.75, 151.45, 152.55, 150.35, 149.45],   # minor differences
        'close': [152.85, 153.70, 154.25, 151.85, 157.40], # significant difference in last value
        'volume': [1010000, 1190000, 1105000, 940000, 850000]  # significant difference in last value
    }
    secondary_df = pd.DataFrame(secondary_data)
    
    # Create reconciliation engine
    reconciliation_engine = DataReconciliation()
    
    # Test price reconciliation
    reconciled_df, report = reconciliation_engine.reconcile_price_data(
        primary_df, secondary_df, 
        key_columns=['date', 'symbol'],
        value_columns=['close', 'volume']
    )
    
    # Print reconciliation report
    logger.info("\nReconciliation Report:")
    logger.info(f"Total Records: {report['total_records']}")
    logger.info(f"Matched Records: {report['matched_records']}")
    logger.info(f"Missing in Secondary: {report['missing_in_secondary']}")
    
    for col, details in report['discrepancies'].items():
        logger.info(f"\n{col.capitalize()} Discrepancies:")
        logger.info(f"  Count: {details['count']}")
        logger.info(f"  Average Difference: {details['average_difference']:.2%}")
        logger.info(f"  Max Difference: {details['max_difference']:.2%}")
    
    # Print reconciled data
    logger.info("\nReconciled DataFrame:")
    logger.info(reconciled_df)
    
    return reconciled_df, report

async def test_multi_source_reconciliation():
    """Test reconciliation of data from multiple sources"""
    logger.info("\nTesting multi-source data reconciliation...")
    
    # Create test data - three different sources with variations
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(3)]
    
    # Source 1
    source1_data = {
        'date': dates,
        'symbol': ['MSFT'] * 3,
        'close': [290.20, 292.40, 289.75],
        'volume': [2500000, 2400000, 2600000]
    }
    source1_df = pd.DataFrame(source1_data)
    
    # Source 2 - slightly different values
    source2_data = {
        'date': dates,
        'symbol': ['MSFT'] * 3,
        'close': [290.35, 292.25, 289.80],
        'volume': [2510000, 2395000, 2605000]
    }
    source2_df = pd.DataFrame(source2_data)
    
    # Source 3 - one outlier value
    source3_data = {
        'date': dates,
        'symbol': ['MSFT'] * 3,
        'close': [290.25, 292.30, 295.00],  # Last value is an outlier
        'volume': [2505000, 2390000, 2610000]
    }
    source3_df = pd.DataFrame(source3_data)
    
    # Create reconciliation engine
    reconciliation_engine = DataReconciliation()
    
    # Test multi-source reconciliation
    reconciled_df = reconciliation_engine.reconcile_multiple_sources(
        [source1_df, source2_df, source3_df],
        key_columns=['date', 'symbol'],
        value_columns=['close', 'volume']
    )
    
    # Print reconciled data
    logger.info("\nMulti-source Reconciled DataFrame:")
    logger.info(reconciled_df)
    
    return reconciled_df

async def main():
    """Run all reconciliation tests"""
    logger.info("Starting data reconciliation tests...")
    
    await test_price_reconciliation()
    await test_multi_source_reconciliation()
    
    logger.info("All reconciliation tests completed")

if __name__ == "__main__":
    asyncio.run(main())