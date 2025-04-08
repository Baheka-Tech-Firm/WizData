"""
Test script for the Standards Compliance module
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import json

from processing.standards import StandardsCompliance

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_price_standards():
    """Test standardization of price data"""
    logger.info("Testing price data standardization...")
    
    # Create test data with some non-standard formatting
    data = {
        'timestamp': [  # Non-standard date field name
            '2025-03-01', '2025-03-02', '2025-03-03', '2025-03-04', '2025-03-05'
        ],
        'ticker': ['AAPL', 'AAPL', 'AAPL', 'AAPL', 'AAPL'],  # Non-standard symbol field
        'opening_price': [150.25, 152.30, 153.45, 151.20, 150.75],  # Non-standard price field names
        'highest_price': [153.10, 154.25, 155.40, 152.90, 153.20],
        'lowest_price': [149.80, 151.40, 152.60, 150.30, 149.50],
        'closing_price': [152.80, 153.75, 154.20, 151.80, 152.40],
        'vol': [1000000, 1200000, 1100000, 950000, 1050000],
        'currency': ['$', 'US DOLLAR', 'USD', 'USD', 'USD']  # Non-standard currency codes
    }
    df = pd.DataFrame(data)
    
    # Create standards compliance engine
    standards_engine = StandardsCompliance()
    
    # Test price data standardization
    df_std, report = standards_engine.standardize_data(df, data_type='price')
    
    # Print standardization report
    logger.info("\nPrice Data Standardization Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Issues Fixed: {report['issues_fixed']}")
    
    # Field compliance
    logger.info("\nField Compliance:")
    for field_type, items in report['field_compliance'].items():
        if isinstance(items, dict):
            logger.info(f"  {field_type}:")
            for k, v in items.items():
                logger.info(f"    {k}: {v}")
        elif isinstance(items, list):
            logger.info(f"  {field_type}: {', '.join(items)}")
        else:
            logger.info(f"  {field_type}: {items}")
    
    # Currency compliance
    logger.info("\nCurrency Compliance:")
    for field, value in report['currency_compliance'].items():
        logger.info(f"  {field}: {value}")
    
    # Print standardized data
    logger.info("\nStandardized Data Sample:")
    logger.info(df_std.head())
    
    return df_std, report

def test_dividend_standards():
    """Test standardization of dividend data"""
    logger.info("\nTesting dividend data standardization...")
    
    # Create test data with some non-standard formatting
    data = {
        'ticker': ['MSFT', 'MSFT', 'AAPL', 'AAPL', 'GOOGL'],
        'ex_dividend_date': [  # Non-standard field name
            '2024-01-15', '2024-04-15', '2024-02-10', '2024-05-10', '2024-03-20'
        ],
        'pay_date': [  # Non-standard field name
            '2024-02-15', '2024-05-15', '2024-03-10', '2024-06-10', '2024-04-20'
        ],
        'div_amount': [0.68, 0.70, 0.24, -0.25, 0.35],  # Non-standard name and negative value
        'currency': ['US$', 'DOLLAR', 'USD', 'USD', 'EURO'],  # Non-standard currency codes
        'frequency': ['Q', 'QUARTERLY', 'ANNUAL', 'A', 'SEMI']  # Non-standard frequency values
    }
    df = pd.DataFrame(data)
    
    # Create standards compliance engine
    standards_engine = StandardsCompliance()
    
    # Test dividend data standardization
    df_std, report = standards_engine.standardize_data(df, data_type='dividend')
    
    # Print standardization report
    logger.info("\nDividend Data Standardization Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Issues Fixed: {report['issues_fixed']}")
    
    # Print specific dividend compliance
    if 'dividend_compliance' in report:
        logger.info("\nDividend Compliance:")
        for field, value in report['dividend_compliance'].items():
            logger.info(f"  {field}: {value}")
    
    # Print standardized data
    logger.info("\nStandardized Data Sample:")
    logger.info(df_std.head())
    
    return df_std, report

def test_earnings_standards():
    """Test standardization of earnings data"""
    logger.info("\nTesting earnings data standardization...")
    
    # Create test data with some non-standard formatting
    data = {
        'ticker': ['MSFT', 'MSFT', 'AAPL', 'AAPL', 'GOOGL'],
        'earnings_date': [  # Non-standard field name
            '2024-01-25', '2024-04-25', '2024-02-01', '2024-05-01', '2024-01-31'
        ],
        'qtr_end': [  # Non-standard field name
            '2023-12-31', '2024-03-31', '2023-12-31', '2024-03-31', '2023-12-31'
        ],
        'actual_eps': ['2.48', '2.55', '1.88', '1.52', '1.64'],  # String values, should be numeric
        'expected_earnings': [2.45, 2.50, 1.80, 1.45, 1.59],  # Non-standard field name
        'surprise': [1.22, 2.00, 4.44, 4.83, 3.14],  # Non-standard field name
        'fiscal_qtr': ['Q1', 'Q2', 'Q1', 'Q2', 'Q4'],  # Non-standard format
        'fiscal_yr': [2024, 2024, 2024, 2024, 2023],  # Non-standard field name
        'currency': ['US$', 'DOLLAR', 'USD', 'USD', 'EURO'],  # Non-standard currency codes
        'rev': [56000000000, 58000000000, 120000000000, 90000000000, 86000000000]  # Non-standard field name
    }
    df = pd.DataFrame(data)
    
    # Create standards compliance engine
    standards_engine = StandardsCompliance()
    
    # Test earnings data standardization
    df_std, report = standards_engine.standardize_data(df, data_type='earnings')
    
    # Print standardization report
    logger.info("\nEarnings Data Standardization Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Issues Fixed: {report['issues_fixed']}")
    
    # Print specific earnings compliance
    if 'earnings_compliance' in report:
        logger.info("\nEarnings Compliance:")
        for field, value in report['earnings_compliance'].items():
            logger.info(f"  {field}: {value}")
    
    # Print standardized data
    logger.info("\nStandardized Data Sample:")
    logger.info(df_std.head())
    
    return df_std, report

def test_data_lineage():
    """Test adding data lineage information"""
    logger.info("\nTesting data lineage...")
    
    # Create simple test data
    data = {
        'date': ['2025-03-01', '2025-03-02', '2025-03-03'],
        'symbol': ['AAPL', 'AAPL', 'AAPL'],
        'close': [152.80, 153.75, 154.20]
    }
    df = pd.DataFrame(data)
    
    # Create standards compliance engine
    standards_engine = StandardsCompliance()
    
    # Add data lineage information
    source = "Alpha Vantage API"
    process_info = {
        "processor": "WizData Pipeline",
        "version": "1.0.0",
        "processing_steps": ["fetch", "clean", "transform"]
    }
    
    df_with_lineage = standards_engine.add_data_lineage(df, source, process_info)
    
    # Print data with lineage
    logger.info("\nData with Lineage Information:")
    logger.info(df_with_lineage)
    
    return df_with_lineage

def main():
    """Run all standards compliance tests"""
    logger.info("Starting standards compliance tests...")
    
    # Run tests
    test_price_standards()
    test_dividend_standards()
    test_earnings_standards()
    test_data_lineage()
    
    logger.info("All standards compliance tests completed")

if __name__ == "__main__":
    main()