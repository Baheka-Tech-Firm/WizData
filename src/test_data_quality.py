"""
Test script for the Data Quality Monitoring module
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import os
import json

from processing.data_quality import DataQualityMonitor

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_price_data_quality():
    """Test data quality monitoring for price data"""
    logger.info("Testing price data quality monitoring...")
    
    # Create test data with some anomalies
    dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(10)]
    
    data = {
        'date': dates,
        'symbol': ['AAPL'] * 10,
        'open': [150.25, 152.30, 153.45, 151.20, 150.75, 150.80, 151.20, 151.40, 151.40, 151.40],
        'high': [153.10, 154.25, 155.40, 152.90, 153.20, 153.00, 153.10, 153.40, 153.40, 153.40],
        'low': [149.80, 151.40, 152.60, 150.30, 149.50, 149.90, 150.00, 150.20, 150.20, 150.20],
        'close': [152.80, 153.75, 154.20, 151.80, 152.40, 152.45, 152.80, 180.00, 152.60, 152.60],  # anomaly on day 8
        'volume': [1000000, 1200000, 1100000, 950000, 1050000, 980000, 0, 1020000, 1500000, 7000000]  # zero vol on day 7, spike on day 10
    }
    df = pd.DataFrame(data)
    
    # Create quality monitor
    monitor = DataQualityMonitor(log_dir="logs/test_quality")
    
    # Test price data quality
    df_quality, report = monitor.check_data_quality(df, data_type='price')
    
    # Print quality report
    logger.info("\nData Quality Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Anomalies Detected: {report['anomalies_detected']}")
    
    for anomaly_type, count in report['anomaly_types'].items():
        logger.info(f"  {anomaly_type}: {count}")
    
    logger.info("\nQuality Metrics:")
    for metric, value in report['metrics'].items():
        if isinstance(value, dict):
            logger.info(f"  {metric}:")
            for k, v in value.items():
                logger.info(f"    {k}: {v}")
        else:
            logger.info(f"  {metric}: {value}")
    
    # Print rows with anomalies
    logger.info("\nRows with Anomalies:")
    anomaly_rows = df_quality[df_quality['has_anomaly']]
    logger.info(anomaly_rows[['date', 'symbol', 'close', 'volume', 'has_anomaly']])
    
    # Test source reliability report
    reliability_report = monitor.get_source_reliability_report()
    logger.info("\nSource Reliability Report:")
    logger.info(json.dumps(reliability_report, indent=2))
    
    return df_quality, report

def test_dividend_data_quality():
    """Test data quality monitoring for dividend data"""
    logger.info("\nTesting dividend data quality monitoring...")
    
    # Create test data with some anomalies
    data = {
        'symbol': ['MSFT', 'MSFT', 'MSFT', 'AAPL', 'AAPL', 'AAPL', 'GOOGL'],
        'ex_date': [
            '2024-01-15', '2024-04-15', '2024-07-15', 
            '2024-02-10', '2024-05-10', '2024-08-10',
            '2024-03-20'
        ],
        'payment_date': [
            '2024-02-15', '2024-05-15', '2024-08-15', 
            '2024-03-10', '2024-01-10',  # anomaly: payment before ex-date
            '2024-09-10', '2024-04-20'
        ],
        'dividend_amount': [0.68, 0.70, 0.72, 0.24, 0.25, -0.26, 0.35],  # negative dividend
        'currency': ['USD'] * 7,
        'frequency': ['quarterly'] * 7,
        'exchange': ['NASDAQ'] * 7
    }
    df = pd.DataFrame(data)
    
    # Convert date columns to datetime
    df['ex_date'] = pd.to_datetime(df['ex_date'])
    df['payment_date'] = pd.to_datetime(df['payment_date'])
    
    # Create quality monitor
    monitor = DataQualityMonitor(log_dir="logs/test_quality")
    
    # Test dividend data quality
    df_quality, report = monitor.check_data_quality(df, data_type='dividend')
    
    # Print quality report
    logger.info("\nDividend Data Quality Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Anomalies Detected: {report['anomalies_detected']}")
    
    for anomaly_type, count in report['anomaly_types'].items():
        logger.info(f"  {anomaly_type}: {count}")
    
    # Print rows with anomalies
    logger.info("\nRows with Anomalies:")
    anomaly_rows = df_quality[df_quality['has_anomaly']]
    logger.info(anomaly_rows[['symbol', 'ex_date', 'payment_date', 'dividend_amount', 'has_anomaly']])
    
    return df_quality, report

def test_earnings_data_quality():
    """Test data quality monitoring for earnings data"""
    logger.info("\nTesting earnings data quality monitoring...")
    
    # Create test data with some anomalies
    data = {
        'symbol': ['MSFT', 'MSFT', 'MSFT', 'AAPL', 'AAPL', 'GOOGL', 'GOOGL'],
        'report_date': [
            '2024-01-25', '2024-04-25', '2024-07-25', 
            '2024-02-01', '2024-05-01', '2024-01-31', '2024-04-30'
        ],
        'quarter_end_date': [
            '2023-12-31', '2024-03-31', '2024-06-30', 
            '2023-12-31', '2024-05-15',  # anomaly: quarter_end after report
            '2023-12-31', '2024-03-31'
        ],
        'eps': [2.48, 2.55, 2.46, 1.88, 1.52, 1.64, -20.50],  # anomaly in last value
        'expected_eps': [2.45, 2.50, 2.60, 1.80, 1.45, 1.59, 1.61],
        'surprise_pct': [1.22, 2.00, -5.38, 4.44, 4.83, 3.14, -1372.67],  # extreme surprise
        'revenue': [56000000000, 58000000000, 59000000000, 120000000000, 90000000000, 86000000000, -1000000],  # negative revenue
        'currency': ['USD'] * 7,
        'market': ['NASDAQ'] * 7
    }
    df = pd.DataFrame(data)
    
    # Convert date columns to datetime
    df['report_date'] = pd.to_datetime(df['report_date'])
    df['quarter_end_date'] = pd.to_datetime(df['quarter_end_date'])
    
    # Create quality monitor
    monitor = DataQualityMonitor(log_dir="logs/test_quality")
    
    # Test earnings data quality
    df_quality, report = monitor.check_data_quality(df, data_type='earnings')
    
    # Print quality report
    logger.info("\nEarnings Data Quality Report:")
    logger.info(f"Total Records: {report['record_count']}")
    logger.info(f"Anomalies Detected: {report['anomalies_detected']}")
    
    for anomaly_type, count in report['anomaly_types'].items():
        logger.info(f"  {anomaly_type}: {count}")
    
    # Print rows with anomalies
    logger.info("\nRows with Anomalies:")
    anomaly_rows = df_quality[df_quality['has_anomaly']]
    logger.info(anomaly_rows[['symbol', 'report_date', 'eps', 'surprise_pct', 'revenue', 'has_anomaly']])
    
    return df_quality, report

def main():
    """Run all data quality tests"""
    logger.info("Starting data quality monitoring tests...")
    
    # Create test directory
    os.makedirs("logs/test_quality", exist_ok=True)
    
    # Run tests
    test_price_data_quality()
    test_dividend_data_quality()
    test_earnings_data_quality()
    
    logger.info("All data quality tests completed")

if __name__ == "__main__":
    main()