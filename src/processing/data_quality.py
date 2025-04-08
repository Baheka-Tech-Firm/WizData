"""
Data Quality Monitoring Module

This module provides functionality to monitor and report on data quality
metrics across the WizData platform, including anomaly detection and data
source reliability tracking.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
import logging
import json
from datetime import datetime, timedelta
import os

# Set up logging
logger = logging.getLogger(__name__)


class DataQualityMonitor:
    """
    Class to monitor data quality, detect anomalies, and track source reliability
    for financial data across the WizData platform.
    """
    
    def __init__(self, log_dir: str = "logs/data_quality"):
        """
        Initialize the data quality monitoring system
        
        Args:
            log_dir (str): Directory to store data quality logs
        """
        logger.info("Initializing data quality monitoring system")
        self.log_dir = log_dir
        
        # Create log directory if it doesn't exist
        os.makedirs(self.log_dir, exist_ok=True)
        
        # Initialize source reliability tracking
        self.source_metrics = {}
        self.anomaly_thresholds = {
            'price_jump': 0.10,  # 10% price jump threshold
            'volume_spike': 5.0,  # 5x normal volume threshold
            'zero_volume': 0,     # Zero volume threshold
            'negative_price': 0,  # Negative price threshold
            'price_staleness': 0  # Identical price for N periods
        }
    
    def _convert_numpy_types(self, obj):
        """
        Convert numpy types to Python native types for JSON serialization
        
        Args:
            obj: Any object potentially containing numpy types
            
        Returns:
            Object with numpy types converted to Python native types
        """
        if isinstance(obj, (np.int8, np.int16, np.int32, np.int64,
                          np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        elif isinstance(obj, (np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, dict):
            return {k: self._convert_numpy_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._convert_numpy_types(item) for item in obj]
        return obj
    
    def check_data_quality(self, df: pd.DataFrame, data_type: str = 'price') -> Tuple[pd.DataFrame, Dict]:
        """
        Check data quality and detect anomalies
        
        Args:
            df (pd.DataFrame): DataFrame to check
            data_type (str): Type of data ('price', 'dividend', 'earnings', etc.)
            
        Returns:
            Tuple[pd.DataFrame, Dict]: DataFrame with quality flags and quality report
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to check_data_quality")
            return df, {"status": "error", "message": "Empty data"}
        
        # Initialize quality report
        quality_report = {
            "status": "success",
            "data_type": data_type,
            "timestamp": datetime.now().isoformat(),
            "record_count": len(df),
            "anomalies_detected": 0,
            "anomaly_types": {},
            "completeness": {},
            "metrics": {}
        }
        
        # Create a copy of the input DataFrame to add quality flags
        df_quality = df.copy()
        
        # Check data completeness
        for col in df.columns:
            missing_count = df[col].isna().sum()
            quality_report["completeness"][col] = {
                "missing_count": int(missing_count),
                "missing_percentage": float(missing_count / len(df)) if len(df) > 0 else 0
            }
        
        # Initialize anomaly flags
        df_quality['has_anomaly'] = False
        
        # Specific checks based on data type
        if data_type == 'price':
            # Check for price anomalies
            anomalies = self._check_price_anomalies(df_quality)
            
            # Update quality report
            for anomaly_type, flags in anomalies.items():
                anomaly_count = flags.sum()
                quality_report["anomaly_types"][anomaly_type] = int(anomaly_count)
                quality_report["anomalies_detected"] += anomaly_count
                
                # Add flag columns to DataFrame
                df_quality[f"anomaly_{anomaly_type}"] = flags
                
                # Update overall anomaly flag
                df_quality['has_anomaly'] = df_quality['has_anomaly'] | flags
            
            # Calculate metrics
            quality_report["metrics"] = self._calculate_price_metrics(df_quality)
        
        elif data_type == 'dividend':
            # Check for dividend anomalies
            anomalies = self._check_dividend_anomalies(df_quality)
            
            # Update quality report and DataFrame
            for anomaly_type, flags in anomalies.items():
                anomaly_count = flags.sum()
                quality_report["anomaly_types"][anomaly_type] = int(anomaly_count)
                quality_report["anomalies_detected"] += anomaly_count
                
                # Add flag columns to DataFrame
                df_quality[f"anomaly_{anomaly_type}"] = flags
                
                # Update overall anomaly flag
                df_quality['has_anomaly'] = df_quality['has_anomaly'] | flags
            
            # Calculate metrics
            quality_report["metrics"] = self._calculate_dividend_metrics(df_quality)
        
        elif data_type == 'earnings':
            # Check for earnings anomalies
            anomalies = self._check_earnings_anomalies(df_quality)
            
            # Update quality report and DataFrame
            for anomaly_type, flags in anomalies.items():
                anomaly_count = flags.sum()
                quality_report["anomaly_types"][anomaly_type] = int(anomaly_count)
                quality_report["anomalies_detected"] += anomaly_count
                
                # Add flag columns to DataFrame
                df_quality[f"anomaly_{anomaly_type}"] = flags
                
                # Update overall anomaly flag
                df_quality['has_anomaly'] = df_quality['has_anomaly'] | flags
            
            # Calculate metrics
            quality_report["metrics"] = self._calculate_earnings_metrics(df_quality)
        
        # Log the quality report
        self._log_quality_report(data_type, quality_report)
        
        # Update source reliability metrics
        source_col = 'exchange' if 'exchange' in df.columns else 'source' if 'source' in df.columns else None
        if source_col is not None:
            self._update_source_metrics(df_quality, source_col, quality_report)
        
        return df_quality, quality_report
    
    def _check_price_anomalies(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Check for anomalies in price data
        
        Args:
            df (pd.DataFrame): DataFrame with price data
            
        Returns:
            Dict[str, pd.Series]: Dictionary of anomaly flags by type
        """
        anomalies = {}
        
        # Ensure required columns exist
        required_cols = ['date', 'symbol', 'close']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Missing required columns for price anomaly detection")
            return anomalies
        
        # Convert date to datetime if needed
        if not pd.api.types.is_datetime64_any_dtype(df['date']):
            df['date'] = pd.to_datetime(df['date'])
        
        # Sort data by symbol and date
        df_sorted = df.sort_values(['symbol', 'date'])
        
        # Check for negative prices
        if 'close' in df.columns:
            anomalies['negative_price'] = df['close'] < self.anomaly_thresholds['negative_price']
        
        # Check for price jumps
        if 'close' in df.columns:
            # Calculate percentage change by symbol
            df_sorted['prev_close'] = df_sorted.groupby('symbol')['close'].shift(1)
            df_sorted['price_change_pct'] = (df_sorted['close'] - df_sorted['prev_close']) / df_sorted['prev_close']
            
            # Flag large price jumps
            anomalies['price_jump'] = df_sorted['price_change_pct'].abs() > self.anomaly_thresholds['price_jump']
            
            # Remove NaN values (first row for each symbol)
            anomalies['price_jump'] = anomalies['price_jump'].fillna(False)
        
        # Check for volume anomalies
        if 'volume' in df.columns:
            # Flag zero volume
            anomalies['zero_volume'] = df['volume'] == self.anomaly_thresholds['zero_volume']
            
            # Calculate average volume by symbol
            avg_volume = df.groupby('symbol')['volume'].transform('mean')
            
            # Flag volume spikes
            anomalies['volume_spike'] = df['volume'] > (avg_volume * self.anomaly_thresholds['volume_spike'])
        
        # Check for price staleness (identical prices for consecutive periods)
        if 'close' in df.columns:
            # Calculate consecutive identical prices by symbol
            df_sorted['price_diff'] = df_sorted.groupby('symbol')['close'].diff() == 0
            
            # Create a group counter that resets when price_diff is False
            df_sorted['stale_group'] = (~df_sorted['price_diff']).cumsum()
            
            # Count consecutive identical prices within each group
            df_sorted['stale_count'] = df_sorted.groupby(['symbol', 'stale_group'])['price_diff'].cumsum()
            
            # Flag price staleness (3 or more consecutive identical prices)
            stale_threshold = 3
            anomalies['price_staleness'] = (
                (df_sorted['price_diff']) & 
                (df_sorted['stale_count'] >= stale_threshold - 1)
            )
            
            # Clean up temporary columns
            df_sorted.drop(columns=['price_diff', 'stale_group', 'stale_count'], inplace=True)
        
        return anomalies
    
    def _check_dividend_anomalies(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Check for anomalies in dividend data
        
        Args:
            df (pd.DataFrame): DataFrame with dividend data
            
        Returns:
            Dict[str, pd.Series]: Dictionary of anomaly flags by type
        """
        anomalies = {}
        
        # Ensure required columns exist
        required_cols = ['symbol', 'ex_date', 'dividend_amount']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Missing required columns for dividend anomaly detection")
            return anomalies
        
        # Convert date columns to datetime if needed
        for date_col in ['ex_date', 'payment_date']:
            if date_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col])
        
        # Check for negative dividend amounts
        anomalies['negative_dividend'] = df['dividend_amount'] < 0
        
        # Check for unusually high dividend amounts
        # Calculate average dividend by symbol
        avg_dividend = df.groupby('symbol')['dividend_amount'].transform('mean')
        std_dividend = df.groupby('symbol')['dividend_amount'].transform('std').fillna(0)
        
        # Flag dividends that are more than 3 standard deviations from the mean
        anomalies['unusual_dividend'] = (
            (df['dividend_amount'] > (avg_dividend + 3 * std_dividend)) | 
            (df['dividend_amount'] < (avg_dividend - 3 * std_dividend))
        )
        
        # Check for payment date before ex-date
        if 'payment_date' in df.columns:
            anomalies['invalid_dates'] = df['payment_date'] < df['ex_date']
        
        return anomalies
    
    def _check_earnings_anomalies(self, df: pd.DataFrame) -> Dict[str, pd.Series]:
        """
        Check for anomalies in earnings data
        
        Args:
            df (pd.DataFrame): DataFrame with earnings data
            
        Returns:
            Dict[str, pd.Series]: Dictionary of anomaly flags by type
        """
        anomalies = {}
        
        # Ensure required columns exist
        required_cols = ['symbol', 'report_date', 'eps']
        if not all(col in df.columns for col in required_cols):
            logger.warning(f"Missing required columns for earnings anomaly detection")
            return anomalies
        
        # Convert date columns to datetime if needed
        for date_col in ['report_date', 'quarter_end_date']:
            if date_col in df.columns and not pd.api.types.is_datetime64_any_dtype(df[date_col]):
                df[date_col] = pd.to_datetime(df[date_col])
        
        # Check for extreme EPS surprises
        if 'surprise_pct' in df.columns:
            anomalies['extreme_surprise'] = df['surprise_pct'].abs() > 50  # 50% surprise threshold
        
        # Check for unreasonable EPS values (based on historical standards)
        # We'll use a simple rule: EPS should typically be within a certain range
        if 'eps' in df.columns:
            anomalies['unreasonable_eps'] = (df['eps'].abs() > 1000)  # Arbitrary large threshold
        
        # Check for revenue issues
        if 'revenue' in df.columns:
            # Negative revenue is unusual
            anomalies['negative_revenue'] = df['revenue'] < 0
            
            # Unusual revenue jumps
            df_sorted = df.sort_values(['symbol', 'report_date'])
            df_sorted['prev_revenue'] = df_sorted.groupby('symbol')['revenue'].shift(1)
            df_sorted['revenue_change_pct'] = (df_sorted['revenue'] - df_sorted['prev_revenue']) / df_sorted['prev_revenue']
            
            # Flag large revenue jumps/drops (more than 100% change)
            anomalies['revenue_jump'] = df_sorted['revenue_change_pct'].abs() > 1.0
            anomalies['revenue_jump'] = anomalies['revenue_jump'].fillna(False)
        
        # Check for report date before quarter end date
        if 'quarter_end_date' in df.columns:
            anomalies['invalid_dates'] = df['report_date'] < df['quarter_end_date']
        
        return anomalies
    
    def _calculate_price_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate quality metrics for price data
        
        Args:
            df (pd.DataFrame): DataFrame with price data
            
        Returns:
            Dict: Quality metrics
        """
        metrics = {}
        
        # Calculate volatility metrics
        if all(col in df.columns for col in ['close', 'symbol']):
            # Calculate daily returns by symbol
            df_sorted = df.sort_values(['symbol', 'date'])
            df_sorted['daily_return'] = df_sorted.groupby('symbol')['close'].pct_change()
            
            # Calculate volatility (standard deviation of returns)
            volatility = df_sorted.groupby('symbol')['daily_return'].std().to_dict()
            metrics['volatility_by_symbol'] = volatility
            metrics['avg_volatility'] = float(np.mean(list(volatility.values())))
        
        # Calculate trading volume metrics
        if 'volume' in df.columns:
            metrics['avg_volume'] = float(df['volume'].mean())
            metrics['zero_volume_pct'] = float((df['volume'] == 0).mean())
        
        # Calculate price range metrics
        if all(col in df.columns for col in ['high', 'low', 'close']):
            # Calculate daily range as percentage of closing price
            df['daily_range_pct'] = (df['high'] - df['low']) / df['close']
            metrics['avg_daily_range_pct'] = float(df['daily_range_pct'].mean())
        
        return metrics
    
    def _calculate_dividend_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate quality metrics for dividend data
        
        Args:
            df (pd.DataFrame): DataFrame with dividend data
            
        Returns:
            Dict: Quality metrics
        """
        metrics = {}
        
        # Calculate dividend yield metrics
        if 'dividend_amount' in df.columns:
            metrics['avg_dividend'] = float(df['dividend_amount'].mean())
            metrics['median_dividend'] = float(df['dividend_amount'].median())
        
        # Calculate frequency metrics if we have payment dates
        if 'payment_date' in df.columns and 'symbol' in df.columns:
            # Get average days between dividend payments by symbol
            df_sorted = df.sort_values(['symbol', 'payment_date'])
            df_sorted['prev_payment_date'] = df_sorted.groupby('symbol')['payment_date'].shift(1)
            df_sorted['days_between_payments'] = (df_sorted['payment_date'] - df_sorted['prev_payment_date']).dt.days
            
            metrics['avg_days_between_payments'] = float(df_sorted['days_between_payments'].mean())
        
        return metrics
    
    def _calculate_earnings_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Calculate quality metrics for earnings data
        
        Args:
            df (pd.DataFrame): DataFrame with earnings data
            
        Returns:
            Dict: Quality metrics
        """
        metrics = {}
        
        # Calculate EPS metrics
        if 'eps' in df.columns:
            metrics['avg_eps'] = float(df['eps'].mean())
            metrics['median_eps'] = float(df['eps'].median())
        
        # Calculate surprise metrics
        if 'surprise_pct' in df.columns:
            metrics['avg_surprise_pct'] = float(df['surprise_pct'].mean())
            metrics['avg_abs_surprise_pct'] = float(df['surprise_pct'].abs().mean())
        
        # Calculate revenue metrics
        if 'revenue' in df.columns:
            metrics['avg_revenue'] = float(df['revenue'].mean())
        
        return metrics
    
    def _log_quality_report(self, data_type: str, report: Dict) -> None:
        """
        Log quality report to file
        
        Args:
            data_type (str): Type of data
            report (Dict): Quality report
        """
        # Create log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"{data_type}_quality_{timestamp}.json")

        # Convert numpy types in the report
        report_serializable = self._convert_numpy_types(report)
        
        # Write report to file
        with open(log_file, 'w') as f:
            json.dump(report_serializable, f, indent=2)
        
        logger.info(f"Data quality report saved to {log_file}")
    
    def _update_source_metrics(self, df: pd.DataFrame, source_col: str, quality_report: Dict) -> None:
        """
        Update source reliability metrics
        
        Args:
            df (pd.DataFrame): DataFrame with data
            source_col (str): Column containing source information
            quality_report (Dict): Quality report
        """
        # Get unique sources
        sources = df[source_col].unique()
        
        for source in sources:
            # Get data for this source
            source_df = df[df[source_col] == source]
            
            # Calculate anomaly rate for this source
            anomaly_count = source_df['has_anomaly'].sum()
            anomaly_rate = anomaly_count / len(source_df) if len(source_df) > 0 else 0
            
            # Initialize source metrics if not exists
            if source not in self.source_metrics:
                self.source_metrics[source] = {
                    'total_records': 0,
                    'anomaly_records': 0,
                    'last_updated': None,
                    'reliability_score': 1.0  # Start with perfect score
                }
            
            # Update source metrics
            self.source_metrics[source]['total_records'] += len(source_df)
            self.source_metrics[source]['anomaly_records'] += anomaly_count
            self.source_metrics[source]['last_updated'] = datetime.now().isoformat()
            
            # Calculate reliability score (simple formula: 1 - anomaly rate)
            total_anomaly_rate = (
                self.source_metrics[source]['anomaly_records'] / 
                self.source_metrics[source]['total_records']
                if self.source_metrics[source]['total_records'] > 0 else 0
            )
            self.source_metrics[source]['reliability_score'] = 1.0 - total_anomaly_rate
    
    def get_source_reliability_report(self) -> Dict:
        """
        Get source reliability report
        
        Returns:
            Dict: Source reliability report
        """
        report = {
            'timestamp': datetime.now().isoformat(),
            'sources': self.source_metrics,
            'overall_reliability': 0.0
        }
        
        # Calculate overall reliability (weighted average)
        total_records = sum(s['total_records'] for s in self.source_metrics.values())
        if total_records > 0:
            weighted_reliability = sum(
                s['reliability_score'] * s['total_records'] / total_records
                for s in self.source_metrics.values()
            )
            report['overall_reliability'] = weighted_reliability
        
        return report
    
    def save_source_reliability_report(self) -> str:
        """
        Save source reliability report to file
        
        Returns:
            str: Path to the saved report
        """
        report = self.get_source_reliability_report()
        
        # Create log filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = os.path.join(self.log_dir, f"source_reliability_{timestamp}.json")
        
        # Convert numpy types to Python native types
        report_serializable = self._convert_numpy_types(report)
        
        # Write report to file
        with open(log_file, 'w') as f:
            json.dump(report_serializable, f, indent=2)
        
        logger.info(f"Source reliability report saved to {log_file}")
        return log_file