"""
Data Reconciliation Module

This module provides functionality to reconcile financial data from multiple sources,
ensuring consistency and accuracy across the WizData platform.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
import logging
from datetime import datetime
import json

# Set up logging
logger = logging.getLogger(__name__)


class DataReconciliation:
    """
    Class to reconcile financial data from multiple sources,
    detecting discrepancies and creating a consolidated, verified dataset.
    """
    
    def __init__(self):
        """Initialize the data reconciliation engine"""
        logger.info("Initializing data reconciliation engine")
        
        # Define tolerance thresholds for different financial values
        self.tolerance = {
            'price': 0.005,  # 0.5% tolerance for price differences
            'volume': 0.10,   # 10% tolerance for volume differences
            'eps': 0.02,     # 2% tolerance for EPS differences
            'revenue': 0.05  # 5% tolerance for revenue differences
        }
    
    def reconcile_price_data(self, primary_df: pd.DataFrame, secondary_df: pd.DataFrame, 
                             key_columns: List[str], value_columns: List[str]) -> Tuple[pd.DataFrame, Dict]:
        """
        Reconcile price data from two sources
        
        Args:
            primary_df (pd.DataFrame): Primary source data
            secondary_df (pd.DataFrame): Secondary source data
            key_columns (List[str]): Columns used for matching (e.g., ['date', 'symbol'])
            value_columns (List[str]): Value columns to reconcile (e.g., ['close', 'volume'])
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Reconciled DataFrame and reconciliation report
        """
        logger.info(f"Reconciling price data with {len(primary_df)} primary and {len(secondary_df)} secondary records")
        
        # Create reconciliation report
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_records": len(primary_df),
            "matched_records": 0,
            "missing_in_secondary": 0,
            "discrepancies": {}
        }
        
        # Create a copy of the primary data to avoid modifying the original
        reconciled_df = primary_df.copy()
        
        # Convert key columns to string to ensure consistent matching
        for col in key_columns:
            if col in primary_df.columns and col in secondary_df.columns:
                reconciled_df[col] = reconciled_df[col].astype(str)
                secondary_df[col] = secondary_df[col].astype(str)
        
        # Create a unique key for each record in both dataframes
        reconciled_df['_merge_key'] = reconciled_df[key_columns].apply(lambda x: '_'.join(x), axis=1)
        secondary_df['_merge_key'] = secondary_df[key_columns].apply(lambda x: '_'.join(x), axis=1)
        
        # Find records in primary that are missing in secondary
        primary_keys = set(reconciled_df['_merge_key'])
        secondary_keys = set(secondary_df['_merge_key'])
        missing_keys = primary_keys - secondary_keys
        report["missing_in_secondary"] = len(missing_keys)
        
        # Add a flag for matched records
        reconciled_df['_matched'] = ~reconciled_df['_merge_key'].isin(missing_keys)
        report["matched_records"] = reconciled_df['_matched'].sum()
        
        # Create a secondary lookup for matched records
        secondary_lookup = secondary_df.set_index('_merge_key')
        
        # Initialize reconciliation flags for each value column
        for value_col in value_columns:
            reconciled_df[f'{value_col}_discrepancy'] = False
            reconciled_df[f'{value_col}_reconciled'] = False
            reconciled_df[f'{value_col}_secondary'] = None
            
            # Initialize discrepancy report for this column
            report["discrepancies"][value_col] = {
                "count": 0,
                "average_difference": 0.0,
                "max_difference": 0.0,
                "details": []
            }
        
        # Process matched records
        for value_col in value_columns:
            # Get tolerance for this column
            tolerance = self.tolerance.get(value_col.lower(), self.tolerance['price'])
            
            # Initialize lists to track differences
            differences = []
            
            # Process each matched record
            for idx, row in reconciled_df[reconciled_df['_matched']].iterrows():
                merge_key = row['_merge_key']
                
                # Get values from both sources
                primary_value = row[value_col]
                secondary_value = secondary_lookup.at[merge_key, value_col]
                
                # Store secondary value
                reconciled_df.at[idx, f'{value_col}_secondary'] = secondary_value
                
                # Calculate percentage difference
                if pd.notna(primary_value) and pd.notna(secondary_value) and primary_value != 0:
                    pct_difference = abs((secondary_value - primary_value) / primary_value)
                    differences.append(pct_difference)
                    
                    # Check if difference is above tolerance
                    if pct_difference > tolerance:
                        reconciled_df.at[idx, f'{value_col}_discrepancy'] = True
                        report["discrepancies"][value_col]["count"] += 1
                        
                        # Add detail to report
                        report["discrepancies"][value_col]["details"].append({
                            "key": merge_key,
                            "primary_value": float(primary_value),
                            "secondary_value": float(secondary_value),
                            "difference_pct": float(pct_difference)
                        })
                    else:
                        # Values are within tolerance, mark as reconciled
                        reconciled_df.at[idx, f'{value_col}_reconciled'] = True
                elif pd.isna(primary_value) and pd.notna(secondary_value):
                    # Primary value is missing, use secondary
                    reconciled_df.at[idx, value_col] = secondary_value
                    reconciled_df.at[idx, f'{value_col}_reconciled'] = True
                elif pd.notna(primary_value) and pd.isna(secondary_value):
                    # Secondary value is missing, keep primary
                    reconciled_df.at[idx, f'{value_col}_reconciled'] = True
                elif pd.isna(primary_value) and pd.isna(secondary_value):
                    # Both are missing, can't reconcile
                    pass
            
            # Calculate summary statistics for differences
            if differences:
                report["discrepancies"][value_col]["average_difference"] = float(np.mean(differences))
                report["discrepancies"][value_col]["max_difference"] = float(np.max(differences))
        
        # Add reconciliation metadata
        reconciled_df['reconciliation_date'] = datetime.now().isoformat()
        reconciled_df['reconciliation_sources'] = 2  # Two sources were used
        
        # Clean up temporary columns
        reconciled_df = reconciled_df.drop(columns=['_merge_key', '_matched'])
        
        logger.info(f"Reconciliation complete: {report['matched_records']} matched, " 
                   f"{report['missing_in_secondary']} missing in secondary")
        
        return reconciled_df, report
    
    def reconcile_multiple_sources(self, dfs: List[pd.DataFrame], 
                                   key_columns: List[str],
                                   value_columns: List[str]) -> pd.DataFrame:
        """
        Reconcile data from multiple sources using weighted averaging
        
        Args:
            dfs (List[pd.DataFrame]): List of DataFrames from different sources
            key_columns (List[str]): Columns to use as keys
            value_columns (List[str]): Value columns to reconcile
            
        Returns:
            pd.DataFrame: Reconciled DataFrame
        """
        if not dfs or len(dfs) == 0:
            logger.warning("No DataFrames provided for reconciliation")
            return pd.DataFrame()
        
        logger.info(f"Reconciling data from {len(dfs)} sources")
        
        # If only one source, return it
        if len(dfs) == 1:
            logger.info("Only one source provided, no reconciliation needed")
            return dfs[0].copy()
        
        # Create a copy of each DataFrame to avoid modifying the originals
        df_copies = [df.copy() for df in dfs]
        
        # Convert key columns to string in all DataFrames for consistent matching
        for df in df_copies:
            for col in key_columns:
                if col in df.columns:
                    df[col] = df[col].astype(str)
        
        # Create a unique merge key in each DataFrame
        for i, df in enumerate(df_copies):
            df['_source_id'] = i  # Add source identifier
            df['_merge_key'] = df[key_columns].apply(lambda x: '_'.join(x), axis=1)
        
        # Combine all DataFrames
        combined_df = pd.concat(df_copies, ignore_index=True)
        
        # Group by the merge key
        grouped = combined_df.groupby('_merge_key')
        
        # Create output DataFrame structure
        result_rows = []
        
        # Process each group (each group represents the same record from different sources)
        for merge_key, group in grouped:
            # Start with a base record using the first source's key columns
            first_source = group.iloc[0]
            result_row = {col: first_source[col] for col in key_columns}
            
            # Add number of sources for this record
            result_row['source_count'] = len(group)
            
            # Process each value column
            for value_col in value_columns:
                if value_col not in group.columns:
                    continue
                
                # Get values from all sources for this record
                values = group[value_col].tolist()
                valid_values = [v for v in values if pd.notna(v)]
                
                if not valid_values:
                    # No valid values from any source
                    result_row[value_col] = None
                    result_row[f'{value_col}_confidence'] = 0.0
                    continue
                
                if len(valid_values) == 1:
                    # Only one valid value
                    result_row[value_col] = valid_values[0]
                    result_row[f'{value_col}_confidence'] = 1.0
                    continue
                
                # Multiple valid values, need to reconcile
                
                # Check if values are numeric
                if all(isinstance(v, (int, float)) for v in valid_values):
                    # Use median for numeric values
                    median_value = np.median(valid_values)
                    
                    # Calculate standard deviation
                    std_dev = np.std(valid_values)
                    
                    # Calculate z-scores to identify outliers
                    z_scores = [(v - median_value) / std_dev if std_dev != 0 else 0 for v in valid_values]
                    
                    # Filter out outliers (z-score > 2)
                    non_outlier_values = [v for i, v in enumerate(valid_values) if abs(z_scores[i]) <= 2]
                    
                    if non_outlier_values:
                        # Use mean of non-outlier values
                        result_row[value_col] = np.mean(non_outlier_values)
                        
                        # Calculate confidence based on standard deviation
                        # Lower standard deviation = higher confidence
                        confidence = 1.0 / (1.0 + np.std(non_outlier_values))
                        result_row[f'{value_col}_confidence'] = min(confidence, 1.0)
                    else:
                        # All values are outliers, use median
                        result_row[value_col] = median_value
                        result_row[f'{value_col}_confidence'] = 0.5
                else:
                    # For non-numeric values, use the most common value
                    value_counts = pd.Series(valid_values).value_counts()
                    most_common_value = value_counts.index[0]
                    most_common_count = value_counts.iloc[0]
                    
                    result_row[value_col] = most_common_value
                    result_row[f'{value_col}_confidence'] = most_common_count / len(valid_values)
            
            # Add source identifiers
            result_row['source_ids'] = ','.join(map(str, group['_source_id'].tolist()))
            
            # Add reconciliation metadata
            result_row['reconciliation_date'] = datetime.now().isoformat()
            result_row['reconciliation_type'] = 'multi_source'
            
            result_rows.append(result_row)
        
        # Create result DataFrame
        result_df = pd.DataFrame(result_rows)
        
        logger.info(f"Multi-source reconciliation complete: {len(result_df)} unique records from {len(dfs)} sources")
        
        return result_df
    
    def reconcile_dividend_data(self, primary_df: pd.DataFrame, secondary_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Reconcile dividend data from two sources
        
        Args:
            primary_df (pd.DataFrame): Primary source data
            secondary_df (pd.DataFrame): Secondary source data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Reconciled DataFrame and reconciliation report
        """
        # For dividend data, we use symbol, ex_date as keys and dividend_amount as value
        return self.reconcile_price_data(
            primary_df, secondary_df,
            key_columns=['symbol', 'ex_date'],
            value_columns=['dividend_amount']
        )
    
    def reconcile_earnings_data(self, primary_df: pd.DataFrame, secondary_df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Reconcile earnings data from two sources
        
        Args:
            primary_df (pd.DataFrame): Primary source data
            secondary_df (pd.DataFrame): Secondary source data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Reconciled DataFrame and reconciliation report
        """
        # For earnings data, we use symbol, report_date as keys and eps, revenue as values
        return self.reconcile_price_data(
            primary_df, secondary_df,
            key_columns=['symbol', 'report_date'],
            value_columns=['eps', 'revenue'] if 'revenue' in primary_df.columns and 'revenue' in secondary_df.columns else ['eps']
        )