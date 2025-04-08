import pandas as pd
import numpy as np
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)

class DataCleaner:
    """
    Class to clean financial data by removing outliers,
    handling missing values, and ensuring data quality
    """
    
    def __init__(self):
        """Initialize the data cleaner"""
        logger.info("Initializing data cleaner")
    
    def clean_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean OHLCV (Open, High, Low, Close, Volume) data
        
        Args:
            df (pd.DataFrame): Input DataFrame with OHLCV data
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to clean_ohlcv")
            return df
        
        # Make a copy to avoid modifying the original
        df_clean = df.copy()
        
        try:
            # Ensure date column is datetime
            if 'date' in df_clean.columns:
                df_clean['date'] = pd.to_datetime(df_clean['date'])
            
            # Check for and handle missing values
            missing_counts = df_clean.isnull().sum()
            if missing_counts.sum() > 0:
                logger.info(f"Found {missing_counts.sum()} missing values")
                
                # Handle missing values
                numeric_cols = ['open', 'high', 'low', 'close', 'volume']
                present_cols = [col for col in numeric_cols if col in df_clean.columns]
                
                if present_cols:
                    df_clean = self.handle_missing_values(df_clean, method='ffill')
            
            # Check for and handle outliers in price columns
            price_cols = [col for col in ['open', 'high', 'low', 'close'] if col in df_clean.columns]
            if price_cols:
                # Group by symbol to handle outliers within each symbol separately
                if 'symbol' in df_clean.columns:
                    symbols = df_clean['symbol'].unique()
                    
                    cleaned_dfs = []
                    for sym in symbols:
                        sym_df = df_clean[df_clean['symbol'] == sym].copy()
                        sym_df = self._remove_extreme_outliers(sym_df, price_cols)
                        cleaned_dfs.append(sym_df)
                    
                    df_clean = pd.concat(cleaned_dfs, ignore_index=True)
                else:
                    df_clean = self._remove_extreme_outliers(df_clean, price_cols)
            
            # Ensure price relationships are valid (high ≥ open/close ≥ low)
            if all(col in df_clean.columns for col in ['open', 'high', 'low', 'close']):
                # Find rows with invalid price relationships
                invalid_high = df_clean['high'] < df_clean[['open', 'close']].max(axis=1)
                invalid_low = df_clean['low'] > df_clean[['open', 'close']].min(axis=1)
                
                invalid_rows = invalid_high | invalid_low
                if invalid_rows.any():
                    logger.warning(f"Found {invalid_rows.sum()} rows with invalid price relationships")
                    
                    # Fix invalid high values
                    df_clean.loc[invalid_high, 'high'] = df_clean.loc[invalid_high, ['open', 'close', 'high']].max(axis=1)
                    
                    # Fix invalid low values
                    df_clean.loc[invalid_low, 'low'] = df_clean.loc[invalid_low, ['open', 'close', 'low']].min(axis=1)
            
            # Ensure volume is non-negative
            if 'volume' in df_clean.columns:
                negative_volume = df_clean['volume'] < 0
                if negative_volume.any():
                    logger.warning(f"Found {negative_volume.sum()} rows with negative volume")
                    df_clean.loc[negative_volume, 'volume'] = 0
            
            logger.info(f"Successfully cleaned DataFrame with {len(df_clean)} rows")
            return df_clean
            
        except Exception as e:
            logger.error(f"Error cleaning OHLCV data: {str(e)}")
            return df  # Return original DataFrame on error
    
    def handle_missing_values(self, df: pd.DataFrame, method: str = 'ffill') -> pd.DataFrame:
        """
        Handle missing values in the DataFrame
        
        Args:
            df (pd.DataFrame): Input DataFrame
            method (str): Method for handling missing values
                          ('ffill', 'bfill', 'interpolate', 'drop')
            
        Returns:
            pd.DataFrame: DataFrame with missing values handled
        """
        if df.empty:
            return df
        
        df_result = df.copy()
        
        try:
            if method == 'ffill':
                # Forward fill (use previous value)
                df_result = df_result.ffill()
                
                # If there are still missing values (e.g., at the beginning), use backward fill
                if df_result.isnull().sum().sum() > 0:
                    df_result = df_result.bfill()
                    
            elif method == 'bfill':
                # Backward fill (use next value)
                df_result = df_result.bfill()
                
                # If there are still missing values (e.g., at the end), use forward fill
                if df_result.isnull().sum().sum() > 0:
                    df_result = df_result.ffill()
                    
            elif method == 'interpolate':
                # Linear interpolation for continuous data
                numeric_cols = df_result.select_dtypes(include=[np.number]).columns
                for col in numeric_cols:
                    df_result[col] = df_result[col].interpolate(method='linear')
                
                # Forward and backward fill for any remaining values
                df_result = df_result.ffill().bfill()
                
            elif method == 'drop':
                # Drop rows with any missing values
                df_result = df_result.dropna()
                
            else:
                logger.warning(f"Unknown method '{method}' for handling missing values, using ffill")
                df_result = df_result.ffill().bfill()
            
            missing_after = df_result.isnull().sum().sum()
            if missing_after > 0:
                logger.warning(f"There are still {missing_after} missing values after {method}")
            
            return df_result
            
        except Exception as e:
            logger.error(f"Error handling missing values: {str(e)}")
            return df  # Return original DataFrame on error
    
    def _remove_extreme_outliers(self, df: pd.DataFrame, columns: List[str], 
                                z_threshold: float = 3.0) -> pd.DataFrame:
        """
        Remove extreme outliers using z-score
        
        Args:
            df (pd.DataFrame): Input DataFrame
            columns (List[str]): Columns to check for outliers
            z_threshold (float): Z-score threshold for outliers
            
        Returns:
            pd.DataFrame: DataFrame with outliers removed or clipped
        """
        df_result = df.copy()
        
        for col in columns:
            if col in df_result.columns:
                # Calculate z-scores
                mean = df_result[col].mean()
                std = df_result[col].std()
                
                if std > 0:  # Avoid division by zero
                    z_scores = (df_result[col] - mean) / std
                    
                    # Identify extreme outliers
                    outliers = abs(z_scores) > z_threshold
                    outlier_count = outliers.sum()
                    
                    if outlier_count > 0:
                        logger.info(f"Found {outlier_count} outliers in column '{col}'")
                        
                        # Clip outliers to threshold values instead of removing them
                        upper_bound = mean + (z_threshold * std)
                        lower_bound = mean - (z_threshold * std)
                        
                        df_result.loc[df_result[col] > upper_bound, col] = upper_bound
                        df_result.loc[df_result[col] < lower_bound, col] = lower_bound
        
        return df_result
    
    def validate_data(self, df: pd.DataFrame, rules: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Validate data against a set of rules
        
        Args:
            df (pd.DataFrame): Input DataFrame
            rules (Dict[str, Any]): Validation rules
            
        Returns:
            Tuple[pd.DataFrame, Dict[str, Any]]: 
                - DataFrame with validation flags
                - Validation report
        """
        if df.empty:
            return df, {"valid": True, "message": "Empty DataFrame", "details": {}}
        
        df_result = df.copy()
        validation_report = {
            "valid": True,
            "message": "Data validation passed",
            "details": {}
        }
        
        try:
            # Add a validation column
            df_result['is_valid'] = True
            
            # Apply validation rules
            for rule_name, rule_params in rules.items():
                if rule_name == "range_check":
                    # Check if values are within specified range
                    for col, (min_val, max_val) in rule_params.items():
                        if col in df_result.columns:
                            invalid = (df_result[col] < min_val) | (df_result[col] > max_val)
                            if invalid.any():
                                df_result.loc[invalid, 'is_valid'] = False
                                validation_report["details"][f"{col}_range"] = {
                                    "invalid_count": invalid.sum(),
                                    "message": f"{invalid.sum()} values outside range [{min_val}, {max_val}]"
                                }
                                validation_report["valid"] = False
                
                elif rule_name == "completeness_check":
                    # Check for missing values in specified columns
                    required_cols = rule_params.get("required_columns", [])
                    for col in required_cols:
                        if col in df_result.columns:
                            missing = df_result[col].isnull()
                            if missing.any():
                                df_result.loc[missing, 'is_valid'] = False
                                validation_report["details"][f"{col}_missing"] = {
                                    "invalid_count": missing.sum(),
                                    "message": f"{missing.sum()} missing values in column '{col}'"
                                }
                                validation_report["valid"] = False
                
                elif rule_name == "uniqueness_check":
                    # Check if values in specified columns are unique
                    unique_cols = rule_params.get("unique_columns", [])
                    for col in unique_cols:
                        if col in df_result.columns:
                            duplicates = df_result.duplicated(subset=[col])
                            if duplicates.any():
                                df_result.loc[duplicates, 'is_valid'] = False
                                validation_report["details"][f"{col}_duplicates"] = {
                                    "invalid_count": duplicates.sum(),
                                    "message": f"{duplicates.sum()} duplicate values in column '{col}'"
                                }
                                validation_report["valid"] = False
            
            # Update validation message if invalid
            if not validation_report["valid"]:
                validation_report["message"] = "Data validation failed"
            
            return df_result, validation_report
            
        except Exception as e:
            logger.error(f"Error validating data: {str(e)}")
            return df, {
                "valid": False,
                "message": f"Error during validation: {str(e)}",
                "details": {}
            }