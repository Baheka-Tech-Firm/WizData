import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timezone

from src.utils.logger import get_processing_logger

class DataCleaner:
    """
    Class to clean financial data by removing outliers,
    handling missing values, and ensuring data quality
    """
    
    def __init__(self):
        self.logger = get_processing_logger()
    
    def clean_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean OHLCV (Open, High, Low, Close, Volume) data
        
        Args:
            df (pd.DataFrame): Input DataFrame
            
        Returns:
            pd.DataFrame: Cleaned DataFrame
        """
        self.logger.info(f"Cleaning OHLCV data with {len(df)} rows")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            # Remove rows with NaN in essential columns
            essential_cols = ['date', 'open', 'high', 'low', 'close']
            essential_cols = [col for col in essential_cols if col in result.columns]
            if essential_cols:
                initial_len = len(result)
                result = result.dropna(subset=essential_cols)
                dropped_rows = initial_len - len(result)
                if dropped_rows > 0:
                    self.logger.info(f"Dropped {dropped_rows} rows with NaN values in essential columns")
            
            # Ensure OHLC values are valid
            if all(col in result.columns for col in ['open', 'high', 'low', 'close']):
                # High should be the highest value
                result['high'] = result[['open', 'high', 'low', 'close']].max(axis=1)
                
                # Low should be the lowest value
                result['low'] = result[['open', 'high', 'low', 'close']].min(axis=1)
                
                # Ensure all prices are positive
                for col in ['open', 'high', 'low', 'close']:
                    invalid_count = sum(result[col] <= 0)
                    if invalid_count > 0:
                        self.logger.warning(f"Found {invalid_count} rows with non-positive {col} values")
                        result = result[result[col] > 0]
            
            # Handle volume separately (can be zero but not negative)
            if 'volume' in result.columns:
                invalid_vol_count = sum(result['volume'] < 0)
                if invalid_vol_count > 0:
                    self.logger.warning(f"Found {invalid_vol_count} rows with negative volume")
                    result['volume'] = result['volume'].clip(lower=0)
            
            # Remove extreme outliers using z-score
            numeric_cols = ['open', 'high', 'low', 'close', 'volume']
            numeric_cols = [col for col in numeric_cols if col in result.columns]
            if numeric_cols:
                result = self._remove_extreme_outliers(result, numeric_cols)
            
            # Ensure data is sorted by date
            if 'date' in result.columns:
                result = result.sort_values('date')
            
            self.logger.info(f"Successfully cleaned data, {len(result)} rows remain")
            return result
            
        except Exception as e:
            self.logger.error(f"Error cleaning OHLCV data: {str(e)}")
            # Return original dataframe if cleaning fails
            return df
    
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
        self.logger.info(f"Handling missing values using method: {method}")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            if method == 'drop':
                initial_len = len(result)
                result = result.dropna()
                dropped_rows = initial_len - len(result)
                if dropped_rows > 0:
                    self.logger.info(f"Dropped {dropped_rows} rows with NaN values")
            
            elif method == 'ffill':
                # Forward fill (use previous value)
                result = result.ffill()
                # If there are still NaNs at the beginning, back fill
                result = result.bfill()
            
            elif method == 'bfill':
                # Backward fill (use next value)
                result = result.bfill()
                # If there are still NaNs at the end, forward fill
                result = result.ffill()
            
            elif method == 'interpolate':
                # Use linear interpolation for numeric columns
                numeric_cols = result.select_dtypes(include=['number']).columns
                for col in numeric_cols:
                    result[col] = result[col].interpolate(method='linear')
                
                # For non-numeric columns, use forward fill
                non_numeric_cols = result.select_dtypes(exclude=['number']).columns
                for col in non_numeric_cols:
                    result[col] = result[col].ffill().bfill()
            
            else:
                self.logger.warning(f"Unknown method: {method}, no action taken")
            
            # Count remaining NaNs
            remaining_nans = result.isna().sum().sum()
            if remaining_nans > 0:
                self.logger.warning(f"{remaining_nans} NaN values remain after {method}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error handling missing values: {str(e)}")
            # Return original dataframe if handling fails
            return df
    
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
        result = df.copy()
        
        # Process each column separately
        for col in columns:
            if col not in result.columns:
                continue
                
            # Skip columns with all NaN values
            if result[col].isna().all():
                continue
                
            # Calculate z-scores
            mean = result[col].mean()
            std = result[col].std()
            
            # Avoid division by zero
            if std == 0:
                continue
                
            z_scores = np.abs((result[col] - mean) / std)
            
            # Identify outliers
            outliers = z_scores > z_threshold
            outlier_count = outliers.sum()
            
            if outlier_count > 0:
                self.logger.info(f"Found {outlier_count} outliers in {col} using z-score > {z_threshold}")
                
                # If more than 1% of data are outliers, clip instead of removing
                if outlier_count > len(result) * 0.01:
                    self.logger.info(f"Clipping {col} outliers instead of removing rows")
                    upper_bound = mean + z_threshold * std
                    lower_bound = mean - z_threshold * std
                    result[col] = result[col].clip(lower=lower_bound, upper=upper_bound)
                else:
                    # Otherwise remove the rows with extreme outliers
                    result = result[~outliers]
                    self.logger.info(f"Removed {outlier_count} rows with {col} outliers")
        
        return result
    
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
        self.logger.info(f"Validating data with {len(df)} rows")
        
        try:
            # Create a copy to avoid modifying the original
            result = df.copy()
            
            # Add a column to track validation issues
            result['validation_issues'] = ""
            
            validation_report = {
                "total_rows": len(result),
                "rules_checked": [],
                "issues_found": 0,
                "issues_by_rule": {}
            }
            
            for rule_name, rule_config in rules.items():
                validation_report["rules_checked"].append(rule_name)
                validation_report["issues_by_rule"][rule_name] = 0
                
                column = rule_config.get("column")
                condition = rule_config.get("condition")
                
                if not column or not condition or column not in result.columns:
                    continue
                
                # Apply validation condition
                if condition == "not_null":
                    invalid_mask = result[column].isna()
                elif condition == "positive":
                    invalid_mask = result[column] <= 0
                elif condition == "in_range":
                    min_val = rule_config.get("min")
                    max_val = rule_config.get("max")
                    invalid_mask = ~result[column].between(min_val, max_val)
                elif condition == "unique":
                    invalid_mask = result.duplicated(subset=[column])
                else:
                    self.logger.warning(f"Unknown condition: {condition} for rule {rule_name}")
                    continue
                
                # Count issues
                issues_count = invalid_mask.sum()
                validation_report["issues_by_rule"][rule_name] = issues_count
                validation_report["issues_found"] += issues_count
                
                # Add rule name to validation_issues column for rows that fail
                if issues_count > 0:
                    # Append rule name for rows with issues
                    result.loc[invalid_mask, 'validation_issues'] += f"{rule_name};"
            
            # Remove trailing semicolon
            result['validation_issues'] = result['validation_issues'].str.rstrip(';')
            
            # Add validation timestamp
            validation_report["timestamp"] = datetime.now(timezone.utc).isoformat()
            
            self.logger.info(f"Validation complete. Found {validation_report['issues_found']} issues.")
            return result, validation_report
            
        except Exception as e:
            self.logger.error(f"Error during data validation: {str(e)}")
            # Return original dataframe if validation fails
            return df, {"error": str(e)}
