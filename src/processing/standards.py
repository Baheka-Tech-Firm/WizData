"""
Standards Compliance Module

This module ensures that all financial data in the WizData platform adheres to
industry standards for formatting, field names, and metadata, including:
- ISO 8601 for dates and times
- ISO 4217 for currency codes
- Standard OHLCV format for price data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
import logging
from datetime import datetime
import re
import json

# Set up logging
logger = logging.getLogger(__name__)


class StandardsCompliance:
    """
    Class to ensure data standards compliance across the WizData platform,
    including date formats, currency codes, and asset class-specific standards.
    """
    
    def __init__(self):
        """Initialize the standards compliance engine"""
        logger.info("Initializing standards compliance engine")
        
        # Load ISO 4217 currency codes
        self.valid_currencies = self._load_iso_4217_currencies()
        
        # Define standard field names by data type
        self.standard_fields = {
            'price': {
                'required': ['date', 'symbol', 'open', 'high', 'low', 'close'],
                'optional': ['volume', 'adj_close', 'exchange', 'asset_type', 'currency']
            },
            'dividend': {
                'required': ['symbol', 'ex_date', 'dividend_amount'],
                'optional': ['payment_date', 'declaration_date', 'record_date', 'frequency', 'currency', 'exchange']
            },
            'earnings': {
                'required': ['symbol', 'report_date', 'eps'],
                'optional': ['fiscal_year', 'fiscal_quarter', 'expected_eps', 'surprise_pct', 'revenue', 
                            'quarter_end_date', 'currency', 'market']
            }
        }
    
    def standardize_data(self, df: pd.DataFrame, data_type: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Standardize data to comply with industry standards
        
        Args:
            df (pd.DataFrame): DataFrame to standardize
            data_type (str): Type of data ('price', 'dividend', 'earnings', etc.)
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Standardized DataFrame and compliance report
        """
        if df.empty:
            logger.warning("Empty DataFrame provided to standardize_data")
            return df, {"status": "error", "message": "Empty data"}
        
        # Initialize compliance report
        compliance_report = {
            "status": "success",
            "data_type": data_type,
            "timestamp": datetime.now().isoformat(),
            "record_count": len(df),
            "issues_fixed": 0,
            "field_compliance": {},
            "currency_compliance": {},
            "date_compliance": {}
        }
        
        # Create a copy of the input DataFrame to standardize
        df_std = df.copy()
        
        # Check and standardize field names
        df_std, field_report = self._standardize_field_names(df_std, data_type)
        compliance_report["field_compliance"] = field_report
        compliance_report["issues_fixed"] += field_report.get("issues_fixed", 0)
        
        # Standardize date fields
        df_std, date_report = self._standardize_dates(df_std, data_type)
        compliance_report["date_compliance"] = date_report
        compliance_report["issues_fixed"] += date_report.get("issues_fixed", 0)
        
        # Standardize currency codes
        df_std, currency_report = self._standardize_currencies(df_std)
        compliance_report["currency_compliance"] = currency_report
        compliance_report["issues_fixed"] += currency_report.get("issues_fixed", 0)
        
        # Apply data type-specific standards
        if data_type == 'price':
            df_std, price_report = self._standardize_price_data(df_std)
            compliance_report["price_compliance"] = price_report
            compliance_report["issues_fixed"] += price_report.get("issues_fixed", 0)
        elif data_type == 'dividend':
            df_std, dividend_report = self._standardize_dividend_data(df_std)
            compliance_report["dividend_compliance"] = dividend_report
            compliance_report["issues_fixed"] += dividend_report.get("issues_fixed", 0)
        elif data_type == 'earnings':
            df_std, earnings_report = self._standardize_earnings_data(df_std)
            compliance_report["earnings_compliance"] = earnings_report
            compliance_report["issues_fixed"] += earnings_report.get("issues_fixed", 0)
        
        # Add compliance metadata
        df_std['standards_compliant'] = True
        df_std['compliance_version'] = '1.0'
        df_std['compliance_date'] = datetime.now().isoformat()
        
        return df_std, compliance_report
    
    def _standardize_field_names(self, df: pd.DataFrame, data_type: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Standardize field names according to WizData standards
        
        Args:
            df (pd.DataFrame): DataFrame to standardize
            data_type (str): Type of data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: DataFrame with standardized field names and report
        """
        field_report = {
            "missing_required_fields": [],
            "renamed_fields": {},
            "issues_fixed": 0
        }
        
        if data_type not in self.standard_fields:
            logger.warning(f"Unknown data type: {data_type}")
            return df, field_report
        
        # Check for required fields
        required_fields = self.standard_fields[data_type]['required']
        all_standard_fields = required_fields + self.standard_fields[data_type]['optional']
        
        # Check for missing required fields
        for field in required_fields:
            if field not in df.columns:
                field_report["missing_required_fields"].append(field)
        
        # Map common field name variants to standard names
        field_mapping = {
            # Date fields
            'date': 'date',
            'timestamp': 'date',
            'datetime': 'date',
            'time': 'date',
            'trade_date': 'date',
            
            # Price fields
            'opening_price': 'open',
            'open_price': 'open',
            'highest_price': 'high',
            'high_price': 'high',
            'lowest_price': 'low',
            'low_price': 'low',
            'closing_price': 'close',
            'close_price': 'close',
            'last_price': 'close',
            'adj_close_price': 'adj_close',
            'adjusted_close': 'adj_close',
            'adjusted_closing_price': 'adj_close',
            
            # Symbol fields
            'ticker': 'symbol',
            'ticker_symbol': 'symbol',
            'code': 'symbol',
            
            # Volume fields
            'trading_volume': 'volume',
            'vol': 'volume',
            
            # Dividend fields
            'ex_dividend_date': 'ex_date',
            'exdate': 'ex_date',
            'payment_dt': 'payment_date',
            'pay_date': 'payment_date',
            'declaration_dt': 'declaration_date',
            'declared_date': 'declaration_date',
            'announce_date': 'declaration_date',
            'record_dt': 'record_date',
            'div_amount': 'dividend_amount',
            'dividend': 'dividend_amount',
            'div_value': 'dividend_amount',
            
            # Earnings fields
            'earnings_date': 'report_date',
            'release_date': 'report_date',
            'announcement_date': 'report_date',
            'fiscal_yr': 'fiscal_year',
            'fiscal_qtr': 'fiscal_quarter',
            'expected_earnings': 'expected_eps',
            'consensus_eps': 'expected_eps',
            'actual_eps': 'eps',
            'earnings_per_share': 'eps',
            'eps_surprise': 'surprise_pct',
            'surprise': 'surprise_pct',
            'rev': 'revenue',
            'qtr_end': 'quarter_end_date',
            'quarter_end': 'quarter_end_date'
        }
        
        # Rename columns based on mapping
        rename_dict = {}
        for col in df.columns:
            # Convert to lowercase for comparison
            col_lower = col.lower().strip()
            
            # Check if this column should be mapped to a standard name
            if col_lower in field_mapping and field_mapping[col_lower] in all_standard_fields:
                std_name = field_mapping[col_lower]
                if col != std_name:  # Only rename if different
                    rename_dict[col] = std_name
                    field_report["renamed_fields"][col] = std_name
        
        # Apply renaming
        if rename_dict:
            df = df.rename(columns=rename_dict)
            field_report["issues_fixed"] += len(rename_dict)
        
        return df, field_report
    
    def _standardize_dates(self, df: pd.DataFrame, data_type: str) -> Tuple[pd.DataFrame, Dict]:
        """
        Standardize date fields to ISO 8601 format
        
        Args:
            df (pd.DataFrame): DataFrame to standardize
            data_type (str): Type of data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: DataFrame with standardized dates and report
        """
        date_report = {
            "date_fields_processed": [],
            "non_standard_dates_fixed": 0,
            "issues_fixed": 0
        }
        
        # Identify date fields based on data type
        date_fields = []
        if data_type == 'price':
            date_fields = ['date']
        elif data_type == 'dividend':
            date_fields = ['ex_date', 'payment_date', 'declaration_date', 'record_date']
        elif data_type == 'earnings':
            date_fields = ['report_date', 'quarter_end_date']
        
        # Filter to date fields that actually exist in the DataFrame
        date_fields = [f for f in date_fields if f in df.columns]
        date_report["date_fields_processed"] = date_fields
        
        for field in date_fields:
            # Check if field is already datetime
            if not pd.api.types.is_datetime64_any_dtype(df[field]):
                # Count non-standard dates
                non_standard_count = len(df)
                
                # Convert to datetime with error handling
                try:
                    df[field] = pd.to_datetime(df[field], errors='coerce')
                    
                    # Count issues fixed (non-null values after conversion)
                    fixed_count = df[field].notna().sum()
                    date_report["non_standard_dates_fixed"] += non_standard_count - fixed_count
                    date_report["issues_fixed"] += non_standard_count - fixed_count
                    
                except Exception as e:
                    logger.error(f"Error converting {field} to datetime: {str(e)}")
            
            # Ensure consistent date representation (ISO 8601)
            if pd.api.types.is_datetime64_any_dtype(df[field]):
                # Keep as is for internal processing, would format as ISO 8601 for export
                pass
        
        return df, date_report
    
    def _standardize_currencies(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Standardize currency codes to ISO 4217
        
        Args:
            df (pd.DataFrame): DataFrame to standardize
            
        Returns:
            Tuple[pd.DataFrame, Dict]: DataFrame with standardized currencies and report
        """
        currency_report = {
            "non_standard_currencies": [],
            "currencies_standardized": 0,
            "issues_fixed": 0
        }
        
        # Skip if no currency column
        if 'currency' not in df.columns:
            return df, currency_report
        
        # Check and standardize currencies
        non_standard_mask = ~df['currency'].isin(self.valid_currencies)
        non_standard_currencies = df.loc[non_standard_mask, 'currency'].unique().tolist()
        currency_report["non_standard_currencies"] = non_standard_currencies
        
        # Map common non-standard currencies to their ISO codes
        currency_map = {
            'US DOLLAR': 'USD',
            'US$': 'USD',
            '$': 'USD',
            'DOLLAR': 'USD',
            'EURO': 'EUR',
            '€': 'EUR',
            'POUND': 'GBP',
            '£': 'GBP',
            'YEN': 'JPY',
            '¥': 'JPY',
            'RAND': 'ZAR',
            'R': 'ZAR',
            'YUAN': 'CNY',
            'AUSTRALIAN DOLLAR': 'AUD',
            'CANADIAN DOLLAR': 'CAD',
            'SWISS FRANC': 'CHF',
            'INDIAN RUPEE': 'INR',
            'BRAZILIAN REAL': 'BRL',
            'RUSSIAN RUBLE': 'RUB',
            'MEXICAN PESO': 'MXN',
            'SOUTH KOREAN WON': 'KRW',
            'SINGAPORE DOLLAR': 'SGD',
            'SOUTH AFRICAN RAND': 'ZAR'
        }
        
        # Initialize count of fixed currencies
        fixed_count = 0
        
        # Standardize currencies
        for non_std in non_standard_currencies:
            if non_std is not None:
                # Try to map to standard code
                upper_currency = str(non_std).strip().upper()
                if upper_currency in currency_map:
                    std_code = currency_map[upper_currency]
                    df.loc[df['currency'] == non_std, 'currency'] = std_code
                    fixed_count += (df['currency'] == std_code).sum()
                elif len(upper_currency) == 3 and re.match(r'^[A-Z]{3}$', upper_currency):
                    # It's already a 3-letter code, might be valid but not in our list
                    # Keep as is, could be a less common currency
                    pass
                else:
                    # Default to USD if unrecognized
                    logger.warning(f"Unrecognized currency: {non_std}, defaulting to USD")
                    df.loc[df['currency'] == non_std, 'currency'] = 'USD'
                    fixed_count += (df['currency'] == 'USD').sum()
        
        currency_report["currencies_standardized"] = fixed_count
        currency_report["issues_fixed"] = fixed_count
        
        return df, currency_report
    
    def _standardize_price_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply price data-specific standardizations
        
        Args:
            df (pd.DataFrame): DataFrame with price data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Standardized DataFrame and report
        """
        price_report = {
            "price_order_standardized": False,
            "ohlc_validation_fixed": 0,
            "issues_fixed": 0
        }
        
        # Ensure all required OHLC columns exist
        required_cols = ['open', 'high', 'low', 'close']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            logger.warning(f"Missing required price columns: {missing_cols}")
            return df, price_report
        
        # Convert price columns to float if needed
        for col in required_cols:
            if not pd.api.types.is_float_dtype(df[col]):
                try:
                    df[col] = df[col].astype(float)
                except Exception as e:
                    logger.error(f"Error converting {col} to float: {str(e)}")
        
        # Validate OHLC relationships and fix if necessary
        # High should be >= Low
        high_low_violation = df['high'] < df['low']
        if high_low_violation.any():
            # Swap high and low values where relationship is violated
            temp_high = df.loc[high_low_violation, 'high'].copy()
            df.loc[high_low_violation, 'high'] = df.loc[high_low_violation, 'low']
            df.loc[high_low_violation, 'low'] = temp_high
            price_report["ohlc_validation_fixed"] += high_low_violation.sum()
            price_report["issues_fixed"] += high_low_violation.sum()
        
        # High should be >= Open and Close
        high_open_violation = df['high'] < df['open']
        if high_open_violation.any():
            df.loc[high_open_violation, 'high'] = df.loc[high_open_violation, 'open']
            price_report["ohlc_validation_fixed"] += high_open_violation.sum()
            price_report["issues_fixed"] += high_open_violation.sum()
            
        high_close_violation = df['high'] < df['close']
        if high_close_violation.any():
            df.loc[high_close_violation, 'high'] = df.loc[high_close_violation, 'close']
            price_report["ohlc_validation_fixed"] += high_close_violation.sum()
            price_report["issues_fixed"] += high_close_violation.sum()
        
        # Low should be <= Open and Close
        low_open_violation = df['low'] > df['open']
        if low_open_violation.any():
            df.loc[low_open_violation, 'low'] = df.loc[low_open_violation, 'open']
            price_report["ohlc_validation_fixed"] += low_open_violation.sum()
            price_report["issues_fixed"] += low_open_violation.sum()
            
        low_close_violation = df['low'] > df['close']
        if low_close_violation.any():
            df.loc[low_close_violation, 'low'] = df.loc[low_close_violation, 'close']
            price_report["ohlc_validation_fixed"] += low_close_violation.sum()
            price_report["issues_fixed"] += low_close_violation.sum()
        
        # Standardize column order for OHLCV
        std_columns = ['date', 'symbol']
        for col in required_cols + (['volume'] if 'volume' in df.columns else []):
            if col in df.columns:
                std_columns.append(col)
        
        # Add any remaining columns
        for col in df.columns:
            if col not in std_columns:
                std_columns.append(col)
        
        # Reorder columns
        df = df[std_columns]
        price_report["price_order_standardized"] = True
        
        return df, price_report
    
    def _standardize_dividend_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply dividend data-specific standardizations
        
        Args:
            df (pd.DataFrame): DataFrame with dividend data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Standardized DataFrame and report
        """
        dividend_report = {
            "frequency_standardized": 0,
            "negative_dividends_fixed": 0,
            "issues_fixed": 0
        }
        
        # Convert dividend_amount to float if needed
        if 'dividend_amount' in df.columns and not pd.api.types.is_float_dtype(df['dividend_amount']):
            try:
                df['dividend_amount'] = df['dividend_amount'].astype(float)
            except Exception as e:
                logger.error(f"Error converting dividend_amount to float: {str(e)}")
        
        # Fix negative dividends
        if 'dividend_amount' in df.columns:
            negative_mask = df['dividend_amount'] < 0
            if negative_mask.any():
                df.loc[negative_mask, 'dividend_amount'] = df.loc[negative_mask, 'dividend_amount'].abs()
                dividend_report["negative_dividends_fixed"] = negative_mask.sum()
                dividend_report["issues_fixed"] += negative_mask.sum()
        
        # Standardize frequency values
        if 'frequency' in df.columns:
            frequency_map = {
                'Q': 'quarterly',
                'QUARTERLY': 'quarterly',
                'QTR': 'quarterly',
                'QUARTER': 'quarterly',
                'A': 'annual',
                'ANNUAL': 'annual',
                'YEARLY': 'annual',
                'ANNUALLY': 'annual',
                'Y': 'annual',
                'SA': 'semi-annual',
                'SEMI': 'semi-annual',
                'SEMI-ANNUAL': 'semi-annual',
                'SEMIANNUAL': 'semi-annual',
                'S': 'semi-annual',
                'M': 'monthly',
                'MONTHLY': 'monthly',
                'MONTH': 'monthly',
                'SPECIAL': 'special',
                'SP': 'special',
                'EXTRA': 'special',
                'IRREGULAR': 'irregular',
                'IRR': 'irregular',
                'I': 'irregular'
            }
            
            non_standard_count = 0
            for old_freq, new_freq in frequency_map.items():
                mask = df['frequency'].str.upper() == old_freq
                if mask.any():
                    df.loc[mask, 'frequency'] = new_freq
                    non_standard_count += mask.sum()
            
            dividend_report["frequency_standardized"] = non_standard_count
            dividend_report["issues_fixed"] += non_standard_count
        
        return df, dividend_report
    
    def _standardize_earnings_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict]:
        """
        Apply earnings data-specific standardizations
        
        Args:
            df (pd.DataFrame): DataFrame with earnings data
            
        Returns:
            Tuple[pd.DataFrame, Dict]: Standardized DataFrame and report
        """
        earnings_report = {
            "quarter_format_fixed": 0,
            "numeric_conversions": 0,
            "issues_fixed": 0
        }
        
        # Convert numeric columns to float
        numeric_cols = ['eps', 'expected_eps', 'surprise_pct', 'revenue']
        for col in numeric_cols:
            if col in df.columns and not pd.api.types.is_float_dtype(df[col]):
                try:
                    df[col] = df[col].astype(float)
                    earnings_report["numeric_conversions"] += 1
                    earnings_report["issues_fixed"] += 1
                except Exception as e:
                    logger.error(f"Error converting {col} to float: {str(e)}")
        
        # Standardize fiscal quarter format (ensure it's 1-4)
        if 'fiscal_quarter' in df.columns:
            # Initialize counter
            quarter_fixes = 0
            
            # Convert string quarters to integers if needed
            if not pd.api.types.is_integer_dtype(df['fiscal_quarter']):
                # Handle string quarter formats
                quarter_map = {
                    'Q1': 1, 'Q2': 2, 'Q3': 3, 'Q4': 4,
                    'QTR1': 1, 'QTR2': 2, 'QTR3': 3, 'QTR4': 4,
                    'QUARTER1': 1, 'QUARTER2': 2, 'QUARTER3': 3, 'QUARTER4': 4
                }
                
                for q_str, q_int in quarter_map.items():
                    if df['fiscal_quarter'].dtype == object:  # Only if string type
                        mask = df['fiscal_quarter'].str.upper() == q_str
                        if mask.any():
                            df.loc[mask, 'fiscal_quarter'] = q_int
                            quarter_fixes += mask.sum()
                
                # Try to convert to integer
                try:
                    df['fiscal_quarter'] = df['fiscal_quarter'].astype(int)
                    quarter_fixes += 1
                except Exception as e:
                    logger.error(f"Error converting fiscal_quarter to int: {str(e)}")
            
            # Ensure quarters are in range 1-4
            if pd.api.types.is_numeric_dtype(df['fiscal_quarter']):
                out_of_range = ~df['fiscal_quarter'].isin([1, 2, 3, 4])
                if out_of_range.any():
                    # Map quarters to valid range
                    df.loc[out_of_range, 'fiscal_quarter'] = df.loc[out_of_range, 'fiscal_quarter'].apply(
                        lambda x: ((int(x) - 1) % 4) + 1 if pd.notna(x) else None
                    )
                    quarter_fixes += out_of_range.sum()
            
            earnings_report["quarter_format_fixed"] = quarter_fixes
            earnings_report["issues_fixed"] += quarter_fixes
        
        return df, earnings_report
    
    def _load_iso_4217_currencies(self) -> List[str]:
        """
        Load list of valid ISO 4217 currency codes
        
        Returns:
            List[str]: List of valid currency codes
        """
        # Common currency codes (not exhaustive)
        common_codes = [
            'USD', 'EUR', 'JPY', 'GBP', 'AUD', 'CAD', 'CHF', 'CNY', 'HKD',
            'NZD', 'SEK', 'KRW', 'SGD', 'NOK', 'MXN', 'INR', 'RUB', 'ZAR',
            'TRY', 'BRL', 'TWD', 'DKK', 'PLN', 'THB', 'IDR', 'HUF', 'CZK',
            'ILS', 'CLP', 'PHP', 'AED', 'COP', 'SAR', 'MYR', 'RON'
        ]
        
        # In a production system, we would load this from a more comprehensive source
        return common_codes
    
    def add_data_lineage(self, df: pd.DataFrame, source: str, process_info: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        """
        Add data lineage information to a DataFrame
        
        Args:
            df (pd.DataFrame): DataFrame to update
            source (str): Source of the data
            process_info (Dict, optional): Additional processing information
            
        Returns:
            pd.DataFrame: DataFrame with lineage information
        """
        df = df.copy()
        
        # Add lineage columns
        df['data_source'] = source
        df['data_timestamp'] = datetime.now().isoformat()
        
        if process_info:
            df['process_info'] = json.dumps(process_info)
        
        return df