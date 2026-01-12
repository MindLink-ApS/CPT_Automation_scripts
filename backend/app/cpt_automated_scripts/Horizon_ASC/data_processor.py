import pandas as pd
from pathlib import Path
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessorHorizonASC:
    """Process and clean Horizon ASC fee schedule data"""

    SOURCE_HCPCS_COL = "HCPCS Code"
    SOURCE_DESC_COL = "Short Descriptor"
    SOURCE_RATE_COL = "Horizon ASC FS"

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame"""
        logger.info(f"Reading Excel file: {file_path}")

        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            raise ValueError(f"File is not an Excel file: {file_path}")

        try:
            # Try to read with header detection
            df = pd.read_excel(file_path, header=0)
            
            # If header row not found, search for it
            if self.SOURCE_HCPCS_COL not in df.columns:
                df_temp = pd.read_excel(file_path, header=None)
                header_row_idx = None
                
                for idx, row in df_temp.iterrows():
                    row_str = " ".join(row.astype(str).fillna("")).upper()
                    if (self.SOURCE_HCPCS_COL.upper() in row_str and 
                        self.SOURCE_DESC_COL.upper() in row_str):
                        header_row_idx = idx
                        logger.info(f"Found header row at index: {header_row_idx}")
                        break
                
                if header_row_idx is not None:
                    df = pd.read_excel(file_path, header=header_row_idx)
            
            logger.info(f"Loaded {len(df)} rows (raw)")
            logger.info(f"Raw columns found: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"Error reading Excel file: {e}")
            raise

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and transform Horizon ASC data"""
        logger.info("Cleaning Horizon ASC data...")

        # Find and map columns
        columns_mapping = {}
        available_columns = df.columns.astype(str)

        # Find HCPCS Code column
        hcpcs_col = next(
            (col for col in available_columns if self.SOURCE_HCPCS_COL.lower() in col.lower()),
            None
        )
        if hcpcs_col:
            columns_mapping['code'] = hcpcs_col

        # Find Short Descriptor column
        desc_col = next(
            (col for col in available_columns if self.SOURCE_DESC_COL.lower() in col.lower()),
            None
        )
        if desc_col:
            columns_mapping['code_description'] = desc_col

        # Find Horizon ASC FS (rate) column
        rate_col = next(
            (col for col in available_columns if self.SOURCE_RATE_COL.lower() in col.lower() or 'horizon' in col.lower()),
            None
        )
        if rate_col:
            columns_mapping['80th'] = rate_col
            logger.info(f"Found rate column: '{rate_col}'")

        logger.info(f"Column mapping: {columns_mapping}")

        # Check required columns
        required_keys = ['code', 'code_description', '80th']
        missing_keys = [key for key in required_keys if key not in columns_mapping]
        if missing_keys:
            raise ValueError(f"Missing required columns: {missing_keys}. Available: {list(available_columns)}")

        # Create cleaned DataFrame with mapped columns
        df_cleaned = pd.DataFrame()
        df_cleaned['code'] = df[columns_mapping['code']].astype(str).str.strip()
        df_cleaned['code_description'] = df[columns_mapping['code_description']].astype(str).str.strip()
        
        # Clean rate column - remove currency symbols, commas
        rate_series = df[columns_mapping['80th']].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
        df_cleaned['80th'] = pd.to_numeric(rate_series, errors='coerce')

        # Set data type
        df_cleaned['data_type'] = 'ASC Commercial'
        
        # Set geozip (Horizon is NJ-based, but rates may be national)
        # Default to USA, can be adjusted based on actual data
        df_cleaned['geozip'] = 'USA'
        
        # Set source
        df_cleaned['source'] = 'Horizon_ASC'
        
        # Set release date (current year, can be extracted from file if available)
        current_year = datetime.now().year
        df_cleaned['release_date'] = f'{current_year}-01-01'
        df_cleaned['rel_date'] = f'January {current_year}'

        # Remove rows with null codes or rates
        initial_count = len(df_cleaned)
        df_cleaned = df_cleaned[
            df_cleaned['code'].notna() & 
            (df_cleaned['code'] != '') & 
            (df_cleaned['code'] != 'nan') &
            df_cleaned['80th'].notna()
        ]
        
        removed_count = initial_count - len(df_cleaned)
        if removed_count > 0:
            logger.warning(f"Removed {removed_count} rows with null/empty codes or rates")

        logger.info(f"✅ Cleaned data: {len(df_cleaned)} rows")
        return df_cleaned
