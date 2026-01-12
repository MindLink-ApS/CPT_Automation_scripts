import pandas as pd
from pathlib import Path
import logging
import numpy as np
from datetime import datetime

logger = logging.getLogger(__name__)

class DataProcessorCLFS:
    """Process and clean the downloaded CLFS Excel file"""

    # Source column names (as they appear in the Excel file)
    SOURCE_COLUMNS = {
        'HCPCS': 'code',
        'RATE': '80th',
        'SHORTDESC': 'code_description',
        'LONGDESC': 'full_description',
        'EFF_DATE': 'rel_date'
    }
    

    MONTH_MAPPING = {
        '01': 'Jan', '02': 'Feb', '03': 'Mar', '04': 'Apr',
        '05': 'May', '06': 'Jun', '07': 'Jul', '08': 'Aug',
        '09': 'Sep', '10': 'Oct', '11': 'Nov', '12': 'Dec'
    }

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame, finding the correct header row"""
        logger.info(f"📖 Reading Excel file: {file_path}")
        
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            raise ValueError(f"File is not an Excel file: {file_path}")

        try:
            df_temp = pd.read_excel(file_path, header=None)
            
            header_row_idx = None
            
            # Find the header row by searching for key columns
            for idx, row in df_temp.iterrows():
                row_str = " ".join(row.astype(str).fillna("")).upper()
                if 'HCPCS' in row_str and 'RATE' in row_str:
                    header_row_idx = idx 
                    logger.info(f"🔍 Found header row at index: {header_row_idx}")
                    break
            
            if header_row_idx is None:
                raise ValueError("Could not find header row with 'HCPCS' and 'RATE' columns")

            df = pd.read_excel(file_path, header=header_row_idx)
            
            logger.info(f"✅ Loaded {len(df)} rows (raw)")
            logger.info(f"📋 Raw columns found: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            raise

    def clean_eff_date(self, date_value) -> str:
        """
        Clean EFF_DATE column: Convert from YYYYMMDD to 'Month YYYY'
        
        Args:
            date_value: Can be string like '20250101', int like 20250101, or datetime
            
        Returns:
            str: Formatted date like 'Jan 2025'
        """
        if pd.isna(date_value):
            return None
            
        try:
            date_str = str(date_value).strip()
            
            if len(date_str) == 8: 
                year = date_str[:4]
                month = date_str[4:6]
              
                month_name = self.MONTH_MAPPING.get(month, 'Unknown')
                
                return f"{month_name} {year}"
            
            elif isinstance(date_value, (pd.Timestamp, datetime)):
                month_name = date_value.strftime('%b')  # 'Jan', 'Feb', etc.
                year = date_value.strftime('%Y')
                return f"{month_name} {year}"
            
            else:
                logger.warning(f"Unexpected date format: {date_value}")
                return None
                
        except Exception as e:
            logger.warning(f"Error cleaning date '{date_value}': {e}")
            return None

    def clean_rate(self, rate_value) -> float:
        """
        Clean RATE column: Remove leading zeros and convert to float
        
        Args:
            rate_value: Can be string, float, or int
            
        Returns:
            float: Cleaned rate value or None
        """
        if pd.isna(rate_value):
            return None
            
        try:
        
            rate_str = str(rate_value).strip()
            
            rate_float = float(rate_str)
            
            return rate_float
            
        except (ValueError, TypeError) as e:
            logger.warning(f"Error cleaning rate '{rate_value}': {e}")
            return None

    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the raw DataFrame based on CLFS requirements"""
        logger.info(" Cleaning data...")
        
        available_columns = df.columns.tolist()
        logger.info(f" Available columns: {available_columns}")
        
        missing_columns = []
        for source_col in self.SOURCE_COLUMNS.keys():
            if source_col not in available_columns:
                missing_columns.append(source_col)
        
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # --- Step 2: Select and rename columns ---
        logger.info(" Selecting and renaming columns...")
        df_cleaned = df[list(self.SOURCE_COLUMNS.keys())].copy()
        df_cleaned.columns = list(self.SOURCE_COLUMNS.values())
        
        logger.info(f" Columns after renaming: {list(df_cleaned.columns)}")

        # --- Step 3: Clean EFF_DATE → rel_date ---
        logger.info(" Cleaning 'rel_date' (EFF_DATE) column...")
        df_cleaned['rel_date'] = df_cleaned['rel_date'].apply(self.clean_eff_date)
        
        # --- Step 4: Clean RATE → 80th ---
        logger.info(" Cleaning '80th' (RATE) column...")
        df_cleaned['80th'] = df_cleaned['80th'].apply(self.clean_rate)

        # --- Step 5: Clean code (HCPCS) ---
        logger.info(" Cleaning 'code' (HCPCS) column...")
        # Remove rows where code is empty or null
        df_cleaned = df_cleaned.dropna(subset=['code'])
        df_cleaned = df_cleaned[df_cleaned['code'].astype(str).str.strip().str.len() > 0]
        
        # Convert to string and strip whitespace
        df_cleaned['code'] = df_cleaned['code'].astype(str).str.strip()

        # --- Step 6: Clean description columns (remove extra whitespace) ---
        logger.info(" Cleaning description columns...")
        for col in ['code_description', 'full_description']:
            if col in df_cleaned.columns:
                df_cleaned[col] = df_cleaned[col].astype(str).str.strip()
                # Replace 'nan' string with None
                df_cleaned[col] = df_cleaned[col].replace('nan', None)

        # --- Step 7: Add data_type column ---
        logger.info("➕ Adding 'data_type' column...")
        df_cleaned['data_type'] = 'Medicare Lab'

        # --- Step 8: Universal NaN to None conversion for JSON compliance ---
        logger.info(" Converting ALL NaN values to None for JSON compliance...")
        
        for col in df_cleaned.columns:
            if df_cleaned[col].isnull().any():
                logger.info(f"    -> Fixing NaNs in column '{col}'")
                df_cleaned[col] = df_cleaned[col].astype(object).where(pd.notnull(df_cleaned[col]), None)

        # --- Step 9: Drop completely empty rows ---
        df_cleaned = df_cleaned.dropna(how='all')
        df_cleaned.reset_index(drop=True, inplace=True)

        # --- Step 10: Final validation ---
        logger.info(f" Cleaned data: {len(df_cleaned)} rows remaining")
        logger.info(f" Final columns: {list(df_cleaned.columns)}")
        logger.info(f" Sample data (first 3 rows):\n{df_cleaned.head(3).to_string()}")
        
        # Show data types
        logger.info(f" Data types:\n{df_cleaned.dtypes}")
        
        return df_cleaned