import pandas as pd
from pathlib import Path
import logging
import numpy as np
import re

logger = logging.getLogger(__name__)

class DataProcessorFairHealth:
    """Process and clean the downloaded Fair Health file"""
    
    SOURCE_PRODUCT_COL = "Product"
    SOURCE_REL_DATE_COL = "Rel Date"
    SOURCE_GEOZIP_COL = "Geozip"
    SOURCE_CODE_COL = "Code"
    SOURCE_FULL_DESC_COL = "Full Description"
    
    SOURCE_50TH_COL = "50th"
    SOURCE_60TH_COL = "60th"
    SOURCE_70TH_COL = "70th"
    SOURCE_75TH_COL = "75th"
    SOURCE_80TH_COL = "80th"
    SOURCE_85TH_COL = "85th"
    SOURCE_90TH_COL = "90th"
    SOURCE_95TH_COL = "95th"

    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """Read Excel file into DataFrame, finding the correct header row"""
        logger.info(f"📖 Reading Excel file: {file_path}")
        
        if file_path.suffix.lower() not in ['.xlsx', '.xls']:
            raise ValueError(f"File is not an Excel file: {file_path}")

        try:
            # Load the file without a header first to find the real one
            df_temp = pd.read_excel(file_path, header=None)
            
            # --- This is where header_row_idx is defined ---
            header_row_idx = None
            
            # Find the header row by searching for key columns
            for idx, row in df_temp.iterrows():
                row_str = " ".join(row.astype(str).fillna("")).upper()
                if (self.SOURCE_CODE_COL.upper() in row_str and 
                    self.SOURCE_GEOZIP_COL.upper() in row_str):
                    
                    # --- This is where it gets its value ---
                    header_row_idx = idx 
                    logger.info(f"🔍 Found header row at index: {header_row_idx}")
                    break
            
            # --- This is the line you saw ---
            if header_row_idx is None:
                raise ValueError(f"Could not find header row with '{self.SOURCE_CODE_COL}' and '{self.SOURCE_GEOZIP_COL}'")

            # Now, read the file again using the correct header row
            df = pd.read_excel(file_path, header=header_row_idx)
            
            logger.info(f"✅ Loaded {len(df)} rows (raw)")
            logger.info(f"📋 Raw columns found: {list(df.columns)}")
            return df

        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            raise

    def _clean_currency_value(self, value):
        """
        Clean currency formatting from a value.
        Removes: $, commas, spaces
        Examples: 
            "$2,334.4" -> 2334.4
            "$1,234" -> 1234.0
            "N/A" -> None
        """
        if pd.isna(value):
            return None
        
        # Convert to string
        value_str = str(value).strip()
        
        # Remove currency symbols, commas, and spaces
        cleaned = re.sub(r'[$,\s]', '', value_str)
        
        # Try to convert to float
        try:
            return float(cleaned)
        except (ValueError, TypeError):
            # If conversion fails, return None
            logger.debug(f"Could not convert value to float: '{value_str}' -> '{cleaned}'")
            return None

    def format_geozip(self, geozip):
        """
        Format geozip as a 3-digit string with leading zeros.
        Special handling for "USA" - keep it as is.
        Examples:
            70 -> "070"
            74 -> "074"
            "070" -> "070"
            123 -> "123"
            "USA" -> "USA"
        """
        if pd.isna(geozip):
            return None
        
        # Convert to string and remove any whitespace
        geozip_str = str(geozip).strip().upper()
        
        # If it's "USA", return as-is
        if geozip_str == "USA":
            return "USA"
        
        # Remove .0 if present (from Excel numeric conversion)
        if '.' in geozip_str:
            geozip_str = geozip_str.split('.')[0]
        
        # Pad with leading zeros to make it 3 digits
        geozip_formatted = geozip_str.zfill(3)
        
        return geozip_formatted
    def clean_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean the raw DataFrame based on Fair Health requirements"""
        logger.info("🧹 Cleaning data...")

        # --- Find and map columns ---
        columns_mapping = {}
        available_columns = df.columns.astype(str)
        
        # Find Rel Date
        rel_date_col = next((col for col in available_columns if "rel date" in col.lower() or "rel_date" in col.lower()), None)
        if rel_date_col: columns_mapping['rel_date'] = rel_date_col

        # Find Geozip
        geozip_col = next((col for col in available_columns if "geozip" in col.lower() or "geo zip" in col.lower()), None)
        if geozip_col: columns_mapping['geozip'] = geozip_col
        
        # Find Code
        code_col = next((col for col in available_columns if col.strip().lower() == "code"), None)
        if code_col: columns_mapping['code'] = code_col

        # Find Full Description
        full_desc_col = next((col for col in available_columns if "full description" in col.lower()), None)
        if full_desc_col: columns_mapping['full_description'] = full_desc_col

        # Find all percentile columns (exact match)
        percentile_50_col = next((col for col in available_columns if col.strip() == "50th"), None)
        if percentile_50_col: columns_mapping['50th'] = percentile_50_col
        
        percentile_60_col = next((col for col in available_columns if col.strip() == "60th"), None)
        if percentile_60_col: columns_mapping['60th'] = percentile_60_col
        
        percentile_70_col = next((col for col in available_columns if col.strip() == "70th"), None)
        if percentile_70_col: columns_mapping['70th'] = percentile_70_col
        
        percentile_75_col = next((col for col in available_columns if col.strip() == "75th"), None)
        if percentile_75_col: columns_mapping['75th'] = percentile_75_col
        
        percentile_80_col = next((col for col in available_columns if col.strip() == "80th"), None)
        if percentile_80_col: columns_mapping['80th'] = percentile_80_col
        
        percentile_85_col = next((col for col in available_columns if col.strip() == "85th"), None)
        if percentile_85_col: columns_mapping['85th'] = percentile_85_col
        
        percentile_90_col = next((col for col in available_columns if col.strip() == "90th"), None)
        if percentile_90_col: columns_mapping['90th'] = percentile_90_col
        
        percentile_95_col = next((col for col in available_columns if col.strip() == "95th"), None)
        if percentile_95_col: columns_mapping['95th'] = percentile_95_col

        logger.info(f"📊 Column mapping: {columns_mapping}")

        # --- Check required columns ---
        required_keys = ['rel_date', 'geozip', 'code', 'full_description']
        # Percentiles are optional - we'll include whichever ones exist
        optional_keys = ['50th', '60th', '70th', '75th', '80th', '85th', '90th', '95th']
        
        missing_keys = [key for key in required_keys if key not in columns_mapping]
        if missing_keys:
            raise ValueError(f"Missing required columns: {missing_keys}. Available columns: {list(available_columns)}")

        # --- Keep and rename columns ---
        # Include all columns that were found (required + available optional)
        columns_to_keep = required_keys + [key for key in optional_keys if key in columns_mapping]
        
        df_cleaned = df[[columns_mapping[key] for key in columns_to_keep]].copy()
        df_cleaned.columns = columns_to_keep

        logger.info(f"📋 Columns included: {columns_to_keep}")

        # --- Clean data ---
        # Drop rows where the 'code' is empty
        df_cleaned = df_cleaned.dropna(subset=["code"])
        df_cleaned = df_cleaned[df_cleaned["code"].astype(str).str.strip().str.len() > 0]

        # --- CRITICAL: Format geozip FIRST before creating data_type ---
        logger.info("🔢 Formatting geozip with leading zeros...")
        df_cleaned['geozip'] = df_cleaned['geozip'].apply(self.format_geozip)
        
        # Log sample geozips after formatting
        sample_geozips = df_cleaned['geozip'].head(10).tolist()
        logger.info(f"📋 Sample formatted geozips: {sample_geozips}")

        # --- Add 'data_type' column based on formatted geozip ---
        logger.info("➕ Adding 'data_type' column based on geozip...")
        
        def create_data_type(geozip):
            """Create data_type string based on geozip value"""
            if pd.isna(geozip) or geozip is None:
                return 'Fair Health Facility'  # Default if no geozip
            
            # Geozip is already formatted with leading zeros
            return f'Facility {geozip}'
        
        df_cleaned['data_type'] = df_cleaned['geozip'].apply(create_data_type)
        
        # Log unique data_types created
        unique_data_types = df_cleaned['data_type'].unique()
        logger.info(f"📋 Unique data_types created: {list(unique_data_types)}")

        # --- CRITICAL: Clean percentile columns (remove $, commas) BEFORE numeric conversion ---
        logger.info("💰 Cleaning currency formatting from percentile columns...")
        
        percentile_columns = ['50th', '60th', '70th', '75th', '80th', '85th', '90th', '95th']
        for col in percentile_columns:
            if col in df_cleaned.columns:
                logger.info(f"    -> Cleaning currency from '{col}'")
                
                # Show sample before cleaning
                sample_before = df_cleaned[col].head(3).tolist()
                logger.info(f"       Sample BEFORE: {sample_before}")
                
                # Apply currency cleaning
                df_cleaned[col] = df_cleaned[col].apply(self._clean_currency_value)
                
                # Show sample after cleaning
                sample_after = df_cleaned[col].head(3).tolist()
                logger.info(f"       Sample AFTER:  {sample_after}")
                
                # Count how many valid numbers we have
                valid_count = df_cleaned[col].notna().sum()
                total_count = len(df_cleaned)
                logger.info(f"       Valid values: {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)")
        
        # --- Convert percentile columns to proper float type ---
        logger.info("🔄 Converting percentile columns to float...")
        
        for col in percentile_columns:
            if col in df_cleaned.columns:
                # Already cleaned above, just ensure proper type
                df_cleaned[col] = df_cleaned[col].astype('float64')
                logger.info(f"    -> '{col}' converted to float64")

        # --- Handle NaN values for non-percentile columns ---
        logger.info("🔄 Converting NaN values to None for text columns...")
        
        text_columns = ['rel_date', 'geozip', 'code', 'full_description', 'data_type']
        for col in text_columns:
            if col in df_cleaned.columns and df_cleaned[col].isnull().any():
                logger.info(f"    -> Fixing NaNs in column '{col}'")
                df_cleaned[col] = df_cleaned[col].astype(object).where(pd.notnull(df_cleaned[col]), None)

        # Drop rows that are completely empty
        df_cleaned = df_cleaned.dropna(how="all")
        df_cleaned.reset_index(drop=True, inplace=True)

        logger.info(f"✅ Cleaned data: {len(df_cleaned)} rows remaining")
        logger.info(f"📋 Sample data (post-fix):\n{df_cleaned.head().to_string()}")
        
        # Show final column order and types
        logger.info(f"📋 Final columns: {list(df_cleaned.columns)}")
        logger.info(f"📋 Column dtypes:\n{df_cleaned.dtypes}")
        
        return df_cleaned