import pandas as pd
import logging
import re
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

class DataProcessor:
    """Process and clean NJ Medical PIP data from Excel file"""
    
    def __init__(self):
        self.required_columns = ['FAC IND', 'PROC CODE', 'MODIFIER', 'PAR FEE']
        
    def extract_date_from_filename(self, file_path: Path) -> str:
        """
        Extract date from filename like 'January 2025 Medicare Part B Fee Schedule...'
        Returns date in format 'January 2025' or 'February 2025'
        """
        filename = file_path.stem  # Get filename without extension
        logger.info(f"📅 Extracting date from filename: {filename}")
        
        # Pattern to match "Month Year" at the beginning of filename
        # Matches patterns like "January 2025", "February 2025", etc.
        pattern = r'^([A-Z][a-z]+\s+\d{4})'
        
        match = re.match(pattern, filename)
        if match:
            date_str = match.group(1)
            logger.info(f"✅ Extracted date: {date_str}")
            return date_str
        else:
            # Fallback: try to find any "Month Year" pattern in filename
            pattern_flexible = r'([A-Z][a-z]+)\s+(\d{4})'
            match = re.search(pattern_flexible, filename)
            if match:
                date_str = f"{match.group(1)} {match.group(2)}"
                logger.info(f"✅ Extracted date (flexible): {date_str}")
                return date_str
            
            logger.warning(f"⚠️ Could not extract date from filename: {filename}")
            raise ValueError(f"Could not extract date from filename: {filename}")
    
    def read_excel(self, file_path: Path) -> pd.DataFrame:
        """
        Read Excel file and return raw DataFrame
        """
        logger.info(f"📖 Reading Excel file: {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Excel file not found: {file_path}")
        
        try:
            # Read Excel file - it might have headers in first row
            df = pd.read_excel(file_path)
            logger.info(f"✅ Successfully read Excel file with {len(df)} rows")
            logger.info(f"📋 Columns found: {df.columns.tolist()}")
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Error reading Excel file: {e}")
            raise
    
    def clean_data(self, df: pd.DataFrame, file_path: Optional[Path] = None) -> pd.DataFrame:
        """
        Clean and transform the DataFrame according to requirements:
        1. Keep only needed columns: PROC CODE, PAR FEE, FAC IND, MODIFIER
        2. Rename: PROC CODE -> code, PAR FEE -> 80th
        3. Add rel_date column with extracted date from filename
        4. Add data_type column based on FAC IND value
        5. Filter out records where MODIFIER is not empty
        """
        logger.info(f"🧹 Starting data cleaning process...")
        logger.info(f"📊 Input DataFrame shape: {df.shape}")
        
        # Create a copy to avoid modifying original
        df_clean = df.copy()
        
        # Check if required columns exist
        missing_cols = [col for col in self.required_columns if col not in df_clean.columns]
        if missing_cols:
            logger.error(f"❌ Missing required columns: {missing_cols}")
            logger.info(f"Available columns: {df_clean.columns.tolist()}")
            raise ValueError(f"Missing required columns: {missing_cols}")
        
        # Step 1: Filter out records where MODIFIER is not empty/null
        logger.info("🔍 Filtering records where MODIFIER is empty...")
        initial_count = len(df_clean)
        
        # Keep only rows where MODIFIER is NaN, None, or empty string
        df_clean = df_clean[
            df_clean['MODIFIER'].isna() | 
            (df_clean['MODIFIER'] == '') | 
            (df_clean['MODIFIER'].astype(str).str.strip() == '')
        ]
        
        filtered_count = initial_count - len(df_clean)
        logger.info(f"✅ Filtered out {filtered_count} records with non-empty MODIFIER")
        logger.info(f"📊 Remaining records: {len(df_clean)}")
        
        # Step 2: Create data_type column based on FAC IND
        logger.info("🏥 Creating data_type column based on FAC IND...")
        
        def determine_data_type(fac_ind):
            """Determine data_type based on FAC IND value"""
            if pd.isna(fac_ind) or str(fac_ind).strip() == '':
                return 'OBL'
            elif '#' in str(fac_ind):
                return 'Medicare Professional'
            else:
                return 'OBL'  # Default to OBL if no # found
        
        df_clean['data_type'] = df_clean['FAC IND'].apply(determine_data_type)
        
        # Log distribution of data_type
        data_type_counts = df_clean['data_type'].value_counts()
        logger.info(f"📈 data_type distribution:\n{data_type_counts}")
        
        # Step 3: Add rel_date column
        if file_path:
            rel_date = self.extract_date_from_filename(file_path)
            df_clean['rel_date'] = rel_date
            logger.info(f"📅 Added rel_date column: {rel_date}")
        else:
            logger.warning("⚠️ No file_path provided, rel_date will be empty")
            df_clean['rel_date'] = None
        
        # Step 4: Select and rename columns
        logger.info("🔄 Selecting and renaming columns...")
        
        df_clean = df_clean[['PROC CODE', 'PAR FEE', 'data_type', 'rel_date']]
        
        # Rename columns
        df_clean = df_clean.rename(columns={
            'PROC CODE': 'code',
            'PAR FEE': '80th'
        })
        
        # Step 5: Clean up data types and handle NaN values
        logger.info("🧼 Cleaning data types and handling NaN values...")
        
        # Convert code to string and strip whitespace
        df_clean['code'] = df_clean['code'].astype(str).str.strip()
        
        # Ensure 80th is numeric (float), convert to None if can't be converted
        df_clean['80th'] = pd.to_numeric(df_clean['80th'], errors='coerce')
        
        # Replace NaN with None for JSON serialization
        df_clean = df_clean.where(pd.notna(df_clean), None)
        
        # Remove any rows where code is empty or 'nan'
        df_clean = df_clean[
            (df_clean['code'] != '') & 
            (df_clean['code'] != 'nan') & 
            (df_clean['code'].notna())
        ]
        
        logger.info(f"✅ Data cleaning completed!")
        logger.info(f"📊 Final DataFrame shape: {df_clean.shape}")
        logger.info(f"📋 Final columns: {df_clean.columns.tolist()}")
        logger.info(f"🔢 Sample records:\n{df_clean.head()}")
        
        return df_clean
    
    def validate_cleaned_data(self, df: pd.DataFrame) -> bool:
        """
        Validate cleaned DataFrame meets requirements
        """
        logger.info("✔️ Validating cleaned data...")
        
        required_final_columns = ['code', '80th', 'data_type', 'rel_date']
        
        # Check columns
        if not all(col in df.columns for col in required_final_columns):
            logger.error(f"❌ Missing required columns in final DataFrame")
            return False
        
        # Check for empty DataFrame
        if len(df) == 0:
            logger.warning("⚠️ DataFrame is empty after cleaning")
            return False
        
        # Check for null codes
        if df['code'].isna().any():
            logger.warning(f"⚠️ Found {df['code'].isna().sum()} null codes")
        
        # Check data_type values
        valid_data_types = {'Medicare Professional', 'OBL'}
        invalid_types = set(df['data_type'].unique()) - valid_data_types
        if invalid_types:
            logger.warning(f"⚠️ Found unexpected data_type values: {invalid_types}")
        
        logger.info("✅ Data validation completed")
        return True


class NJMedicalScraper:
    """Wrapper class for the scraper functionality"""
    
    def __init__(self, output_dir=None):
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def download_excel_file(self, headless=False) -> Path:
        """
        Download the Excel file using the scraper module
        Returns Path to downloaded file
        """
        logger.info("🌐 Starting file download...")
        
        # Import the scraper function
        from scraper import download_novitas_fee_schedule, PROXY_SERVER, PROXY_USERNAME, PROXY_PASSWORD
        
        try:
            file_path = download_novitas_fee_schedule(
                output_dir=str(self.output_dir),
                headless=headless,
                proxy_server=PROXY_SERVER,
                proxy_user=PROXY_USERNAME,
                proxy_pass=PROXY_PASSWORD,
            )
            
            logger.info(f"✅ File downloaded successfully: {file_path}")
            return Path(file_path)
            
        except Exception as e:
            logger.error(f"❌ Error downloading file: {e}")
            raise