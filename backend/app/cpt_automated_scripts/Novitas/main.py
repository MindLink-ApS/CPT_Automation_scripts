import logging
import math
from pathlib import Path
from .scraper import NovitasScraper
from .data_processor import DataProcessor
from .database import SupabaseHandler

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def clean_nan_values(records: list) -> list:
    """
    Convert any NaN values to None for JSON compliance.
    This is a safety net in case pandas NaN slips through.
    """
    cleaned_records = []
    for record in records:
        cleaned_record = {}
        for key, value in record.items():
            # Check for NaN values (works for both numpy and math nan)
            if isinstance(value, float) and math.isnan(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value
        cleaned_records.append(cleaned_record)
    return cleaned_records

def run_pipeline(headless=False, skip_download=False, file_path=None):
    """
    Complete pipeline: Scrape -> Clean -> Save
    
    Args:
        headless: Run browser in headless mode
        skip_download: Skip download step and use existing file
        file_path: Path to existing file (used when skip_download=True)
    """
    try:
        # Step 1: Scrape/Download
        if not skip_download:
            logger.info("=" * 50)
            logger.info("STEP 1: DOWNLOADING FILE")
            logger.info("=" * 50)
            scraper = NovitasScraper()
            file_path = scraper.download_excel_file(headless=headless)
        else:
            if not file_path:
                raise ValueError("file_path must be provided when skip_download=True")
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")
            logger.info(f"📂 Using existing file: {file_path}")
        
        # Step 2: Clean Data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: CLEANING DATA")
        logger.info("=" * 50)
        processor = DataProcessor()
        
        # Read the Excel file into a raw DataFrame
        df_raw = processor.read_excel(file_path)
        
        # Clean and transform the raw DataFrame (pass file_path for date extraction)
        df_cleaned = processor.clean_data(df_raw, file_path=file_path)
        
        # Validate cleaned data
        processor.validate_cleaned_data(df_cleaned)
        
        # Convert DataFrame to list of dictionaries for Supabase
        records = df_cleaned.to_dict('records')
        
        # CRITICAL: Clean any remaining NaN values
        logger.info(f"🧹 Cleaning NaN values from {len(records)} records...")
        records = clean_nan_values(records)
        
        logger.info(f"✅ Prepared {len(records)} records for database")
        
        # Log sample record for verification
        if records:
            logger.info(f"📋 Sample record: {records[0]}")
        
        # Step 3: Save to Supabase
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: SAVING TO SUPABASE")
        logger.info("=" * 50)
        db = SupabaseHandler()
        result = db.insert_records(records)
        
        # Final Summary
        logger.info("\n" + "=" * 50)
        logger.info("✅ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"📄 Downloaded: {file_path.name}")
        logger.info(f"📊 Records processed: {len(records)}")
        logger.info(f"💾 Records inserted: {result['records_inserted']}")
        logger.info(f"🗄️ Table: {result['table']}")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    # Run with headless=True for production, False to watch the browser
    run_pipeline(headless=True)
    
    # To use an existing file instead of downloading:
    # run_pipeline(skip_download=True, file_path="January 2025 Medicare Part B Fee Schedule - New Jersey Locality 01.xlsx")