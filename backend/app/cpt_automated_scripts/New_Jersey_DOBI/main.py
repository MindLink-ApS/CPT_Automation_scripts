import logging
import math
from pathlib import Path
from .scraper import NJMedicalScraper
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

def run_pipeline():
    """
    Complete pipeline: Scrape -> Clean -> Save
    """
    try:
        # Step 1: Scrape/Download
        logger.info("=" * 50)
        logger.info("STEP 1: DOWNLOADING FILE")
        logger.info("=" * 50)
        scraper = NJMedicalScraper()
        file_path = scraper.download_excel_file()
        
        print("cleaning data")

        # Step 2: Clean Data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: CLEANING DATA")
        logger.info("=" * 50)
        processor = DataProcessor()
        
        # Read the Excel file into a raw DataFrame
        df_raw = processor.read_excel(file_path)
        
        # Clean and transform the raw DataFrame
        df_cleaned = processor.clean_data(df_raw)
        
        # Convert DataFrame to list of dictionaries for Supabase
        records = df_cleaned.to_dict('records')
        
        #  CRITICAL: Clean any remaining NaN values
        logger.info(f" Cleaning NaN values from {len(records)} records...")
        records = clean_nan_values(records)
        
        logger.info(f" Prepared {len(records)} records for database")
        
        print("saving to supabase")

        # Step 3: Save to Supabase
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: SAVING TO SUPABASE")
        logger.info("=" * 50)
        db = SupabaseHandler()
        result = db.insert_records(records)
        
        # Final Summary
        logger.info("\n" + "=" * 50)
        logger.info(" PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"Downloaded: {file_path.name}")
        logger.info(f"Records processed: {len(records)}")
        logger.info(f"Records inserted: {result['records_inserted']}")
        logger.info(f"Table: {result['table']}")

        print("done")    
        
        return result
    
    except Exception as e:
        logger.error(f"\n PIPELINE FAILED: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    run_pipeline()