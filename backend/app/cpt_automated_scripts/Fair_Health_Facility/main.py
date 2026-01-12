import logging
from pathlib import Path
# --- Import new FairHealth-specific classes ---
from .scraper import FairHealthScraper
from .data_processor import DataProcessorFairHealth
from .database import SupabaseHandlerFairHealth

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("fairhealth_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_fairhealth_pipeline():
    """
    Complete pipeline for FairHealth data: Scrape -> Clean -> Save
    """
    try:
        # --- STEP 1: DOWNLOAD FILE ---
        logger.info("=" * 50)
        logger.info("STEP 1: DOWNLOADING FAIRHEALTH FILE")
        logger.info("=" * 50)
        
        # Define a download directory for this specific scraper
        download_dir = Path.cwd() / "downloads_fairhealth"
        
        scraper = FairHealthScraper(download_dir=download_dir)
        
        # Set headless=True for production runs, False for debugging
        # This now calls your scraper and gets the *real* file path
        data_file_path = scraper.download_file(headless=True) 
        
        if not data_file_path or not data_file_path.exists():
            raise FileNotFoundError(f"Scraper failed to download or find the file. Looked in: {download_dir}")
            
        logger.info(f"✅ Scraper downloaded file to: {data_file_path}")
        # --- End of Step 1 ---

        # Step 2: Clean Data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: CLEANING FAIRHEALTH DATA")
        logger.info("=" * 50)
        processor = DataProcessorFairHealth()
        
        # Use the read_csv method with the *correct* file path
        df_raw = processor.read_excel(data_file_path)
        df_cleaned = processor.clean_data(df_raw)
        
        records = df_cleaned.to_dict('records')
        logger.info(f"📊 Prepared {len(records)} records for database")
        
        # Step 3: Save to Supabase
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: SAVING FAIRHEALTH DATA TO SUPABASE")
        logger.info("=" * 50)
        db = SupabaseHandlerFairHealth()
        result = db.insert_records(records)
        
        # Final Summary
        logger.info("\n" + "=" * 50)
        logger.info("✅ FAIRHEALTH PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"Processed: {data_file_path.name}")
        logger.info(f"Records processed: {len(records)}")
        logger.info(f"Records inserted: {result.get('records_inserted', 0)}")
        logger.info(f"Table: {result.get('table', 'N/A')}")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ FAIRHEALTH PIPELINE FAILED: {str(e)}")
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    run_fairhealth_pipeline()