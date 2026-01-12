import logging
from pathlib import Path
from .scraper import HorizonASCScraper
from .data_processor import DataProcessorHorizonASC
from .database import SupabaseHandlerHorizonASC

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("horizon_asc_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_horizon_asc_pipeline():
    """
    Complete pipeline for Horizon ASC data: Scrape -> Clean -> Save
    """
    try:
        # Step 1: Download
        logger.info("=" * 50)
        logger.info("STEP 1: DOWNLOADING HORIZON ASC FEE SCHEDULE")
        logger.info("=" * 50)
        
        download_dir = Path.cwd() / "downloads_horizon_asc"
        scraper = HorizonASCScraper(download_dir=download_dir)
        
        # Set headless=True for production runs, False for debugging
        data_file_path = scraper.download_file(headless=True)
        
        if not data_file_path or not data_file_path.exists():
            raise FileNotFoundError(f"Scraper failed to download file. Looked in: {download_dir}")
            
        logger.info(f"✅ Scraper downloaded file to: {data_file_path}")

        # Step 2: Clean Data
        logger.info("\n" + "=" * 50)
        logger.info("STEP 2: CLEANING HORIZON ASC DATA")
        logger.info("=" * 50)
        processor = DataProcessorHorizonASC()
        
        # Read and clean the Excel file
        df_raw = processor.read_excel(data_file_path)
        df_cleaned = processor.clean_data(df_raw)
        
        records = df_cleaned.to_dict('records')
        logger.info(f"📊 Prepared {len(records)} records for database")
        
        # Step 3: Save to Supabase
        logger.info("\n" + "=" * 50)
        logger.info("STEP 3: SAVING HORIZON ASC DATA TO SUPABASE")
        logger.info("=" * 50)
        db = SupabaseHandlerHorizonASC()
        result = db.insert_records(records)
        
        # Final Summary
        logger.info("\n" + "=" * 50)
        logger.info("HORIZON ASC PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 50)
        logger.info(f"Downloaded: {data_file_path.name}")
        logger.info(f"Records processed: {len(records)}")
        logger.info(f"Records inserted: {result.get('records_inserted', 0)}")
        logger.info(f"Table: {result.get('table', 'N/A')}")
        
        return result
        
    except Exception as e:
        logger.error(f"\n❌ HORIZON ASC PIPELINE FAILED: {str(e)}")
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    run_horizon_asc_pipeline()
