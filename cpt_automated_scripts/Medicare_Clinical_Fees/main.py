import logging
import asyncio
from pathlib import Path
from scraper import CLFSDownloader
from data_processor import DataProcessorCLFS
from database import SupabaseHandlerCLFS

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("clfs_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

async def run_clfs_pipeline():
    """
    Complete pipeline for CLFS data: Scrape → Clean → Save
    """
    try:
        # ====================================================================
        # STEP 1: DOWNLOAD & EXTRACT CLFS FILE
        # ====================================================================
        logger.info("=" * 70)
        logger.info("STEP 1: DOWNLOADING CLFS FILE")
        logger.info("=" * 70)
        
        downloader = CLFSDownloader()
        xlsx_path = await downloader.run()
        
        if not xlsx_path or not xlsx_path.exists():
            raise Exception("Failed to download and extract CLFS file")
        
        logger.info(f" File downloaded successfully: {xlsx_path}")

        # ====================================================================
        # STEP 2: CLEAN DATA
        # ====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: CLEANING CLFS DATA")
        logger.info("=" * 70)
        
        processor = DataProcessorCLFS()
        
        # Read the Excel file
        df_raw = processor.read_excel(xlsx_path)
        logger.info(f" Raw data loaded: {len(df_raw)} rows")
        
        # Clean the data
        df_cleaned = processor.clean_data(df_raw)
        logger.info(f" Data cleaned: {len(df_cleaned)} rows")
        
        # Convert DataFrame to list of dictionaries for Supabase
        records = df_cleaned.to_dict('records')
        logger.info(f" Prepared {len(records)} records for database insertion")

        # ====================================================================
        # STEP 3: SAVE TO SUPABASE
        # ====================================================================
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: SAVING CLFS DATA TO SUPABASE")
        logger.info("=" * 70)
        
        db = SupabaseHandlerCLFS()
        result = db.insert_records(records)
        
        # ====================================================================
        # FINAL SUMMARY
        # ====================================================================
        logger.info("\n" + "=" * 70)
        logger.info(" CLFS PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 70)
        logger.info(f" Source file: {xlsx_path.name}")
        logger.info(f" Records processed: {len(records)}")
        logger.info(f" Records inserted: {result.get('records_inserted', 0)}")
        logger.info(f"  Table: {result.get('table', 'N/A')}")
        logger.info("=" * 70)
        
        return result
        
    except Exception as e:
        logger.error(f"\n CLFS PIPELINE FAILED: {str(e)}")
        logger.exception("Full traceback:")
        raise

def main():
    """Entry point that handles async execution"""
    try:
        result = asyncio.run(run_clfs_pipeline())
        return result
    except KeyboardInterrupt:
        logger.info("\n Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"\n Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()