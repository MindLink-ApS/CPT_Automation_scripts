import logging
from pathlib import Path
from .scraper import FairHealthPhysicianScraper
from .data_processor import DataProcessorPhysician
from .database import SupabaseHandlerPhysician
from .config import (
    GEOZIP_BATCHES, 
    FAIRHEALTH_URL, 
    EMAIL, 
    PASSWORD,
    PROXY_SERVER,
    PROXY_USERNAME,
    PROXY_PASSWORD,
    PRODUCT_CATEGORY,
    PRODUCT_NAME,
    DOWNLOAD_DIR_NAME,
    HEADLESS
)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("physician_pipeline.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def run_physician_pipeline(skip_existing=False):
    """
    Complete pipeline for Fair Health Physician data with batch processing:
    For each geozip batch: Scrape -> Clean -> Save
    
    Args:
        skip_existing: If True, skip batches that already have data in database
    """
    
    all_results = []
    total_records = 0
    
    try:
        logger.info("=" * 70)
        logger.info("FAIR HEALTH PHYSICIAN PIPELINE - BATCH PROCESSING")
        logger.info("=" * 70)
        logger.info(f"📦 Total batches to process: {len(GEOZIP_BATCHES)}")
        logger.info(f"📍 Batches: {GEOZIP_BATCHES}")
        logger.info("=" * 70)
        
        # Initialize scraper once
        scraper = FairHealthPhysicianScraper(
            fairhealth_url=FAIRHEALTH_URL,
            email=EMAIL,
            password=PASSWORD,
            proxy_server=PROXY_SERVER,
            proxy_username=PROXY_USERNAME,
            proxy_password=PROXY_PASSWORD,
            download_dir=Path.cwd() / DOWNLOAD_DIR_NAME
        )
        
        # Initialize processor and database handler
        processor = DataProcessorPhysician()
        db = SupabaseHandlerPhysician()
        
        # Process each batch
        for batch_num, geozips in enumerate(GEOZIP_BATCHES, 1):
            logger.info("\n" + "=" * 70)
            logger.info(f"BATCH {batch_num}/{len(GEOZIP_BATCHES)}: Processing Geozips {geozips}")
            logger.info("=" * 70)
            
            # Check if we should skip this batch
            if skip_existing:
                try:
                    # Check if data already exists for this geozip
                    geozip_str = geozips[0] if geozips else None
                    existing_count = db.client.table(db.TABLE_NAME).select("id", count="exact").eq("source", db.SOURCE_NAME).eq("geozip", geozip_str).execute()
                    
                    if existing_count.count and existing_count.count > 0:
                        logger.info(f"⏭️  Skipping batch {batch_num} - {existing_count.count} records already exist for geozip {geozip_str}")
                        batch_result = {
                            "batch_num": batch_num,
                            "geozips": geozips,
                            "skipped": True,
                            "existing_records": existing_count.count,
                            "success": True
                        }
                        all_results.append(batch_result)
                        continue
                except Exception as check_error:
                    logger.warning(f"⚠️ Could not check existing data, proceeding anyway: {check_error}")
            
            try:
                # Step 1: Scrape/Download for this batch
                logger.info(f"\n🔽 STEP 1: DOWNLOADING DATA FOR BATCH {batch_num}")
                logger.info("-" * 70)
                
                batch_name = f"batch{batch_num}_{'_'.join(geozips)}"
                data_file_path = scraper.download_file(
                    geozips=geozips,
                    product_category=PRODUCT_CATEGORY,
                    product_name=PRODUCT_NAME,
                    headless=HEADLESS,
                    batch_name=batch_name
                )
                
                logger.info(f"✅ Downloaded: {data_file_path.name}")
                
                # Step 2: Clean Data
                logger.info(f"\n🧹 STEP 2: CLEANING DATA FOR BATCH {batch_num}")
                logger.info("-" * 70)
                
                # Get the geozip that was searched for (first one in the list)
                searched_geozip = geozips[0] if geozips else None
                logger.info(f"📍 Using geozip from search: {searched_geozip}")
                
                df_raw = processor.read_excel(data_file_path)
                # Pass the searched geozip to override geozip from file
                df_cleaned = processor.clean_data(df_raw, expected_geozip=searched_geozip)
                
                # Convert DataFrame to list of dictionaries for Supabase
                records = df_cleaned.to_dict('records')
                logger.info(f"📊 Prepared {len(records)} records for database")
                
                # Step 3: Save to Supabase
                logger.info(f"\n💾 STEP 3: SAVING DATA TO SUPABASE FOR BATCH {batch_num}")
                logger.info("-" * 70)
                
                result = db.insert_records(records)
                
                # Track results
                batch_result = {
                    "batch_num": batch_num,
                    "geozips": geozips,
                    "file": data_file_path.name,
                    "records_processed": len(records),
                    "records_inserted": result.get('records_inserted', 0),
                    "success": True
                }
                all_results.append(batch_result)
                total_records += result.get('records_inserted', 0)
                
                logger.info(f"✅ Batch {batch_num} completed successfully")
                logger.info(f"   📝 Records inserted: {result.get('records_inserted', 0)}")
                
            except Exception as batch_error:
                logger.error(f"❌ Batch {batch_num} failed: {str(batch_error)}")
                logger.exception("Full traceback:")
                
                batch_result = {
                    "batch_num": batch_num,
                    "geozips": geozips,
                    "error": str(batch_error),
                    "success": False
                }
                all_results.append(batch_result)
                
                # Continue with next batch instead of stopping
                logger.warning(f"⚠️ Continuing with remaining batches...")
                continue
        
        # Final Summary
        logger.info("\n" + "=" * 70)
        logger.info("📊 PIPELINE SUMMARY")
        logger.info("=" * 70)
        
        successful_batches = sum(1 for r in all_results if r.get('success', False))
        failed_batches = len(all_results) - successful_batches
        
        logger.info(f"Total batches processed: {len(all_results)}")
        logger.info(f"✅ Successful: {successful_batches}")
        logger.info(f"❌ Failed: {failed_batches}")
        logger.info(f"📊 Total records inserted: {total_records}")
        
        logger.info("\nBatch Details:")
        for result in all_results:
            if result.get('success'):
                logger.info(f"  ✅ Batch {result['batch_num']} ({result['geozips']}): "
                          f"{result['records_inserted']} records")
            else:
                logger.info(f"  ❌ Batch {result['batch_num']} ({result['geozips']}): "
                          f"FAILED - {result.get('error', 'Unknown error')}")
        
        logger.info("=" * 70)
        
        if failed_batches > 0:
            logger.warning(f"⚠️ PIPELINE COMPLETED WITH {failed_batches} FAILED BATCH(ES)")
        else:
            logger.info("✅ ALL BATCHES COMPLETED SUCCESSFULLY")
        
        logger.info("=" * 70)
        
        return {
            "total_batches": len(all_results),
            "successful_batches": successful_batches,
            "failed_batches": failed_batches,
            "total_records_inserted": total_records,
            "batch_results": all_results
        }
        
    except Exception as e:
        logger.error(f"\n❌ PIPELINE FAILED: {str(e)}")
        logger.exception("Full traceback:")
        raise

if __name__ == "__main__":
    run_physician_pipeline()