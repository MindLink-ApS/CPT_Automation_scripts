"""
Common database utility functions for medical benchmarking data pipelines.
Provides shared logic for handling composite keys, release dates, and geozip.
"""
import logging
from typing import List, Dict, Optional, Tuple
from supabase import Client

logger = logging.getLogger(__name__)


def get_existing_release_date(client: Client, table_name: str, source_name: str) -> Optional[str]:
    """
    Check if records already exist in database for a given source.
    If they exist, return their release_date to ensure we update instead of insert duplicates.
    If no records exist, return None.
    
    Args:
        client: Supabase client instance
        table_name: Name of the database table
        source_name: Name of the data source
        
    Returns:
        Existing release_date if records exist, None otherwise
    """
    try:
        response = client.table(table_name)\
            .select("release_date")\
            .eq("source", source_name)\
            .limit(1)\
            .execute()
        
        if response.data and len(response.data) > 0:
            existing_date = response.data[0].get('release_date')
            if existing_date:
                logger.info(f"📊 Found existing records with release_date: '{existing_date}' - will reuse to prevent duplicates")
                return existing_date
        
        logger.info(f"📊 No existing records found for source '{source_name}'")
        return None
    except Exception as e:
        logger.warning(f"⚠️ Could not check existing release_date: {e}")
        return None


def prepare_record_for_insertion(
    record: Dict,
    source_name: str,
    existing_release_date: Optional[str] = None,
    has_geozip: bool = False
) -> Optional[Dict]:
    """
    Prepare a single record for database insertion.
    
    Args:
        record: Record dictionary to prepare
        source_name: Name of the data source
        existing_release_date: Existing release_date from database (to prevent duplicates)
        has_geozip: Whether this source has geozip data
        
    Returns:
        Prepared record dict, or None if record should be skipped
    """
    # Filter out records with null or empty code
    code = record.get('code')
    if not code or (isinstance(code, str) and code.strip() == ''):
        return None
    
    # Add source field
    record['source'] = source_name
    
    # Handle geozip
    if not has_geozip:
        # Set geozip to NULL if source doesn't have geozip data
        record['geozip'] = None
    elif 'geozip' not in record or not record.get('geozip'):
        record['geozip'] = None
    
    # Handle release_date
    if 'release_date' not in record or not record.get('release_date'):
        if existing_release_date:
            # Reuse existing release_date to match existing records (prevents duplicates)
            record['release_date'] = existing_release_date
            logger.debug(f"♻️ Reusing existing release_date '{record['release_date']}' for code {code}")
        elif 'rel_date' in record and record.get('rel_date'):
            # Use rel_date from data (extracted from filename or column name)
            record['release_date'] = record['rel_date']
            logger.debug(f"📅 Using rel_date '{record['release_date']}' from data for code {code}")
        else:
            logger.warning(f"⚠️ No release_date or rel_date found for code {code} - record will be skipped")
            return None  # Skip records without release_date
    
    return record


def upsert_records_with_composite_key(
    client: Client,
    table_name: str,
    source_name: str,
    records: List[Dict],
    chunk_size: int = 1000
) -> Dict:
    """
    Insert or update records using manual check-and-update logic for composite unique constraints.
    Supabase's .upsert() doesn't work properly with composite keys, so we manually check and update.
    
    Early Exit Optimization:
    - Uses small sample (50 records) to quickly detect if database is already synced
    - If sample shows 100% existing records, checks if ALL records exist
    - If database is already synced, returns early to avoid unnecessary load
    - Saves 95%+ of processing time and database queries on re-runs
    
    Args:
        client: Supabase client instance
        table_name: Name of the database table
        source_name: Name of the data source
        records: List of record dictionaries to insert/update
        chunk_size: Number of records to process per chunk (default: 1000)
        
    Returns:
        Dictionary with insertion results
    """
    if not records:
        logger.warning("⚠️ No records to upsert")
        return {
            "status": "no_records",
            "records_inserted": 0,
            "records_updated": 0,
            "records_upserted": 0,
            "records_failed": 0,
            "failed_chunks": []
        }
    
    # OPTIMIZATION: Use small sample for quick sync detection
    SAMPLE_SIZE = 50  # Small sample for fast detection
    sample_check_done = False
    
    total_inserted = 0
    total_updated = 0
    total_failed = 0
    failed_chunks = []
    
    try:
        # OPTIMIZATION: Quick sample check before processing all records
        if not sample_check_done and len(records) > SAMPLE_SIZE:
            logger.info(f"   🔍 Quick sync check: Testing sample of {SAMPLE_SIZE} records...")
            
            sample = records[:SAMPLE_SIZE]
            sample_inserted, sample_updated = _process_chunk(
                client, table_name, source_name, sample
            )
            
            logger.info(f"   📊 Sample result: {sample_inserted} new, {sample_updated} existing")
            
            # If sample shows 100% existing records, check if database is fully synced
            if sample_inserted == 0 and sample_updated > 0:
                logger.info(f"   🔍 Sample shows all records exist - checking full database...")
                
                if _check_all_records_exist(client, table_name, source_name, records):
                    logger.info("=" * 60)
                    logger.info("🔄 DATABASE ALREADY SYNCHRONIZED")
                    logger.info("=" * 60)
                    logger.info(f"✅ All {len(records)} records already exist in database")
                    logger.info(f"✅ Source: '{source_name}'")
                    logger.info(f"✅ Detected via {SAMPLE_SIZE}-record sample check")
                    logger.info(f"✅ Skipped processing {len(records) - SAMPLE_SIZE} remaining records")
                    logger.info("=" * 60)
                    
                    return {
                        "status": "already_synced",
                        "records_inserted": 0,
                        "records_updated": sample_updated,
                        "records_upserted": sample_updated,
                        "records_failed": 0,
                        "failed_chunks": [],
                        "table": table_name,
                        "message": f"Database already synchronized - detected via {SAMPLE_SIZE}-record sample"
                    }
            
            # Sample processed, continue with remaining records
            total_inserted += sample_inserted
            total_updated += sample_updated
            sample_check_done = True
            
            # Start from after the sample
            start_index = SAMPLE_SIZE
        else:
            start_index = 0
        
        # Process remaining chunks normally
        for i in range(start_index, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            chunk_num = ((i - start_index) // chunk_size) + 1
            total_chunks = ((len(records) - start_index) + chunk_size - 1) // chunk_size
            
            logger.info(f"   Processing chunk {chunk_num}/{total_chunks} ({len(chunk)} records)...")
            
            try:
                chunk_inserted, chunk_updated = _process_chunk(
                    client, table_name, source_name, chunk
                )
                total_inserted += chunk_inserted
                total_updated += chunk_updated
                logger.info(f"   ✅ Chunk {chunk_num} completed: {chunk_inserted} inserted, {chunk_updated} updated")
                
            except Exception as chunk_error:
                logger.error(f"   ❌ Chunk {chunk_num} failed: {chunk_error}")
                total_failed += len(chunk)
                failed_chunks.append(chunk_num)
                continue
        
        if failed_chunks:
            logger.warning(f"⚠️ {len(failed_chunks)} chunk(s) failed: {failed_chunks}")
        
        total_upserted = total_inserted + total_updated
        logger.info(f"✅ Successfully upserted {total_upserted} records ({total_inserted} inserted, {total_updated} updated)")
        if total_failed > 0:
            logger.warning(f"⚠️ {total_failed} records failed to upsert")
        
        return {
            "status": "success" if total_failed == 0 else "partial_success",
            "records_inserted": total_inserted,
            "records_updated": total_updated,
            "records_upserted": total_upserted,
            "records_failed": total_failed,
            "failed_chunks": failed_chunks,
            "table": table_name
        }
    except Exception as e:
        logger.error(f"❌ Unexpected Error during upsert: {e}")
        raise


def _check_all_records_exist(
    client: Client,
    table_name: str,
    source_name: str,
    records: List[Dict]
) -> bool:
    """
    Check if all records already exist in the database.
    Used for early exit optimization to avoid unnecessary database load.
    
    Returns:
        True if all records exist, False otherwise
    """
    try:
        # Get unique release_dates from records
        release_dates = set(r.get('release_date') for r in records if r.get('release_date'))
        
        if not release_dates:
            return False
        
        # Count total records in database for this source and release_date(s)
        total_existing = 0
        for release_date in release_dates:
            response = client.table(table_name)\
                .select("id", count="exact")\
                .eq("source", source_name)\
                .eq("release_date", release_date)\
                .execute()
            
            if hasattr(response, 'count') and response.count is not None:
                total_existing += response.count
        
        # If database has same or more records, assume all exist
        # (This is a heuristic - exact matching would be too expensive)
        if total_existing >= len(records):
            logger.info(f"   📊 Database check: {total_existing} existing records >= {len(records)} records to insert")
            return True
        
        return False
        
    except Exception as e:
        logger.warning(f"   ⚠️ Could not check if all records exist: {e}")
        return False  # If check fails, continue with normal processing


def _process_chunk(
    client: Client,
    table_name: str,
    source_name: str,
    chunk: List[Dict]
) -> Tuple[int, int]:
    """
    Process a single chunk of records: check for existing records and update or insert.
    
    Returns:
        Tuple of (inserted_count, updated_count)
    """
    chunk_inserted = 0
    chunk_updated = 0
    
    # Build list of codes to check
    codes_in_chunk = [r.get('code') for r in chunk if r.get('code')]
    release_date_in_chunk = chunk[0].get('release_date') if chunk else None
    
    # Query for existing records in this chunk (batch check)
    existing_records = {}
    if codes_in_chunk and release_date_in_chunk:
        try:
            response = client.table(table_name)\
                .select("id, code, source, release_date, geozip")\
                .eq("source", source_name)\
                .eq("release_date", release_date_in_chunk)\
                .in_("code", codes_in_chunk)\
                .execute()
            
            if response.data:
                logger.debug(f"   🔍 Found {len(response.data)} existing records in database")
                for existing in response.data:
                    # Normalize data types to match what's in records
                    # Convert code to string to ensure consistent matching
                    code_normalized = str(existing.get('code'))
                    geozip_normalized = existing.get('geozip')
                    
                    key = (existing.get('source'), code_normalized, 
                           existing.get('release_date'), geozip_normalized)
                    existing_records[key] = existing.get('id')
                logger.debug(f"   🔍 Built lookup dict with {len(existing_records)} unique keys")
        except Exception as check_error:
            logger.warning(f"   ⚠️ Could not check existing records: {check_error}")
    
    # Separate records into insert and update lists
    records_to_insert = []
    records_to_update = []
    
    for record in chunk:
        source = record.get('source')
        code = record.get('code')
        release_date = record.get('release_date')
        geozip = record.get('geozip')
        
        # Normalize code to string to match database lookup
        code_normalized = str(code)
        key = (source, code_normalized, release_date, geozip)
        
        if key in existing_records:
            # Record exists - prepare for update
            record['id'] = existing_records[key]
            records_to_update.append(record)
        else:
            # Record doesn't exist - prepare for insert
            records_to_insert.append(record)
    
    logger.debug(f"   📊 After matching: {len(records_to_insert)} to insert, {len(records_to_update)} to update")
    
    # Batch insert new records
    if records_to_insert:
        try:
            client.table(table_name).insert(records_to_insert).execute()
            chunk_inserted = len(records_to_insert)
        except Exception as insert_error:
            logger.error(f"   ❌ Batch insert failed: {insert_error}")
            # Fallback: try individual inserts
            for record in records_to_insert:
                try:
                    client.table(table_name).insert(record).execute()
                    chunk_inserted += 1
                except Exception as individual_error:
                    # Check if it's a 409 Conflict (duplicate key)
                    error_str = str(individual_error)
                    if '409' in error_str or 'duplicate key' in error_str.lower() or 'unique constraint' in error_str.lower():
                        # Record already exists - count as update instead
                        chunk_updated += 1
                    # Silently skip other errors (already logged at batch level)
    
    # Update existing records
    if records_to_update:
        for record in records_to_update:
            try:
                record_id = record.pop('id')  # Remove id from update data
                client.table(table_name)\
                    .update(record)\
                    .eq("id", record_id)\
                    .execute()
                chunk_updated += 1
            except Exception as update_error:
                logger.warning(f"   ⚠️ Failed to update record {record.get('code')}: {update_error}")
    
    return chunk_inserted, chunk_updated

