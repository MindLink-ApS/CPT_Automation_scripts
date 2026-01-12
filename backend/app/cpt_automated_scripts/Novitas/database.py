from supabase import create_client, Client
from typing import List, Dict
import logging
import os
import sys
from pathlib import Path
import dotenv

# Add parent directory to path to import common utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from database_utils import (
    get_existing_release_date,
    prepare_record_for_insertion,
    upsert_records_with_composite_key
)

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandler:
    """Handle Supabase database operations for Novitas data"""
    
    def __init__(self):
        # Load from environment variables
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Using historical table to store all scraped data
        # Composite key: (source, code, release_date, geozip) - but Novitas doesn't have geozip, so effectively (source, code, release_date)
        self.table_name = "new_updated_historical_medical_benchmarking_data"
        self.source_name = "Novitas"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error(" Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f" Supabase client initialized for table: '{self.table_name}'")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise

    def _validate_and_prepare_records(self, records: List[Dict]) -> List[Dict]:
        """
        Validate and prepare records for insertion using common utilities.
        Uses rel_date from data processor (extracted from filename).
        """
        initial_count = len(records)
        
        # Check if records already exist - if so, use their release_date to prevent duplicates
        existing_release_date = get_existing_release_date(
            self.client, self.table_name, self.source_name
        )
        
        validated_records = []
        for record in records:
            prepared = prepare_record_for_insertion(
                record=record,
                source_name=self.source_name,
                existing_release_date=existing_release_date,
                has_geozip=False  # Novitas doesn't have geozip
            )
            if prepared:
                validated_records.append(prepared)
        
        filtered_count = initial_count - len(validated_records)
        if filtered_count > 0:
            logger.warning(f"⚠️ Filtered out {filtered_count} records with null/empty code or missing release_date")
        
        release_date_used = existing_release_date or (validated_records[0].get('release_date') if validated_records else 'N/A')
        logger.info(f"✅ Validated {len(validated_records)} records (filtered {filtered_count})")
        logger.info(f"📅 Using release_date: '{release_date_used}' (reused from existing or from data)")
        return validated_records
    
    def insert_records(self, records: List[Dict]) -> dict:
        """
        Insert multiple records into Supabase
        Returns: Summary of insertion results
        """
        if not records:
            logger.warning(" No records to insert.")
            return {"status": "no_records", "records_inserted": 0, "table": self.table_name}
        
        # Validate and prepare records (add source, filter null codes)
        validated_records = self._validate_and_prepare_records(records)
        
        if not validated_records:
            logger.warning("⚠️ No valid records to insert after validation.")
            return {"status": "no_valid_records", "records_inserted": 0, "table": self.table_name}
            
        # Remove duplicates within the batch itself using (source, code, release_date, geozip) as key
        # For Novitas, geozip is NULL, so it's effectively (source, code, release_date)
        # Historical table composite key: (source, code, release_date, geozip)
        seen_keys = {}
        deduplicated_records = []
        duplicates_removed = 0
        
        for record in validated_records:
            # Use (source, code, release_date, geozip) as unique key for historical table
            geozip = record.get('geozip') or None  # Keep None for NULL geozip
            release_date = record.get('release_date') or record.get('rel_date', 'Unknown')
            key = (record.get('source'), record.get('code'), release_date, geozip)
            
            if key in seen_keys:
                duplicates_removed += 1
                # Replace with newer record (keep last occurrence)
                index = seen_keys[key]
                deduplicated_records[index] = record
            else:
                seen_keys[key] = len(deduplicated_records)
                deduplicated_records.append(record)
        
        if duplicates_removed > 0:
            logger.warning(f"⚠️ Removed {duplicates_removed} duplicate records within batch (same source+code+release_date+geozip)")
        
        logger.info(f"📤 Upserting {len(deduplicated_records)} records into '{self.table_name}'...")
        logger.info(f"   (Will update existing records or insert new ones based on source+code+release_date+geozip)")
        logger.info(f"   Processing in chunks of 1000 records to avoid bulk insert failures...")
        
        # Use common utility function for upsert with composite key support
        result = upsert_records_with_composite_key(
            client=self.client,
            table_name=self.table_name,
            source_name=self.source_name,
            records=deduplicated_records,
            chunk_size=1000
        )
        
        return result