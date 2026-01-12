import os
import logging
import sys
from pathlib import Path
from typing import List, Dict
import dotenv
from supabase import create_client, Client
from postgrest import APIError

# Add parent directory to path to import common utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from database_utils import (
    get_existing_release_date,
    prepare_record_for_insertion,
    upsert_records_with_composite_key
)

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

# Constant release date for New Jersey DOBI data
# The file ex1_130104.xls doesn't contain date information, so we use a fixed date
# This ensures re-running the pipeline uses the same date and prevents duplicates
NJ_DOBI_RELEASE_DATE = "January 2024"  # Update this if you know the actual release date

class SupabaseHandler:
    """Handle Supabase database operations for NJ Medical PIP data"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Using historical table to store all scraped data
        self.table_name = "new_updated_historical_medical_benchmarking_data"
        self.source_name = "New Jersey DOBI"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error(" Missing Supabase credentials. Ensure SUPABASE_URL and SUPABASE_KEY are in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(" Supabase client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise

    def _validate_and_prepare_records(self, records: List[Dict]) -> List[Dict]:
        """
        Validate and prepare records for insertion using common utilities.
        Uses constant NJ_DOBI_RELEASE_DATE since file has no date information.
        """
        initial_count = len(records)
        
        # Check if records already exist - if so, use their release_date to prevent duplicates
        existing_release_date = get_existing_release_date(
            self.client, self.table_name, self.source_name
        )
        
        # If no existing records, use constant date as fallback
        # Set it as rel_date so prepare_record_for_insertion can use it
        fallback_date = existing_release_date or NJ_DOBI_RELEASE_DATE
        
        validated_records = []
        for record in records:
            # Set rel_date to constant if not present (so prepare_record_for_insertion can use it)
            if 'rel_date' not in record or not record.get('rel_date'):
                record['rel_date'] = fallback_date
            
            prepared = prepare_record_for_insertion(
                record=record,
                source_name=self.source_name,
                existing_release_date=existing_release_date,
                has_geozip=False  # NJ DOBI doesn't have geozip
            )
            if prepared:
                validated_records.append(prepared)
        
        filtered_count = initial_count - len(validated_records)
        if filtered_count > 0:
            logger.warning(f"⚠️ Filtered out {filtered_count} records with null/empty code or missing release_date")
        
        release_date_used = existing_release_date or NJ_DOBI_RELEASE_DATE
        logger.info(f"✅ Validated {len(validated_records)} records (filtered {filtered_count})")
        logger.info(f"📅 Using release_date: '{release_date_used}' (reused from existing or constant)")
        return validated_records

    def insert_records(self, records: List[Dict]) -> dict:
        """
        Insert multiple records into Supabase.
        
        Args:
            records: List of dictionaries with keys: code, code_description, 80th, data_type
        
        Returns: 
            Summary of insertion results
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
        # Historical table composite key: (source, code, release_date, geozip)
        # For NJ DOBI, geozip is NULL
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