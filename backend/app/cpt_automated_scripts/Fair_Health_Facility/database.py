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

class SupabaseHandlerFairHealth:
    """Handle Supabase database operations for FairHealth data"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Using historical table to store all scraped data
        self.table_name = "new_updated_historical_medical_benchmarking_data"
        self.source_name = "Fair Health Facility"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("❌ Missing Supabase credentials. Ensure SUPABASE_URL and SUPABASE_KEY are in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info("✅ Supabase client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            raise

    def _validate_data_source(self, records: List[Dict]) -> bool:
        """
        Validate that the data structure matches expected Fair Health Facility format.
        This helps ensure we're not accidentally processing data from wrong source.
        
        Expected fields for Fair Health Facility:
        - Should have percentile columns (50th, 60th, 70th, etc.)
        - Should have geozip and data_type fields
        - Should have rel_date
        """
        if not records:
            return False
        
        # Check first record for expected fields
        sample_record = records[0]
        expected_fields = ['geozip', 'data_type', 'rel_date']
        percentile_fields = ['50th', '60th', '70th', '75th', '80th', '85th', '90th', '95th']
        
        # Check for expected fields
        missing_expected = [field for field in expected_fields if field not in sample_record]
        has_percentiles = any(field in sample_record for field in percentile_fields)
        
        if missing_expected:
            logger.warning(f"⚠️ Data validation: Missing expected fields for Fair Health Facility: {missing_expected}")
            logger.warning(f"   This might indicate data from wrong source!")
        
        if not has_percentiles:
            logger.warning(f"⚠️ Data validation: No percentile columns found. This might not be Fair Health Facility data!")
        
        # If we have the expected structure, return True
        return len(missing_expected) == 0 and has_percentiles

    def _validate_and_prepare_records(self, records: List[Dict]) -> List[Dict]:
        """
        Validate and prepare records for insertion using common utilities.
        Fair Health Facility has geozip data.
        """
        initial_count = len(records)
        
        # First, validate that data structure matches expected source
        if not self._validate_data_source(records):
            logger.warning(f"⚠️ Data structure validation failed for {self.source_name}")
            logger.warning(f"   Proceeding anyway, but please verify data source is correct!")
        
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
                has_geozip=True  # Fair Health Facility has geozip
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
        Insert multiple records into Supabase.
        Returns: Summary of insertion results
        """
        if not records:
            logger.warning("⚠️ No records to insert.")
            return {"status": "no_records", "records_inserted": 0, "table": self.table_name}

        # Validate and prepare records (add source, filter null codes)
        validated_records = self._validate_and_prepare_records(records)
        
        if not validated_records:
            logger.warning("⚠️ No valid records to insert after validation.")
            return {"status": "no_valid_records", "records_inserted": 0, "table": self.table_name}

        # Remove duplicates within the batch itself using (source, code, release_date, geozip) as key
        # This prevents duplicate key errors within the same batch
        # IMPORTANT: For Fair Health, same code can exist for different geozips (070, 074, USA)
        # Historical table composite key: (source, code, release_date, geozip)
        seen_keys = {}
        deduplicated_records = []
        duplicates_removed = 0
        
        for record in validated_records:
            # Use (source, code, release_date, geozip) as unique key for historical table
            geozip = record.get('geozip') or None  # Keep None for NULL geozips
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