import logging
import sys
from pathlib import Path
from supabase import create_client, Client
from typing import List, Dict
import os
from dotenv import load_dotenv

# Add parent directory to path to import common utilities
sys.path.insert(0, str(Path(__file__).parent.parent))
from database_utils import (
    get_existing_release_date,
    prepare_record_for_insertion,
    upsert_records_with_composite_key
)

logger = logging.getLogger(__name__)

class SupabaseHandlerPhysician:
    """Handle Supabase operations for Fair Health Physician data"""
    
    TABLE_NAME = "new_updated_historical_medical_benchmarking_data"
    SOURCE_NAME = "Fair Health Physicians"
    
    def __init__(self):
        """Initialize Supabase client"""
        load_dotenv()
        
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        self.client: Client = create_client(supabase_url, supabase_key)
        logger.info(f"✅ Supabase client initialized for table: {self.TABLE_NAME}")

    def _validate_data_source(self, records: List[Dict]) -> bool:
        """
        Validate that the data structure matches expected Fair Health Physicians format.
        Expected: percentile columns, geozip, data_type, rel_date
        """
        if not records:
            return False
        
        sample_record = records[0]
        expected_fields = ['geozip', 'data_type', 'rel_date']
        percentile_fields = ['50th', '60th', '70th', '75th', '80th', '85th', '90th', '95th']
        
        missing_expected = [field for field in expected_fields if field not in sample_record]
        has_percentiles = any(field in sample_record for field in percentile_fields)
        
        if missing_expected or not has_percentiles:
            logger.warning(f"⚠️ Data validation: Structure doesn't match expected Fair Health Physicians format!")
            logger.warning(f"   Missing: {missing_expected}, Has percentiles: {has_percentiles}")
        
        return len(missing_expected) == 0 and has_percentiles

    def _validate_and_prepare_records(self, records: List[Dict]) -> List[Dict]:
        """
        Validate and prepare records for insertion using common utilities.
        Fair Health Physicians has geozip data.
        """
        initial_count = len(records)
        
        # Validate data structure matches expected source
        if not self._validate_data_source(records):
            logger.warning(f"⚠️ Data structure validation warning for {self.SOURCE_NAME}")
        
        # Check if records already exist - if so, use their release_date to prevent duplicates
        existing_release_date = get_existing_release_date(
            self.client, self.TABLE_NAME, self.SOURCE_NAME
        )
        
        validated_records = []
        for record in records:
            prepared = prepare_record_for_insertion(
                record=record,
                source_name=self.SOURCE_NAME,
                existing_release_date=existing_release_date,
                has_geozip=True  # Fair Health Physicians has geozip
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
    
    def insert_records(self, records: List[Dict]) -> Dict:
        """
        Insert records into Supabase table
        
        Args:
            records: List of dictionaries containing the data
            
        Returns:
            Dictionary with insertion results
        """
        if not records:
            logger.warning("No records to insert")
            return {"records_inserted": 0, "table": self.TABLE_NAME}
        
        # Validate and prepare records (add source, filter null codes)
        validated_records = self._validate_and_prepare_records(records)
        
        if not validated_records:
            logger.warning("⚠️ No valid records to insert after validation.")
            return {"records_inserted": 0, "table": self.TABLE_NAME, "success": False}
        
        # Remove duplicates within the batch itself using (source, code, release_date, geozip) as key
        # IMPORTANT: For Fair Health, same code can exist for different geozips
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
        
        logger.info(f"📤 Upserting {len(deduplicated_records)} records into {self.TABLE_NAME}...")
        logger.info(f"   (Will update existing records or insert new ones based on source+code+release_date+geozip)")
        logger.info(f"   Processing in chunks of 1000 records to avoid bulk insert failures...")
        
        # Use common utility function for upsert with composite key support
        result = upsert_records_with_composite_key(
            client=self.client,
            table_name=self.TABLE_NAME,
            source_name=self.SOURCE_NAME,
            records=deduplicated_records,
            chunk_size=1000
        )
        
        # Ensure backward compatible response format
        result['success'] = result.get('status') == 'success'
        return result
    