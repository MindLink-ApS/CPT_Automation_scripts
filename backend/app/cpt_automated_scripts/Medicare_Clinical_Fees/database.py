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

class SupabaseHandlerCLFS:
    """Handle Supabase database operations for the CLFS table"""
    
    def __init__(self):
        # Load from environment variables
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Table name for CLFS data
        self.table_name = "new_updated_historical_medical_benchmarking_data"
        self.source_name = "Medicare Clinical Fees"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error(" Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f" Supabase client initialized for table: '{self.table_name}'")
        except Exception as e:
            logger.error(f" Failed to initialize Supabase client: {e}")
            raise

    def _validate_data_source(self, records: List[Dict]) -> bool:
        """
        Validate that the data structure matches expected Medicare CLFS format.
        Expected: code (HCPCS), 80th (RATE), code_description, full_description, rel_date
        """
        if not records:
            return False
        
        sample_record = records[0]
        expected_fields = ['code', '80th', 'code_description', 'full_description', 'rel_date']
        missing_fields = [field for field in expected_fields if field not in sample_record]
        
        if missing_fields:
            logger.warning(f"⚠️ Data validation: Missing expected CLFS fields: {missing_fields}")
            logger.warning(f"   This might indicate data from wrong source!")
        
        # Check for data_type that should be 'Medicare Lab'
        if 'data_type' in sample_record and sample_record.get('data_type') != 'Medicare Lab':
            logger.warning(f"⚠️ Data validation: Unexpected data_type '{sample_record.get('data_type')}' for CLFS data")
        
        return len(missing_fields) == 0

    def _validate_and_prepare_records(self, records: List[Dict]) -> List[Dict]:
        """
        Validate and prepare records for insertion using common utilities.
        Medicare Clinical Fees (CLFS) does not have geozip data.
        """
        initial_count = len(records)
        
        # Validate data structure matches expected source
        if not self._validate_data_source(records):
            logger.warning(f"⚠️ Data structure validation warning for {self.source_name}")
        
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
                has_geozip=False  # CLFS data doesn't have geozip
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
        
        Args:
            records: List of dictionaries containing the data to insert
            
        Returns:
            dict: Summary of insertion results
        """
        if not records:
            logger.warning(" No records to insert.")
            return {"status": "no_records", "records_inserted": 0, "table": self.table_name}
        
        # Validate and prepare records (add source, filter null codes)
        validated_records = self._validate_and_prepare_records(records)
        
        if not validated_records:
            logger.warning("⚠️ No valid records to insert after validation.")
            return {"status": "no_valid_records", "records_inserted": 0, "table": self.table_name}
        
        logger.info(f" Preparing to insert {len(validated_records)} records into '{self.table_name}'...")
        
        # Log sample record structure for verification
        if validated_records:
            logger.info(f" Sample record structure: {list(validated_records[0].keys())}")
            logger.info(f" Sample record (first): {validated_records[0]}")
        
        # Remove duplicates within the batch itself using (source, code, release_date, geozip) as key
        # Historical table composite key: (source, code, release_date, geozip)
        # For CLFS, geozip is NULL
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
                index = seen_keys[key]
                deduplicated_records[index] = record
            else:
                seen_keys[key] = len(deduplicated_records)
                deduplicated_records.append(record)
        
        if duplicates_removed > 0:
            logger.warning(f"⚠️ Removed {duplicates_removed} duplicate records within batch (same source+code+release_date+geozip)")
        
        logger.info(f"📤 Upserting {len(deduplicated_records)} records into Supabase...")
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
        
        # Add response_data for backward compatibility
        result['response_data'] = None
        return result
    