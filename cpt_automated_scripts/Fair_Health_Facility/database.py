import os
import logging
from typing import List, Dict
import dotenv
from supabase import create_client, Client
from postgrest import APIError

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandlerFairHealth:
    """Handle Supabase database operations for FairHealth data"""
    
    def __init__(self):
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # --- NEW TABLE NAME ---
        # Assuming this data goes into the same table as the ASC data
        self.table_name = "updated_medical_benchmarking_data" 
        
        if not self.supabase_url or not self.supabase_key:
            logger.error("❌ Missing Supabase credentials. Ensure SUPABASE_URL and SUPABASE_KEY are in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info("✅ Supabase client initialized")
        except Exception as e:
            logger.error(f"❌ Failed to initialize Supabase client: {e}")
            raise

    def insert_records(self, records: List[Dict]) -> dict:
        """
        Insert multiple records into Supabase.
        Returns: Summary of insertion results
        """
        if not records:
            logger.warning("⚠️ No records to insert.")
            return {"status": "no_records", "records_inserted": 0, "table": self.table_name}

        logger.info(f"📤 Inserting {len(records)} records into '{self.table_name}'...")
        
        try:
            # Supabase Python client handles chunking automatically
            response = self.client.table(self.table_name).insert(records).execute()
            
            inserted_count = 0
            if response.data:
                inserted_count = len(response.data)
            
            logger.info(f"✅ Successfully inserted {inserted_count} records")
            
            return {
                "status": "success",
                "records_inserted": inserted_count,
                "table": self.table_name
            }
            
        except APIError as e:
            logger.error(f"❌ API Error inserting records: {e.message}")
            logger.error(f"    Details: {e.details}")
            logger.error(f"    Hint: {e.hint}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected Error inserting records: {e}")
            raise