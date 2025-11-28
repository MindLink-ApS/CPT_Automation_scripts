from supabase import create_client, Client
from typing import List, Dict
import logging
import os
import dotenv

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandler:
    """Handle Supabase database operations for the ASC table"""
    
    def __init__(self):
        # Load from environment variables
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # --- NEW TABLE NAME ---
        self.table_name = "historical_medical_benchmarking_data"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error(" Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f" Supabase client initialized for table: '{self.table_name}'")
        except Exception as e:
            logger.error(f"Failed to initialize Supabase client: {e}")
            raise
    
    def insert_records(self, records: List[Dict]) -> dict:
        """
        Insert multiple records into Supabase
        Returns: Summary of insertion results
        """
        if not records:
            logger.warning(" No records to insert.")
            return {"status": "no_records", "records_inserted": 0, "table": self.table_name}
            
        logger.info(f"📤 Inserting {len(records)} records into '{self.table_name}'...")
        
        try:
            # Supabase Python client handles bulk inserts efficiently
            response = self.client.table(self.table_name).insert(records).execute()
            
            logger.info(f"Successfully sent {len(records)} records to Supabase.")
            
            return {
                "status": "success",
                "records_inserted": len(records), # Assumes all were inserted
                "table": self.table_name
            }
            
        except Exception as e:
            logger.error(f" Error inserting records: {e}")
            raise