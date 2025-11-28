import logging
from supabase import create_client, Client
from typing import List, Dict
import os
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class SupabaseHandlerPhysician:
    """Handle Supabase operations for Fair Health Physician data"""
    
    TABLE_NAME = "updated_medical_benchmarking_data"  
    
    def __init__(self):
        """Initialize Supabase client"""
        load_dotenv()
        
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if not supabase_url or not supabase_key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment variables")
        
        self.client: Client = create_client(supabase_url, supabase_key)
        logger.info(f"✅ Supabase client initialized for table: {self.TABLE_NAME}")
    
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
        
        logger.info(f"📤 Inserting {len(records)} records into {self.TABLE_NAME}...")
        
        try:
            # Supabase batch insert
            response = self.client.table(self.TABLE_NAME).insert(records).execute()
            
            inserted_count = len(response.data) if response.data else 0
            
            logger.info(f"✅ Successfully inserted {inserted_count} records")
            
            return {
                "records_inserted": inserted_count,
                "table": self.TABLE_NAME,
                "success": True
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to insert records: {e}")
            logger.exception("Full traceback:")
            raise
    