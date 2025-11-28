from supabase import create_client, Client
from typing import List, Dict
import logging
import os
import dotenv

logger = logging.getLogger(__name__)
dotenv.load_dotenv()

class SupabaseHandlerCLFS:
    """Handle Supabase database operations for the CLFS table"""
    
    def __init__(self):
        # Load from environment variables
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Table name for CLFS data
        self.table_name = "historical_medical_benchmarking_data"
        
        if not self.supabase_url or not self.supabase_key:
            logger.error(" Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY in .env file.")
            raise ValueError("Missing Supabase credentials in environment variables")
        
        try:
            self.client: Client = create_client(self.supabase_url, self.supabase_key)
            logger.info(f" Supabase client initialized for table: '{self.table_name}'")
        except Exception as e:
            logger.error(f" Failed to initialize Supabase client: {e}")
            raise
    
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
        
        logger.info(f" Preparing to insert {len(records)} records into '{self.table_name}'...")
        
        # Log sample record structure for verification
        if records:
            logger.info(f" Sample record structure: {list(records[0].keys())}")
            logger.info(f" Sample record (first): {records[0]}")
        
        try:
            # Insert records into Supabase
            # The Supabase Python client handles bulk inserts efficiently
            logger.info(f" Inserting records into Supabase...")
            response = self.client.table(self.table_name).insert(records).execute()
            
            # Check if insertion was successful
            if response.data:
                inserted_count = len(response.data)
                logger.info(f" Successfully inserted {inserted_count} records to Supabase.")
            else:
                # If response.data is empty but no error, assume all records were inserted
                inserted_count = len(records)
                logger.info(f" Successfully sent {inserted_count} records to Supabase.")
            
            return {
                "status": "success",
                "records_inserted": inserted_count,
                "table": self.table_name,
                "response_data": response.data if hasattr(response, 'data') else None
            }
            
        except Exception as e:
            logger.error(f"Error inserting records: {e}")
            logger.error(f"Error details: {str(e)}")
            
            # Try to provide more helpful error information
            if "duplicate key" in str(e).lower():
                logger.error(" Hint: This might be a duplicate key constraint violation. Check if records already exist.")
            elif "foreign key" in str(e).lower():
                logger.error(" Hint: This might be a foreign key constraint violation. Check referenced tables.")
            elif "not null" in str(e).lower():
                logger.error(" Hint: A required column might be missing or null in your data.")
            
            raise
    