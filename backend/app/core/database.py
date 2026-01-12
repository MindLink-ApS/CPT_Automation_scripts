"""
Database connection and initialization
"""

from supabase import create_client, Client
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)


class Database:
    """
    Supabase database connection manager
    """
    
    def __init__(self):
        self._client: Client | None = None
    
    def get_client(self) -> Client:
        """
        Get or create Supabase client instance
        
        Returns:
            Client: Supabase client instance
        """
        if self._client is None:
            try:
                self._client = create_client(
                    settings.SUPABASE_URL,
                    settings.SUPABASE_KEY
                )
                logger.info("✅ Supabase client initialized successfully")
            except Exception as e:
                logger.error(f"❌ Failed to initialize Supabase client: {e}")
                raise
        
        return self._client
    
    def close(self):
        """
        Close database connection (if needed)
        """
        # Supabase client doesn't require explicit closing
        # but we can reset the instance
        self._client = None
        logger.info("Database connection closed")


# Global database instance
db = Database()


def get_db() -> Client:
    """
    Dependency function to get database client
    
    Returns:
        Client: Supabase client instance
    """
    return db.get_client()

