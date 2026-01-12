"""
Configuration settings for the FastAPI backend
"""

from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import Optional


class Settings(BaseSettings):
    """
    Application settings loaded from environment variables
    """
    
    # Application settings
    APP_NAME: str = "CPT Automation Scripts API"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = False
    BACKEND_PORT: int = 8000
    
    # Execution mode: "docker" or "local"
    EXECUTION_MODE: str = "docker"  # Set to "local" for local testing without Docker
    
    # Supabase configuration
    SUPABASE_URL: str
    SUPABASE_KEY: str
    
    # Docker configuration
    DOCKER_HOST: Optional[str] = None  # Default: unix:///var/run/docker.sock
    DOCKER_IMAGE_NAME: str = "cpt-scraper-image"
    
    # CORS settings
    CORS_ORIGINS: list[str] = ["*"]  # Configure for production
    CORS_CREDENTIALS: bool = True
    CORS_METHODS: list[str] = ["*"]
    CORS_HEADERS: list[str] = ["*"]
    
    # Scraper configuration
    SCRAPERS_BASE_PATH: str = "/app/cpt_automated_scripts"
    
    # Job settings
    MAX_CONCURRENT_JOBS: int = 3
    JOB_TIMEOUT_SECONDS: int = 3600  # 1 hour
    
    # Cron job settings - Annual scraper execution
    CRON_ENABLED: bool = True
    CRON_MONTH: int = 11  # November
    CRON_DAY: int = 25
    CRON_HOUR: int = 0
    CRON_MINUTE: int = 0
    CRON_TIMEZONE: str = "America/Chicago"
    
    # Supabase Edge Function settings - Daily refresh
    SUPABASE_EDGE_FUNCTION_ENABLED: bool = True
    SUPABASE_EDGE_FUNCTION_URL: str = "https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark"
    EDGE_FUNCTION_CRON_HOUR: int = 2  # 2 AM Chicago time
    EDGE_FUNCTION_CRON_MINUTE: int = 0
    EDGE_FUNCTION_TIMEZONE: str = "America/Chicago"
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )


# Global settings instance
settings = Settings()

