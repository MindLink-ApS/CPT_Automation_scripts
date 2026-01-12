"""
Utility helper functions
"""

import uuid
from datetime import datetime
from typing import Optional, Union
from dateutil import parser as date_parser


def generate_job_id() -> str:
    """
    Generate a unique job ID
    
    Returns:
        str: Unique job ID in format: job-{timestamp}-{uuid}
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    unique_id = str(uuid.uuid4())[:8]
    return f"job-{timestamp}-{unique_id}"


def calculate_duration(start_time: Optional[Union[datetime, str]], end_time: Optional[Union[datetime, str]]) -> Optional[int]:
    """
    Calculate duration in seconds between two timestamps
    
    Args:
        start_time: Start timestamp (datetime or ISO string)
        end_time: End timestamp (datetime or ISO string)
    
    Returns:
        int: Duration in seconds, or None if either timestamp is missing
    """
    if not start_time or not end_time:
        return None
    
    # Convert strings to datetime if needed
    if isinstance(start_time, str):
        start_time = date_parser.isoparse(start_time)
    if isinstance(end_time, str):
        end_time = date_parser.isoparse(end_time)
    
    return int((end_time - start_time).total_seconds())


def format_scraper_name(scraper_type: str) -> str:
    """
    Convert scraper type (module name) to display name
    
    Args:
        scraper_type: Module name (e.g., "Fair_Health_Physicians")
    
    Returns:
        str: Display name (e.g., "FairHealth Physician")
    """
    mapping = {
        "Fair_Health_Physicians": "FairHealth Physician",
        "Fair_Health_Facility": "FairHealth ASC",
        "Medicare_Clinical_Fees": "Medicare Lab",
        "Medicare_ASC_Addenda": "Medicare Facility",
        "Novitas": "Novitas OBL",
        "New_Jersey_DOBI": "NJ PIP"
    }
    return mapping.get(scraper_type, scraper_type)


def get_scraper_type(scraper_name: str) -> Optional[str]:
    """
    Convert display name to scraper type (module name)
    
    Args:
        scraper_name: Display name (e.g., "FairHealth Physician")
    
    Returns:
        str: Module name (e.g., "Fair_Health_Physicians"), or None if not found
    """
    mapping = {
        "FairHealth Physician": "Fair_Health_Physicians",
        "FairHealth ASC": "Fair_Health_Facility",
        "Medicare Lab": "Medicare_Clinical_Fees",
        "Medicare Facility": "Medicare_ASC_Addenda",
        "Novitas OBL": "Novitas",
        "NJ PIP": "New_Jersey_DOBI"
    }
    return mapping.get(scraper_name)


def get_all_scrapers() -> list[dict]:
    """
    Get list of all available scrapers with metadata
    
    Returns:
        list: List of scraper dictionaries with name, type, and description
    """
    return [
        {
            "name": "FairHealth Physician",
            "type": "Fair_Health_Physicians",
            "description": "Physician fee schedules",
            "icon": "👨‍⚕️"
        },
        {
            "name": "FairHealth ASC",
            "type": "Fair_Health_Facility",
            "description": "Ambulatory Surgery Center rates",
            "icon": "🏥"
        },
        {
            "name": "Medicare Lab",
            "type": "Medicare_Clinical_Fees",
            "description": "Clinical Lab Fee Schedule",
            "icon": "💰"
        },
        {
            "name": "Medicare Facility",
            "type": "Medicare_ASC_Addenda",
            "description": "Medicare Facility rates",
            "icon": "📋"
        },
        {
            "name": "Novitas OBL",
            "type": "Novitas",
            "description": "Office-Based Lab rates",
            "icon": "📊"
        },
        {
            "name": "NJ PIP",
            "type": "New_Jersey_DOBI",
            "description": "New Jersey Personal Injury Protection",
            "icon": "🏛️"
        }
    ]


def validate_scraper_name(scraper_name: str) -> bool:
    """
    Validate if scraper name is valid
    
    Args:
        scraper_name: Display name to validate
    
    Returns:
        bool: True if valid, False otherwise
    """
    return get_scraper_type(scraper_name) is not None

