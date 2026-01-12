"""
Pydantic models for API requests and responses
"""

from pydantic import BaseModel, Field, validator
from typing import Optional
from datetime import datetime
from enum import Enum


# ============================================================================
# Enums
# ============================================================================

class JobStatus(str, Enum):
    """Job status enumeration"""
    PENDING = "pending"
    APPROVED = "approved"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


# ============================================================================
# Request Models
# ============================================================================

class ScraperRequestCreate(BaseModel):
    """
    Request model for creating a new scraper job
    """
    scraper_name: str = Field(
        ...,
        description="Display name of the scraper (e.g., 'FairHealth Physician')",
        examples=["FairHealth Physician", "Medicare Lab", "Novitas OBL"]
    )
    created_by: Optional[str] = Field(
        default="system",
        description="User or system that created the request"
    )
    
    @validator('scraper_name')
    def validate_scraper_name(cls, v):
        """Validate that scraper name is valid"""
        from app.utils.helpers import validate_scraper_name
        if not validate_scraper_name(v):
            raise ValueError(f"Invalid scraper name: {v}")
        return v


class JobApprovalRequest(BaseModel):
    """
    Request model for approving a job (optional body parameters)
    """
    approved_by: Optional[str] = Field(
        default="system",
        description="User who approved the job"
    )


class JobHistoryQuery(BaseModel):
    """
    Query parameters for job history endpoint
    """
    page: int = Field(default=1, ge=1, description="Page number")
    limit: int = Field(default=20, ge=1, le=100, description="Items per page")
    scraper_name: Optional[str] = Field(default=None, description="Filter by scraper name")
    status: Optional[JobStatus] = Field(default=None, description="Filter by status")
    
    class Config:
        use_enum_values = True


# ============================================================================
# Response Models
# ============================================================================

class ScraperInfo(BaseModel):
    """
    Information about an available scraper
    """
    name: str = Field(..., description="Display name")
    type: str = Field(..., description="Module/type identifier")
    description: str = Field(..., description="Scraper description")
    icon: str = Field(..., description="Icon emoji")


class JobResponse(BaseModel):
    """
    Response model for a single job
    """
    id: str = Field(..., description="Database UUID")
    job_id: str = Field(..., description="Unique job identifier")
    scraper_name: str = Field(..., description="Display name of scraper")
    scraper_type: str = Field(..., description="Module name of scraper")
    status: JobStatus = Field(..., description="Current job status")
    
    # Timestamps
    requested_at: datetime = Field(..., description="When job was requested")
    approved_at: Optional[datetime] = Field(None, description="When job was approved")
    started_at: Optional[datetime] = Field(None, description="When job started running")
    completed_at: Optional[datetime] = Field(None, description="When job completed")
    
    # Execution details
    container_id: Optional[str] = Field(None, description="Docker container ID")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    records_processed: Optional[int] = Field(None, description="Number of records processed")
    
    # Audit
    created_by: str = Field(default="system", description="Who created the job")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    
    # Computed fields
    duration_seconds: Optional[int] = Field(None, description="Job duration in seconds")
    
    class Config:
        from_attributes = True
        use_enum_values = True


class JobCreateResponse(BaseModel):
    """
    Response model for job creation
    """
    job_id: str = Field(..., description="Unique job identifier")
    scraper_name: str = Field(..., description="Display name of scraper")
    status: JobStatus = Field(..., description="Current job status")
    message: str = Field(..., description="Success message")
    requested_at: datetime = Field(..., description="When job was requested")
    
    class Config:
        use_enum_values = True


class JobActionResponse(BaseModel):
    """
    Response model for job actions (approve, dismiss, cancel)
    """
    job_id: str = Field(..., description="Job identifier")
    status: JobStatus = Field(..., description="New job status")
    message: str = Field(..., description="Action result message")
    
    class Config:
        use_enum_values = True


class JobHistoryResponse(BaseModel):
    """
    Response model for job history with pagination
    """
    jobs: list[JobResponse] = Field(..., description="List of jobs")
    total: int = Field(..., description="Total number of jobs")
    page: int = Field(..., description="Current page number")
    limit: int = Field(..., description="Items per page")
    total_pages: int = Field(..., description="Total number of pages")
    
    class Config:
        use_enum_values = True


class ScraperListResponse(BaseModel):
    """
    Response model for list of available scrapers
    """
    scrapers: list[ScraperInfo] = Field(..., description="List of available scrapers")
    total: int = Field(..., description="Total number of scrapers")


class HealthCheckResponse(BaseModel):
    """
    Response model for health check endpoint
    """
    status: str = Field(..., description="Health status")
    version: str = Field(..., description="API version")
    database: str = Field(..., description="Database connection status")
    error: Optional[str] = Field(None, description="Error message if unhealthy")


class ErrorResponse(BaseModel):
    """
    Standard error response model
    """
    detail: str = Field(..., description="Error message")
    error_code: Optional[str] = Field(None, description="Error code")
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Error timestamp")


class MessageResponse(BaseModel):
    """
    Simple message response
    """
    message: str = Field(..., description="Response message")


# ============================================================================
# SSE Log Models
# ============================================================================

class LogEvent(BaseModel):
    """
    Model for a single log event in SSE stream
    """
    timestamp: datetime = Field(default_factory=datetime.utcnow, description="Log timestamp")
    message: str = Field(..., description="Log message")
    level: Optional[str] = Field(default="INFO", description="Log level")
    
    def to_sse(self) -> str:
        """
        Convert to SSE format
        
        Returns:
            str: SSE formatted string
        """
        return f"data: {self.model_dump_json()}\n\n"


class LogStreamStatus(BaseModel):
    """
    Status message for log stream
    """
    status: str = Field(..., description="Stream status")
    job_id: str = Field(..., description="Job identifier")
    message: str = Field(..., description="Status message")
    
    def to_sse(self) -> str:
        """
        Convert to SSE format
        
        Returns:
            str: SSE formatted string
        """
        return f"data: {self.model_dump_json()}\n\n"


# ============================================================================
# Database Models (for internal use)
# ============================================================================

class JobCreate(BaseModel):
    """
    Internal model for creating a job in database
    """
    job_id: str
    scraper_name: str
    scraper_type: str
    status: JobStatus = JobStatus.PENDING
    created_by: str = "system"
    
    class Config:
        use_enum_values = True


class JobUpdate(BaseModel):
    """
    Internal model for updating a job in database
    """
    status: Optional[JobStatus] = None
    approved_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    container_id: Optional[str] = None
    error_message: Optional[str] = None
    records_processed: Optional[int] = None
    
    class Config:
        use_enum_values = True
        # Allow None values to be set
        validate_assignment = True

