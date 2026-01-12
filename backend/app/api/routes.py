"""
API Routes - REST endpoints for scraper management
"""

from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks, Query
from typing import Optional
import logging
import math

from app.api.models import (
    ScraperRequestCreate,
    JobCreateResponse,
    JobActionResponse,
    JobResponse,
    JobHistoryResponse,
    ScraperListResponse,
    ScraperInfo,
    JobStatus,
    JobHistoryQuery,
    JobApprovalRequest
)
from app.repositories.job_repository import JobRepository, get_job_repository
from app.utils.helpers import (
    generate_job_id,
    get_scraper_type,
    get_all_scrapers,
    format_scraper_name
)
from app.api.models import JobCreate
from app.core.scheduler import get_scheduler

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/scraper", tags=["Scraper Management"])


# ============================================================================
# Scraper Information Endpoints
# ============================================================================

@router.get("/list", response_model=ScraperListResponse)
async def list_scrapers():
    """
    Get list of all available scrapers
    
    Returns:
        ScraperListResponse: List of available scrapers with metadata
    """
    try:
        scrapers = get_all_scrapers()
        
        return ScraperListResponse(
            scrapers=[ScraperInfo(**scraper) for scraper in scrapers],
            total=len(scrapers)
        )
    except Exception as e:
        logger.error(f"Failed to list scrapers: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Job Request and Approval Endpoints
# ============================================================================

@router.post("/request", response_model=JobCreateResponse, status_code=201)
async def request_scrape(
    request: ScraperRequestCreate,
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Request a new scraper job (creates job in pending status)
    
    Args:
        request: Scraper request details
        repo: Job repository dependency
    
    Returns:
        JobCreateResponse: Created job details
    """
    try:
        # Generate unique job ID
        job_id = generate_job_id()
        
        # Get scraper type from display name
        scraper_type = get_scraper_type(request.scraper_name)
        if not scraper_type:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid scraper name: {request.scraper_name}"
            )
        
        # Create job in database
        job_data = JobCreate(
            job_id=job_id,
            scraper_name=request.scraper_name,
            scraper_type=scraper_type,
            status=JobStatus.PENDING,
            created_by=request.created_by
        )
        
        job = repo.create_job(job_data)
        
        logger.info(f"✅ Scraper job requested: {job_id} ({request.scraper_name})")
        
        return JobCreateResponse(
            job_id=job['job_id'],
            scraper_name=job['scraper_name'],
            status=JobStatus.PENDING,
            message=f"Scraper job requested successfully. Awaiting approval.",
            requested_at=job['requested_at']
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to request scraper job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/pending", response_model=list[JobResponse])
async def get_pending_jobs(
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Get all pending jobs awaiting approval
    
    Args:
        repo: Job repository dependency
    
    Returns:
        list[JobResponse]: List of pending jobs
    """
    try:
        logger.info(f"🔍 Fetching pending jobs with status: {JobStatus.PENDING}")
        jobs = repo.get_pending_jobs()
        
        logger.info(f"📋 Retrieved {len(jobs)} pending jobs")
        
        if jobs:
            logger.info(f"📝 First job: {jobs[0]}")
        
        return [JobResponse(**job) for job in jobs]
        
    except Exception as e:
        logger.error(f"Failed to get pending jobs: {e}")
        logger.exception("Full traceback:")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/approve/{job_id}", response_model=JobActionResponse)
async def approve_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    approval_request: Optional[JobApprovalRequest] = None,
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Approve a pending job and start execution
    
    Args:
        job_id: Unique job identifier
        background_tasks: FastAPI background tasks
        approval_request: Optional approval metadata
        repo: Job repository dependency
    
    Returns:
        JobActionResponse: Approval result
    """
    try:
        # Get the job
        job = repo.get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        
        # Verify job is in pending status
        if job['status'] != JobStatus.PENDING:
            raise HTTPException(
                status_code=400,
                detail=f"Job is not in pending status. Current status: {job['status']}"
            )
        
        # Approve the job
        approved_job = repo.approve_job(job_id)
        
        # Start execution in background with Docker
        from app.services.scraper_service import get_scraper_service
        scraper_service = get_scraper_service()
        
        background_tasks.add_task(
            scraper_service.execute_scraper_job,
            job_id,
            approved_job['scraper_type']
        )
        
        logger.info(f"✅ Job approved and started: {job_id}")
        
        return JobActionResponse(
            job_id=job_id,
            status=JobStatus.APPROVED,
            message=f"Job approved and execution started"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to approve job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/dismiss/{job_id}", response_model=JobActionResponse)
async def dismiss_job(
    job_id: str,
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Dismiss/cancel a job
    
    Args:
        job_id: Unique job identifier
        repo: Job repository dependency
    
    Returns:
        JobActionResponse: Dismissal result
    """
    try:
        # Get the job
        job = repo.get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        
        current_status = job['status']
        
        # If job is running, stop the container
        if current_status == JobStatus.RUNNING:
            from app.services.scraper_service import get_scraper_service
            scraper_service = get_scraper_service()
            scraper_service.cancel_job(job_id)
        else:
            # Just update status in database
            repo.cancel_job(job_id, reason="Dismissed by user")
        
        logger.info(f"🚫 Job dismissed: {job_id}")
        
        return JobActionResponse(
            job_id=job_id,
            status=JobStatus.CANCELLED,
            message=f"Job dismissed successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to dismiss job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Job Query Endpoints
# ============================================================================

@router.get("/job/{job_id}", response_model=JobResponse)
async def get_job(
    job_id: str,
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Get details of a specific job
    
    Args:
        job_id: Unique job identifier
        repo: Job repository dependency
    
    Returns:
        JobResponse: Job details
    """
    try:
        job = repo.get_job_by_id(job_id)
        
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        
        return JobResponse(**job)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/history", response_model=JobHistoryResponse)
async def get_job_history(
    page: int = Query(1, ge=1, description="Page number"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    scraper_name: Optional[str] = Query(None, description="Filter by scraper name"),
    status: Optional[JobStatus] = Query(None, description="Filter by status"),
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Get job history with pagination and filtering
    
    Args:
        page: Page number (1-indexed)
        limit: Items per page
        scraper_name: Filter by scraper name (optional)
        status: Filter by status (optional)
        repo: Job repository dependency
    
    Returns:
        JobHistoryResponse: Paginated job history
    """
    try:
        # Get jobs and total count
        jobs, total = repo.get_job_history(
            page=page,
            limit=limit,
            scraper_name=scraper_name,
            status=status
        )
        
        # Calculate total pages
        total_pages = math.ceil(total / limit) if total > 0 else 0
        
        logger.info(f"📊 Retrieved job history: page {page}/{total_pages}, {len(jobs)} jobs")
        
        return JobHistoryResponse(
            jobs=[JobResponse(**job) for job in jobs],
            total=total,
            page=page,
            limit=limit,
            total_pages=total_pages
        )
        
    except Exception as e:
        logger.error(f"Failed to get job history: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/running", response_model=list[JobResponse])
async def get_running_jobs(
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Get all currently running jobs
    
    Args:
        repo: Job repository dependency
    
    Returns:
        list[JobResponse]: List of running jobs
    """
    try:
        jobs = repo.get_running_jobs()
        
        logger.info(f"🔄 Retrieved {len(jobs)} running jobs")
        
        return [JobResponse(**job) for job in jobs]
        
    except Exception as e:
        logger.error(f"Failed to get running jobs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Statistics Endpoint
# ============================================================================

@router.get("/statistics")
async def get_statistics(
    repo: JobRepository = Depends(get_job_repository)
):
    """
    Get job statistics (counts by status)
    
    Args:
        repo: Job repository dependency
    
    Returns:
        dict: Statistics with counts for each status
    """
    try:
        stats = repo.get_job_statistics()
        
        logger.info(f"📊 Retrieved job statistics")
        
        return stats
        
    except Exception as e:
        logger.error(f"Failed to get statistics: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cron/trigger", tags=["System"])
async def trigger_cron_manually():
    """
    Manually trigger the annual scraper cron job (for testing)
    
    This will execute all scrapers immediately, just like the scheduled job.
    Use this endpoint to test the cron functionality without waiting for November 25th.
    
    Returns:
        dict: Confirmation message
    """
    try:
        scheduler = get_scheduler()
        
        if not scheduler:
            raise HTTPException(
                status_code=503,
                detail="Scheduler is not initialized"
            )
        
        logger.info("🔧 Manual scraper cron trigger requested via API")
        
        # Trigger the cron job immediately
        scheduler.trigger_now()
        
        return {
            "message": "Annual scraper cron job triggered successfully",
            "status": "executing",
            "note": "All 6 scrapers will be requested and auto-approved. Check job history for progress."
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger scraper cron job: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cron/edge-function/trigger", tags=["System"])
async def trigger_edge_function_manually():
    """
    Manually trigger the Supabase Edge Function (for testing)
    
    This will call the edge function immediately to refresh medical benchmark data.
    Use this endpoint to test the daily edge function without waiting for 2 AM.
    
    Returns:
        dict: Confirmation message
    """
    try:
        scheduler = get_scheduler()
        
        if not scheduler:
            raise HTTPException(
                status_code=503,
                detail="Scheduler is not initialized"
            )
        
        logger.info("🔧 Manual edge function trigger requested via API")
        
        # Trigger the edge function immediately
        scheduler.trigger_edge_function_now()
        
        return {
            "message": "Edge function triggered successfully",
            "status": "executing",
            "url": "https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark",
            "note": "Check backend logs for execution result."
        }
        
    except Exception as e:
        logger.error(f"Failed to trigger edge function: {e}")
        raise HTTPException(status_code=500, detail=str(e))



