"""
Job Repository - Data access layer for scraper jobs
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import logging
from supabase import Client

from app.api.models import JobStatus, JobCreate, JobUpdate, JobResponse
from app.core.database import get_db
from app.utils.helpers import calculate_duration

logger = logging.getLogger(__name__)


class JobRepository:
    """
    Repository for managing scraper jobs in Supabase
    """
    
    TABLE_NAME = "scraper_jobs"
    
    def __init__(self, db_client: Optional[Client] = None):
        """
        Initialize job repository
        
        Args:
            db_client: Supabase client instance (optional, will use default if not provided)
        """
        self.db = db_client or get_db()
    
    def create_job(self, job_data: JobCreate) -> Dict[str, Any]:
        """
        Create a new job in the database
        
        Args:
            job_data: Job creation data
        
        Returns:
            Dict: Created job record
        
        Raises:
            Exception: If job creation fails
        """
        try:
            data = {
                "job_id": job_data.job_id,
                "scraper_name": job_data.scraper_name,
                "scraper_type": job_data.scraper_type,
                "status": job_data.status,
                "created_by": job_data.created_by
            }
            
            response = self.db.table(self.TABLE_NAME).insert(data).execute()
            
            if response.data and len(response.data) > 0:
                logger.info(f"✅ Job created: {job_data.job_id}")
                return response.data[0]
            else:
                raise Exception("No data returned from insert operation")
                
        except Exception as e:
            logger.error(f"❌ Failed to create job {job_data.job_id}: {e}")
            raise
    
    def get_job_by_id(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a job by its job_id
        
        Args:
            job_id: Unique job identifier
        
        Returns:
            Dict: Job record or None if not found
        """
        try:
            response = self.db.table(self.TABLE_NAME)\
                .select("*")\
                .eq("job_id", job_id)\
                .execute()
            
            if response.data and len(response.data) > 0:
                job = response.data[0]
                # Add computed duration field
                job['duration_seconds'] = calculate_duration(
                    job.get('started_at'),
                    job.get('completed_at')
                )
                return job
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to get job {job_id}: {e}")
            raise
    
    def update_job(self, job_id: str, update_data: JobUpdate) -> Dict[str, Any]:
        """
        Update a job's fields
        
        Args:
            job_id: Unique job identifier
            update_data: Fields to update
        
        Returns:
            Dict: Updated job record
        
        Raises:
            Exception: If update fails
        """
        try:
            # Build update dict with only non-None values
            data = {}
            if update_data.status is not None:
                data["status"] = update_data.status
            if update_data.approved_at is not None:
                data["approved_at"] = update_data.approved_at.isoformat()
            if update_data.started_at is not None:
                data["started_at"] = update_data.started_at.isoformat()
            if update_data.completed_at is not None:
                data["completed_at"] = update_data.completed_at.isoformat()
            if update_data.container_id is not None:
                data["container_id"] = update_data.container_id
            if update_data.error_message is not None:
                data["error_message"] = update_data.error_message
            if update_data.records_processed is not None:
                data["records_processed"] = update_data.records_processed
            
            if not data:
                logger.warning(f"No fields to update for job {job_id}")
                return self.get_job_by_id(job_id)
            
            response = self.db.table(self.TABLE_NAME)\
                .update(data)\
                .eq("job_id", job_id)\
                .execute()
            
            if response.data and len(response.data) > 0:
                logger.info(f"✅ Job updated: {job_id}")
                return response.data[0]
            else:
                raise Exception(f"Job not found or update failed: {job_id}")
                
        except Exception as e:
            logger.error(f"❌ Failed to update job {job_id}: {e}")
            raise
    
    def update_job_status(
        self,
        job_id: str,
        status: JobStatus,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Update job status and optionally other fields
        
        Args:
            job_id: Unique job identifier
            status: New job status
            **kwargs: Additional fields to update (container_id, error_message, etc.)
        
        Returns:
            Dict: Updated job record
        """
        update_data = JobUpdate(status=status)
        
        # Set timestamp based on status
        if status == JobStatus.APPROVED:
            update_data.approved_at = datetime.utcnow()
        elif status == JobStatus.RUNNING:
            update_data.started_at = datetime.utcnow()
        elif status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]:
            update_data.completed_at = datetime.utcnow()
        
        # Add any additional fields from kwargs
        for key, value in kwargs.items():
            if hasattr(update_data, key):
                setattr(update_data, key, value)
        
        return self.update_job(job_id, update_data)
    
    def get_pending_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all jobs with pending status
        
        Returns:
            List[Dict]: List of pending jobs
        """
        try:
            logger.info(f"🔍 Querying table '{self.TABLE_NAME}' for status: '{JobStatus.PENDING}' (value: '{JobStatus.PENDING.value}')")
            
            response = self.db.table(self.TABLE_NAME)\
                .select("*")\
                .eq("status", JobStatus.PENDING.value)\
                .order("requested_at", desc=True)\
                .execute()
            
            logger.info(f"📊 Supabase response: {response}")
            logger.info(f"📊 Response data: {response.data}")
            logger.info(f"📊 Response count: {response.count if hasattr(response, 'count') else 'N/A'}")
            
            jobs = response.data or []
            logger.info(f"📋 Found {len(jobs)} pending jobs")
            
            if jobs:
                logger.info(f"📝 Sample job: {jobs[0]}")
            
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Failed to get pending jobs: {e}")
            logger.exception("Full traceback:")
            raise
    
    def get_running_jobs(self) -> List[Dict[str, Any]]:
        """
        Get all jobs with running status
        
        Returns:
            List[Dict]: List of running jobs
        """
        try:
            response = self.db.table(self.TABLE_NAME)\
                .select("*")\
                .eq("status", JobStatus.RUNNING.value)\
                .order("started_at", desc=True)\
                .execute()
            
            jobs = response.data or []
            logger.info(f"🔄 Found {len(jobs)} running jobs")
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Failed to get running jobs: {e}")
            raise
    
    def get_job_history(
        self,
        page: int = 1,
        limit: int = 20,
        scraper_name: Optional[str] = None,
        status: Optional[JobStatus] = None
    ) -> tuple[List[Dict[str, Any]], int]:
        """
        Get job history with pagination and filtering
        
        Args:
            page: Page number (1-indexed)
            limit: Items per page
            scraper_name: Filter by scraper name (optional)
            status: Filter by status (optional)
        
        Returns:
            tuple: (list of jobs, total count)
        """
        try:
            # Build query
            query = self.db.table(self.TABLE_NAME).select("*", count="exact")
            
            # Apply filters
            if scraper_name:
                query = query.eq("scraper_name", scraper_name)
            
            if status:
                query = query.eq("status", status.value if isinstance(status, JobStatus) else status)
            else:
                # By default, show completed, failed, and cancelled jobs
                query = query.in_("status", [
                    JobStatus.COMPLETED.value,
                    JobStatus.FAILED.value,
                    JobStatus.CANCELLED.value
                ])
            
            # Apply pagination
            offset = (page - 1) * limit
            query = query.order("requested_at", desc=True)\
                .range(offset, offset + limit - 1)
            
            response = query.execute()
            
            jobs = response.data or []
            total = response.count or 0
            
            # Add computed duration for each job
            for job in jobs:
                job['duration_seconds'] = calculate_duration(
                    job.get('started_at'),
                    job.get('completed_at')
                )
            
            logger.info(f"📊 Retrieved {len(jobs)} jobs (page {page}, total {total})")
            return jobs, total
            
        except Exception as e:
            logger.error(f"❌ Failed to get job history: {e}")
            raise
    
    def approve_job(self, job_id: str) -> Dict[str, Any]:
        """
        Approve a pending job
        
        Args:
            job_id: Unique job identifier
        
        Returns:
            Dict: Updated job record
        
        Raises:
            Exception: If job is not in pending status
        """
        try:
            # First, check if job exists and is pending
            job = self.get_job_by_id(job_id)
            if not job:
                raise Exception(f"Job not found: {job_id}")
            
            if job['status'] != JobStatus.PENDING:
                raise Exception(f"Job is not in pending status: {job['status']}")
            
            # Update to approved status
            return self.update_job_status(job_id, JobStatus.APPROVED)
            
        except Exception as e:
            logger.error(f"❌ Failed to approve job {job_id}: {e}")
            raise
    
    def cancel_job(self, job_id: str, reason: Optional[str] = None) -> Dict[str, Any]:
        """
        Cancel a job (pending or running)
        
        Args:
            job_id: Unique job identifier
            reason: Cancellation reason (optional)
        
        Returns:
            Dict: Updated job record
        
        Raises:
            Exception: If job cannot be cancelled
        """
        try:
            # Check if job exists
            job = self.get_job_by_id(job_id)
            if not job:
                raise Exception(f"Job not found: {job_id}")
            
            current_status = job['status']
            
            # Can only cancel pending or running jobs
            if current_status not in [JobStatus.PENDING, JobStatus.RUNNING]:
                raise Exception(f"Cannot cancel job with status: {current_status}")
            
            # Update to cancelled status
            kwargs = {}
            if reason:
                kwargs['error_message'] = f"Cancelled: {reason}"
            
            return self.update_job_status(job_id, JobStatus.CANCELLED, **kwargs)
            
        except Exception as e:
            logger.error(f"❌ Failed to cancel job {job_id}: {e}")
            raise
    
    def delete_job(self, job_id: str) -> bool:
        """
        Delete a job from the database (use with caution)
        
        Args:
            job_id: Unique job identifier
        
        Returns:
            bool: True if deleted successfully
        """
        try:
            response = self.db.table(self.TABLE_NAME)\
                .delete()\
                .eq("job_id", job_id)\
                .execute()
            
            logger.warning(f"🗑️ Job deleted: {job_id}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to delete job {job_id}: {e}")
            raise
    
    def get_jobs_by_status(self, status: JobStatus) -> List[Dict[str, Any]]:
        """
        Get all jobs with a specific status
        
        Args:
            status: Job status to filter by
        
        Returns:
            List[Dict]: List of jobs with the specified status
        """
        try:
            response = self.db.table(self.TABLE_NAME)\
                .select("*")\
                .eq("status", status.value if isinstance(status, JobStatus) else status)\
                .order("requested_at", desc=True)\
                .execute()
            
            jobs = response.data or []
            logger.info(f"📋 Found {len(jobs)} jobs with status: {status}")
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Failed to get jobs by status {status}: {e}")
            raise
    
    def get_jobs_by_scraper(self, scraper_name: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get recent jobs for a specific scraper
        
        Args:
            scraper_name: Scraper display name
            limit: Maximum number of jobs to return
        
        Returns:
            List[Dict]: List of recent jobs for the scraper
        """
        try:
            response = self.db.table(self.TABLE_NAME)\
                .select("*")\
                .eq("scraper_name", scraper_name)\
                .order("requested_at", desc=True)\
                .limit(limit)\
                .execute()
            
            jobs = response.data or []
            logger.info(f"📋 Found {len(jobs)} recent jobs for {scraper_name}")
            return jobs
            
        except Exception as e:
            logger.error(f"❌ Failed to get jobs for scraper {scraper_name}: {e}")
            raise
    
    def get_job_statistics(self) -> Dict[str, Any]:
        """
        Get job statistics (counts by status)
        
        Returns:
            Dict: Statistics with counts for each status
        """
        try:
            stats = {
                "total": 0,
                "pending": 0,
                "approved": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "cancelled": 0
            }
            
            # Get total count
            response = self.db.table(self.TABLE_NAME)\
                .select("*", count="exact")\
                .execute()
            stats["total"] = response.count or 0
            
            # Get counts by status
            for status in JobStatus:
                response = self.db.table(self.TABLE_NAME)\
                    .select("*", count="exact")\
                    .eq("status", status.value)\
                    .execute()
                stats[status.value] = response.count or 0
            
            logger.info(f"📊 Job statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"❌ Failed to get job statistics: {e}")
            raise


# Global repository instance
job_repository = JobRepository()


def get_job_repository() -> JobRepository:
    """
    Dependency function to get job repository instance
    
    Returns:
        JobRepository: Job repository instance
    """
    return job_repository

