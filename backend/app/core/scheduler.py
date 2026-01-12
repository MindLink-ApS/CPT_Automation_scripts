"""
APScheduler - Cron job scheduler for automated scraper execution
"""

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import logging
from datetime import datetime
import httpx

from app.core.config import settings
from app.utils.helpers import get_all_scrapers, generate_job_id
from app.repositories.job_repository import get_job_repository
from app.services.scraper_service import get_scraper_service
from app.api.models import JobCreate, JobStatus

logger = logging.getLogger(__name__)


class CronScheduler:
    """
    Scheduler for automated scraper execution
    """
    
    def __init__(self):
        """Initialize scheduler"""
        self.scheduler = AsyncIOScheduler()
        self.is_running = False
    
    async def execute_all_scrapers(self):
        """
        Execute all scrapers sequentially
        This function is called by the cron job on November 25th
        """
        try:
            logger.info("=" * 70)
            logger.info("🗓️  CRON JOB TRIGGERED - November 25th Annual Execution")
            logger.info("=" * 70)
            
            scrapers = get_all_scrapers()
            repo = get_job_repository()
            scraper_service = get_scraper_service()
            
            logger.info(f"📋 Scheduling {len(scrapers)} scrapers for execution")
            
            job_ids = []
            
            # Create jobs for all scrapers
            for scraper in scrapers:
                try:
                    job_id = generate_job_id()
                    
                    # Create job in database
                    job_data = JobCreate(
                        job_id=job_id,
                        scraper_name=scraper['name'],
                        scraper_type=scraper['type'],
                        status=JobStatus.PENDING,
                        created_by="cron_scheduler"
                    )
                    
                    repo.create_job(job_data)
                    logger.info(f"✅ Created job {job_id} for {scraper['name']}")
                    
                    # Auto-approve the job
                    repo.approve_job(job_id)
                    logger.info(f"✅ Auto-approved job {job_id}")
                    
                    job_ids.append((job_id, scraper['type']))
                    
                except Exception as e:
                    logger.error(f"❌ Failed to create job for {scraper['name']}: {e}")
                    continue
            
            logger.info(f"📊 Created and approved {len(job_ids)} jobs")
            logger.info("🚀 Starting sequential execution...")
            
            # Execute jobs sequentially
            completed = 0
            failed = 0
            
            for job_id, scraper_type in job_ids:
                try:
                    logger.info(f"▶️  Executing job {job_id} ({scraper_type})")
                    
                    # Execute the scraper
                    success = await scraper_service.execute_scraper_job(
                        job_id,
                        scraper_type
                    )
                    
                    if success:
                        completed += 1
                        logger.info(f"✅ Job {job_id} completed successfully")
                    else:
                        failed += 1
                        logger.error(f"❌ Job {job_id} failed")
                    
                except Exception as e:
                    failed += 1
                    logger.error(f"❌ Job {job_id} failed with exception: {e}")
                    continue
            
            # Summary
            logger.info("=" * 70)
            logger.info("📊 CRON JOB EXECUTION SUMMARY")
            logger.info("=" * 70)
            logger.info(f"Total jobs: {len(job_ids)}")
            logger.info(f"✅ Completed: {completed}")
            logger.info(f"❌ Failed: {failed}")
            logger.info(f"Success rate: {(completed/len(job_ids)*100):.1f}%")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"❌ Cron job execution failed: {e}")
            logger.exception("Full traceback:")
    
    async def call_edge_function(self):
        """
        Call Supabase Edge Function to refresh medical benchmark data
        This function is called daily at 2 AM Chicago time
        """
        try:
            logger.info("=" * 70)
            logger.info("🔄 EDGE FUNCTION CRON JOB TRIGGERED - Daily Refresh")
            logger.info("=" * 70)
            logger.info(f"📡 Calling: {settings.SUPABASE_EDGE_FUNCTION_URL}")
            
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    settings.SUPABASE_EDGE_FUNCTION_URL,
                    timeout=30.0
                )
                
                if response.status_code == 200:
                    logger.info("✅ Edge function called successfully")
                    logger.info(f"📊 Response: {response.json()}")
                else:
                    logger.error(f"❌ Edge function failed with status {response.status_code}")
                    logger.error(f"📋 Response: {response.text}")
            
            logger.info("=" * 70)
            
        except httpx.TimeoutException:
            logger.error("⏱️ Edge function call timed out after 30 seconds")
        except Exception as e:
            logger.error(f"❌ Edge function call failed: {e}")
            logger.exception("Full traceback:")
    
    def start(self):
        """
        Start the scheduler
        """
        if not settings.CRON_ENABLED and not settings.SUPABASE_EDGE_FUNCTION_ENABLED:
            logger.info("⏸️  All cron jobs are disabled in configuration")
            return
        
        try:
            jobs_added = []
            
            # Add cron job for November 25th (Annual scraper execution)
            if settings.CRON_ENABLED:
                self.scheduler.add_job(
                    self.execute_all_scrapers,
                    trigger=CronTrigger(
                        month=settings.CRON_MONTH,
                        day=settings.CRON_DAY,
                        hour=settings.CRON_HOUR,
                        minute=settings.CRON_MINUTE,
                        timezone=settings.CRON_TIMEZONE
                    ),
                    id="annual_scraper_execution",
                    name="Annual Scraper Execution (November 25th Chicago Time)",
                    replace_existing=True
                )
                jobs_added.append("annual_scraper_execution")
            
            # Add daily edge function cron job (2 AM Chicago time)
            if settings.SUPABASE_EDGE_FUNCTION_ENABLED:
                self.scheduler.add_job(
                    self.call_edge_function,
                    trigger=CronTrigger(
                        hour=settings.EDGE_FUNCTION_CRON_HOUR,
                        minute=settings.EDGE_FUNCTION_CRON_MINUTE,
                        timezone=settings.EDGE_FUNCTION_TIMEZONE
                    ),
                    id="daily_edge_function",
                    name="Daily Edge Function Call (2 AM Chicago)",
                    replace_existing=True
                )
                jobs_added.append("daily_edge_function")
            
            # Start the scheduler
            self.scheduler.start()
            self.is_running = True
            
            # Log startup information
            logger.info("=" * 70)
            logger.info("⏰ CRON SCHEDULER STARTED")
            logger.info("=" * 70)
            
            # Log annual scraper job
            if settings.CRON_ENABLED:
                job = self.scheduler.get_job("annual_scraper_execution")
                next_run = job.next_run_time if job else None
                logger.info(f"📅 Annual Scraper Job:")
                logger.info(f"   Schedule: November {settings.CRON_DAY} at {settings.CRON_HOUR:02d}:{settings.CRON_MINUTE:02d} {settings.CRON_TIMEZONE}")
                logger.info(f"   Next run: {next_run}")
                logger.info(f"   Scrapers: {len(get_all_scrapers())}")
            
            # Log edge function job
            if settings.SUPABASE_EDGE_FUNCTION_ENABLED:
                job = self.scheduler.get_job("daily_edge_function")
                next_run = job.next_run_time if job else None
                logger.info(f"🔄 Daily Edge Function Job:")
                logger.info(f"   Schedule: Daily at {settings.EDGE_FUNCTION_CRON_HOUR:02d}:{settings.EDGE_FUNCTION_CRON_MINUTE:02d} {settings.EDGE_FUNCTION_TIMEZONE}")
                logger.info(f"   Next run: {next_run}")
                logger.info(f"   URL: {settings.SUPABASE_EDGE_FUNCTION_URL}")
            
            logger.info(f"📊 Total jobs scheduled: {len(jobs_added)}")
            logger.info("=" * 70)
            
        except Exception as e:
            logger.error(f"❌ Failed to start cron scheduler: {e}")
            raise
    
    def stop(self):
        """
        Stop the scheduler
        """
        if self.is_running:
            self.scheduler.shutdown()
            self.is_running = False
            logger.info("⏹️  Cron scheduler stopped")
    
    def get_next_run_time(self) -> datetime:
        """
        Get next scheduled run time
        
        Returns:
            datetime: Next run time or None
        """
        if not self.is_running:
            return None
        
        job = self.scheduler.get_job("annual_scraper_execution")
        return job.next_run_time if job else None
    
    def trigger_now(self):
        """
        Manually trigger the annual scraper cron job immediately (for testing)
        """
        logger.info("🔧 Manually triggering annual scraper cron job...")
        self.scheduler.add_job(
            self.execute_all_scrapers,
            id="manual_trigger_scrapers",
            replace_existing=True
        )
    
    def trigger_edge_function_now(self):
        """
        Manually trigger the edge function immediately (for testing)
        """
        logger.info("🔧 Manually triggering edge function...")
        self.scheduler.add_job(
            self.call_edge_function,
            id="manual_trigger_edge_function",
            replace_existing=True
        )


# Global scheduler instance
_scheduler: CronScheduler = None


def get_scheduler() -> CronScheduler:
    """
    Get or create scheduler instance
    
    Returns:
        CronScheduler: Scheduler singleton
    """
    global _scheduler
    if _scheduler is None:
        _scheduler = CronScheduler()
    return _scheduler

