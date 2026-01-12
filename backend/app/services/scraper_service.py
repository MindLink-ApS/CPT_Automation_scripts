"""
Scraper Service - Orchestrates scraper job execution
"""

import asyncio
import logging
import subprocess
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path

from app.services.docker_service import get_docker_service, DockerService
from app.repositories.job_repository import JobRepository, get_job_repository
from app.api.models import JobStatus
from app.core.config import settings

logger = logging.getLogger(__name__)


class ScraperService:
    """
    Service for executing scraper jobs in Docker containers
    """
    
    def __init__(
        self,
        docker_service: Optional[DockerService] = None,
        job_repository: Optional[JobRepository] = None
    ):
        """
        Initialize scraper service
        
        Args:
            docker_service: Docker service instance (optional)
            job_repository: Job repository instance (optional)
        """
        self.docker = docker_service or get_docker_service()
        self.repo = job_repository or get_job_repository()
    
    async def execute_scraper_job(
        self,
        job_id: str,
        scraper_type: str
    ) -> bool:
        """
        Execute a scraper job (in Docker or locally based on EXECUTION_MODE)
        
        Args:
            job_id: Unique job identifier
            scraper_type: Scraper module name (e.g., "Fair_Health_Physicians")
        
        Returns:
            bool: True if execution completed successfully
        """
        if settings.EXECUTION_MODE == "local":
            return await self._execute_local(job_id, scraper_type)
        else:
            return await self._execute_docker(job_id, scraper_type)
    
    async def _execute_local(
        self,
        job_id: str,
        scraper_type: str
    ) -> bool:
        """
        Execute a scraper job locally (without Docker)
        
        Args:
            job_id: Unique job identifier
            scraper_type: Scraper module name (e.g., "Fair_Health_Physicians")
        
        Returns:
            bool: True if execution completed successfully
        """
        try:
            logger.info(f"🚀 Starting LOCAL execution for job {job_id} ({scraper_type})")
            
            # Update job status to running
            self.repo.update_job_status(
                job_id,
                JobStatus.RUNNING,
                started_at=datetime.utcnow()
            )
            
            logger.info(f"🔄 Job {job_id} is now running locally")
            
            # Run the scraper module directly
            module_path = f"app.cpt_automated_scripts.{scraper_type}.main"
            
            # Execute in a subprocess to isolate it
            process = await asyncio.create_subprocess_exec(
                sys.executable, "-m", module_path,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
                cwd=Path(__file__).parent.parent.parent  # backend directory
            )
            
            # Wait for completion with timeout
            try:
                stdout, _ = await asyncio.wait_for(
                    process.communicate(),
                    timeout=settings.JOB_TIMEOUT_SECONDS
                )
                exit_code = process.returncode
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                raise asyncio.TimeoutError(f"Job timed out after {settings.JOB_TIMEOUT_SECONDS} seconds")
            
            logs_text = stdout.decode('utf-8') if stdout else ""
            
            if exit_code == 0:
                # Success
                logger.info(f"✅ Job {job_id} completed successfully")
                
                # Try to extract records processed from logs
                records_processed = self._extract_records_from_logs(logs_text)
                
                # Update job status to completed
                self.repo.update_job_status(
                    job_id,
                    JobStatus.COMPLETED,
                    records_processed=records_processed,
                    completed_at=datetime.utcnow()
                )
                
                return True
            else:
                # Failure
                logger.error(f"❌ Job {job_id} failed with exit code {exit_code}")
                
                # Update job status to failed
                self.repo.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    error_message=f"Process exited with code {exit_code}. Logs:\n{logs_text[-500:]}",
                    completed_at=datetime.utcnow()
                )
                
                logger.error(f"📋 Full error logs for job {job_id}:\n{logs_text}")
                
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"⏱️ Job {job_id} timed out after {settings.JOB_TIMEOUT_SECONDS} seconds")
            
            # Update job status to failed
            self.repo.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=f"Job timed out after {settings.JOB_TIMEOUT_SECONDS} seconds",
                completed_at=datetime.utcnow()
            )
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Job {job_id} failed with exception: {e}")
            logger.exception("Full traceback:")
            
            # Update job status to failed
            self.repo.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=str(e),
                completed_at=datetime.utcnow()
            )
            
            return False
    
    async def _execute_docker(
        self,
        job_id: str,
        scraper_type: str
    ) -> bool:
        """
        Execute a scraper job in a Docker container
        
        Args:
            job_id: Unique job identifier
            scraper_type: Scraper module name (e.g., "Fair_Health_Physicians")
        
        Returns:
            bool: True if execution completed successfully
        """
        container = None
        
        try:
            logger.info(f"🚀 Starting DOCKER execution for job {job_id} ({scraper_type})")
            
            # Create and start Docker container
            container = self.docker.create_and_run_container(
                scraper_type=scraper_type,
                job_id=job_id
            )
            
            # Update job status to running with container ID
            self.repo.update_job_status(
                job_id,
                JobStatus.RUNNING,
                container_id=container.id,
                started_at=datetime.utcnow()
            )
            
            logger.info(f"🔄 Job {job_id} is now running in container {container.id[:12]}")
            
            # Wait for container to complete (with timeout)
            result = await asyncio.get_event_loop().run_in_executor(
                None,
                self.docker.wait_for_container,
                container.id,
                settings.JOB_TIMEOUT_SECONDS
            )
            
            # Check exit code
            exit_code = result.get("StatusCode", -1)
            
            if exit_code == 0:
                # Success
                logger.info(f"✅ Job {job_id} completed successfully")
                
                # Get logs to extract records processed (if available)
                logs = self.docker.get_container_logs(container.id, stream=False)
                logs_text = logs.decode('utf-8') if isinstance(logs, bytes) else str(logs)
                
                # Try to extract records processed from logs
                records_processed = self._extract_records_from_logs(logs_text)
                
                # Update job status to completed
                self.repo.update_job_status(
                    job_id,
                    JobStatus.COMPLETED,
                    records_processed=records_processed,
                    completed_at=datetime.utcnow()
                )
                
                # Cleanup container after a delay (keep logs available for a bit)
                await asyncio.sleep(60)  # Keep container for 1 minute
                self.docker.cleanup_container(container.id)
                
                return True
                
            else:
                # Failure
                logger.error(f"❌ Job {job_id} failed with exit code {exit_code}")
                
                # Get error logs
                logs = self.docker.get_container_logs(container.id, stream=False, tail=50)
                error_message = logs.decode('utf-8') if isinstance(logs, bytes) else str(logs)
                
                # Update job status to failed
                self.repo.update_job_status(
                    job_id,
                    JobStatus.FAILED,
                    error_message=f"Container exited with code {exit_code}. Last logs:\n{error_message[-500:]}",
                    completed_at=datetime.utcnow()
                )
                
                logger.error(f"📋 Full error logs for job {job_id}:\n{error_message}")
                
                # Cleanup container
                await asyncio.sleep(300)  # Keep container for 5 minutes for debugging
                self.docker.cleanup_container(container.id)
                
                return False
                
        except asyncio.TimeoutError:
            logger.error(f"⏱️ Job {job_id} timed out after {settings.JOB_TIMEOUT_SECONDS} seconds")
            
            # Stop and cleanup container
            if container:
                self.docker.stop_container(container.id)
                self.docker.cleanup_container(container.id)
            
            # Update job status to failed
            self.repo.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=f"Job timed out after {settings.JOB_TIMEOUT_SECONDS} seconds",
                completed_at=datetime.utcnow()
            )
            
            return False
            
        except Exception as e:
            logger.error(f"❌ Job {job_id} failed with exception: {e}")
            logger.exception("Full traceback:")
            
            # Cleanup container if it exists
            if container:
                try:
                    self.docker.cleanup_container(container.id)
                except Exception as cleanup_error:
                    logger.error(f"Failed to cleanup container: {cleanup_error}")
            
            # Update job status to failed
            self.repo.update_job_status(
                job_id,
                JobStatus.FAILED,
                error_message=str(e),
                completed_at=datetime.utcnow()
            )
            
            return False
    
    def _extract_records_from_logs(self, logs: str) -> Optional[int]:
        """
        Try to extract number of records processed from logs
        
        Args:
            logs: Container logs text
        
        Returns:
            int: Number of records processed or None
        """
        try:
            # Look for common patterns in logs
            patterns = [
                "Records processed:",
                "records processed:",
                "Prepared",
                "records for database"
            ]
            
            for line in logs.split('\n'):
                for pattern in patterns:
                    if pattern in line:
                        # Try to extract number
                        import re
                        numbers = re.findall(r'\d+', line)
                        if numbers:
                            return int(numbers[0])
            
            return None
            
        except Exception as e:
            logger.warning(f"Failed to extract records from logs: {e}")
            return None
    
    def get_job_container_id(self, job_id: str) -> Optional[str]:
        """
        Get container ID for a job
        
        Args:
            job_id: Job identifier
        
        Returns:
            str: Container ID or None
        """
        try:
            job = self.repo.get_job_by_id(job_id)
            if job:
                return job.get('container_id')
            return None
        except Exception as e:
            logger.error(f"Error getting container ID for job {job_id}: {e}")
            return None
    
    def is_job_running(self, job_id: str) -> bool:
        """
        Check if a job is currently running
        
        Args:
            job_id: Job identifier
        
        Returns:
            bool: True if job is running
        """
        try:
            container_id = self.get_job_container_id(job_id)
            if not container_id:
                return False
            
            status = self.docker.get_container_status(container_id)
            return status == "running"
            
        except Exception as e:
            logger.error(f"Error checking if job {job_id} is running: {e}")
            return False
    
    def cancel_job(self, job_id: str) -> bool:
        """
        Cancel a running job
        
        Args:
            job_id: Job identifier
        
        Returns:
            bool: True if cancelled successfully
        """
        try:
            container_id = self.get_job_container_id(job_id)
            if not container_id:
                logger.warning(f"No container found for job {job_id}")
                return False
            
            logger.info(f"🛑 Cancelling job {job_id} (container {container_id[:12]})")
            
            # Stop container
            self.docker.stop_container(container_id)
            
            # Update job status
            self.repo.update_job_status(
                job_id,
                JobStatus.CANCELLED,
                error_message="Job cancelled by user",
                completed_at=datetime.utcnow()
            )
            
            # Cleanup container
            self.docker.cleanup_container(container_id)
            
            logger.info(f"✅ Job {job_id} cancelled successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error cancelling job {job_id}: {e}")
            return False


# Global scraper service instance
_scraper_service: Optional[ScraperService] = None


def get_scraper_service() -> ScraperService:
    """
    Get or create scraper service instance
    
    Returns:
        ScraperService: Scraper service singleton
    """
    global _scraper_service
    if _scraper_service is None:
        _scraper_service = ScraperService()
    return _scraper_service

