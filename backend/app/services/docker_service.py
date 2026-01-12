"""
Docker Service - Wrapper for Docker API operations
"""

import docker
from docker.models.containers import Container
from docker.errors import DockerException, NotFound, APIError
import logging
from typing import Optional, Dict, Any
from pathlib import Path

from app.core.config import settings

logger = logging.getLogger(__name__)


class DockerService:
    """
    Service for managing Docker containers for scraper execution
    """
    
    def __init__(self):
        """Initialize Docker client"""
        try:
            # Connect to Docker daemon
            if settings.DOCKER_HOST:
                self.client = docker.DockerClient(base_url=settings.DOCKER_HOST)
            else:
                # Use default socket (unix:///var/run/docker.sock)
                self.client = docker.from_env()
            
            # Test connection
            self.client.ping()
            logger.info("✅ Docker client connected successfully")
            
        except DockerException as e:
            logger.error(f"❌ Failed to connect to Docker: {e}")
            raise
    
    def create_and_run_container(
        self,
        scraper_type: str,
        job_id: str,
        env_vars: Optional[Dict[str, str]] = None
    ) -> Container:
        """
        Create and run a Docker container for a scraper job
        
        Args:
            scraper_type: Scraper module name (e.g., "Fair_Health_Physicians")
            job_id: Unique job identifier
            env_vars: Environment variables to pass to container
        
        Returns:
            Container: Docker container object
        
        Raises:
            DockerException: If container creation fails
        """
        try:
            # Container name
            container_name = f"scraper-{job_id}"
            
            # Check if container with same name exists and remove it
            try:
                existing_container = self.client.containers.get(container_name)
                logger.warning(f"⚠️ Container {container_name} already exists. Removing it...")
                existing_container.remove(force=True)
                logger.info(f"✅ Removed existing container: {container_name}")
            except NotFound:
                # Container doesn't exist, which is fine
                pass
            except DockerException as e:
                logger.warning(f"⚠️ Error checking/removing existing container: {e}")
            
            # Prepare environment variables
            container_env = {
                "SUPABASE_URL": settings.SUPABASE_URL,
                "SUPABASE_KEY": settings.SUPABASE_KEY,
                "PYTHONUNBUFFERED": "1",  # Ensure logs are not buffered
            }
            if env_vars:
                container_env.update(env_vars)
            
            # Python command to run the scraper
            # Scripts are in backend/app/cpt_automated_scripts/
            command = f"python -m backend.app.cpt_automated_scripts.{scraper_type}.main"
            
            logger.info(f"🐳 Creating container: {container_name}")
            logger.info(f"📦 Image: {settings.DOCKER_IMAGE_NAME}")
            logger.info(f"🔧 Command: {command}")
            
            # Create and start container
            container = self.client.containers.run(
                image=settings.DOCKER_IMAGE_NAME,
                name=container_name,
                command=command,
                detach=True,  # Run in background
                remove=False,  # Don't auto-remove (we need logs)
                environment=container_env,
                working_dir="/app",
                # Resource limits
                mem_limit="2g",
                memswap_limit="2g",
                cpu_quota=100000,  # 1 CPU core
                cpu_period=100000,
                # Network
                network_mode="bridge",
                # Auto-restart on failure (max 3 times)
                restart_policy={"Name": "on-failure", "MaximumRetryCount": 3}
            )
            
            logger.info(f"✅ Container created: {container.id[:12]}")
            return container
            
        except DockerException as e:
            logger.error(f"❌ Failed to create container: {e}")
            raise
    
    def get_container(self, container_id: str) -> Optional[Container]:
        """
        Get a container by ID
        
        Args:
            container_id: Container ID
        
        Returns:
            Container: Docker container object or None if not found
        """
        try:
            return self.client.containers.get(container_id)
        except NotFound:
            logger.warning(f"Container not found: {container_id}")
            return None
        except DockerException as e:
            logger.error(f"Error getting container {container_id}: {e}")
            raise
    
    def get_container_status(self, container_id: str) -> Optional[str]:
        """
        Get container status
        
        Args:
            container_id: Container ID
        
        Returns:
            str: Container status (running, exited, etc.) or None if not found
        """
        container = self.get_container(container_id)
        if container:
            container.reload()  # Refresh container info
            return container.status
        return None
    
    def wait_for_container(self, container_id: str, timeout: Optional[int] = None) -> Dict[str, Any]:
        """
        Wait for container to finish execution
        
        Args:
            container_id: Container ID
            timeout: Maximum time to wait in seconds (None = no timeout)
        
        Returns:
            dict: Container exit status {"StatusCode": 0, "Error": None}
        """
        try:
            container = self.get_container(container_id)
            if not container:
                raise ValueError(f"Container not found: {container_id}")
            
            logger.info(f"⏳ Waiting for container {container_id[:12]} to complete...")
            result = container.wait(timeout=timeout)
            
            status_code = result.get("StatusCode", -1)
            if status_code == 0:
                logger.info(f"✅ Container {container_id[:12]} completed successfully")
            else:
                logger.error(f"❌ Container {container_id[:12]} failed with code {status_code}")
            
            return result
            
        except DockerException as e:
            logger.error(f"Error waiting for container {container_id}: {e}")
            raise
    
    def get_container_logs(
        self,
        container_id: str,
        stream: bool = False,
        follow: bool = False,
        tail: Optional[int] = None
    ):
        """
        Get container logs
        
        Args:
            container_id: Container ID
            stream: Stream logs in real-time
            follow: Follow log output (requires stream=True)
            tail: Number of lines from end of logs (None = all)
        
        Returns:
            Generator or bytes: Log output
        """
        try:
            container = self.get_container(container_id)
            if not container:
                raise ValueError(f"Container not found: {container_id}")
            
            return container.logs(
                stdout=True,
                stderr=True,
                stream=stream,
                follow=follow,
                tail=tail if tail else "all"
            )
            
        except DockerException as e:
            logger.error(f"Error getting logs for container {container_id}: {e}")
            raise
    
    def stop_container(self, container_id: str, timeout: int = 10) -> bool:
        """
        Stop a running container
        
        Args:
            container_id: Container ID
            timeout: Seconds to wait before killing
        
        Returns:
            bool: True if stopped successfully
        """
        try:
            container = self.get_container(container_id)
            if not container:
                logger.warning(f"Container not found: {container_id}")
                return False
            
            logger.info(f"🛑 Stopping container {container_id[:12]}...")
            container.stop(timeout=timeout)
            logger.info(f"✅ Container stopped: {container_id[:12]}")
            return True
            
        except DockerException as e:
            logger.error(f"Error stopping container {container_id}: {e}")
            raise
    
    def remove_container(self, container_id: str, force: bool = False) -> bool:
        """
        Remove a container
        
        Args:
            container_id: Container ID
            force: Force removal even if running
        
        Returns:
            bool: True if removed successfully
        """
        try:
            container = self.get_container(container_id)
            if not container:
                logger.warning(f"Container not found: {container_id}")
                return False
            
            logger.info(f"🗑️ Removing container {container_id[:12]}...")
            container.remove(force=force)
            logger.info(f"✅ Container removed: {container_id[:12]}")
            return True
            
        except DockerException as e:
            logger.error(f"Error removing container {container_id}: {e}")
            raise
    
    def cleanup_container(self, container_id: str) -> bool:
        """
        Stop and remove a container
        
        Args:
            container_id: Container ID
        
        Returns:
            bool: True if cleaned up successfully
        """
        try:
            container = self.get_container(container_id)
            if not container:
                return False
            
            # Check if running
            container.reload()
            if container.status == "running":
                self.stop_container(container_id)
            
            # Remove container
            return self.remove_container(container_id)
            
        except DockerException as e:
            logger.error(f"Error cleaning up container {container_id}: {e}")
            return False
    
    def list_scraper_containers(self) -> list[Container]:
        """
        List all scraper containers
        
        Returns:
            list: List of scraper containers
        """
        try:
            # Get all containers with name starting with "scraper-"
            containers = self.client.containers.list(
                all=True,
                filters={"name": "scraper-"}
            )
            return containers
        except DockerException as e:
            logger.error(f"Error listing containers: {e}")
            raise
    
    def get_docker_info(self) -> Dict[str, Any]:
        """
        Get Docker system information
        
        Returns:
            dict: Docker system info
        """
        try:
            return self.client.info()
        except DockerException as e:
            logger.error(f"Error getting Docker info: {e}")
            raise
    
    def close(self):
        """Close Docker client connection"""
        try:
            self.client.close()
            logger.info("Docker client connection closed")
        except Exception as e:
            logger.error(f"Error closing Docker client: {e}")


# Global Docker service instance
_docker_service: Optional[DockerService] = None


def get_docker_service() -> DockerService:
    """
    Get or create Docker service instance
    
    Returns:
        DockerService: Docker service singleton
    """
    global _docker_service
    if _docker_service is None:
        _docker_service = DockerService()
    return _docker_service

