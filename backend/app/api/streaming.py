"""
SSE (Server-Sent Events) Log Streaming
"""

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import StreamingResponse
import logging
import asyncio
from typing import AsyncGenerator

from app.repositories.job_repository import JobRepository, get_job_repository
from app.services.docker_service import get_docker_service, DockerService
from app.api.models import JobStatus

logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/api/scraper", tags=["Log Streaming"])


async def log_stream_generator(
    job_id: str,
    container_id: str,
    docker_service: DockerService
) -> AsyncGenerator[str, None]:
    """
    Generate SSE events from container logs
    
    Args:
        job_id: Job identifier
        container_id: Docker container ID
        docker_service: Docker service instance
    
    Yields:
        str: SSE formatted log events
    """
    try:
        logger.info(f"📡 Starting log stream for job {job_id} (container {container_id[:12]})")
        
        # Send initial connection message
        yield f"data: {{\"status\": \"connected\", \"job_id\": \"{job_id}\", \"message\": \"Log stream started\"}}\n\n"
        
        # Get container
        container = docker_service.get_container(container_id)
        if not container:
            yield f"data: {{\"status\": \"error\", \"job_id\": \"{job_id}\", \"message\": \"Container not found\"}}\n\n"
            return
        
        # Stream logs from container
        log_stream = container.logs(
            stdout=True,
            stderr=True,
            stream=True,
            follow=True,
            timestamps=True
        )
        
        # Stream each log line as SSE event
        for log_line in log_stream:
            try:
                # Decode log line
                log_text = log_line.decode('utf-8').strip()
                
                if log_text:
                    # Escape quotes and newlines for JSON
                    log_text = log_text.replace('"', '\\"').replace('\n', '\\n')
                    
                    # Send as SSE event
                    yield f"data: {{\"type\": \"log\", \"message\": \"{log_text}\"}}\n\n"
                    
                    # Small delay to prevent overwhelming the client
                    await asyncio.sleep(0.01)
                    
            except Exception as e:
                logger.error(f"Error processing log line: {e}")
                continue
        
        # Container finished - send completion message
        container.reload()
        exit_code = container.attrs.get('State', {}).get('ExitCode', -1)
        
        if exit_code == 0:
            yield f"data: {{\"status\": \"completed\", \"job_id\": \"{job_id}\", \"message\": \"Job completed successfully\"}}\n\n"
        else:
            yield f"data: {{\"status\": \"failed\", \"job_id\": \"{job_id}\", \"message\": \"Job failed with exit code {exit_code}\"}}\n\n"
        
        logger.info(f"✅ Log stream ended for job {job_id}")
        
    except Exception as e:
        logger.error(f"Error streaming logs for job {job_id}: {e}")
        yield f"data: {{\"status\": \"error\", \"job_id\": \"{job_id}\", \"message\": \"Stream error: {str(e)}\"}}\n\n"


@router.get("/logs/{job_id}")
async def stream_job_logs(
    job_id: str,
    repo: JobRepository = Depends(get_job_repository),
    docker_service: DockerService = Depends(get_docker_service)
):
    """
    Stream logs for a job in real-time using Server-Sent Events (SSE)
    
    Args:
        job_id: Unique job identifier
        repo: Job repository dependency
        docker_service: Docker service dependency
    
    Returns:
        StreamingResponse: SSE stream of log events
    
    Example client-side usage:
    ```javascript
    const eventSource = new EventSource('/api/scraper/logs/job-123');
    eventSource.onmessage = (event) => {
        const data = JSON.parse(event.data);
        console.log(data.message);
    };
    ```
    """
    try:
        # Get the job
        job = repo.get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        
        # Get container ID
        container_id = job.get('container_id')
        if not container_id:
            raise HTTPException(
                status_code=400,
                detail=f"Job has no container ID. Status: {job['status']}"
            )
        
        # Check if container exists
        container = docker_service.get_container(container_id)
        if not container:
            raise HTTPException(
                status_code=404,
                detail=f"Container not found: {container_id[:12]}"
            )
        
        logger.info(f"📡 Client connected to log stream for job {job_id}")
        
        # Return SSE streaming response
        return StreamingResponse(
            log_stream_generator(job_id, container_id, docker_service),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            }
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to start log stream for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/logs/{job_id}/history")
async def get_job_logs_history(
    job_id: str,
    tail: int = 100,
    repo: JobRepository = Depends(get_job_repository),
    docker_service: DockerService = Depends(get_docker_service)
):
    """
    Get historical logs for a completed job (non-streaming)
    
    Args:
        job_id: Unique job identifier
        tail: Number of lines from end of logs (default: 100)
        repo: Job repository dependency
        docker_service: Docker service dependency
    
    Returns:
        dict: Job logs as text
    """
    try:
        # Get the job
        job = repo.get_job_by_id(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job not found: {job_id}")
        
        # Get container ID
        container_id = job.get('container_id')
        if not container_id:
            raise HTTPException(
                status_code=400,
                detail=f"Job has no container ID. Status: {job['status']}"
            )
        
        # Get logs
        logs = docker_service.get_container_logs(
            container_id,
            stream=False,
            tail=tail
        )
        
        # Decode logs
        logs_text = logs.decode('utf-8') if isinstance(logs, bytes) else str(logs)
        
        return {
            "job_id": job_id,
            "container_id": container_id,
            "logs": logs_text,
            "lines": len(logs_text.split('\n'))
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get logs for job {job_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))

