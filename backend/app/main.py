"""
FastAPI Main Application
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging

from app.core.config import settings
from app.core.database import db

# Configure logging
logging.basicConfig(
    level=logging.INFO if not settings.DEBUG else logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events
    """
    # Startup
    logger.info("🚀 Starting FastAPI application...")
    logger.info(f"📝 App Name: {settings.APP_NAME}")
    logger.info(f"📝 Version: {settings.APP_VERSION}")
    logger.info(f"📝 Debug Mode: {settings.DEBUG}")
    
    # Initialize database connection
    try:
        db.get_client()
        logger.info("✅ Database connection established")
    except Exception as e:
        logger.error(f"❌ Failed to connect to database: {e}")
        raise
    
    # Initialize cron scheduler
    try:
        from app.core.scheduler import get_scheduler
        scheduler = get_scheduler()
        scheduler.start()
    except Exception as e:
        logger.error(f"❌ Failed to start cron scheduler: {e}")
    
    # Initialize Docker service
    try:
        from app.services.docker_service import get_docker_service
        docker_service = get_docker_service()
        logger.info("✅ Docker service initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Docker service: {e}")
    
    logger.info("✅ Application startup complete")
    
    yield
    
    # Shutdown
    logger.info("🛑 Shutting down FastAPI application...")
    
    # Stop scheduler
    try:
        from app.core.scheduler import get_scheduler
        scheduler = get_scheduler()
        scheduler.stop()
    except Exception as e:
        logger.error(f"Error stopping scheduler: {e}")
    
    # Close database
    db.close()
    
    # Close Docker service
    try:
        from app.services.docker_service import get_docker_service
        docker_service = get_docker_service()
        docker_service.close()
    except Exception as e:
        logger.error(f"Error closing Docker service: {e}")
    
    logger.info("✅ Application shutdown complete")


# Create FastAPI application
# Disable docs in production for security
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    description="Backend API for CPT Automation Scripts with job approval workflow and real-time log streaming",
    lifespan=lifespan,
    docs_url="/docs" if settings.DEBUG else None,
    redoc_url="/redoc" if settings.DEBUG else None,
    openapi_url="/openapi.json" if settings.DEBUG else None
)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=settings.CORS_CREDENTIALS,
    allow_methods=settings.CORS_METHODS,
    allow_headers=settings.CORS_HEADERS,
)

# Include API routers
from app.api.routes import router as scraper_router
from app.api.streaming import router as streaming_router

app.include_router(scraper_router)
app.include_router(streaming_router)


@app.get("/")
async def root():
    """
    Root endpoint
    """
    response = {
        "message": "CPT Automation Scripts API",
        "version": settings.APP_VERSION
    }
    
    # Only expose docs URL in development
    if settings.DEBUG:
        response["docs"] = "/docs"
    
    return response


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    try:
        # Test database connection
        db_client = db.get_client()
        
        return {
            "status": "healthy",
            "version": settings.APP_VERSION,
            "database": "connected"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "version": settings.APP_VERSION,
            "database": "disconnected",
            "error": str(e)
        }


if __name__ == "__main__":
    import uvicorn
    
    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=settings.BACKEND_PORT,
        reload=settings.DEBUG,
        log_level="debug" if settings.DEBUG else "info"
    )

