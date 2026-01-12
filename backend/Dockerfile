# ============================================================================
# Dockerfile for CPT Automation Scripts Backend
# ============================================================================
# This image contains both the FastAPI backend AND the scraper scripts
# It will be used to:
# 1. Run the FastAPI server
# 2. Run individual scraper jobs in containers
# ============================================================================

FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    # Required for Playwright
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    # Other utilities
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better caching)
COPY backend/requirements.txt /app/backend/requirements.txt

# Install Python dependencies
RUN pip install --no-cache-dir -r /app/backend/requirements.txt

# Install Playwright browsers
RUN playwright install chromium
RUN playwright install-deps chromium

# Copy the entire project
COPY . /app/

# Set Python path to include both backend and root
ENV PYTHONPATH=/app:/app/backend:$PYTHONPATH

# Expose FastAPI port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

# Default command (can be overridden)
# For FastAPI server: CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]
# For scraper: CMD ["python", "-m", "cpt_automated_scripts.Fair_Health_Physicians.main"]
CMD ["uvicorn", "backend.app.main:app", "--host", "0.0.0.0", "--port", "8000"]

