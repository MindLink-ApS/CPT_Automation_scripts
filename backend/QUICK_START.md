# Quick Start Guide - CPT Automation Scripts

## 🚀 Get Started in 5 Minutes

### Prerequisites

- Python 3.11+
- Docker & Docker Compose
- Supabase account (for database)

---

## 📦 Installation

### 1. Clone & Setup

```bash
cd backend
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
pip install -r requirements.txt
playwright install chromium
```

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env with your Supabase credentials
```

### 3. Setup Database

```sql
-- Run in Supabase SQL Editor
CREATE TABLE scraper_jobs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    job_id VARCHAR(50) UNIQUE NOT NULL,
    scraper_name VARCHAR(100) NOT NULL,
    scraper_type VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    requested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    approved_at TIMESTAMP WITH TIME ZONE,
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    container_id VARCHAR(100),
    error_message TEXT,
    records_processed INTEGER,
    created_by VARCHAR(100) DEFAULT 'system',
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

---

## 🏃 Running the Application

### Option 1: Local Development (Fastest)

```bash
# Set execution mode to local
echo "EXECUTION_MODE=local" >> .env

# Start backend
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

**Pros:** Fast, no Docker build needed, easy debugging
**Cons:** No container isolation

---

### Option 2: Docker Development

```bash
# Build and run
docker-compose up -d

# View logs
docker-compose logs -f backend
```

**Pros:** Container isolation, closer to production
**Cons:** Slower iteration, requires Docker rebuild

---

### Option 3: Docker Production (Optimized)

```bash
# Build optimized image
./build-optimized.sh

# Run with optimized compose
docker-compose -f docker-compose.optimized.yml up -d
```

**Pros:** Smallest image, production-ready, secure
**Cons:** Longest build time

---

## 🧪 Testing

### 1. Check Health

```bash
curl http://localhost:8000/health
```

### 2. List Available Scrapers

```bash
curl http://localhost:8000/api/scraper/list | jq
```

### 3. Request a Job

```bash
curl -X POST http://localhost:8000/api/scraper/request \
  -H "Content-Type: application/json" \
  -d '{"scraper_name": "FairHealth Physician"}' | jq
```

### 4. Get Pending Jobs

```bash
curl http://localhost:8000/api/scraper/pending | jq
```

### 5. Approve a Job

```bash
# Replace JOB_ID with actual job ID from step 3
curl -X POST http://localhost:8000/api/scraper/approve/JOB_ID | jq
```

### 6. Watch Logs in Real-time

```bash
# Replace JOB_ID with actual job ID
curl -N http://localhost:8000/api/scraper/logs/JOB_ID
```

### 7. Check Job Status

```bash
curl http://localhost:8000/api/scraper/job/JOB_ID | jq
```

---

## 📊 API Documentation

Open in browser: http://localhost:8000/docs

Interactive Swagger UI with all endpoints documented.

---

## 🔧 Common Commands

### View Backend Logs

```bash
# Local
tail -f /tmp/uvicorn.log

# Docker
docker-compose logs -f backend
```

### Rebuild Docker Image

```bash
docker-compose build --no-cache
docker-compose up -d
```

### Stop Everything

```bash
# Docker
docker-compose down

# Local
pkill -f "uvicorn app.main:app"
```

### Clean Up Containers

```bash
# Remove all scraper containers
docker ps -a | grep scraper | awk '{print $1}' | xargs docker rm -f
```

---

## 🎯 Quick Flow Diagram

```
User Request → API → Create Job (pending) → User Approves → 
Docker Container Created → Scraper Runs → Data Stored → 
Job Completed → Container Cleaned Up
```

---

## 📁 Project Structure

```
backend/
├── app/
│   ├── main.py                    # FastAPI entry point
│   ├── api/
│   │   ├── routes.py              # API endpoints
│   │   ├── models.py              # Pydantic models
│   │   └── streaming.py           # SSE log streaming
│   ├── core/
│   │   ├── config.py              # Settings
│   │   ├── database.py            # Supabase client
│   │   └── scheduler.py           # Cron jobs
│   ├── services/
│   │   ├── docker_service.py      # Docker API wrapper
│   │   └── scraper_service.py     # Job orchestration
│   ├── repositories/
│   │   └── job_repository.py      # Database operations
│   └── cpt_automated_scripts/     # Scraper scripts
│       ├── Fair_Health_Physicians/
│       ├── Fair_Health_Facility/
│       ├── Medicare_Clinical_Fees/
│       ├── Medicare_ASC_Addenda/
│       ├── Novitas/
│       └── New_Jersey_DOBI/
├── Dockerfile                     # Standard Docker image
├── Dockerfile.optimized           # Optimized image (production)
├── docker-compose.yml             # Standard compose
├── docker-compose.optimized.yml   # Optimized compose
├── requirements.txt               # Python dependencies
└── .env                           # Environment variables
```

---

## 🐛 Troubleshooting

### "Module not found" errors

**Solution:** Ensure all scraper directories have `__init__.py` files.

```bash
touch app/cpt_automated_scripts/__init__.py
touch app/cpt_automated_scripts/Fair_Health_Physicians/__init__.py
# ... repeat for all scraper directories
```

### "Container name already in use"

**Solution:** System auto-removes old containers. If issue persists:

```bash
docker rm -f scraper-{job_id}
```

### Logs not streaming

**Solution:** Ensure `PYTHONUNBUFFERED=1` in environment.

### Port 8000 already in use

**Solution:** Kill existing process:

```bash
lsof -ti:8000 | xargs kill -9
```

---

## 🔐 Environment Variables

```bash
# Required
SUPABASE_URL=https://xxxxx.supabase.co
SUPABASE_KEY=your_key_here

# Optional (with defaults)
EXECUTION_MODE=docker              # or "local"
DOCKER_IMAGE_NAME=cpt-scraper-image:latest
MAX_CONCURRENT_JOBS=3
JOB_TIMEOUT_SECONDS=3600
CRON_ENABLED=true
CRON_MONTH=11
CRON_DAY=25
CRON_HOUR=0
CRON_MINUTE=0
```

---

## 📚 Next Steps

1. ✅ Read [ARCHITECTURE_DETAILED.md](./ARCHITECTURE_DETAILED.md) for complete system overview
2. ✅ Read [DOCKER_COMPARISON.md](./DOCKER_COMPARISON.md) for Docker optimization details
3. ✅ Check [TESTING_GUIDE.md](./TESTING_GUIDE.md) for comprehensive testing
4. ✅ Review [DEPLOYMENT_GUIDE.md](../Docs/DEPLOYMENT_GUIDE.md) for Digital Ocean deployment
5. ✅ Use [DIGITAL_OCEAN_CHECKLIST.md](../Docs/DIGITAL_OCEAN_CHECKLIST.md) for step-by-step deployment

---

## 🆘 Need Help?

- Check logs: `docker-compose logs -f backend`
- View API docs: http://localhost:8000/docs
- Test endpoints: `python test_api.py`
- Test log streaming: Open `test_log_stream.html` in browser

---

**Happy Scraping! 🎉**

