# Price Verification & Remaining Tasks

**Date**: 2026-02-03  
**Status**: ✅ Duplicate rows fixed, now verifying prices

---

## Current Status

Based on your screenshot, I can see:
- **Latest Facility data**: August 2025 (11,931 records)
- **Issue**: Andrew's screenshot shows July 2025 FairHealth data, but you have August 2025

---

## Task 1: Verify Code 36475 Prices

### Step 1: Check Current Data for Code 36475

Run this query in Supabase SQL Editor:

```sql
-- Get all data for code 36475
SELECT 
  code,
  data_type,
  geozip,
  "80th" as price_80th,
  "90th" as price_90th,
  release_date,
  source,
  scraped_at
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip, data_type;
```

### Step 2: Compare Against FairHealth Online

**From Andrew's screenshot (July 2025 FairHealth Online)**:
- **Geozip 070**: Facility 80th = $10,761.00
- **Geozip 074**: Facility 80th = $25,139.00
- **USA**: Facility 80th = $10,761.00

**What dashboard was showing (WRONG)**:
- Geozip 070: $2,356
- Geozip 074: $5,678
- USA: $2,396

### Step 3: Identify the Issue

**Possible causes**:

1. **Different release dates**
   - Andrew compared against July 2025
   - Your data is August 2025
   - Prices may have changed between releases

2. **Wrong data source**
   - Data might be from a different source (not FairHealth Facility)
   - Check the `source` column

3. **Missing latest data**
   - FairHealth scraper may not have run for latest release
   - Need to check if January 2026 data exists

4. **Data type mismatch**
   - Dashboard might be showing wrong data_type
   - Check if it's showing "Facility 070" vs "Physician 070"

---

## Task 2: Check Available FairHealth Releases

### Query All FairHealth Facility Releases

```sql
-- Check what FairHealth Facility releases we have
SELECT 
  DISTINCT release_date,
  source,
  data_type,
  COUNT(*) as record_count,
  MIN(scraped_at) as first_scraped,
  MAX(scraped_at) as last_scraped
FROM new_updated_medical_benchmarking_data
WHERE source LIKE '%Fair%'
  AND data_type LIKE 'Facility%'
GROUP BY release_date, source, data_type
ORDER BY release_date DESC;
```

### Expected Results

**If FairHealth scraper is working**:
- Should see January 2026 release (latest)
- Should see previous releases (Nov 2025, Aug 2025, Jul 2025, etc.)

**If missing January 2026**:
- Need to run FairHealth ASC scraper
- See Task 4 below

---

## Task 3: Verify Data in Historical Table

### Check Historical Table for Code 36475

```sql
-- Check historical table (where scrapers write)
SELECT 
  code,
  data_type,
  geozip,
  "80th" as price_80th,
  release_date,
  source,
  scraped_at
FROM new_updated_historical_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY scraped_at DESC, geozip;
```

### Check Historical Table Releases

```sql
-- Check what releases are in historical table
SELECT 
  DISTINCT release_date,
  source,
  data_type,
  COUNT(*) as record_count,
  MAX(scraped_at) as last_scraped
FROM new_updated_historical_medical_benchmarking_data
WHERE source LIKE '%Fair%'
  AND data_type LIKE 'Facility%'
GROUP BY release_date, source, data_type
ORDER BY release_date DESC;
```

---

## Task 4: Run FairHealth ASC Scraper (If Needed)

### Check If January 2026 Data Exists

```sql
-- Look for January 2026 FairHealth data
SELECT COUNT(*) as jan_2026_records
FROM new_updated_medical_benchmarking_data
WHERE source LIKE '%Fair%'
  AND (
    release_date LIKE '%Jan%2026%' 
    OR release_date LIKE '%January%2026%'
    OR release_date LIKE '%2026-01%'
  );
```

### If Missing, Trigger FairHealth Scraper

**Via Backend API** (assuming backend is running):

```bash
# List available scrapers
curl http://localhost:8000/api/scraper/list

# Request FairHealth ASC scraper job
curl -X POST http://localhost:8000/api/scraper/request \
  -H "Content-Type: application/json" \
  -d '{
    "scraper_name": "FairHealth ASC",
    "created_by": "admin"
  }'

# Get pending jobs
curl http://localhost:8000/api/scraper/pending

# Approve the job (replace JOB_ID with actual ID from previous response)
curl -X POST http://localhost:8000/api/scraper/approve/JOB_ID

# Monitor job status
curl http://localhost:8000/api/scraper/history
```

### After Scraper Completes

1. **Trigger edge function** to sync new data:
   ```bash
   curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark
   ```

2. **Re-run verification queries** to check for January 2026 data

---

## Task 5: Verify Scraper Configuration

### Check FairHealth Credentials

**File**: `backend/.env`

**Required variables**:
```bash
FAIRHEALTH_EMAIL=your-email@example.com
FAIRHEALTH_PASSWORD=your-password
PROXY_SERVER=your-proxy-server
PROXY_USERNAME=your-proxy-username
PROXY_PASSWORD=your-proxy-password
```

### Check Scraper Database Handler

**File**: `backend/app/cpt_automated_scripts/Fair_Health_Facility/database.py`

**Verify it writes to**: `new_updated_historical_medical_benchmarking_data`

```python
# Should see this line:
self.table_name = "new_updated_historical_medical_benchmarking_data"
```

---

## Task 6: Frontend Verification (Optional)

### Check What Dashboard Queries

**If you have access to frontend code**, search for:

```bash
# Search for table references
grep -r "new_updated_medical_benchmarking_data" frontend/src/

# Search for code 36475 or data queries
grep -r "36475" frontend/src/
grep -r "medical_benchmarking" frontend/src/
```

### Verify Percentile Display Logic

**From original plan** (`cpt_dashboard_implementation_plan_0e148abe.plan.md`):

**Expected behavior**:
- "Physician USA" → displays 90th percentile
- All other data types → displays 80th percentile

**Location**: `src/lib/pivotUtils.ts` line 52

```typescript
const value = row.data_type === "Physician USA" ? row["90th"] : row["80th"];
```

---

## Remaining Tasks from Original Plan

### ✅ Completed
- [x] B1: Map plan SQL to Supabase tables (verified via scripts)
- [x] B4: Normalize codes ending in `.0` (DONE - edge function triggered)
- [x] Frontend: Add `normalizeCode()` function (optional, but recommended)

### ⏳ In Progress
- [ ] B2: Run investigative SQL for code 36475 (doing now)
- [ ] B3: Check available FairHealth Facility releases (doing now)
- [ ] B5: Verify prices match FairHealth Online (doing now)

### 🔜 To Do
- [ ] C1: Verify backend secrets exist (FairHealth credentials, proxy)
- [ ] C4: Test FairHealth ASC scraper (if January 2026 missing)
- [ ] C5: Re-verify code 36475 after fresh scrape
- [ ] Frontend: Implement code normalization (defense-in-depth)

### ❌ Skipped (Per Your Request)
- [ ] C2: Validate scraper orchestration via API (Docker handles this)
- [ ] C3: Test Medicare scraper (Docker handles this)
- [ ] C6: Operational monitoring & documentation (DevOps task)

---

## Immediate Next Steps

### Step 1: Run Price Verification Queries (5 min)

Run the queries in **Task 1** and **Task 2** above to:
1. Check current prices for code 36475
2. Identify which release dates you have
3. Compare against FairHealth Online

### Step 2: Identify Price Discrepancy (10 min)

Based on query results, determine:
- Are prices correct for August 2025?
- Is January 2026 data missing?
- Is the data from the right source?

### Step 3: Take Action (varies)

**If prices are correct for August 2025**:
- ✅ Done! Just need to update Andrew that data is from newer release

**If January 2026 data is missing**:
- Run FairHealth ASC scraper (Task 4)
- Wait for completion
- Trigger edge function
- Verify new data

**If prices are still wrong**:
- Check data source (might be from wrong scraper)
- Check if FairHealth scraper is configured correctly
- May need to re-run scraper

---

## Success Criteria

### Immediate
- ✅ Duplicate rows fixed (DONE)
- [ ] Prices for code 36475 match FairHealth Online
- [ ] Latest release data (January 2026) exists in database

### Complete
- [ ] All FairHealth Facility releases present
- [ ] Scrapers configured with correct credentials
- [ ] Edge function syncing data successfully
- [ ] Dashboard shows accurate, up-to-date prices

---

## Timeline Estimate

| Task | Time | Status |
|------|------|--------|
| Run verification queries | 5 min | Ready |
| Analyze results | 10 min | Ready |
| Run FairHealth scraper (if needed) | 30-60 min | Depends |
| Verify prices match | 10 min | After scraper |
| Frontend normalization (optional) | 30 min | Optional |

**Total**: 1-2 hours (if scraper needed), or 30 min (if just verification)

---

## Questions to Answer

1. **What release date is in your database for code 36475?**
   - Run Task 1 query to find out

2. **Do you have January 2026 FairHealth data?**
   - Run Task 2 query to check

3. **What are the actual prices for code 36475 in your database?**
   - Compare against FairHealth Online for same release date

4. **Are FairHealth credentials configured?**
   - Check `backend/.env` file

---

## Next Action

**Run this query first**:

```sql
SELECT 
  code,
  data_type,
  geozip,
  "80th" as price_80th,
  release_date,
  source,
  scraped_at
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
ORDER BY scraped_at DESC, geozip;
```

**Then share the results** so we can determine next steps!
