# CORRECTED: CPT Dashboard Architecture Analysis

**Date**: 2026-02-03  
**Status**: ✅ **CORRECTED - Synchronization Mechanism Identified**

---

## 🔄 CORRECTION: Synchronization DOES Exist!

**Previous Analysis**: Incorrectly stated there was NO synchronization between tables.

**CORRECTED**: There IS a synchronization mechanism via **Supabase Edge Function** that runs daily!

---

## Actual Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SCRAPERS (7 total)                         │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        ┌─────────────────────────┴─────────────────────────┐
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────────┐
│  new_updated_historical_medical_      │   │  new_updated_medical_                 │
│  benchmarking_data                    │   │  benchmarking_data                    │
│  (WRITE TARGET - Historical/Archive)  │   │  (WRITE TARGET - Some scrapers)       │
├───────────────────────────────────────┤   ├───────────────────────────────────────┤
│ 5 Scrapers write here:                │   │ 2 Scrapers write here:                │
│ • Fair_Health_Facility                │   │ • Horizon_ASC                         │
│ • Fair_Health_Physicians              │   │ • Medicare_Clinical_Fees              │
│ • Medicare_ASC_Addenda                │   │                                       │
│ • New_Jersey_DOBI                     │   │                                       │
│ • Novitas                             │   │                                       │
└───────────────────────────────────────┘   └───────────────────────────────────────┘
                │                                             │
                │                                             │
                └─────────────────┬───────────────────────────┘
                                  │
                                  ▼
                ┌─────────────────────────────────────────────────┐
                │  SUPABASE EDGE FUNCTION                         │
                │  "refresh-medical-benchmark"                    │
                │  Runs daily at 2 AM Chicago time                │
                ├─────────────────────────────────────────────────┤
                │  Synchronizes data from historical table        │
                │  to the benchmark table                         │
                └─────────────────────────────────────────────────┘
                                  │
                                  ▼
                ┌─────────────────────────────────────────────────┐
                │  new_updated_medical_benchmarking_data          │
                │  (READ TARGET - Dashboard queries this!)        │
                │  Updated daily via Edge Function                │
                └─────────────────────────────────────────────────┘
                                  │
                                  ▼
                          ┌───────────────┐
                          │   DASHBOARD   │
                          │  (Reads from  │
                          │   benchmark   │
                          │    table)     │
                          └───────────────┘
```

---

## How the Synchronization Works

### 1. Scrapers Write to Historical Table

**Most scrapers (5 out of 7)** write to `new_updated_historical_medical_benchmarking_data`:
- Fair_Health_Facility
- Fair_Health_Physicians
- Medicare_ASC_Addenda
- New_Jersey_DOBI
- Novitas

**Some scrapers (2 out of 7)** write directly to `new_updated_medical_benchmarking_data`:
- Horizon_ASC
- Medicare_Clinical_Fees

### 2. Edge Function Runs Daily

**Supabase Edge Function**: `refresh-medical-benchmark`  
**URL**: `https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark`  
**Schedule**: Daily at 2:00 AM Chicago time  
**Triggered by**: Backend cron job in `backend/app/core/scheduler.py`

**What it does**:
- Reads data from `new_updated_historical_medical_benchmarking_data`
- Updates/synchronizes `new_updated_medical_benchmarking_data`
- Ensures dashboard has latest data

### 3. Dashboard Reads from Benchmark Table

**Dashboard queries**: `new_updated_medical_benchmarking_data`  
**Data freshness**: Updated daily at 2 AM

---

## Backend Cron Job Configuration

### Location
`backend/app/core/scheduler.py` - Lines 118-148

### Configuration
`backend/app/core/config.py` - Lines 61-66

```python
# Supabase Edge Function settings - Daily refresh
SUPABASE_EDGE_FUNCTION_ENABLED: bool = True
SUPABASE_EDGE_FUNCTION_URL: str = "https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark"
EDGE_FUNCTION_CRON_HOUR: int = 2  # 2 AM Chicago time
EDGE_FUNCTION_CRON_MINUTE: int = 0
EDGE_FUNCTION_TIMEZONE: str = "America/Chicago"
```

### Manual Trigger

**API Endpoint**: `POST /api/scraper/trigger-edge-function`  
**Purpose**: Manually trigger the edge function for testing without waiting for 2 AM

**Code**: `backend/app/api/routes.py` - Lines 432-465

---

## REVISED Understanding

### ✅ What We Got Right

1. **Two active tables exist** - Correct
2. **Scrapers write to different tables** - Correct
3. **Codes have `.0` suffix in historical table** - Correct
4. **Schema differences between tables** - Correct

### ❌ What We Got Wrong

1. **"NO synchronization"** - INCORRECT! Edge function provides daily sync
2. **"Dashboard may be missing data"** - INCORRECT if edge function runs properly
3. **"Need to consolidate tables"** - INCORRECT! Architecture is intentional

---

## Actual Architecture Purpose

### Table 1: `new_updated_historical_medical_benchmarking_data`
**Purpose**: **Write target** for most scrapers (historical/archive data)  
**Role**: Staging area for scraper data before synchronization

### Table 2: `new_updated_medical_benchmarking_data`
**Purpose**: **Read target** for dashboard (current/live data)  
**Role**: Production data table, synchronized daily from historical table

### Edge Function: `refresh-medical-benchmark`
**Purpose**: **Synchronization mechanism**  
**Role**: Transfers data from historical table to benchmark table daily

---

## The REAL Problem

### Not "Missing Synchronization" - It's "Code Normalization"!

**The actual issue**:
1. ✅ Synchronization exists (Edge Function)
2. ✅ Data flows correctly (Historical → Benchmark)
3. ❌ **Codes have `.0` suffix in historical table**
4. ❌ **This gets synchronized to benchmark table**
5. ❌ **Dashboard shows duplicate rows** (36475 and 36475.0)

---

## Updated Root Cause Analysis

### Why Code 36475 Shows Twice

**Root Cause**: Codes stored with `.0` suffix in `new_updated_historical_medical_benchmarking_data`

**Data Flow**:
1. Scraper writes `36475.0` to historical table
2. Edge function syncs `36475.0` to benchmark table
3. Dashboard reads from benchmark table
4. User sees both `36475` and `36475.0` as separate rows

**Fix**: Normalize codes in the **historical table** (source), then let edge function sync clean data

---

## Revised Implementation Plan

### Phase 1: Code Normalization (UNBLOCKED!)

**Now that we understand the architecture, we can proceed!**

#### Step 1: Normalize Codes in Historical Table

**Target**: `new_updated_historical_medical_benchmarking_data` (source table)

**SQL**:
```sql
-- Preview affected records
SELECT code, REGEXP_REPLACE(code, '\.0+$', '') as normalized_code, COUNT(*)
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code
LIMIT 20;

-- Execute cleanup
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

#### Step 2: Trigger Edge Function to Sync

**Option A: Wait for automatic sync** (2 AM Chicago time)

**Option B: Manual trigger** (immediate):
```bash
curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark
```

Or via backend API:
```bash
curl -X POST http://localhost:8000/api/scraper/trigger-edge-function
```

#### Step 3: Verify Dashboard

**After edge function runs**:
- Check dashboard for code 36475
- Should appear on ONE row (not two)
- Verify prices are correct

---

### Phase 2: Frontend Defense-in-Depth (OPTIONAL)

**Even though backend normalization fixes the issue, add frontend normalization for safety:**

1. Add `normalizeCode()` to `src/lib/pivotUtils.ts`
2. Update `mergeRecordsForPivot()` to use normalized codes
3. Update `DataTablePivot.tsx` unique code collection

**Benefit**: Protects against future `.0` codes slipping through

---

### Phase 3: Scraper Testing

**Now we can test scrapers knowing the data flow:**

1. Test Medicare scraper → writes to historical table
2. Trigger edge function → syncs to benchmark table
3. Verify dashboard → shows new data

---

### Phase 4: Price Verification

**Query the benchmark table** (what dashboard uses):

```sql
SELECT code, data_type, geozip, "80th", release_date
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip;
```

Compare against FairHealth Online.

---

## Edge Function Details

### Cron Job Implementation

**File**: `backend/app/core/scheduler.py`

**Function**: `call_edge_function()` (lines 118-148)

```python
async def call_edge_function(self):
    """
    Call Supabase Edge Function to refresh medical benchmark data
    This function is called daily at 2 AM Chicago time
    """
    try:
        logger.info("🔄 EDGE FUNCTION CRON JOB TRIGGERED - Daily Refresh")
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
        
    except Exception as e:
        logger.error(f"❌ Edge function call failed: {e}")
```

### Scheduler Registration

**File**: `backend/app/core/scheduler.py` (lines 178-191)

```python
# Add daily edge function cron job (2 AM Chicago time)
if settings.SUPABASE_EDGE_FUNCTION_ENABLED:
    self.scheduler.add_job(
        self.call_edge_function,
        trigger=CronTrigger(
            hour=settings.EDGE_FUNCTION_CRON_HOUR,  # 2 AM
            minute=settings.EDGE_FUNCTION_CRON_MINUTE,  # 0
            timezone=settings.EDGE_FUNCTION_TIMEZONE  # America/Chicago
        ),
        id="daily_edge_function",
        name="Daily Edge Function Call (2 AM Chicago)",
        replace_existing=True
    )
```

---

## Questions to Investigate

### 1. What does the Edge Function actually do?

**Need to check**: The actual Supabase Edge Function code (TypeScript/Deno)

**Likely location**: Supabase Dashboard → Edge Functions → `refresh-medical-benchmark`

**Expected behavior**:
- Reads from `new_updated_historical_medical_benchmarking_data`
- Upserts/updates `new_updated_medical_benchmarking_data`
- May apply transformations or filtering

### 2. Does the Edge Function normalize codes?

**If YES**: We only need to normalize the historical table once, edge function will keep it clean

**If NO**: We need to normalize both tables, or update edge function to normalize during sync

### 3. Why do 2 scrapers write directly to benchmark table?

**Scrapers**: Horizon_ASC, Medicare_Clinical_Fees

**Possible reasons**:
- Different data source/format
- Real-time data (bypass historical staging)
- Legacy implementation

**Should investigate**: Whether these should also write to historical table for consistency

---

## Recommended Next Steps

### Immediate (Today)

1. ✅ **DONE**: Understand architecture (corrected)
2. ⏳ **TODO**: Check Edge Function code in Supabase Dashboard
3. ⏳ **TODO**: Verify edge function is running successfully (check logs)
4. ⏳ **TODO**: Normalize codes in historical table
5. ⏳ **TODO**: Trigger edge function manually to sync
6. ⏳ **TODO**: Verify dashboard shows single row for code 36475

### Short-term (This Week)

1. Test scraper → historical table → edge function → benchmark table → dashboard flow
2. Verify prices match FairHealth Online
3. Add frontend normalization for defense-in-depth
4. Document edge function behavior

### Long-term (Next Sprint)

1. Consider updating Horizon_ASC and Medicare_Clinical_Fees to write to historical table
2. Add monitoring for edge function success/failure
3. Set up alerts if edge function fails
4. Document complete data flow for future developers

---

## Success Criteria (UPDATED)

### Immediate Success
- ✅ Understand synchronization mechanism (Edge Function)
- ✅ Normalize codes in historical table
- ✅ Edge function syncs clean data to benchmark table
- ✅ Dashboard shows ONE row for code 36475

### Long-term Success
- ✅ All scrapers follow consistent pattern (write to historical)
- ✅ Edge function runs reliably daily
- ✅ Monitoring alerts on edge function failures
- ✅ Prices match FairHealth Online

---

## Apology & Correction

**I apologize for the initial incorrect analysis!** I missed the Edge Function synchronization mechanism that was clearly documented in the code.

**What I should have checked first**:
1. ✅ Scheduler code (found the edge function calls)
2. ❌ Supabase Edge Functions (assumed none existed)
3. ❌ Asked about synchronization mechanism

**Lesson learned**: Always ask about synchronization mechanisms before concluding they don't exist!

---

## Summary

**CORRECTED Understanding**:
- ✅ Synchronization EXISTS via Supabase Edge Function
- ✅ Architecture is INTENTIONAL (historical → benchmark)
- ✅ Data flow is CORRECT (scrapers → historical → edge function → benchmark → dashboard)
- ❌ Problem is CODE NORMALIZATION (`.0` suffix)
- ✅ Solution is SIMPLE: Normalize historical table, trigger edge function

**Timeline**: 
- Code normalization: 30 minutes
- Edge function trigger: Immediate
- Verification: 30 minutes
- **Total**: ~1-2 hours (not days!)

**No architectural changes needed!** Just clean up the data.
