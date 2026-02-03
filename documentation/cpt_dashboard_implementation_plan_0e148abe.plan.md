---
name: CPT Dashboard Implementation Plan
overview: Split implementation plan for frontend UI fixes (duplicate rows, display issues) and backend data verification (price matching against FairHealth source, scraper testing with January 2026 data).
todos:
  - id: fe-normalize-function
    content: Add normalizeCode() utility function to src/lib/pivotUtils.ts
    status: pending
  - id: fe-merge-logic
    content: Update mergeRecordsForPivot() to use normalized codes for merge keys
    status: pending
  - id: fe-datatable-collection
    content: Update DataTablePivot.tsx unique code collection to normalize codes
    status: pending
  - id: fe-verify-no-duplicates
    content: Test that code 36475 appears on single row after frontend fix
    status: pending
  - id: db-query-36475
    content: Run SQL queries to analyze code 36475 data and identify price discrepancy source
    status: pending
  - id: db-check-releases
    content: Query database to see what release dates exist for FairHealth Facility data
    status: pending
  - id: db-normalize-codes
    content: Run SQL to normalize all codes ending in .0 in both current and historical tables
    status: pending
  - id: scraper-verify-secrets
    content: Verify all required secrets exist in GitHub Actions and Supabase Edge Functions
    status: pending
  - id: scraper-test-medicare
    content: Test Medicare scraper (public source) to verify pipeline works
    status: pending
  - id: scraper-test-fairhealth
    content: Test FairHealth ASC scraper to ingest January 2026 data
    status: pending
  - id: verify-prices-match
    content: Compare database values for 36475 against FairHealth Online source to confirm match
    status: pending
isProject: false
---

# CPT Dashboard Implementation Plan

## Goal

1. Fix UI issues Andrew identified (duplicate rows, display)
2. Verify database prices match online source prices
3. Confirm scrapers work and can ingest January 2026 FairHealth data

---

## Work Stream A: Frontend Fixes (Developer Task)

### Issue 1: Duplicate Row Display (36475 vs 36475.0)

**Root Cause**: CPT codes stored inconsistently as TEXT with some having `.0` suffix

**Files to Modify**:

- `src/lib/pivotUtils.ts`
- `src/components/dashboard/DataTablePivot.tsx`

**Implementation**:

1. Add normalization function to `pivotUtils.ts`:

```typescript
// Add at top of file
export function normalizeCode(code: string | null | undefined): string {
  if (!code) return '';
  return String(code).replace(/\.0+$/, '').trim();
}
```

1. Update merge key creation in `mergeRecordsForPivot()` (line 39):

```typescript
// Change from:
const key = `${row.code}-${specialty}`;

// To:
const normalizedCode = normalizeCode(row.code);
const key = `${normalizedCode}-${specialty}`;

// Also update pivotRow creation:
pivotMap.set(key, {
  code: normalizedCode,  // Use normalized
  code_description: row.code_description || '',
  speciality: specialty,
});
```

1. Update `DataTablePivot.tsx` unique code collection (lines 89, 143):

```typescript
// Import the function
import { mergeRecordsForPivot, normalizeCode, type PivotRow } from "@/lib/pivotUtils";

// Change from:
if (item.code) allCodesSet.add(item.code);

// To:
if (item.code) allCodesSet.add(normalizeCode(item.code));
```

**Expected Result**: Code 36475 appears on ONE row, not two.

---

### Issue 2: Ensure Correct Percentile Display

**Current Logic** (already correct but verify):

- "Physician USA" → displays 90th percentile
- All other data types → displays 80th percentile

**Location**: `src/lib/pivotUtils.ts` line 52

```typescript
const value = row.data_type === "Physician USA" ? row["90th"] : row["80th"];
```

**Verification**: Confirm this matches Andrew's expectation.

---

### Issue 3: Column Header Clarity

**Current**: Headers show data type names
**Andrew's Preference**: May want percentile indicator

**Location**: `src/components/dashboard/DataTablePivot.tsx` lines 789-793

```typescript
{allDataTypes.map(dataType => (
  <TableHead key={dataType} className="...">
    {dataType.replace(/\s*\(80th\)/gi, '')}
    {dataType === "Physician USA" && <span className="...">(90th)</span>}
  </TableHead>
))}
```

**Action**: Keep as-is or add "(80th)" indicator to non-Physician USA columns per Andrew's feedback.

---

## Work Stream B: Database Price Verification (Data Task)

### Step 1: Query Current Data for Code 36475

Run in Supabase SQL Editor:

```sql
-- Get all records for code 36475 variants
SELECT 
  code, 
  data_type, 
  geozip, 
  "80th", 
  "90th",
  source,
  release_date,
  rel_date,
  scraped_at
FROM new_updated_medical_benchmarking_data
WHERE code LIKE '36475%'
ORDER BY code, data_type, geozip;
```

### Step 2: Compare Against FairHealth Source

From Andrew's screenshot (FairHealth Online - Jul 2025 Release):


| Geozip | Record Type | Expected 80th | What Dashboard Shows |
| ------ | ----------- | ------------- | -------------------- |
| 070    | Facility    | $10,761.00    | $2,356 (WRONG)       |
| 074    | Facility    | $25,139.00    | $5,678 (WRONG)       |
| USA    | Facility    | $10,761.00    | $2,396 (WRONG)       |


**Observation**: Dashboard values are ~4-5x lower than source. This suggests:

- Data may be from an older release
- Data may be from wrong source/data_type
- Scraper may have ingested incorrectly

### Step 3: Check Release Dates in Database

```sql
-- Check what releases we have for FairHealth Facility
SELECT DISTINCT 
  source,
  data_type,
  release_date,
  rel_date,
  COUNT(*) as record_count
FROM new_updated_medical_benchmarking_data
WHERE data_type LIKE 'Facility%'
GROUP BY source, data_type, release_date, rel_date
ORDER BY release_date DESC;
```

### Step 4: Identify Data Discrepancy Root Cause

```sql
-- Check if we have multiple versions of 36475
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  release_date,
  scraped_at,
  source
FROM new_updated_medical_benchmarking_data
WHERE code IN ('36475', '36475.0')
ORDER BY scraped_at DESC;
```

### Step 5: Normalize Code Format in Database

After verification, run cleanup:

```sql
-- Preview affected records
SELECT code, REGEXP_REPLACE(code, '\.0+$', '') as normalized_code, COUNT(*)
FROM new_updated_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code;

-- Execute cleanup (after review)
UPDATE new_updated_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';

-- Also update historical table
UPDATE historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

---

## Work Stream C: Scraper Testing & January 2026 Ingestion

### Step 1: Verify GitHub Secrets Exist

In GitHub repo → Settings → Secrets → Actions, confirm:

- `SUPABASE_URL`
- `SUPABASE_SERVICE_KEY`
- `FAIRHEALTH_EMAIL`
- `FAIRHEALTH_PASSWORD`
- `PROXY_SERVER`
- `PROXY_USERNAME`
- `PROXY_PASSWORD`

### Step 2: Verify Supabase Edge Function Secret

In Supabase Dashboard → Edge Functions → scraper-api → Secrets:

- `CPT_SCRAPER_API_URL` must point to running backend

### Step 3: Test Public Scraper First (Medicare)

1. Go to Admin panel in app
2. Request a Medicare Facility scrape
3. Approve the job
4. Monitor logs for success/failure
5. Verify data appears in database

### Step 4: Test FairHealth ASC Scraper

1. Request FairHealth ASC scrape from Admin panel
2. Approve job
3. Monitor for:
  - Successful login to FairHealth
  - Proxy connection working
  - Data download completing
  - Database insertion success

### Step 5: Verify January 2026 Data

After scraper runs:

```sql
-- Check for January 2026 release
SELECT DISTINCT 
  release_date,
  rel_date,
  data_type,
  COUNT(*) as records
FROM new_updated_medical_benchmarking_data
WHERE source = 'FairHealth_ASC'
  AND (release_date LIKE '%2026%' OR rel_date LIKE '%2026%' OR rel_date LIKE '%Jan%2026%')
GROUP BY release_date, rel_date, data_type;
```

### Step 6: Re-verify Code 36475 After Fresh Scrape

```sql
SELECT code, data_type, geozip, "80th", release_date
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip;
```

Compare against FairHealth Online (Jan 2026 release) to confirm match.

---

## Implementation Order

```
Week 1: Frontend + Investigation
├─ Day 1-2: Developer implements code normalization fix
├─ Day 2-3: Run DB queries to understand current data state
├─ Day 3: Verify GitHub/Supabase secrets are configured
└─ Day 4: Test Medicare scraper (public, no credentials needed)

Week 2: Scraper Testing + Verification
├─ Day 1: Test FairHealth ASC scraper with Jan 2026 data
├─ Day 2: Compare scraped data against FairHealth Online
├─ Day 3: Run DB cleanup (normalize .0 codes)
├─ Day 4: Final verification with Andrew
└─ Day 5: Document any remaining discrepancies
```

---

## Deliverables Checklist

### Frontend (Developer)

- `normalizeCode()` function added to `pivotUtils.ts`
- Merge logic updated to use normalized codes
- `DataTablePivot.tsx` unique code collection normalized
- Code 36475 displays on single row (verified)
- No linter errors after changes

### Database (Data Task)

- Query results for code 36475 documented
- Root cause of price mismatch identified
- Codes with `.0` suffix normalized in DB
- Historical table also cleaned

### Scrapers (DevOps Task)

- All GitHub Secrets verified present
- Supabase Edge Function secret verified
- Medicare scraper test successful
- FairHealth ASC scraper test successful
- January 2026 data visible in database
- Code 36475 prices match FairHealth Online source

---

## Success Criteria

1. **UI**: Searching "36475" shows ONE row per specialty, not duplicates
2. **Prices**: Facility 070/074/USA values for 36475 match FairHealth Online exactly
3. **Scrapers**: Admin can trigger FairHealth ASC and see new data ingested
4. **Data Freshness**: January 2026 release data is available in the system

