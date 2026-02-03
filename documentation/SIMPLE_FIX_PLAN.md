# SIMPLIFIED Implementation Plan - Code Normalization Only

**Date**: 2026-02-03  
**Status**: ✅ **READY TO PROCEED**  
**Timeline**: 1-2 hours total

---

## What Changed

**Previous Analysis**: Thought we needed table consolidation (days of work)  
**Corrected Analysis**: Just need code normalization (hours of work)

**Why**: Supabase Edge Function already synchronizes data daily!

---

## The Simple Fix

### Problem
Codes with `.0` suffix in `new_updated_historical_medical_benchmarking_data` get synced to dashboard table, causing duplicate rows.

### Solution
1. Normalize codes in historical table (remove `.0`)
2. Trigger edge function to sync clean data
3. Done!

---

## Implementation Steps

### Step 1: Normalize Codes in Historical Table (15 min)

**Connect to Supabase SQL Editor**

**Preview affected codes**:
```sql
-- See which codes will be affected
SELECT 
  code, 
  REGEXP_REPLACE(code, '\.0+$', '') as normalized_code,
  COUNT(*) as occurrences
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code
ORDER BY occurrences DESC
LIMIT 50;
```

**Execute normalization**:
```sql
-- Normalize all codes ending in .0
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

**Verify**:
```sql
-- Should return 0 rows
SELECT code
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
LIMIT 10;
```

---

### Step 2: Trigger Edge Function to Sync (5 min)

**Option A: Manual Trigger via Backend API** (Recommended)

```bash
# If backend is running locally
curl -X POST http://localhost:8000/api/scraper/trigger-edge-function

# Or via deployed backend
curl -X POST https://your-backend-url.com/api/scraper/trigger-edge-function
```

**Option B: Direct Edge Function Call**

```bash
curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark
```

**Option C: Wait for Automatic Sync**

Edge function runs daily at 2 AM Chicago time. If you can wait, it will sync automatically.

---

### Step 3: Verify Dashboard (10 min)

**Check code 36475**:
```sql
-- Should show ONE row per data_type/geozip combination
SELECT code, data_type, geozip, "80th", release_date
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
ORDER BY data_type, geozip;
```

**Expected result**: No duplicate rows with `36475.0`

**Test in dashboard**:
1. Open dashboard
2. Search for code 36475
3. Verify it appears on ONE row per specialty (not duplicated)

---

### Step 4: Verify Prices Match FairHealth Online (30 min)

**Query facility prices for code 36475**:
```sql
SELECT 
  code,
  data_type,
  geozip,
  "80th" as price_80th,
  "90th" as price_90th,
  release_date,
  source
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip;
```

**Compare against FairHealth Online**:
- Geozip 070: Facility 80th percentile
- Geozip 074: Facility 80th percentile
- USA: Facility 80th percentile

**Expected**: Prices should match FairHealth Online for latest release

---

## Optional: Frontend Defense-in-Depth (30 min)

**Even though backend is fixed, add frontend normalization for safety.**

### File 1: `src/lib/pivotUtils.ts`

Add normalization function:

```typescript
// Add at top of file
export function normalizeCode(code: string | null | undefined): string {
  if (!code) return '';
  return String(code).replace(/\.0+$/, '').trim();
}
```

Update merge function (around line 39):

```typescript
// In mergeRecordsForPivot()
const normalizedCode = normalizeCode(row.code);
const key = `${normalizedCode}-${specialty}`;

pivotMap.set(key, {
  code: normalizedCode,  // Use normalized
  code_description: row.code_description || '',
  speciality: specialty,
});
```

### File 2: `src/components/dashboard/DataTablePivot.tsx`

Import and use normalization:

```typescript
// Add to imports
import { mergeRecordsForPivot, normalizeCode, type PivotRow } from "@/lib/pivotUtils";

// Update code collection (around lines 89, 143)
if (item.code) allCodesSet.add(normalizeCode(item.code));
```

---

## Testing Checklist

### Database Testing
- [ ] Run preview query to see affected codes
- [ ] Execute normalization UPDATE
- [ ] Verify no codes with `.0` suffix remain in historical table
- [ ] Trigger edge function
- [ ] Verify benchmark table has clean codes
- [ ] Check code 36475 appears once per data_type/geozip

### Dashboard Testing
- [ ] Search for code 36475
- [ ] Verify single row per specialty (not duplicated)
- [ ] Check prices match expected values
- [ ] Test with other codes that had `.0` suffix

### Scraper Testing (Optional)
- [ ] Run a scraper (e.g., Medicare)
- [ ] Verify it writes to historical table
- [ ] Trigger edge function
- [ ] Verify data appears in benchmark table
- [ ] Check dashboard shows new data

---

## Rollback Plan

**If something goes wrong**:

### Rollback Step 1: Restore Codes

**If you have a backup**:
```sql
-- Restore from backup table (if created)
UPDATE new_updated_historical_medical_benchmarking_data
SET code = backup_table.original_code
FROM backup_table
WHERE new_updated_historical_medical_benchmarking_data.id = backup_table.id;
```

**If no backup**:
- The `.0` suffix can be re-added if needed, but this is unlikely to be necessary
- Edge function will sync whatever is in historical table

### Rollback Step 2: Re-trigger Edge Function

```bash
curl -X POST http://localhost:8000/api/scraper/trigger-edge-function
```

---

## Monitoring

### Check Edge Function Logs

**Supabase Dashboard**:
1. Go to Edge Functions
2. Select `refresh-medical-benchmark`
3. Check logs for:
   - Last run time
   - Success/failure status
   - Any errors

### Check Backend Logs

**If backend is running**:
```bash
# Look for edge function cron job logs
grep "EDGE FUNCTION" backend/logs/*.log

# Or check live logs
tail -f backend/logs/app.log | grep "EDGE FUNCTION"
```

### Verify Sync is Working

**Query to check last sync time**:
```sql
-- Check most recent scraped_at timestamp in benchmark table
SELECT MAX(scraped_at) as last_sync
FROM new_updated_medical_benchmarking_data;
```

Should be updated daily after 2 AM Chicago time.

---

## Success Criteria

### Immediate (After Step 3)
- ✅ No codes with `.0` suffix in historical table
- ✅ No codes with `.0` suffix in benchmark table
- ✅ Code 36475 appears on ONE row in dashboard

### Short-term (After Step 4)
- ✅ Prices match FairHealth Online
- ✅ No duplicate rows in dashboard
- ✅ Edge function runs successfully

### Long-term (Ongoing)
- ✅ Edge function runs daily without errors
- ✅ New scraper data syncs correctly
- ✅ Dashboard always shows clean data

---

## Timeline

| Step | Task | Time | Total |
|------|------|------|-------|
| 1 | Normalize codes in historical table | 15 min | 15 min |
| 2 | Trigger edge function | 5 min | 20 min |
| 3 | Verify dashboard | 10 min | 30 min |
| 4 | Verify prices | 30 min | 1 hour |
| 5 | Frontend normalization (optional) | 30 min | 1.5 hours |

**Total**: 1-1.5 hours (vs. days for table consolidation!)

---

## Next Steps After Completion

### Documentation
1. Update architecture docs with edge function details
2. Document data flow: scrapers → historical → edge function → benchmark → dashboard
3. Add monitoring guide for edge function

### Monitoring Setup
1. Set up alerts for edge function failures
2. Monitor edge function execution time
3. Track data freshness in benchmark table

### Future Improvements
1. Consider updating Horizon_ASC and Medicare_Clinical_Fees to write to historical table for consistency
2. Add code normalization to scraper data processors (prevent `.0` at source)
3. Add automated tests for edge function sync

---

## Questions & Answers

### Q: Why not normalize the benchmark table directly?

**A**: The benchmark table is synced from the historical table by the edge function. If we only normalize the benchmark table, the next edge function run will re-introduce the `.0` codes from the historical table.

### Q: Will this affect existing data?

**A**: Yes, but in a good way! Codes like `36475.0` will become `36475`, which is the correct format. This will eliminate duplicate rows.

### Q: What if a scraper writes `.0` codes again?

**A**: 
- **Short-term**: Frontend normalization will handle it
- **Long-term**: Update scraper data processors to normalize codes before insertion

### Q: How often does the edge function run?

**A**: Daily at 2 AM Chicago time. You can also trigger it manually for testing.

### Q: What if the edge function fails?

**A**: Check logs in Supabase Dashboard → Edge Functions. The backend also logs edge function calls in `backend/app/core/scheduler.py`.

---

## Summary

**The fix is simple**:
1. Normalize codes in historical table (15 min)
2. Trigger edge function to sync (5 min)
3. Verify dashboard (10 min)

**Total time**: ~30 minutes to 1 hour

**No architectural changes needed!** The synchronization mechanism already exists and works correctly.

---

## Ready to Proceed?

✅ **Yes!** The architecture is correct, we just need to clean up the data.

**Start with Step 1**: Run the SQL queries to normalize codes in the historical table.
