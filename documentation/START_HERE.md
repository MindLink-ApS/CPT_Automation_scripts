# START HERE - Quick Fix Guide

**Problem**: Code 36475 shows duplicate rows in dashboard  
**Cause**: Codes with `.0` suffix  
**Fix**: Normalize codes (30 min - 1 hour)  
**Status**: ✅ Ready to proceed

---

## 🚀 Quick Start (30 minutes)

### 1. Normalize Codes (15 min)

**Open Supabase SQL Editor** and run:

```sql
-- Step 1: Preview what will change
SELECT 
  code, 
  REGEXP_REPLACE(code, '\.0+$', '') as normalized_code,
  COUNT(*) as count
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code
ORDER BY count DESC
LIMIT 20;

-- Step 2: Execute normalization
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';

-- Step 3: Verify (should return 0 rows)
SELECT code
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
LIMIT 10;
```

### 2. Sync to Dashboard (5 min)

**Trigger the edge function** to sync clean data:

```bash
# Option A: Via backend API (if running)
curl -X POST http://localhost:8000/api/scraper/trigger-edge-function

# Option B: Direct edge function call
curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark

# Option C: Wait for automatic sync (2 AM Chicago time)
```

### 3. Verify Dashboard (10 min)

**Check in Supabase SQL Editor**:

```sql
-- Should show ONE row per data_type/geozip (no duplicates)
SELECT code, data_type, geozip, "80th", release_date
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
ORDER BY data_type, geozip;
```

**Check in Dashboard**:
- Search for code 36475
- Should appear on ONE row per specialty
- No more duplicates!

---

## ✅ Done!

That's it! The duplicate row issue should be fixed.

---

## 📚 Want More Details?

- **Complete guide**: Read `SIMPLE_FIX_PLAN.md`
- **Architecture explanation**: Read `CORRECTED_ARCHITECTURE_ANALYSIS.md`
- **Investigation summary**: Read `INVESTIGATION_SUMMARY.md`

---

## ❓ Troubleshooting

### Edge function didn't sync?

**Check logs**:
1. Supabase Dashboard → Edge Functions → `refresh-medical-benchmark` → Logs
2. Look for errors or timeout issues

**Try again**:
```bash
curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark
```

### Still seeing duplicates?

**Check if normalization worked**:
```sql
-- Should return 0
SELECT COUNT(*) 
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$';
```

**Check benchmark table**:
```sql
-- Should return 0
SELECT COUNT(*) 
FROM new_updated_medical_benchmarking_data
WHERE code ~ '\.0+$';
```

### Prices don't match FairHealth Online?

**Check release dates**:
```sql
SELECT DISTINCT release_date, COUNT(*) as records
FROM new_updated_medical_benchmarking_data
WHERE data_type LIKE 'Facility%'
GROUP BY release_date
ORDER BY release_date DESC;
```

Make sure you're comparing against the same release date.

---

## 🎯 Success Criteria

- ✅ No codes with `.0` suffix in historical table
- ✅ No codes with `.0` suffix in benchmark table  
- ✅ Code 36475 appears ONCE per specialty in dashboard
- ✅ Prices match FairHealth Online

---

## 📞 Need Help?

See detailed troubleshooting in `SIMPLE_FIX_PLAN.md`
