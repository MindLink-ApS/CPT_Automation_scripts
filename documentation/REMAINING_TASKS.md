# Remaining Tasks Checklist

**Date**: 2026-02-03  
**Status**: Duplicate rows ✅ FIXED | Prices ⏳ IN PROGRESS

---

## ✅ Completed Tasks

- [x] **Code normalization**: Removed `.0` suffix from codes
- [x] **Edge function sync**: Triggered successfully
- [x] **Duplicate rows**: Code 36475 now appears once per specialty

---

## ⏳ Current Task: Price Verification

### Step 1: Check Current Prices for Code 36475

**Run this query**:

```sql
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
ORDER BY data_type, geozip;
```

**Expected from FairHealth Online (July 2025)**:
- Geozip 070: $10,761.00
- Geozip 074: $25,139.00
- USA: $10,761.00

**What dashboard was showing (WRONG)**:
- Geozip 070: $2,356
- Geozip 074: $5,678
- USA: $2,396

### Step 2: Check Available Releases

**Run this query**:

```sql
SELECT 
  DISTINCT release_date,
  source,
  data_type,
  COUNT(*) as record_count
FROM new_updated_medical_benchmarking_data
WHERE source LIKE '%Fair%'
  AND data_type LIKE 'Facility%'
GROUP BY release_date, source, data_type
ORDER BY release_date DESC;
```

**From your screenshot**: You have **August 2025** data (11,931 records)

**Questions**:
1. Do you have July 2025 data? (Andrew's comparison)
2. Do you have January 2026 data? (latest release)
3. Are the prices correct for August 2025?

---

## 🔜 Pending Tasks

### Backend Tasks

#### 1. Verify Secrets Configuration (5 min)

**Check**: `backend/.env`

**Required variables**:
```bash
# Supabase
SUPABASE_URL=https://uyozdfwohdpcnyliebni.supabase.co
SUPABASE_KEY=your-service-key

# FairHealth credentials
FAIRHEALTH_EMAIL=your-email@example.com
FAIRHEALTH_PASSWORD=your-password

# Proxy configuration
PROXY_SERVER=your-proxy-server
PROXY_USERNAME=your-proxy-username
PROXY_PASSWORD=your-proxy-password
```

**Action**: Verify these exist and are correct

#### 2. Test FairHealth ASC Scraper (30-60 min, if needed)

**Only if January 2026 data is missing**

**Steps**:
```bash
# 1. Request scraper job
curl -X POST http://localhost:8000/api/scraper/request \
  -H "Content-Type: application/json" \
  -d '{"scraper_name": "FairHealth ASC", "created_by": "admin"}'

# 2. Get job ID from response, then approve
curl -X POST http://localhost:8000/api/scraper/approve/JOB_ID

# 3. Monitor progress
curl http://localhost:8000/api/scraper/history

# 4. After completion, trigger edge function
curl -X POST https://uyozdfwohdpcnyliebni.supabase.co/functions/v1/refresh-medical-benchmark
```

#### 3. Re-verify Prices After Scraper (10 min)

**After scraper completes**, re-run price verification query

---

### Frontend Tasks (Optional but Recommended)

#### 1. Add Code Normalization Function (15 min)

**File**: `src/lib/pivotUtils.ts`

**Add function**:
```typescript
export function normalizeCode(code: string | null | undefined): string {
  if (!code) return '';
  return String(code).replace(/\.0+$/, '').trim();
}
```

**Update merge function** (around line 39):
```typescript
const normalizedCode = normalizeCode(row.code);
const key = `${normalizedCode}-${specialty}`;

pivotMap.set(key, {
  code: normalizedCode,
  code_description: row.code_description || '',
  speciality: specialty,
});
```

#### 2. Update DataTablePivot Component (10 min)

**File**: `src/components/dashboard/DataTablePivot.tsx`

**Import**:
```typescript
import { mergeRecordsForPivot, normalizeCode, type PivotRow } from "@/lib/pivotUtils";
```

**Update code collection** (around lines 89, 143):
```typescript
if (item.code) allCodesSet.add(normalizeCode(item.code));
```

#### 3. Verify Percentile Display Logic (5 min)

**File**: `src/lib/pivotUtils.ts` (around line 52)

**Verify this exists**:
```typescript
const value = row.data_type === "Physician USA" ? row["90th"] : row["80th"];
```

**Expected behavior**:
- "Physician USA" → shows 90th percentile
- All other data types → shows 80th percentile

---

## 📊 Verification Checklist

### Database Verification
- [ ] Code 36475 prices match FairHealth Online (for same release)
- [ ] Latest release data (January 2026) exists
- [ ] All historical releases present (July 2025, Aug 2025, Nov 2025, Jan 2026)
- [ ] No codes with `.0` suffix in either table

### Dashboard Verification
- [ ] Code 36475 appears on ONE row per specialty
- [ ] Prices match database values
- [ ] Correct percentile displayed (80th for Facility, 90th for Physician USA)
- [ ] Latest release data visible

### Scraper Verification
- [ ] FairHealth credentials configured
- [ ] Scrapers write to correct table (historical)
- [ ] Edge function syncs data successfully
- [ ] New scraper data appears in dashboard after sync

---

## 🎯 Success Criteria

### Immediate Success
- ✅ Duplicate rows fixed
- [ ] Prices for code 36475 match FairHealth Online
- [ ] Latest release (January 2026) data exists

### Complete Success
- [ ] All FairHealth releases present in database
- [ ] Dashboard shows accurate, up-to-date prices
- [ ] Scrapers running successfully
- [ ] Edge function syncing daily
- [ ] Frontend normalization implemented (defense-in-depth)

---

## ⏱️ Timeline Estimate

| Task | Time | Priority |
|------|------|----------|
| Price verification queries | 5 min | 🔴 HIGH |
| Analyze price discrepancy | 10 min | 🔴 HIGH |
| Verify secrets configuration | 5 min | 🟡 MEDIUM |
| Run FairHealth scraper (if needed) | 30-60 min | 🟡 MEDIUM |
| Frontend normalization | 30 min | 🟢 LOW |
| Final verification | 15 min | 🔴 HIGH |

**Total**: 1-2 hours (depending on whether scraper needs to run)

---

## 🚀 Next Immediate Actions

1. **Run price verification query** (see Step 1 above)
2. **Check release dates** (see Step 2 above)
3. **Compare prices** against FairHealth Online for same release
4. **Determine if scraper needs to run** (if January 2026 missing)

---

## 📝 Notes

- **DevOps tasks skipped** per your request (Docker handles scraper execution)
- **Edge function working** (confirmed by successful trigger)
- **Code normalization complete** (duplicates fixed)
- **Focus now**: Price verification and ensuring latest data exists

---

## ❓ Questions to Answer

1. **What prices does your database show for code 36475?**
   - Run the query in Step 1

2. **What release dates do you have?**
   - Run the query in Step 2

3. **Do you have January 2026 data?**
   - Check query results

4. **Are FairHealth credentials configured?**
   - Check `backend/.env`

**Share the query results and we'll determine next steps!**
