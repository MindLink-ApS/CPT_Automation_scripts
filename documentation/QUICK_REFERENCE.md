# Quick Reference: CPT Dashboard Investigation Results

**Date**: 2026-02-03  
**Investigation**: Supabase table verification for CPT Dashboard implementation

---

## 🔴 CRITICAL FINDING

**Data is fragmented across TWO active tables with NO synchronization!**

---

## The Two Active Tables

### Table 1: `new_updated_historical_medical_benchmarking_data`

- **Scrapers**: 5 (FairHealth Facility, FairHealth Physicians, Medicare ASC, NJ DOBI, Novitas)
- **Latest Data**: Nov 2025
- **Issue**: Codes have `.0` suffix (e.g., `36475.0`)
- **Special Column**: `rag_ingested` (boolean)

### Table 2: `new_updated_medical_benchmarking_data`

- **Scrapers**: 2 (Horizon ASC, Medicare Clinical Fees)
- **Latest Data**: January 2026
- **Also Used By**: `specialty_prioritizer.py`
- **Missing Column**: No `rag_ingested`

---

## The Problem

**If the dashboard queries only ONE table, it's missing data from the other!**

Example scenarios:
- Dashboard queries Table 1 → Missing Horizon & Medicare Clinical data
- Dashboard queries Table 2 → Missing FairHealth, Medicare ASC, NJ DOBI, Novitas data

---

## What We Need to Know URGENTLY

### Question 1: Which table does the dashboard query?

**How to find out**:
```bash
# Search frontend for table references
grep -r "new_updated_historical_medical_benchmarking_data" frontend/
grep -r "new_updated_medical_benchmarking_data" frontend/

# Search backend API
grep -r "new_updated_historical_medical_benchmarking_data" backend/app/api/
grep -r "new_updated_medical_benchmarking_data" backend/app/api/
```

### Question 2: Should we consolidate tables?

**Three options**:

1. **Consolidate to one table** (recommended)
   - Migrate all data to `new_updated_medical_benchmarking_data`
   - Update 5 scrapers to write to new table
   - Effort: 1-2 days

2. **Create unified view**
   - UNION both tables in a view
   - Update dashboard to query view
   - Effort: Few hours

3. **Keep separate**
   - Document why they're separate
   - Update dashboard to query both
   - Effort: 1-2 days

---

## Files Created

1. **`backend/scripts/verify_supabase_tables.py`**
   - Checks table types (BASE TABLE vs VIEW)
   - Checks for triggers
   - **Result**: No views, no triggers

2. **`backend/scripts/investigate_table_relationships.py`**
   - Compares schemas
   - Shows sample data
   - Counts records
   - **Result**: Schema differences, `.0` suffix confirmed

3. **`backend/SUPABASE_VERIFICATION_FINDINGS.md`**
   - Initial findings from verification
   - Investigation steps

4. **`DATABASE_ARCHITECTURE_ANALYSIS.md`**
   - Comprehensive analysis
   - Scraper-to-table mapping
   - Three strategic options
   - Risk assessment

5. **`UPDATED_IMPLEMENTATION_PLAN.md`**
   - Revised plan based on findings
   - Blocking issues identified
   - Decision tree for next steps

6. **`QUICK_REFERENCE.md`** (this file)
   - TL;DR summary

---

## What's Blocked

❌ **Code normalization** - Don't know which table to normalize  
❌ **Scraper testing** - May write to wrong table  
❌ **Price verification** - Don't know which table to query  

---

## What Can Proceed

✅ **Frontend code normalization** - Can implement `normalizeCode()` function  
✅ **Secrets verification** - Can check GitHub Actions and Supabase secrets  
✅ **Documentation** - Can document current state  

---

## Immediate Action Items

1. **Find dashboard query target** (30 min)
   - Check frontend Supabase client
   - Check backend API endpoints
   - Document which table is queried

2. **Schedule decision meeting** (1 hour)
   - Present findings
   - Choose consolidation strategy
   - Get stakeholder approval

3. **Implement chosen strategy** (hours to days)
   - Follow plan from `UPDATED_IMPLEMENTATION_PLAN.md`

---

## Key Insights

### Why Code 36475 Shows Twice

**Root Cause**: Codes stored with `.0` suffix in `new_updated_historical_medical_benchmarking_data`

**Example**:
- Row 1: `code = "36475"`
- Row 2: `code = "36475.0"`

**Fix**: Run SQL to strip `.0` suffix:
```sql
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

### Why Prices May Be Wrong

**Possible Causes**:
1. Dashboard querying wrong table
2. Scrapers writing to wrong table
3. Old data not being replaced
4. Missing January 2026 data

**Can't verify until we know which table dashboard uses!**

---

## Scraper Mapping Reference

### Write to `new_updated_historical_medical_benchmarking_data`:
- `Fair_Health_Facility` (FairHealth ASC)
- `Fair_Health_Physicians`
- `Medicare_ASC_Addenda`
- `New_Jersey_DOBI`
- `Novitas`

### Write to `new_updated_medical_benchmarking_data`:
- `Horizon_ASC`
- `Medicare_Clinical_Fees`

---

## Next Steps Decision Tree

```
1. Identify dashboard query target
   ↓
2. Choose strategy:
   ├─ A. Consolidate to one table
   │  ├─ Migrate data
   │  ├─ Update 5 scrapers
   │  └─ Update dashboard
   │
   ├─ B. Create unified view
   │  ├─ Create view
   │  └─ Update dashboard
   │
   └─ C. Keep separate
      ├─ Document purposes
      └─ Update dashboard to query both
   ↓
3. Normalize codes (strip .0)
   ↓
4. Test scrapers
   ↓
5. Verify prices
   ↓
6. Deploy
```

---

## Contact

For questions about this investigation:
- See detailed analysis in `DATABASE_ARCHITECTURE_ANALYSIS.md`
- See implementation plan in `UPDATED_IMPLEMENTATION_PLAN.md`
- Run scripts in `backend/scripts/` for fresh data

---

## Summary

**We discovered a critical data architecture issue that blocks the original implementation plan. We need to decide on a table consolidation strategy before proceeding with code normalization, scraper testing, or price verification.**

**Timeline**: 3-6 days total (depending on strategy chosen)

**Risk**: HIGH if we proceed without resolving table fragmentation

**Recommendation**: Choose Option A (consolidate to one table) for long-term maintainability
