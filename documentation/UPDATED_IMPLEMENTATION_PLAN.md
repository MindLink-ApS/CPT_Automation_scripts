# Updated CPT Dashboard Implementation Plan

**Date**: 2026-02-03  
**Status**: 🔴 **BLOCKED - Awaiting Architectural Decision**

---

## Critical Findings Summary

The verification process has revealed a **critical data architecture issue** that blocks the original implementation plan:

### 🔴 Problem: Data Fragmentation

**Two active tables exist with NO synchronization:**

1. **`new_updated_historical_medical_benchmarking_data`**
   - Used by: 5 scrapers (FairHealth Facility, FairHealth Physicians, Medicare ASC, NJ DOBI, Novitas)
   - Latest data: "Nov 2025"
   - Has `.0` suffix on codes (causing duplicate row issue)

2. **`new_updated_medical_benchmarking_data`**
   - Used by: 2 scrapers (Horizon ASC, Medicare Clinical Fees)
   - Latest data: "January 2026"
   - Also used by `specialty_prioritizer.py`

**Impact**: If the dashboard queries only ONE table, it's missing data from the other!

---

## What We've Completed

### ✅ Phase 1: Database Verification (DONE)

1. ✅ Created and ran `backend/scripts/verify_supabase_tables.py`
   - **Result**: All three tables are BASE TABLEs (not views)
   - **Result**: No triggers exist for data synchronization

2. ✅ Created `backend/scripts/investigate_table_relationships.py`
   - **Result**: Discovered schema differences between tables
   - **Result**: Confirmed `.0` suffix issue in `new_updated_historical_medical_benchmarking_data`

3. ✅ Analyzed scraper database handlers
   - **Result**: Mapped which scrapers write to which tables
   - **Result**: Identified data fragmentation issue

4. ✅ Created comprehensive documentation:
   - `backend/SUPABASE_VERIFICATION_FINDINGS.md`
   - `DATABASE_ARCHITECTURE_ANALYSIS.md`

---

## What's Blocked (Cannot Proceed)

### 🔴 Blocked: Code Normalization (Task B4)

**Why Blocked**: We don't know which table(s) to normalize until we:
1. Determine which table the dashboard queries
2. Decide on table consolidation strategy

**Risk**: Normalizing the wrong table wastes effort and doesn't fix the dashboard issue.

### 🔴 Blocked: Scraper Testing (Tasks C3-C4)

**Why Blocked**: Even if scrapers work perfectly, data may not appear in the dashboard if:
- Scrapers write to `new_updated_historical_medical_benchmarking_data`
- Dashboard queries `new_updated_medical_benchmarking_data`

**Risk**: "Successful" scraper runs that don't fix the dashboard because of table mismatch.

### 🔴 Blocked: Price Verification (Task B5)

**Why Blocked**: We don't know which table to query for verification until we know which table the dashboard uses.

**Risk**: Verifying prices in the wrong table.

---

## Required Decisions (URGENT)

### Decision 1: Which Table Does the Dashboard Query?

**Action Required**: Check frontend/backend code to find the table name in dashboard queries.

**Where to Look**:
- Frontend Supabase client calls
- Backend API endpoints serving dashboard data
- Any `.from('table_name')` or `.table('table_name')` references

**Expected Answer**: One of:
- `new_updated_historical_medical_benchmarking_data`
- `new_updated_medical_benchmarking_data`
- Both (via UNION or multiple queries)
- A view we haven't discovered yet

### Decision 2: Table Consolidation Strategy

**After answering Decision 1, choose ONE:**

#### Option A: Consolidate to Single Table ⭐ RECOMMENDED

**Target Table**: `new_updated_medical_benchmarking_data` (newer, cleaner schema)

**Steps**:
1. Migrate data from `new_updated_historical_medical_benchmarking_data`
2. Update 5 scrapers to write to consolidated table:
   - `Fair_Health_Facility/database.py`
   - `Fair_Health_Physicians/database.py`
   - `Medicare_ASC_Addenda/database.py`
   - `New_Jersey_DOBI/database.py`
   - `Novitas/database.py`
3. Add `rag_ingested` column to target table if needed
4. Update dashboard to query single table
5. Deprecate `new_updated_historical_medical_benchmarking_data`

**Pros**: Single source of truth, no data fragmentation  
**Cons**: Requires scraper updates and data migration  
**Effort**: Medium (1-2 days)

#### Option B: Create Unified View

**Implementation**:
```sql
CREATE VIEW unified_medical_benchmarking_data AS
SELECT 
  id, source, code, rel_date, geozip, speciality, code_description,
  physician_codes, asc_allowable, asc_payment_type, full_description,
  plan_type, mean, "50th", "60th", "70th", "75th", "80th", "85th", "90th", "95th",
  data_type, release_date, scraped_at,
  rag_ingested
FROM new_updated_historical_medical_benchmarking_data
UNION ALL
SELECT 
  id, source, code, rel_date, geozip, speciality, code_description,
  physician_codes, asc_allowable, asc_payment_type, full_description,
  plan_type, mean, "50th", "60th", "70th", "75th", "80th", "85th", "90th", "95th",
  data_type, release_date, scraped_at,
  FALSE as rag_ingested
FROM new_updated_medical_benchmarking_data;
```

**Then**: Update dashboard to query the view instead.

**Pros**: No scraper changes, quick implementation  
**Cons**: Still maintaining two tables, potential performance impact  
**Effort**: Low (few hours)

#### Option C: Keep Separate Tables

**Only if**: There's a documented business reason for separation.

**Requirements**:
- Document purpose of each table
- Update dashboard to query BOTH tables
- Set up monitoring for data gaps

**Pros**: Preserves existing architecture  
**Cons**: Complex queries, ongoing fragmentation risk  
**Effort**: Medium (requires dashboard updates)

---

## Revised Implementation Plan

### Phase 1: Architectural Decision (BLOCKING) 🔴

**Owner**: Technical Lead + Stakeholders  
**Timeline**: ASAP (blocks all other work)

1. ⏳ Identify which table the dashboard currently queries
2. ⏳ Review business requirements for table separation
3. ⏳ Choose consolidation strategy (A, B, or C)
4. ⏳ Get stakeholder approval
5. ⏳ Document decision and rationale

### Phase 2: Table Consolidation (DEPENDS ON PHASE 1)

**Owner**: Backend Developer  
**Timeline**: 1-2 days after Phase 1

#### If Option A (Consolidate):
1. ⏳ Create data migration script
2. ⏳ Test migration on staging environment
3. ⏳ Update 5 scraper database handlers
4. ⏳ Update dashboard query
5. ⏳ Execute migration
6. ⏳ Verify data integrity

#### If Option B (Unified View):
1. ⏳ Create view in Supabase
2. ⏳ Update dashboard to query view
3. ⏳ Test data completeness
4. ⏳ Deploy changes

#### If Option C (Keep Separate):
1. ⏳ Document table purposes
2. ⏳ Update dashboard to query both tables
3. ⏳ Set up monitoring
4. ⏳ Test data completeness

### Phase 3: Code Normalization (DEPENDS ON PHASE 2)

**Owner**: Backend Developer  
**Timeline**: 1 hour

1. ⏳ Run `.0` suffix cleanup on target table(s):
   ```sql
   UPDATE [target_table]
   SET code = REGEXP_REPLACE(code, '\.0+$', '')
   WHERE code ~ '\.0+$';
   ```
2. ⏳ Verify no duplicates remain
3. ⏳ Test dashboard shows single row for code 36475

### Phase 4: Frontend Fixes (CAN RUN IN PARALLEL)

**Owner**: Frontend Developer  
**Timeline**: 1-2 hours

1. ⏳ Add `normalizeCode()` to `src/lib/pivotUtils.ts`
2. ⏳ Update `mergeRecordsForPivot()` to use normalized codes
3. ⏳ Update `DataTablePivot.tsx` unique code collection
4. ⏳ Test that code 36475 appears on single row

**Note**: Frontend fix provides defense-in-depth even after DB normalization.

### Phase 5: Scraper Testing (DEPENDS ON PHASE 2)

**Owner**: DevOps/Backend Developer  
**Timeline**: 1 day

1. ⏳ Verify secrets in GitHub Actions and Supabase Edge Functions
2. ⏳ Test Medicare scraper (public, no credentials)
3. ⏳ Test FairHealth ASC scraper
4. ⏳ Verify data appears in correct table(s)
5. ⏳ Confirm dashboard shows new data

### Phase 6: Price Verification (DEPENDS ON PHASES 3 & 5)

**Owner**: Data Analyst/QA  
**Timeline**: 1 hour

1. ⏳ Query code 36475 from dashboard table(s)
2. ⏳ Compare against FairHealth Online (latest release)
3. ⏳ Verify 070/074/USA facility 80th percentiles match
4. ⏳ Document any remaining discrepancies

---

## Success Criteria (Updated)

### Data Architecture
- ✅ Single source of truth established (via consolidation or view)
- ✅ All scraper data visible in dashboard
- ✅ No data fragmentation

### Code Normalization
- ✅ No codes with `.0` suffix in active table(s)
- ✅ Code 36475 appears on ONE row in dashboard

### Scraper Functionality
- ✅ Medicare scraper runs successfully
- ✅ FairHealth ASC scraper runs successfully
- ✅ Latest data (January 2026) visible in dashboard

### Price Accuracy
- ✅ Code 36475 facility prices match FairHealth Online
- ✅ 070/074/USA 80th percentiles verified

---

## Risk Mitigation

### Risk: Choosing Wrong Strategy

**Mitigation**: 
- Involve stakeholders in decision
- Document business requirements
- Test on staging before production

### Risk: Data Loss During Migration

**Mitigation**:
- Backup tables before migration
- Test migration script on staging
- Verify record counts before/after

### Risk: Breaking Existing Integrations

**Mitigation**:
- Identify all systems querying these tables
- Test integrations after changes
- Have rollback plan ready

---

## Immediate Next Steps

1. **Find Dashboard Query** (30 minutes)
   - Search frontend codebase for table references
   - Check backend API endpoints
   - Document findings

2. **Schedule Decision Meeting** (1 hour)
   - Present findings to stakeholders
   - Discuss consolidation options
   - Get approval on strategy

3. **Update Plan** (30 minutes)
   - Add specific implementation steps based on chosen strategy
   - Assign owners and timelines
   - Create tracking tickets

---

## Files Created

1. ✅ `backend/scripts/verify_supabase_tables.py` - Table type verification
2. ✅ `backend/scripts/investigate_table_relationships.py` - Schema analysis
3. ✅ `backend/SUPABASE_VERIFICATION_FINDINGS.md` - Initial findings
4. ✅ `DATABASE_ARCHITECTURE_ANALYSIS.md` - Comprehensive analysis
5. ✅ `UPDATED_IMPLEMENTATION_PLAN.md` - This file

---

## Conclusion

**The original implementation plan is BLOCKED due to data architecture fragmentation.** We must resolve the table consolidation strategy before proceeding with code normalization, scraper testing, or price verification.

**CRITICAL PATH**: Identify dashboard query target → Choose consolidation strategy → Implement → Proceed with original plan.

**Estimated Total Timeline** (after decision):
- Option A (Consolidate): 2-3 days
- Option B (View): 1 day
- Option C (Keep Separate): 1-2 days

Plus original plan tasks: 2-3 days

**Total**: 3-6 days depending on strategy chosen.
