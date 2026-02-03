# CPT Dashboard Investigation - Executive Summary

**Date**: 2026-02-03  
**Investigator**: Backend Team  
**Status**: 🔴 **CRITICAL - Requires Immediate Decision**

---

## What We Were Asked to Do

Implement the CPT Dashboard plan which included:
1. Verify Supabase database structure
2. Normalize CPT codes (remove `.0` suffix)
3. Test scrapers (Medicare, FairHealth)
4. Verify prices match FairHealth Online

**Reference Plans**:
- `.cursor/plans/cpt-backend-dashboard-implementation_f0629dba.plan.md`
- `cpt_dashboard_implementation_plan_0e148abe.plan.md`

---

## What We Discovered

### 🔴 CRITICAL ISSUE: Data Fragmentation

**The database has TWO active tables with NO synchronization mechanism:**

1. **`new_updated_historical_medical_benchmarking_data`**
   - 5 scrapers write here (FairHealth, Medicare ASC, etc.)
   - Latest data: Nov 2025
   - Has `.0` suffix on codes (causing duplicate rows)

2. **`new_updated_medical_benchmarking_data`**
   - 2 scrapers write here (Horizon, Medicare Clinical)
   - Latest data: January 2026
   - Cleaner data format

**Impact**: If the dashboard queries only ONE table, it's missing data from the other!

---

## Why This Blocks the Original Plan

### ❌ Can't Normalize Codes Yet
**Reason**: Don't know which table to normalize until we know which one the dashboard uses.

### ❌ Can't Test Scrapers Yet
**Reason**: Even if scrapers work, data may not appear in dashboard due to table mismatch.

### ❌ Can't Verify Prices Yet
**Reason**: Don't know which table to query for verification.

---

## What We've Completed

### ✅ Investigation & Documentation

1. **Created verification script**: `backend/scripts/verify_supabase_tables.py`
   - Confirmed all tables are BASE TABLEs (not views)
   - Confirmed no triggers exist

2. **Created investigation script**: `backend/scripts/investigate_table_relationships.py`
   - Analyzed schemas
   - Compared data
   - Identified `.0` suffix issue

3. **Mapped scraper-to-table relationships**:
   - Identified which scrapers write to which tables
   - Discovered the fragmentation issue

4. **Created comprehensive documentation**:
   - `backend/SUPABASE_VERIFICATION_FINDINGS.md` - Initial findings
   - `DATABASE_ARCHITECTURE_ANALYSIS.md` - Detailed analysis
   - `UPDATED_IMPLEMENTATION_PLAN.md` - Revised plan
   - `QUICK_REFERENCE.md` - TL;DR summary
   - `ARCHITECTURE_DIAGRAMS.md` - Visual diagrams
   - `EXECUTIVE_SUMMARY.md` - This document

---

## What We Need to Decide

### Decision 1: Which table does the dashboard query?

**Action**: Check frontend/backend code for table references.

**Expected Answer**: One of:
- `new_updated_historical_medical_benchmarking_data`
- `new_updated_medical_benchmarking_data`
- Both (via UNION)
- A view we haven't discovered

**Timeline**: 30 minutes to find this

---

### Decision 2: How should we consolidate the tables?

**Three Options:**

#### Option A: Consolidate to One Table ⭐ RECOMMENDED

**What**: Migrate all data to `new_updated_medical_benchmarking_data`, update scrapers.

**Pros**:
- ✅ Single source of truth
- ✅ Best performance
- ✅ Easiest long-term maintenance
- ✅ Eliminates fragmentation

**Cons**:
- ❌ Requires scraper updates
- ❌ 1-2 day implementation

**Timeline**: 1-2 days

---

#### Option B: Create Unified View

**What**: Create a view that UNIONs both tables, update dashboard to query view.

**Pros**:
- ✅ Quick implementation (few hours)
- ✅ No scraper changes needed
- ✅ Low risk

**Cons**:
- ❌ Still maintaining two tables
- ❌ Potential performance impact
- ❌ Doesn't solve root cause

**Timeline**: Few hours

---

#### Option C: Keep Separate Tables

**What**: Document purpose of each table, update dashboard to query both.

**Pros**:
- ✅ Preserves existing architecture

**Cons**:
- ❌ Complex queries
- ❌ Ongoing fragmentation risk
- ❌ High maintenance burden

**Timeline**: 1-2 days

**Note**: Only choose this if there's a documented business reason for separation.

---

## Our Recommendation

### Start with Option B, then migrate to Option A

**Phase 1 (Immediate - Few Hours)**:
1. Create unified view
2. Update dashboard to query view
3. Normalize codes in both tables
4. Verify dashboard works

**Phase 2 (Next Sprint - 1-2 Days)**:
1. Migrate data to single table
2. Update scrapers
3. Deprecate old table
4. Remove view (no longer needed)

**Why This Approach**:
- ✅ Gets dashboard working quickly
- ✅ Buys time for proper migration
- ✅ Reduces immediate risk
- ✅ Provides path to long-term solution

---

## Revised Timeline

### If We Choose Recommended Approach

**Week 1 (Immediate Fix)**:
- Day 1: Create view, update dashboard, normalize codes (4-6 hours)
- Day 2: Test and verify (2-4 hours)
- **Result**: Dashboard shows all data, duplicate rows fixed

**Week 2 (Permanent Fix)**:
- Day 1: Migrate data, update scrapers (6-8 hours)
- Day 2: Test scrapers, verify data flow (4-6 hours)
- **Result**: Single table, no fragmentation

**Total**: ~2 weeks for complete solution

---

### If We Choose Option A Only

**Week 1**:
- Day 1-2: Migrate data, update scrapers (1-2 days)
- Day 3: Test and verify (4-6 hours)
- **Result**: Single table, all issues resolved

**Total**: ~1 week, but higher risk

---

### If We Choose Option B Only

**Week 1**:
- Day 1: Create view, update dashboard (4-6 hours)
- **Result**: Dashboard works, but fragmentation remains

**Total**: Few hours, but technical debt persists

---

## Risk Assessment

### Current State Risks (If We Do Nothing)

- 🔴 **HIGH**: Dashboard showing incomplete data
- 🔴 **HIGH**: Duplicate rows confusing users
- 🔴 **HIGH**: Price discrepancies vs FairHealth Online
- 🟡 **MEDIUM**: Scrapers may be working but data not visible

### Implementation Risks

| Risk | Option A | Option B | Option C |
|------|----------|----------|----------|
| Data loss | 🟡 Medium | 🟢 Low | 🟢 Low |
| Downtime | 🟡 Medium | 🟢 Low | 🟡 Medium |
| Performance | 🟢 Low | 🟡 Medium | 🔴 High |
| Future issues | 🟢 Low | 🟡 Medium | 🔴 High |

---

## Success Criteria

### Immediate Success (After Phase 1)
- ✅ Dashboard shows data from ALL scrapers
- ✅ Code 36475 appears on ONE row (not duplicated)
- ✅ Prices match FairHealth Online
- ✅ Latest data (January 2026) visible

### Long-term Success (After Phase 2)
- ✅ Single table architecture
- ✅ All scrapers writing to same table
- ✅ No data fragmentation
- ✅ Simplified queries
- ✅ Easy to maintain

---

## What Happens Next

### Step 1: Identify Dashboard Query (30 minutes)

**Action**: Search codebase for table references.

**Commands**:
```bash
# Frontend
grep -r "new_updated_historical_medical_benchmarking_data" frontend/
grep -r "new_updated_medical_benchmarking_data" frontend/

# Backend
grep -r "new_updated_historical_medical_benchmarking_data" backend/app/api/
grep -r "new_updated_medical_benchmarking_data" backend/app/api/
```

**Deliverable**: Document which table(s) the dashboard queries.

---

### Step 2: Decision Meeting (1 hour)

**Attendees**: Technical Lead, Product Owner, Backend Developer

**Agenda**:
1. Review findings (10 min)
2. Discuss options (20 min)
3. Choose strategy (15 min)
4. Assign tasks (15 min)

**Deliverable**: Approved implementation strategy.

---

### Step 3: Implementation (hours to days)

**Follow**: `UPDATED_IMPLEMENTATION_PLAN.md` based on chosen strategy.

**Deliverable**: Working dashboard with all data visible.

---

## Key Metrics to Track

### Before Fix
- [ ] How many tables does dashboard query?
- [ ] What % of scraper data is visible?
- [ ] How many duplicate code rows exist?
- [ ] Price accuracy vs FairHealth Online?

### After Fix
- [ ] Dashboard queries: 1 table or view
- [ ] Scraper data visibility: 100%
- [ ] Duplicate rows: 0
- [ ] Price accuracy: 100% match

---

## Questions & Answers

### Q: Why wasn't this discovered earlier?

**A**: The original plan assumed a view/trigger architecture existed. The verification scripts revealed this assumption was incorrect.

### Q: Can we just normalize codes and move on?

**A**: No. Even if we normalize codes, the dashboard may still be missing data from one of the tables due to fragmentation.

### Q: Which option should we choose?

**A**: We recommend starting with Option B (view) for quick fix, then migrating to Option A (consolidation) for long-term solution.

### Q: How long will this delay the project?

**A**: 
- Quick fix (Option B): Few hours
- Permanent fix (Option A): 1-2 days
- Both (recommended): ~2 weeks total

### Q: What if we need to ship quickly?

**A**: Implement Option B (view) first. This gets the dashboard working in hours, then schedule Option A for next sprint.

---

## Files Reference

All documentation is in the project root:

1. **`QUICK_REFERENCE.md`** ← Start here for TL;DR
2. **`EXECUTIVE_SUMMARY.md`** ← This file (overview)
3. **`DATABASE_ARCHITECTURE_ANALYSIS.md`** ← Detailed technical analysis
4. **`ARCHITECTURE_DIAGRAMS.md`** ← Visual diagrams
5. **`UPDATED_IMPLEMENTATION_PLAN.md`** ← Step-by-step plan
6. **`backend/SUPABASE_VERIFICATION_FINDINGS.md`** ← Raw findings

Scripts:
- **`backend/scripts/verify_supabase_tables.py`** ← Table verification
- **`backend/scripts/investigate_table_relationships.py`** ← Schema analysis

---

## Conclusion

**We discovered a critical data architecture issue that requires a decision before proceeding with the original implementation plan.**

**The good news**: We have a clear path forward with three well-defined options.

**The recommendation**: Implement a quick fix (unified view) immediately, then migrate to a permanent solution (single table) in the next sprint.

**Timeline**: 
- Quick fix: Few hours
- Permanent fix: 1-2 days
- Total: ~2 weeks for complete solution

**Next step**: Identify which table the dashboard queries (30 minutes), then schedule decision meeting.

---

## Contact

For questions or clarifications:
- Technical details: See `DATABASE_ARCHITECTURE_ANALYSIS.md`
- Implementation steps: See `UPDATED_IMPLEMENTATION_PLAN.md`
- Quick reference: See `QUICK_REFERENCE.md`

---

**Status**: ⏸️ **PAUSED - Awaiting Decision**  
**Blocker**: Need to choose table consolidation strategy  
**ETA**: Can resume within hours of decision
