# CPT Dashboard Investigation - Documentation Index

**Investigation Date**: 2026-02-03  
**Status**: 🔴 **CRITICAL FINDINGS - Decision Required**

---

## 📋 Quick Navigation

### For Executives / Decision Makers
👉 **Start here**: [`EXECUTIVE_SUMMARY.md`](./EXECUTIVE_SUMMARY.md)
- High-level overview
- Critical findings
- Three strategic options
- Recommendation
- Timeline and costs

### For Developers
👉 **Start here**: [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md)
- TL;DR of technical findings
- Immediate action items
- Code references

### For Technical Leads
👉 **Start here**: [`DATABASE_ARCHITECTURE_ANALYSIS.md`](./DATABASE_ARCHITECTURE_ANALYSIS.md)
- Detailed technical analysis
- Scraper-to-table mapping
- Risk assessment
- Implementation options

---

## 📁 All Documentation Files

### Summary Documents

| File | Purpose | Audience | Read Time |
|------|---------|----------|-----------|
| **[EXECUTIVE_SUMMARY.md](./EXECUTIVE_SUMMARY.md)** | High-level overview, decision framework | Executives, PMs | 10 min |
| **[QUICK_REFERENCE.md](./QUICK_REFERENCE.md)** | TL;DR, immediate actions | Developers | 5 min |
| **[README_INVESTIGATION.md](./README_INVESTIGATION.md)** | This file - navigation guide | Everyone | 2 min |

### Technical Analysis

| File | Purpose | Audience | Read Time |
|------|---------|----------|-----------|
| **[DATABASE_ARCHITECTURE_ANALYSIS.md](./DATABASE_ARCHITECTURE_ANALYSIS.md)** | Detailed technical findings | Tech Leads, Architects | 20 min |
| **[ARCHITECTURE_DIAGRAMS.md](./ARCHITECTURE_DIAGRAMS.md)** | Visual data flow diagrams | Tech Leads, Developers | 15 min |
| **[backend/SUPABASE_VERIFICATION_FINDINGS.md](./backend/SUPABASE_VERIFICATION_FINDINGS.md)** | Raw verification results | Developers | 10 min |

### Implementation Plans

| File | Purpose | Audience | Read Time |
|------|---------|----------|-----------|
| **[UPDATED_IMPLEMENTATION_PLAN.md](./UPDATED_IMPLEMENTATION_PLAN.md)** | Revised step-by-step plan | Developers, PMs | 15 min |
| **[.cursor/plans/cpt-backend-dashboard-implementation_f0629dba.plan.md](./.cursor/plans/cpt-backend-dashboard-implementation_f0629dba.plan.md)** | Original backend plan | Reference | 10 min |
| **[cpt_dashboard_implementation_plan_0e148abe.plan.md](./cpt_dashboard_implementation_plan_0e148abe.plan.md)** | Original full plan | Reference | 15 min |

### Scripts

| File | Purpose | Usage |
|------|---------|-------|
| **[backend/scripts/verify_supabase_tables.py](./backend/scripts/verify_supabase_tables.py)** | Verify table types and triggers | `python3 scripts/verify_supabase_tables.py` |
| **[backend/scripts/investigate_table_relationships.py](./backend/scripts/investigate_table_relationships.py)** | Analyze schemas and data | `python3 scripts/investigate_table_relationships.py` |

---

## 🔍 What We Investigated

### Original Request
Implement the CPT Dashboard plan:
1. Verify Supabase database structure
2. Normalize CPT codes (remove `.0` suffix)
3. Test scrapers (Medicare, FairHealth)
4. Verify prices match FairHealth Online

### What We Found
🔴 **CRITICAL**: Data is fragmented across TWO tables with NO synchronization!

---

## 🎯 Key Findings

### The Problem

**Two active tables exist:**

1. **`new_updated_historical_medical_benchmarking_data`**
   - 5 scrapers write here
   - Latest data: Nov 2025
   - Has `.0` suffix on codes

2. **`new_updated_medical_benchmarking_data`**
   - 2 scrapers write here
   - Latest data: January 2026

**Impact**: Dashboard may be missing data from one table!

### Why This Matters

- ❌ Can't normalize codes until we know which table to target
- ❌ Can't test scrapers until we know data will be visible
- ❌ Can't verify prices until we know which table to query

---

## 📊 Three Strategic Options

### Option A: Consolidate to One Table ⭐ RECOMMENDED
- **Timeline**: 1-2 days
- **Effort**: Medium
- **Long-term**: Best solution

### Option B: Create Unified View
- **Timeline**: Few hours
- **Effort**: Low
- **Long-term**: Temporary fix

### Option C: Keep Separate Tables
- **Timeline**: 1-2 days
- **Effort**: Medium
- **Long-term**: High maintenance

**Our Recommendation**: Start with B, migrate to A.

---

## 📖 Reading Guide by Role

### If you're a **Product Manager**:
1. Read: [`EXECUTIVE_SUMMARY.md`](./EXECUTIVE_SUMMARY.md)
2. Review: [`ARCHITECTURE_DIAGRAMS.md`](./ARCHITECTURE_DIAGRAMS.md) (visual overview)
3. Decide: Which option to pursue
4. Next: Schedule decision meeting

### If you're a **Technical Lead**:
1. Read: [`DATABASE_ARCHITECTURE_ANALYSIS.md`](./DATABASE_ARCHITECTURE_ANALYSIS.md)
2. Review: [`ARCHITECTURE_DIAGRAMS.md`](./ARCHITECTURE_DIAGRAMS.md)
3. Read: [`UPDATED_IMPLEMENTATION_PLAN.md`](./UPDATED_IMPLEMENTATION_PLAN.md)
4. Next: Identify dashboard query target, recommend strategy

### If you're a **Backend Developer**:
1. Read: [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md)
2. Review: [`backend/SUPABASE_VERIFICATION_FINDINGS.md`](./backend/SUPABASE_VERIFICATION_FINDINGS.md)
3. Run: Scripts in `backend/scripts/` to verify findings
4. Next: Search codebase for dashboard query target

### If you're a **Frontend Developer**:
1. Read: [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md)
2. Action: Search frontend for table references:
   ```bash
   grep -r "new_updated_historical_medical_benchmarking_data" frontend/
   grep -r "new_updated_medical_benchmarking_data" frontend/
   ```
3. Next: Report which table(s) the dashboard queries

### If you're a **QA/Tester**:
1. Read: [`EXECUTIVE_SUMMARY.md`](./EXECUTIVE_SUMMARY.md) (Success Criteria section)
2. Review: [`UPDATED_IMPLEMENTATION_PLAN.md`](./UPDATED_IMPLEMENTATION_PLAN.md) (testing steps)
3. Next: Prepare test cases for chosen strategy

---

## ✅ What's Been Completed

- ✅ Database verification scripts created and run
- ✅ Table relationships analyzed
- ✅ Scraper-to-table mapping documented
- ✅ Three strategic options defined
- ✅ Comprehensive documentation created
- ✅ Risk assessment completed

---

## ⏳ What's Blocked

- ❌ Code normalization (don't know which table)
- ❌ Scraper testing (may write to wrong table)
- ❌ Price verification (don't know which table to query)

---

## 🚀 Next Steps

### Step 1: Identify Dashboard Query (30 min)
**Owner**: Frontend/Backend Developer

**Action**: Find which table the dashboard queries.

**Commands**:
```bash
# Frontend
grep -r "new_updated_historical_medical_benchmarking_data" frontend/
grep -r "new_updated_medical_benchmarking_data" frontend/

# Backend API
grep -r "new_updated_historical_medical_benchmarking_data" backend/app/api/
grep -r "new_updated_medical_benchmarking_data" backend/app/api/
```

### Step 2: Decision Meeting (1 hour)
**Owner**: Technical Lead + Product Manager

**Agenda**:
1. Review findings
2. Discuss options
3. Choose strategy
4. Assign tasks

### Step 3: Implementation (hours to days)
**Owner**: Backend Developer

**Follow**: [`UPDATED_IMPLEMENTATION_PLAN.md`](./UPDATED_IMPLEMENTATION_PLAN.md)

---

## 📞 Questions?

### "Which file should I read first?"
- **Executive/PM**: [`EXECUTIVE_SUMMARY.md`](./EXECUTIVE_SUMMARY.md)
- **Developer**: [`QUICK_REFERENCE.md`](./QUICK_REFERENCE.md)
- **Tech Lead**: [`DATABASE_ARCHITECTURE_ANALYSIS.md`](./DATABASE_ARCHITECTURE_ANALYSIS.md)

### "What's the TL;DR?"
Data is split across two tables. Dashboard may be missing data. Need to decide: consolidate tables, create view, or keep separate.

### "How long will this take?"
- Quick fix: Few hours (unified view)
- Permanent fix: 1-2 days (consolidation)
- Recommended: Both (~2 weeks total)

### "What's the risk if we do nothing?"
- Dashboard shows incomplete data
- Duplicate rows confuse users
- Prices don't match FairHealth Online
- Scrapers may work but data not visible

### "What's blocking us?"
Need to know which table the dashboard queries before we can proceed with normalization, testing, or verification.

---

## 📈 Timeline Summary

### Immediate (Today)
- ✅ Investigation complete
- ⏳ Identify dashboard query (30 min)
- ⏳ Decision meeting (1 hour)

### Short-term (This Week)
- ⏳ Implement quick fix (Option B - few hours)
- ⏳ Test and verify (few hours)

### Long-term (Next Sprint)
- ⏳ Migrate to permanent solution (Option A - 1-2 days)
- ⏳ Deprecate old table
- ⏳ Final verification

**Total**: ~2 weeks for complete solution

---

## 🎯 Success Criteria

### Immediate Success
- ✅ Dashboard shows data from ALL scrapers
- ✅ Code 36475 appears on ONE row
- ✅ Prices match FairHealth Online
- ✅ Latest data visible

### Long-term Success
- ✅ Single table architecture
- ✅ No data fragmentation
- ✅ Simplified queries
- ✅ Easy to maintain

---

## 📝 Document Change Log

| Date | File | Change |
|------|------|--------|
| 2026-02-03 | All | Initial investigation and documentation |

---

## 🔗 Related Resources

### Original Plans
- [Backend Implementation Plan](./.cursor/plans/cpt-backend-dashboard-implementation_f0629dba.plan.md)
- [Full Dashboard Plan](./cpt_dashboard_implementation_plan_0e148abe.plan.md)

### Verification Scripts
- [verify_supabase_tables.py](./backend/scripts/verify_supabase_tables.py)
- [investigate_table_relationships.py](./backend/scripts/investigate_table_relationships.py)

### Supabase Tables
- `new_updated_historical_medical_benchmarking_data` (5 scrapers)
- `new_updated_medical_benchmarking_data` (2 scrapers)
- `historical_medical_benchmarking_data` (deprecated)

---

**Status**: ⏸️ **PAUSED - Awaiting Decision**  
**Blocker**: Need to choose table consolidation strategy  
**Next**: Identify dashboard query target, then schedule decision meeting  
**ETA**: Can resume within hours of decision
