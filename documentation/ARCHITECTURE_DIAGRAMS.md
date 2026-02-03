# CPT Dashboard Data Flow Architecture

## Current State (Fragmented)

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SCRAPERS                                   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        ┌─────────────────────────┴─────────────────────────┐
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────────┐
│  new_updated_historical_medical_      │   │  new_updated_medical_                 │
│  benchmarking_data                    │   │  benchmarking_data                    │
├───────────────────────────────────────┤   ├───────────────────────────────────────┤
│ Scrapers (5):                         │   │ Scrapers (2):                         │
│ • Fair_Health_Facility                │   │ • Horizon_ASC                         │
│ • Fair_Health_Physicians              │   │ • Medicare_Clinical_Fees              │
│ • Medicare_ASC_Addenda                │   │                                       │
│ • New_Jersey_DOBI                     │   │ Also used by:                         │
│ • Novitas                             │   │ • specialty_prioritizer.py            │
│                                       │   │                                       │
│ Latest Data: Nov 2025                 │   │ Latest Data: January 2026             │
│ Issue: Codes have .0 suffix           │   │                                       │
│ Has: rag_ingested column              │   │ Missing: rag_ingested column          │
└───────────────────────────────────────┘   └───────────────────────────────────────┘
                │                                             │
                │                                             │
                └─────────────────┬───────────────────────────┘
                                  │
                                  ▼
                          ┌───────────────┐
                          │   DASHBOARD   │
                          │   (QUERIES    │
                          │   WHICH ONE?) │
                          └───────────────┘
                                  │
                                  ▼
                          ❌ MISSING DATA!
```

## Problem Scenarios

### Scenario 1: Dashboard queries Table 1 only
```
Dashboard → new_updated_historical_medical_benchmarking_data
            ✅ Shows: FairHealth, Medicare ASC, NJ DOBI, Novitas
            ❌ Missing: Horizon, Medicare Clinical Fees
            ❌ Missing: January 2026 data
```

### Scenario 2: Dashboard queries Table 2 only
```
Dashboard → new_updated_medical_benchmarking_data
            ✅ Shows: Horizon, Medicare Clinical Fees
            ❌ Missing: FairHealth, Medicare ASC, NJ DOBI, Novitas
            ❌ Missing: Most of the data!
```

---

## Proposed Solution A: Consolidate to One Table (RECOMMENDED)

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SCRAPERS (ALL 7)                           │
│  • Fair_Health_Facility      • Medicare_ASC_Addenda                 │
│  • Fair_Health_Physicians    • New_Jersey_DOBI                      │
│  • Horizon_ASC               • Novitas                              │
│  • Medicare_Clinical_Fees                                           │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                ┌─────────────────────────────────────────┐
                │  new_updated_medical_benchmarking_data  │
                │  (SINGLE SOURCE OF TRUTH)               │
                ├─────────────────────────────────────────┤
                │ All scrapers write here                 │
                │ Latest data: January 2026               │
                │ No .0 suffix (normalized)               │
                │ Has: rag_ingested column (added)        │
                └─────────────────────────────────────────┘
                                  │
                                  ▼
                          ┌───────────────┐
                          │   DASHBOARD   │
                          │  (Queries 1   │
                          │   table only) │
                          └───────────────┘
                                  │
                                  ▼
                          ✅ ALL DATA VISIBLE!
```

### Migration Steps:
1. Add `rag_ingested` column to `new_updated_medical_benchmarking_data`
2. Migrate data from `new_updated_historical_medical_benchmarking_data`
3. Update 5 scrapers to write to consolidated table
4. Normalize codes (strip .0 suffix)
5. Update dashboard if needed
6. Deprecate old table

---

## Proposed Solution B: Unified View

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SCRAPERS                                   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        ┌─────────────────────────┴─────────────────────────┐
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────────┐
│  new_updated_historical_medical_      │   │  new_updated_medical_                 │
│  benchmarking_data                    │   │  benchmarking_data                    │
│  (5 scrapers write here)              │   │  (2 scrapers write here)              │
└───────────────────────────────────────┘   └───────────────────────────────────────┘
                │                                             │
                └─────────────────┬───────────────────────────┘
                                  │
                                  ▼
                ┌─────────────────────────────────────────┐
                │  unified_medical_benchmarking_data      │
                │  (VIEW - UNION of both tables)          │
                └─────────────────────────────────────────┘
                                  │
                                  ▼
                          ┌───────────────┐
                          │   DASHBOARD   │
                          │  (Queries the │
                          │     view)     │
                          └───────────────┘
                                  │
                                  ▼
                          ✅ ALL DATA VISIBLE!
```

### Implementation:
```sql
CREATE VIEW unified_medical_benchmarking_data AS
SELECT *, rag_ingested FROM new_updated_historical_medical_benchmarking_data
UNION ALL
SELECT *, FALSE as rag_ingested FROM new_updated_medical_benchmarking_data;
```

**Pros**: Quick, no scraper changes  
**Cons**: Still maintaining two tables, potential performance impact

---

## Proposed Solution C: Keep Separate (NOT RECOMMENDED)

```
┌─────────────────────────────────────────────────────────────────────┐
│                           SCRAPERS                                   │
└─────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
        ┌─────────────────────────┴─────────────────────────┐
        │                                                     │
        ▼                                                     ▼
┌───────────────────────────────────────┐   ┌───────────────────────────────────────┐
│  new_updated_historical_medical_      │   │  new_updated_medical_                 │
│  benchmarking_data                    │   │  benchmarking_data                    │
│  (Historical/Archive Data)            │   │  (Current/Live Data)                  │
│  Purpose: [DOCUMENT THIS]             │   │  Purpose: [DOCUMENT THIS]             │
└───────────────────────────────────────┘   └───────────────────────────────────────┘
                │                                             │
                └─────────────────┬───────────────────────────┘
                                  │
                                  ▼
                          ┌───────────────┐
                          │   DASHBOARD   │
                          │  (Queries     │
                          │   BOTH with   │
                          │   UNION)      │
                          └───────────────┘
                                  │
                                  ▼
                          ✅ ALL DATA VISIBLE
                          ⚠️  Complex queries
                          ⚠️  Ongoing fragmentation risk
```

**Only choose this if there's a documented business reason for separation!**

---

## Code Normalization Issue

### Current State (in `new_updated_historical_medical_benchmarking_data`)

```
┌──────────────┬─────────────┬────────┬────────┐
│ code         │ data_type   │ geozip │ 80th   │
├──────────────┼─────────────┼────────┼────────┤
│ 36475        │ Facility 070│ 070    │ $2,356 │  ← Row 1
│ 36475.0      │ Facility 074│ 074    │ $5,678 │  ← Row 2 (DUPLICATE!)
└──────────────┴─────────────┴────────┴────────┘
```

### After Normalization

```
┌──────────────┬─────────────┬────────┬────────┐
│ code         │ data_type   │ geozip │ 80th   │
├──────────────┼─────────────┼────────┼────────┤
│ 36475        │ Facility 070│ 070    │ $2,356 │  ← Single row
│ 36475        │ Facility 074│ 074    │ $5,678 │  ← Single row
└──────────────┴─────────────┴────────┴────────┘
```

**SQL to fix**:
```sql
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

---

## Decision Matrix

| Criterion | Option A (Consolidate) | Option B (View) | Option C (Separate) |
|-----------|------------------------|-----------------|---------------------|
| **Data Consistency** | ⭐⭐⭐⭐⭐ Single source | ⭐⭐⭐ View can lag | ⭐⭐ Manual sync needed |
| **Performance** | ⭐⭐⭐⭐⭐ Direct query | ⭐⭐⭐ UNION overhead | ⭐⭐ Multiple queries |
| **Maintainability** | ⭐⭐⭐⭐⭐ Simple | ⭐⭐⭐ Two tables | ⭐⭐ Complex |
| **Implementation Time** | ⭐⭐⭐ 1-2 days | ⭐⭐⭐⭐⭐ Few hours | ⭐⭐⭐ 1-2 days |
| **Risk** | ⭐⭐⭐ Migration risk | ⭐⭐⭐⭐ Low risk | ⭐⭐ Ongoing fragmentation |
| **Long-term Cost** | ⭐⭐⭐⭐⭐ Low | ⭐⭐⭐ Medium | ⭐⭐ High |

**Recommendation**: **Option A (Consolidate)** for best long-term outcome

---

## Timeline Comparison

### Option A: Consolidate to One Table
```
Day 1: Migration script + scraper updates (6-8 hours)
Day 2: Testing + deployment (4-6 hours)
Total: 1-2 days
```

### Option B: Unified View
```
Day 1: Create view + update dashboard (2-4 hours)
Total: Few hours
```

### Option C: Keep Separate
```
Day 1: Document + update dashboard (6-8 hours)
Day 2: Testing + monitoring setup (4-6 hours)
Total: 1-2 days
```

---

## Risk Assessment

### Option A Risks
- ❌ Data loss during migration (Mitigation: Backup + staging test)
- ❌ Scraper downtime (Mitigation: Deploy during low-traffic window)
- ✅ Long-term benefits outweigh short-term risks

### Option B Risks
- ❌ View performance issues (Mitigation: Monitor query times)
- ❌ Still maintaining two tables (Mitigation: Plan future consolidation)
- ✅ Quick to implement, low immediate risk

### Option C Risks
- ❌ Ongoing fragmentation (Mitigation: Strong documentation)
- ❌ Complex queries (Mitigation: Create helper functions)
- ❌ Future maintenance burden (Mitigation: Regular audits)

---

## Recommendation

**Choose Option A (Consolidate to One Table)** because:

1. ✅ Single source of truth
2. ✅ Best performance
3. ✅ Easiest to maintain long-term
4. ✅ Eliminates fragmentation risk
5. ✅ Worth the 1-2 day investment

**If time is critical**, start with **Option B (View)** as a temporary solution, then migrate to **Option A** later.

**Avoid Option C** unless there's a documented business requirement for table separation.
