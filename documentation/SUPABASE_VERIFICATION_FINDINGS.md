# Supabase Database Verification Findings

**Date**: 2026-02-03  
**Verification Script**: `backend/scripts/verify_supabase_tables.py`  
**Related Plans**:
- `.cursor/plans/cpt-backend-dashboard-implementation_f0629dba.plan.md`
- `cpt_dashboard_implementation_plan_0e148abe.plan.md`

---

## Executive Summary

The verification script revealed that **all three medical benchmarking tables are BASE TABLEs** with **no views or triggers** connecting them. This contradicts the assumption in the implementation plans that there might be a view/trigger architecture for data synchronization.

---

## Verification Results

### 1. Table Types (All are BASE TABLEs)

```
Table Name                                          | Type
----------------------------------------------------|------------
historical_medical_benchmarking_data                | BASE TABLE
new_updated_historical_medical_benchmarking_data    | BASE TABLE
new_updated_medical_benchmarking_data               | BASE TABLE
```

### 2. View Definition Check

**Result**: `new_updated_medical_benchmarking_data` is **NOT a VIEW**

This means there is no automatic synchronization from the historical table to the current table via a database view.

### 3. Trigger Check

**Result**: **NO triggers** exist on `new_updated_historical_medical_benchmarking_data`

This means there is no automatic data propagation mechanism at the database level.

---

## Critical Implications

### 🔴 Data Architecture Gap

**Problem**: The three tables exist independently with no automatic synchronization mechanism.

**Questions to Answer**:

1. **What is the relationship between these three tables?**
   - Are they meant to store different data?
   - Are they different versions/schemas?
   - Is one deprecated?

2. **Which table(s) do the scrapers write to?**
   - Need to verify scraper database handlers
   - Check `backend/app/cpt_automated_scripts/*/database.py` files

3. **Which table(s) does the dashboard read from?**
   - Need to verify frontend API queries
   - Check Supabase queries in the frontend codebase

4. **Why do we have three separate tables?**
   - Historical archive vs current data?
   - Different data sources?
   - Migration in progress?

---

## Next Steps: Investigation Required

### Step 1: Identify Scraper Target Tables

**Action**: Check which tables each scraper writes to.

**Files to Review**:
```
backend/app/cpt_automated_scripts/Fair_Health_Facility/database.py
backend/app/cpt_automated_scripts/Fair_Health_Physicians/database.py
backend/app/cpt_automated_scripts/Medicare_ASC_Addenda/database.py
```

**What to Look For**:
- Table names in `INSERT` or `UPSERT` statements
- Supabase client table references
- Any logic that writes to multiple tables

### Step 2: Identify Dashboard Query Tables

**Action**: Determine which table(s) the frontend queries for dashboard data.

**Where to Check**:
- Frontend Supabase queries (if you have access to frontend repo)
- Backend API endpoints that serve dashboard data
- Any database views or RLS policies that might filter data

### Step 3: Compare Table Schemas

**Action**: Run schema comparison to see if tables have identical structures.

**SQL Query**:
```sql
-- Get column definitions for all three tables
SELECT 
  table_name,
  column_name,
  data_type,
  is_nullable,
  column_default
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name IN (
    'historical_medical_benchmarking_data',
    'new_updated_historical_medical_benchmarking_data',
    'new_updated_medical_benchmarking_data'
  )
ORDER BY table_name, ordinal_position;
```

### Step 4: Check Data Overlap

**Action**: See if the same CPT codes exist across multiple tables.

**SQL Query**:
```sql
-- Count records in each table
SELECT 'historical_medical_benchmarking_data' as table_name, COUNT(*) as record_count
FROM historical_medical_benchmarking_data
UNION ALL
SELECT 'new_updated_historical_medical_benchmarking_data', COUNT(*)
FROM new_updated_historical_medical_benchmarking_data
UNION ALL
SELECT 'new_updated_medical_benchmarking_data', COUNT(*)
FROM new_updated_medical_benchmarking_data;

-- Check for code 36475 in all three tables
SELECT 'historical_medical_benchmarking_data' as source_table, code, data_type, geozip, "80th", release_date
FROM historical_medical_benchmarking_data
WHERE code LIKE '36475%'
UNION ALL
SELECT 'new_updated_historical_medical_benchmarking_data', code, data_type, geozip, "80th", release_date
FROM new_updated_historical_medical_benchmarking_data
WHERE code LIKE '36475%'
UNION ALL
SELECT 'new_updated_medical_benchmarking_data', code, data_type, geozip, "80th", release_date
FROM new_updated_medical_benchmarking_data
WHERE code LIKE '36475%'
ORDER BY source_table, code, data_type, geozip;
```

### Step 5: Review Application Code for Table Usage

**Action**: Search codebase for references to each table name.

**Command**:
```bash
cd /Users/bilalsiddique/Downloads/CPT_Automation_scripts-main\ 2/backend
grep -r "historical_medical_benchmarking_data" app/
grep -r "new_updated_historical_medical_benchmarking_data" app/
grep -r "new_updated_medical_benchmarking_data" app/
```

---

## Recommended Investigation Script

Create a new script `backend/scripts/investigate_table_relationships.py` to answer these questions:

```python
#!/usr/bin/env python3
"""
Investigate the relationship between the three medical benchmarking tables.
"""

import os
from pathlib import Path
from dotenv import load_dotenv
import psycopg2
from urllib.parse import urlparse, unquote

# Load environment
backend_dir = Path(__file__).resolve().parent.parent
load_dotenv(backend_dir / ".env")

def get_connection():
    url = os.getenv("DATABASE_URL")
    if url:
        parsed = urlparse(url)
        return psycopg2.connect(
            host=parsed.hostname,
            port=int(parsed.port or 5432),
            dbname=(parsed.path or "/postgres").lstrip("/"),
            user=parsed.username,
            password=unquote(parsed.password or ""),
        )
    raise ValueError("Set DATABASE_URL in backend/.env")

def run_query(cursor, title, sql):
    print(f"\n{'='*80}\n{title}\n{'='*80}")
    cursor.execute(sql)
    rows = cursor.fetchall()
    colnames = [d[0] for d in cursor.description]
    print("Columns:", colnames)
    for row in rows:
        print(row)
    if not rows:
        print("(no rows)")
    return rows

def main():
    conn = get_connection()
    cur = conn.cursor()

    # 1. Record counts
    run_query(cur, "1. Record Counts", """
        SELECT 'historical_medical_benchmarking_data' as table_name, COUNT(*) as records
        FROM historical_medical_benchmarking_data
        UNION ALL
        SELECT 'new_updated_historical_medical_benchmarking_data', COUNT(*)
        FROM new_updated_historical_medical_benchmarking_data
        UNION ALL
        SELECT 'new_updated_medical_benchmarking_data', COUNT(*)
        FROM new_updated_medical_benchmarking_data;
    """)

    # 2. Schema comparison
    run_query(cur, "2. Schema Comparison", """
        SELECT 
          table_name,
          column_name,
          data_type,
          is_nullable
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name IN (
            'historical_medical_benchmarking_data',
            'new_updated_historical_medical_benchmarking_data',
            'new_updated_medical_benchmarking_data'
          )
        ORDER BY table_name, ordinal_position;
    """)

    # 3. Sample data from each table
    run_query(cur, "3a. Sample from historical_medical_benchmarking_data", """
        SELECT code, data_type, geozip, "80th", release_date, source
        FROM historical_medical_benchmarking_data
        LIMIT 5;
    """)

    run_query(cur, "3b. Sample from new_updated_historical_medical_benchmarking_data", """
        SELECT code, data_type, geozip, "80th", release_date, source
        FROM new_updated_historical_medical_benchmarking_data
        LIMIT 5;
    """)

    run_query(cur, "3c. Sample from new_updated_medical_benchmarking_data", """
        SELECT code, data_type, geozip, "80th", release_date, source
        FROM new_updated_medical_benchmarking_data
        LIMIT 5;
    """)

    # 4. Check for code 36475 in all tables
    run_query(cur, "4. Code 36475 in All Tables", """
        SELECT 'historical' as tbl, code, data_type, geozip, "80th", release_date
        FROM historical_medical_benchmarking_data
        WHERE code LIKE '36475%'
        UNION ALL
        SELECT 'new_updated_historical', code, data_type, geozip, "80th", release_date
        FROM new_updated_historical_medical_benchmarking_data
        WHERE code LIKE '36475%'
        UNION ALL
        SELECT 'new_updated', code, data_type, geozip, "80th", release_date
        FROM new_updated_medical_benchmarking_data
        WHERE code LIKE '36475%'
        ORDER BY tbl, code, data_type, geozip;
    """)

    # 5. Check unique sources in each table
    run_query(cur, "5. Unique Sources in Each Table", """
        SELECT 'historical' as tbl, DISTINCT source
        FROM historical_medical_benchmarking_data
        UNION ALL
        SELECT 'new_updated_historical', DISTINCT source
        FROM new_updated_historical_medical_benchmarking_data
        UNION ALL
        SELECT 'new_updated', DISTINCT source
        FROM new_updated_medical_benchmarking_data
        ORDER BY tbl, source;
    """)

    cur.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
```

---

## Updated Implementation Plan

### Phase 1: Data Architecture Clarification (BLOCKING)

**Status**: 🔴 **MUST COMPLETE BEFORE PROCEEDING**

1. ✅ Run `verify_supabase_tables.py` (DONE - revealed no views/triggers)
2. ⏳ Run `investigate_table_relationships.py` (NEW - see script above)
3. ⏳ Review scraper database handlers to identify write targets
4. ⏳ Review frontend/API queries to identify read sources
5. ⏳ Document the intended purpose of each table
6. ⏳ Decide on the canonical table(s) for:
   - Scraper writes
   - Dashboard reads
   - Historical archive

### Phase 2: Code Normalization (DEPENDS ON PHASE 1)

**Only proceed after confirming which table(s) need normalization**

- Run `.0` suffix cleanup on the correct table(s)
- Verify no duplicate codes remain

### Phase 3: Scraper Testing (DEPENDS ON PHASE 1)

**Only proceed after confirming scraper write targets**

- Test Medicare scraper
- Test FairHealth ASC scraper
- Verify data lands in the correct table(s)

### Phase 4: Price Verification (DEPENDS ON PHASES 1-3)

- Query code 36475 from the correct table
- Compare against FairHealth Online
- Identify and resolve discrepancies

---

## Risk Assessment

### 🔴 HIGH RISK: Proceeding Without Clarification

**If we skip Phase 1 and proceed with the original plan**:

- ❌ May normalize codes in the wrong table
- ❌ May test scrapers that write to a different table than the dashboard reads
- ❌ May verify prices from a table that isn't being used
- ❌ May introduce data inconsistencies across tables

### ✅ SAFE PATH: Complete Investigation First

**Benefits**:

- ✅ Understand actual data flow
- ✅ Target the correct tables for normalization
- ✅ Verify scrapers write to the right place
- ✅ Ensure dashboard reads from the right source
- ✅ Avoid wasted effort and potential data corruption

---

## Questions for Stakeholders

1. **What is the intended purpose of each table?**
   - `historical_medical_benchmarking_data` - ?
   - `new_updated_historical_medical_benchmarking_data` - ?
   - `new_updated_medical_benchmarking_data` - ?

2. **Which table should the dashboard read from?**

3. **Which table(s) should scrapers write to?**

4. **Is there a migration in progress?**
   - Are we transitioning from old tables to new ones?
   - Should we consolidate tables?

5. **Do we need all three tables?**
   - Can we deprecate any?
   - Should we set up views/triggers for synchronization?

---

## Conclusion

**The verification script has revealed a critical gap in our understanding of the database architecture.** We must complete the investigation phase before proceeding with data normalization or scraper testing to avoid working on the wrong tables.

**NEXT ACTION**: Run `investigate_table_relationships.py` and review scraper database handlers to map the data flow.
