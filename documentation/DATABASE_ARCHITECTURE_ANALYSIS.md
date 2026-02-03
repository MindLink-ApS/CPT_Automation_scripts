# Database Architecture Analysis - CRITICAL FINDINGS

**Date**: 2026-02-03  
**Status**: 🔴 **REQUIRES IMMEDIATE ATTENTION**

---

## Executive Summary

After running verification scripts and analyzing scraper code, we have discovered a **split architecture** where:

1. **Different scrapers write to different tables**
2. **Tables have different schemas** (especially `release_date` column types)
3. **No synchronization mechanism** exists between tables
4. **The dashboard likely reads from only ONE table**, creating a data visibility problem

---

## Table Architecture Breakdown

### Table 1: `historical_medical_benchmarking_data`

**Purpose**: Legacy/old Medicare data  
**Schema**: 
- `release_date`: **DATE** type
- No `id` column
- No `rag_ingested` column

**Sample Data**:
```
Code: 36585, Data Type: Medicare Facility, 80th: $1547.83, Release: 2024-07-01
Code: 37200, Data Type: Medicare Facility, 80th: $2902.77, Release: 2024-07-01
```

**Scrapers Writing Here**: ❌ **NONE** (appears to be deprecated)

---

### Table 2: `new_updated_historical_medical_benchmarking_data`

**Purpose**: Current scraper target for MOST scrapers  
**Schema**:
- `release_date`: **TEXT** type (e.g., "Nov 2025")
- Has `id` column (bigint, primary key)
- Has `rag_ingested` column (boolean)
- Has `rel_date` column (text)

**Sample Data**:
```
Code: 10008.0, Data Type: Physician 070, Geozip: 070, 80th: $1125.00, Release: "Nov 2025"
Code: 10009.0, Data Type: Physician 070, Geozip: 070, 80th: $1960.00, Release: "Nov 2025"
```

**⚠️ NOTICE**: Codes have `.0` suffix!

**Scrapers Writing Here**: ✅ **5 SCRAPERS**
1. `Fair_Health_Facility` (FairHealth ASC)
2. `Fair_Health_Physicians`
3. `Medicare_ASC_Addenda`
4. `New_Jersey_DOBI`
5. `Novitas`

---

### Table 3: `new_updated_medical_benchmarking_data`

**Purpose**: Current scraper target for SOME scrapers  
**Schema**:
- `release_date`: **TEXT** type (e.g., "January 2026")
- Has `id` column (bigint, primary key)
- No `rag_ingested` column
- Has `rel_date` column (text)

**Sample Data**:
```
Code: 27647, Data Type: OBL, 80th: $983.17, Release: "January 2026"
Code: 27648, Data Type: Medicare Professional, 80th: $47.28, Release: "January 2026"
```

**Scrapers Writing Here**: ✅ **2 SCRAPERS**
1. `Horizon_ASC`
2. `Medicare_Clinical_Fees`

**Also Used By**: `specialty_prioritizer.py` (reads CPT codes from this table)

---

## Critical Problem: Data Fragmentation

### The Issue

**Data is split across TWO active tables with NO synchronization:**

| Table | Scrapers | Latest Data |
|-------|----------|-------------|
| `new_updated_historical_medical_benchmarking_data` | 5 scrapers (FairHealth, Medicare, etc.) | Nov 2025 |
| `new_updated_medical_benchmarking_data` | 2 scrapers (Horizon, Medicare Clinical) | January 2026 |

### Impact on Dashboard

**If the dashboard reads from only ONE table**, it will be missing data from the other table!

**Questions to Answer URGENTLY**:

1. **Which table does the dashboard/frontend query?**
   - If it queries `new_updated_historical_medical_benchmarking_data`, it's missing Horizon and Medicare Clinical data
   - If it queries `new_updated_medical_benchmarking_data`, it's missing FairHealth, Medicare ASC, NJ DOBI, and Novitas data

2. **Why are there two separate tables?**
   - Is this intentional separation?
   - Is this a migration in progress?
   - Should all scrapers write to the same table?

3. **What is the intended architecture?**
   - Should we consolidate to one table?
   - Should we create a view that UNIONs both tables?
   - Should we set up triggers to sync data?

---

## Schema Differences

### Key Differences Between Tables

| Feature | `historical_medical_benchmarking_data` | `new_updated_historical_medical_benchmarking_data` | `new_updated_medical_benchmarking_data` |
|---------|----------------------------------------|---------------------------------------------------|----------------------------------------|
| `id` column | ❌ No | ✅ Yes (bigint) | ✅ Yes (bigint) |
| `release_date` type | DATE | TEXT | TEXT |
| `rag_ingested` column | ❌ No | ✅ Yes | ❌ No |
| Active scrapers | 0 | 5 | 2 |
| Latest data | 2024-07-01 | "Nov 2025" | "January 2026" |

### Code Format Issue

**`new_updated_historical_medical_benchmarking_data` has codes with `.0` suffix:**
```
10008.0, 10009.0, 10010.0, etc.
```

This confirms the duplicate row issue Andrew reported (36475 vs 36475.0).

---

## Scraper-to-Table Mapping

### Scrapers → `new_updated_historical_medical_benchmarking_data`

```python
# Fair_Health_Facility/database.py (line 29)
self.table_name = "new_updated_historical_medical_benchmarking_data"

# Fair_Health_Physicians/database.py (line 22)
TABLE_NAME = "new_updated_historical_medical_benchmarking_data"

# Medicare_ASC_Addenda/database.py (line 29)
self.table_name = "new_updated_historical_medical_benchmarking_data"

# New_Jersey_DOBI/database.py (line 34)
self.table_name = "new_updated_historical_medical_benchmarking_data"

# Novitas/database.py (line 30)
self.table_name = "new_updated_historical_medical_benchmarking_data"
```

### Scrapers → `new_updated_medical_benchmarking_data`

```python
# Horizon_ASC/database.py (line 29)
self.table_name = "new_updated_medical_benchmarking_data"

# Medicare_Clinical_Fees/database.py (line 29)
self.table_name = "new_updated_medical_benchmarking_data"
```

---

## Recommended Actions

### 🔴 URGENT: Determine Dashboard Query Target

**Action**: Find out which table the dashboard queries.

**How**:
1. Check frontend codebase for Supabase queries
2. Check backend API endpoints that serve dashboard data
3. Look for `.from('table_name')` or `.table('table_name')` calls

**Files to Check**:
- Frontend: `src/components/dashboard/DataTablePivot.tsx`
- Frontend: `src/lib/supabase.ts` or similar
- Backend: Any API routes that serve benchmarking data

### 🟡 DECISION REQUIRED: Table Consolidation Strategy

**Option 1: Consolidate to One Table**

**Pros**:
- Single source of truth
- No data fragmentation
- Simpler queries

**Cons**:
- Requires scraper updates
- Potential data migration

**Implementation**:
1. Choose target table (recommend `new_updated_medical_benchmarking_data` as it's newer)
2. Migrate data from `new_updated_historical_medical_benchmarking_data`
3. Update all 5 scrapers to write to the consolidated table
4. Add `rag_ingested` column if needed

**Option 2: Create a Unified View**

**Pros**:
- No scraper changes needed
- Preserves existing data structure
- Quick to implement

**Cons**:
- Doesn't solve schema differences
- View performance may be slower
- Still have two tables to maintain

**Implementation**:
```sql
CREATE VIEW unified_medical_benchmarking_data AS
SELECT * FROM new_updated_historical_medical_benchmarking_data
UNION ALL
SELECT 
  id, source, code, rel_date, geozip, speciality, code_description,
  physician_codes, asc_allowable, asc_payment_type, full_description,
  plan_type, mean, "50th", "60th", "70th", "75th", "80th", "85th", "90th", "95th",
  data_type, release_date, scraped_at,
  FALSE as rag_ingested  -- Default value for missing column
FROM new_updated_medical_benchmarking_data;
```

**Option 3: Keep Separate Tables (Document Purpose)**

**Only if there's a valid business reason** (e.g., different data retention policies, different access patterns)

**Requirements**:
- Document the purpose of each table
- Ensure dashboard queries BOTH tables
- Set up monitoring to prevent data gaps

### 🟢 Code Normalization

**After deciding on table strategy**, run the `.0` cleanup:

```sql
-- For whichever table(s) we decide to use
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

---

## Next Steps (In Order)

1. ✅ **DONE**: Run verification scripts
2. ✅ **DONE**: Identify scraper-to-table mappings
3. ⏳ **TODO**: Determine which table the dashboard queries
4. ⏳ **TODO**: Decide on table consolidation strategy
5. ⏳ **TODO**: Implement chosen strategy
6. ⏳ **TODO**: Run code normalization
7. ⏳ **TODO**: Test scrapers
8. ⏳ **TODO**: Verify dashboard shows all data

---

## Questions for Stakeholders

1. **Is the split between the two tables intentional?**
   - If yes, what is the business logic?
   - If no, which table should be the canonical source?

2. **Which table does the dashboard currently query?**

3. **What is the purpose of the `rag_ingested` column?**
   - Should all tables have it?
   - Is it used for AI/RAG features?

4. **Should we consolidate to one table or create a unified view?**

5. **Are there any other systems/services that query these tables?**
   - Need to ensure we don't break integrations

---

## Risk Assessment

### 🔴 HIGH RISK if we proceed without clarification:

- May normalize codes in the wrong table
- May test scrapers but miss data visibility issues
- May verify prices from a table the dashboard doesn't use
- Dashboard may show incomplete data even after "successful" scraper runs

### ✅ SAFE PATH:

1. Identify dashboard query target
2. Choose consolidation strategy
3. Implement with proper testing
4. Verify end-to-end data flow

---

## Conclusion

**We have uncovered a critical data architecture issue: data fragmentation across two tables with no synchronization.** This explains why the dashboard may be showing incomplete or outdated data.

**NEXT ACTION**: Determine which table the dashboard queries, then decide on a consolidation strategy before proceeding with any data normalization or scraper testing.
