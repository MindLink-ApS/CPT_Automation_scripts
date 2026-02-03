# SQL Queries for CPT Dashboard Implementation

This file contains all SQL queries needed for database investigation, normalization, and verification tasks.

**Note**: Based on `DATABASE_TABLES.md`, FairHealth data is stored in `new_updated_historical_medical_benchmarking_data`. However, some queries may also need to check `new_updated_medical_benchmarking_data` (which may be a view or current data table). Adjust table names as needed based on your Supabase schema.

---

## 1. Code 36475 Investigation

### 1.1 Query All Records for Code 36475 Variants

```sql
-- Get all records for code 36475 variants from historical table
SELECT 
  code, 
  data_type, 
  geozip, 
  "80th", 
  "90th",
  source,
  release_date,
  rel_date,
  scraped_at
FROM new_updated_historical_medical_benchmarking_data
WHERE code LIKE '36475%'
ORDER BY code, data_type, geozip;

-- Also check current/active table if it exists
SELECT 
  code, 
  data_type, 
  geozip, 
  "80th", 
  "90th",
  source,
  release_date,
  rel_date,
  scraped_at
FROM new_updated_medical_benchmarking_data
WHERE code LIKE '36475%'
ORDER BY code, data_type, geozip;
```

### 1.2 Check for Duplicate Codes (36475 vs 36475.0)

```sql
-- Check if we have multiple versions of 36475 in historical table
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  release_date,
  scraped_at,
  source
FROM new_updated_historical_medical_benchmarking_data
WHERE code IN ('36475', '36475.0')
ORDER BY scraped_at DESC;

-- Also check current table
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  release_date,
  scraped_at,
  source
FROM new_updated_medical_benchmarking_data
WHERE code IN ('36475', '36475.0')
ORDER BY scraped_at DESC;
```

### 1.3 Compare Prices Against Expected Values

**Expected values from FairHealth Online (Jan 2026 Release)**:
- Geozip 070, Facility: $10,761.00 (80th percentile)
- Geozip 074, Facility: $25,139.00 (80th percentile)
- Geozip USA, Facility: $10,761.00 (80th percentile)

```sql
-- Query current values for comparison
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  "90th",
  release_date,
  source
FROM new_updated_historical_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
  AND geozip IN ('070', '074', 'USA')
ORDER BY geozip;
```

---

## 2. Release Date Inventory

### 2.1 Check Available FairHealth Facility Releases

```sql
-- Check what releases we have for FairHealth Facility in historical table
SELECT DISTINCT 
  source,
  data_type,
  release_date,
  rel_date,
  COUNT(*) as record_count
FROM new_updated_historical_medical_benchmarking_data
WHERE data_type LIKE 'Facility%'
  AND source LIKE '%FairHealth%'
GROUP BY source, data_type, release_date, rel_date
ORDER BY release_date DESC, rel_date DESC;

-- Also check current table
SELECT DISTINCT 
  source,
  data_type,
  release_date,
  rel_date,
  COUNT(*) as record_count
FROM new_updated_medical_benchmarking_data
WHERE data_type LIKE 'Facility%'
  AND source LIKE '%FairHealth%'
GROUP BY source, data_type, release_date, rel_date
ORDER BY release_date DESC, rel_date DESC;
```

### 2.2 Check for January 2026 Release

```sql
-- Check for January 2026 release in historical table
SELECT DISTINCT 
  release_date,
  rel_date,
  data_type,
  source,
  COUNT(*) as records
FROM new_updated_historical_medical_benchmarking_data
WHERE source LIKE '%FairHealth%'
  AND (
    release_date LIKE '%2026%' 
    OR rel_date LIKE '%2026%' 
    OR rel_date LIKE '%Jan%2026%'
    OR release_date LIKE '%Jan%2026%'
  )
GROUP BY release_date, rel_date, data_type, source
ORDER BY release_date DESC, rel_date DESC;

-- Also check current table
SELECT DISTINCT 
  release_date,
  rel_date,
  data_type,
  source,
  COUNT(*) as records
FROM new_updated_medical_benchmarking_data
WHERE source LIKE '%FairHealth%'
  AND (
    release_date LIKE '%2026%' 
    OR rel_date LIKE '%2026%' 
    OR rel_date LIKE '%Jan%2026%'
    OR release_date LIKE '%Jan%2026%'
  )
GROUP BY release_date, rel_date, data_type, source
ORDER BY release_date DESC, rel_date DESC;
```

---

## 3. Code Normalization Preview

### 3.1 Preview Codes Ending in .0

```sql
-- Preview affected records in historical table
SELECT 
  code, 
  REGEXP_REPLACE(code, '\.0+$', '') as normalized_code, 
  COUNT(*) as count
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code
ORDER BY count DESC;

-- Preview affected records in current table
SELECT 
  code, 
  REGEXP_REPLACE(code, '\.0+$', '') as normalized_code, 
  COUNT(*) as count
FROM new_updated_medical_benchmarking_data
WHERE code ~ '\.0+$'
GROUP BY code
ORDER BY count DESC;

-- Count total affected records
SELECT COUNT(*) as total_affected_records
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$';

SELECT COUNT(*) as total_affected_records
FROM new_updated_medical_benchmarking_data
WHERE code ~ '\.0+$';
```

---

## 4. Code Normalization Execution

### 4.1 Normalize Codes Ending in .0

**⚠️ IMPORTANT: Review the preview queries above before executing these UPDATE statements.**

```sql
-- Execute cleanup on historical table
UPDATE new_updated_historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';

-- Execute cleanup on current table
UPDATE new_updated_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';

-- Also update historical_medical_benchmarking_data if it exists as a separate table
UPDATE historical_medical_benchmarking_data
SET code = REGEXP_REPLACE(code, '\.0+$', '')
WHERE code ~ '\.0+$';
```

---

## 5. Post-Normalization Verification

### 5.1 Verify No .0 Codes Remain

```sql
-- Verify no .0 codes remain in historical table
SELECT COUNT(*) as remaining_dot0_codes
FROM new_updated_historical_medical_benchmarking_data
WHERE code ~ '\.0+$';

-- Verify no .0 codes remain in current table
SELECT COUNT(*) as remaining_dot0_codes
FROM new_updated_medical_benchmarking_data
WHERE code ~ '\.0+$';

-- Should return 0 for both queries
```

### 5.2 Re-check Code 36475 After Normalization

```sql
-- Re-check code 36475 after normalization (should only show '36475', not '36475.0')
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  release_date,
  source
FROM new_updated_historical_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip;

-- Also check current table
SELECT 
  code,
  data_type,
  geozip,
  "80th",
  release_date,
  source
FROM new_updated_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
ORDER BY geozip;
```

---

## 6. January 2026 Release Verification

### 6.1 Verify January 2026 FairHealth Data Exists

```sql
-- Check for January 2026 release after scraper run
SELECT DISTINCT 
  release_date,
  rel_date,
  data_type,
  source,
  COUNT(*) as records
FROM new_updated_historical_medical_benchmarking_data
WHERE source LIKE '%FairHealth%'
  AND (
    release_date LIKE '%2026%' 
    OR rel_date LIKE '%2026%' 
    OR rel_date LIKE '%Jan%2026%'
    OR release_date LIKE '%Jan%2026%'
  )
GROUP BY release_date, rel_date, data_type, source
ORDER BY release_date DESC, rel_date DESC;
```

### 6.2 Verify Code 36475 Prices Match Source (After Fresh Scrape)

```sql
-- Final verification query for code 36475 after scraping and normalization
SELECT 
  code, 
  data_type, 
  geozip, 
  "80th", 
  release_date,
  source
FROM new_updated_historical_medical_benchmarking_data
WHERE code = '36475'
  AND data_type LIKE 'Facility%'
  AND geozip IN ('070', '074', 'USA')
ORDER BY geozip;

-- Compare these values against FairHealth Online (January 2026 release):
-- Expected: 070 = $10,761.00, 074 = $25,139.00, USA = $10,761.00
```

---

## 7. Additional Diagnostic Queries

### 7.1 Check Source Names Used

```sql
-- Verify what source names are actually used in the database
SELECT DISTINCT source
FROM new_updated_historical_medical_benchmarking_data
WHERE source LIKE '%FairHealth%'
ORDER BY source;

-- Note: Scrapers use "Fair Health Facility" (with spaces), but queries may reference "FairHealth_ASC"
```

### 7.2 Check Data Types for Facility Records

```sql
-- See what data_type values exist for Facility records
SELECT DISTINCT data_type
FROM new_updated_historical_medical_benchmarking_data
WHERE data_type LIKE '%Facility%'
ORDER BY data_type;
```

### 7.3 Check for Code Format Issues

```sql
-- Find any codes with unusual formats
SELECT DISTINCT code
FROM new_updated_historical_medical_benchmarking_data
WHERE code LIKE '%.%'
ORDER BY code
LIMIT 100;
```

---

## Usage Notes

1. **Table Selection**: Most queries include both `new_updated_historical_medical_benchmarking_data` and `new_updated_medical_benchmarking_data`. Run the appropriate query based on your Supabase schema. If `new_updated_medical_benchmarking_data` is a view that pulls from the historical table, you may only need to query one.

2. **Source Names**: The actual source name used by FairHealth Facility scraper is `"Fair Health Facility"` (with spaces). Adjust LIKE clauses as needed.

3. **Execution Order**: 
   - First run investigation queries (sections 1-2)
   - Then run normalization preview (section 3)
   - Review results before executing normalization (section 4)
   - Verify normalization worked (section 5)
   - After scraper runs, verify January 2026 data (section 6)

4. **Safety**: Always review preview queries before executing UPDATE statements. Consider running UPDATE statements in a transaction so you can rollback if needed.
