# Database Tables Mapping (Work Stream B1)

This document maps the plan’s SQL references to the actual Supabase tables used by the backend scrapers and clarifies how historical vs current tables relate.

---

## 1. Review: Where Scrapers Write

### SupabaseHandlerFairHealth (Fair Health Facility)

- **File**: `backend/app/cpt_automated_scripts/Fair_Health_Facility/database.py`
- **Table written to**: `new_updated_historical_medical_benchmarking_data`
- **Source name**: `"Fair Health Facility"`
- **Composite key**: `(source, code, release_date, geozip)`
- **Method**: `insert_records()` → `_validate_and_prepare_records()` → `upsert_records_with_composite_key()` from `database_utils.py`

The handler does **not** write to `new_updated_medical_benchmarking_data` or `historical_medical_benchmarking_data`; it writes only to `new_updated_historical_medical_benchmarking_data`.

### Other scrapers (summary)

| Scraper / Handler              | Table written to                                      |
|--------------------------------|--------------------------------------------------------|
| Fair Health Facility           | `new_updated_historical_medical_benchmarking_data`    |
| Fair Health Physicians         | `new_updated_historical_medical_benchmarking_data`   |
| Medicare ASC Addenda           | `new_updated_historical_medical_benchmarking_data`    |
| New Jersey DOBI                | `new_updated_historical_medical_benchmarking_data`    |
| Novitas                       | `new_updated_historical_medical_benchmarking_data`    |
| Medicare Clinical Fees         | `new_updated_medical_benchmarking_data`                |
| Horizon ASC                   | `new_updated_medical_benchmarking_data`               |

- **Reads** (e.g. `specialty_prioritizer.py`): `new_updated_medical_benchmarking_data` is used for `code` selection.

So: **scrapers write** to either `new_updated_historical_medical_benchmarking_data` or `new_updated_medical_benchmarking_data`; they do **not** write to `historical_medical_benchmarking_data` in the codebase.

---

## 2. Relationship Between Tables (from backend code)

From the backend alone we know:

- **`new_updated_historical_medical_benchmarking_data`**  
  - Physical table (or the one the app treats as the historical store).  
  - Fair Health Facility (and most other scrapers) write here.

- **`new_updated_medical_benchmarking_data`**  
  - Used for reads (e.g. specialty_prioritizer) and writes by Medicare Clinical Fees and Horizon ASC.  
  - **Not** written to by Fair Health Facility.  
  - So it is either:
    - A separate table that some scrapers write to and others don’t, or  
    - A **view** (or similar) built on top of the historical table (or vice versa).  
  The code does **not** define this; it only shows who writes where.

- **`historical_medical_benchmarking_data`**  
  - Referenced in the plan’s SQL (e.g. normalization).  
  - **Not** referenced by any backend scraper code.  
  - Could be a legacy table, a view, or a synonym; cannot be determined from the repo.

---

## 3. Align naming: views/triggers (must be verified in Supabase)

**Whether Supabase uses views or triggers to populate `new_updated_medical_benchmarking_data` from the historical table cannot be determined from this codebase.** That must be checked in the Supabase project.

### How to verify in Supabase

1. **Supabase Dashboard**  
   - **Table Editor**: Check if `new_updated_medical_benchmarking_data` and `historical_medical_benchmarking_data` are base tables or views.  
   - **Database → Views**: See if either name appears as a view.

2. **SQL to list views and table types** (run in Supabase SQL Editor):

```sql
-- List views in the public schema
SELECT table_name, table_type
FROM information_schema.tables
WHERE table_schema = 'public'
  AND table_name IN (
    'new_updated_medical_benchmarking_data',
    'new_updated_historical_medical_benchmarking_data',
    'historical_medical_benchmarking_data'
  )
ORDER BY table_name;
```

3. **If `new_updated_medical_benchmarking_data` is a view**, inspect its definition:

```sql
SELECT pg_get_viewdef('public.new_updated_medical_benchmarking_data'::regclass, true);
```

4. **List triggers** on the historical table (if you want to see if something propagates data):

```sql
SELECT trigger_name, event_manipulation, action_statement
FROM information_schema.triggers
WHERE event_object_table = 'new_updated_historical_medical_benchmarking_data';
```

### What to document after running these

- For each of the three names: **base table** or **view**.
- If `new_updated_medical_benchmarking_data` is a view: **definition** (e.g. “select from historical” or “from both historical and another table”).
- Any **triggers** that copy or transform data between these objects.

Then you can “align naming” in the plan and in `SQL_QUERIES.md`: e.g. “queries for current data use `new_updated_medical_benchmarking_data` (view over historical)” or “both are tables; historical is the source of truth for Fair Health.”

---

## 4. Plan SQL targeting (recommendation)

- **Fair Health Facility data (code 36475, releases, etc.)**  
  - Primary table to query/update: **`new_updated_historical_medical_benchmarking_data`** (where the scraper writes).  
  - If the dashboard or other reads use `new_updated_medical_benchmarking_data`, run the same queries there **after** confirming in Supabase whether it’s a view over historical or a separate table.

- **Code normalization (`.0` cleanup)**  
  - Run preview and `UPDATE` on **both**:
    - `new_updated_historical_medical_benchmarking_data`
    - `new_updated_medical_benchmarking_data`  
  - Only run on **`historical_medical_benchmarking_data`** if Supabase confirms it exists and is a separate table (not a view that would be updated automatically).

---

## 5. B1 checklist

| B1 item | Status |
|--------|--------|
| Review how SupabaseHandlerFairHealth writes and to which table | Done (writes to `new_updated_historical_medical_benchmarking_data` only). |
| Document how that relates to `new_updated_medical_benchmarking_data` and `historical_medical_benchmarking_data` | Done above (relationship inferred from code; views/triggers unknown). |
| Align naming: document whether Supabase uses views/triggers to populate `new_updated_medical_benchmarking_data` | **Must be done in Supabase** using the SQL and steps in section 3; then update this doc with the result. |

So: **B1 is done from the backend/code side.** Finishing “align naming” requires running the verification steps in Supabase and recording whether `new_updated_medical_benchmarking_data` (and optionally `historical_medical_benchmarking_data`) are views or tables and how they are populated.
