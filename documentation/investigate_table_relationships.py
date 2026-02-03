#!/usr/bin/env python3
"""
Investigate the relationship between the three medical benchmarking tables.
"""

import os
import sys
from pathlib import Path

# Load .env from backend/
backend_dir = Path(__file__).resolve().parent.parent
env_file = backend_dir / ".env"
if env_file.exists():
    from dotenv import load_dotenv
    load_dotenv(env_file)

def get_connection():
    import psycopg2
    from urllib.parse import urlparse, unquote
    url = os.getenv("DATABASE_URL")
    if url:
        parsed = urlparse(url)
        conn_params = {
            "host": parsed.hostname or "localhost",
            "port": int(parsed.port or 5432),
            "dbname": (parsed.path or "/postgres").lstrip("/") or "postgres",
            "user": parsed.username or "postgres",
            "password": unquote(parsed.password or ""),
        }
        return psycopg2.connect(**conn_params)
    # Build from SUPABASE_URL + SUPABASE_DB_PASSWORD
    supabase_url = os.getenv("SUPABASE_URL", "")
    password = os.getenv("SUPABASE_DB_PASSWORD")
    if not supabase_url or not password:
        print("Set DATABASE_URL or (SUPABASE_URL + SUPABASE_DB_PASSWORD) in backend/.env")
        sys.exit(1)
    from urllib.parse import urlparse
    parsed = urlparse(supabase_url)
    host = parsed.hostname or ""
    if host.startswith("db."):
        db_host = host
    else:
        ref = host.split(".")[0] if "." in host else host
        db_host = f"db.{ref}.supabase.co"
    conn_str = f"postgresql://postgres:{password}@{db_host}:5432/postgres"
    return psycopg2.connect(conn_str)

def run_query(cursor, title, sql):
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)
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
    try:
        import psycopg2
    except ImportError:
        print("Install psycopg2-binary: pip install psycopg2-binary")
        sys.exit(1)

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
    run_query(cur, "2. Schema Comparison (Column Names and Types)", """
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
        SELECT 'historical' as tbl, source, COUNT(*) as count
        FROM historical_medical_benchmarking_data
        GROUP BY source
        UNION ALL
        SELECT 'new_updated_historical', source, COUNT(*)
        FROM new_updated_historical_medical_benchmarking_data
        GROUP BY source
        UNION ALL
        SELECT 'new_updated', source, COUNT(*)
        FROM new_updated_medical_benchmarking_data
        GROUP BY source
        ORDER BY tbl, source;
    """)

    # 6. Check release dates in each table
    run_query(cur, "6. Release Dates in Each Table", """
        SELECT 'historical' as tbl, release_date, COUNT(*) as count
        FROM historical_medical_benchmarking_data
        GROUP BY release_date
        ORDER BY release_date DESC
        LIMIT 10
        UNION ALL
        SELECT 'new_updated_historical', release_date, COUNT(*)
        FROM new_updated_historical_medical_benchmarking_data
        GROUP BY release_date
        ORDER BY release_date DESC
        LIMIT 10
        UNION ALL
        SELECT 'new_updated', release_date, COUNT(*)
        FROM new_updated_medical_benchmarking_data
        GROUP BY release_date
        ORDER BY release_date DESC
        LIMIT 10;
    """)

    # 7. Check for .0 suffix codes in each table
    run_query(cur, "7. Codes with .0 Suffix in Each Table", """
        SELECT 'historical' as tbl, COUNT(*) as codes_with_dot_zero
        FROM historical_medical_benchmarking_data
        WHERE code ~ '\\.0+$'
        UNION ALL
        SELECT 'new_updated_historical', COUNT(*)
        FROM new_updated_historical_medical_benchmarking_data
        WHERE code ~ '\\.0+$'
        UNION ALL
        SELECT 'new_updated', COUNT(*)
        FROM new_updated_medical_benchmarking_data
        WHERE code ~ '\\.0+$';
    """)

    cur.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
