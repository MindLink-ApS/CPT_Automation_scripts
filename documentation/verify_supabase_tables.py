#!/usr/bin/env python3
"""
Run B1 verification queries against Supabase and print results.

Requires one of:
  - DATABASE_URL in .env (full Postgres URL from Supabase Dashboard → Connect)
  - Or SUPABASE_DB_PASSWORD in .env (we derive host from SUPABASE_URL)

Load .env from backend/ when run from repo root: python backend/scripts/verify_supabase_tables.py
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
        # Parse URL so password with special chars (@, :, etc.) works
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
        print("Get the DB password from Supabase Dashboard → Project Settings → Database")
        sys.exit(1)
    # https://uyozdfwohdpcnyliebni.supabase.co -> db.uyozdfwohdpcnyliebni.supabase.co
    from urllib.parse import urlparse
    parsed = urlparse(supabase_url)
    host = parsed.hostname or ""
    if host.startswith("db."):
        db_host = host
    else:
        # ref is the subdomain
        ref = host.split(".")[0] if "." in host else host
        db_host = f"db.{ref}.supabase.co"
    conn_str = f"postgresql://postgres:{password}@{db_host}:5432/postgres"
    return psycopg2.connect(conn_str)

def run_query(cursor, title, sql):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)
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

    # 1. Table types
    run_query(
        cur,
        "1. Table/View types (public schema)",
        """
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_name IN (
            'new_updated_medical_benchmarking_data',
            'new_updated_historical_medical_benchmarking_data',
            'historical_medical_benchmarking_data'
          )
        ORDER BY table_name;
        """,
    )

    # 2. View definition (only if it's a view)
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = 'new_updated_medical_benchmarking_data' AND table_type = 'VIEW'
    """)
    if cur.fetchone():
        run_query(
            cur,
            "2. View definition: new_updated_medical_benchmarking_data",
            "SELECT pg_get_viewdef('public.new_updated_medical_benchmarking_data'::regclass, true) AS definition;",
        )
    else:
        print("\n" + "=" * 60)
        print("2. new_updated_medical_benchmarking_data is not a VIEW (skipping definition)")
        print("=" * 60)

    # 3. Triggers on historical table
    run_query(
        cur,
        "3. Triggers on new_updated_historical_medical_benchmarking_data",
        """
        SELECT trigger_name, event_manipulation, action_statement
        FROM information_schema.triggers
        WHERE event_object_schema = 'public'
          AND event_object_table = 'new_updated_historical_medical_benchmarking_data'
        ORDER BY trigger_name;
        """,
    )

    cur.close()
    conn.close()
    print("\nDone.")

if __name__ == "__main__":
    main()
