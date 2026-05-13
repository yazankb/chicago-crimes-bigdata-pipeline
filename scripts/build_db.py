#!/usr/bin/env python3
"""
build_db.py — Build PostgreSQL database and import Chicago Crimes data.
Usage: python3 scripts/build_db.py
Ensure .psql.pass exists in secrets/ with the database password.
"""

import psycopg2 as psql
import os
import sys

# --- Configuration ---
DB_HOST = "hadoop-04.uni.innopolis.ru"
DB_PORT = 5432
DB_USER = "team2"
DB_NAME = "team2_projectdb"

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(SCRIPT_DIR)
SQL_DIR = os.path.join(PROJECT_DIR, "sql")
DATA_DIR = os.path.join(PROJECT_DIR, "data")
SECRETS_DIR = os.path.join(PROJECT_DIR, "secrets")


def get_password():
    """Read database password from secrets file."""
    pass_file = os.path.join(SECRETS_DIR, ".psql.pass")
    if not os.path.exists(pass_file):
        print(f"ERROR: Password file not found: {pass_file}")
        print("Create secrets/.psql.pass with your database password.")
        sys.exit(1)
    with open(pass_file, "r") as f:
        return f.read().strip()


def execute_sql_file(cur, filepath, description):
    """Execute all SQL statements from a file."""
    print(f"  Executing: {description}...")
    with open(filepath, "r") as f:
        content = f.read()
    cur.execute(content)
    print(f"  ✓ Done: {description}")


def main():
    print("=" * 60)
    print("Stage I — Build PostgreSQL Database")
    print("=" * 60)

    # Step 1: Connect to PostgreSQL
    password = get_password()
    conn_string = (
        f"host={DB_HOST} port={DB_PORT} "
        f"user={DB_USER} dbname={DB_NAME} password={password}"
    )
    print(f"\n[1/5] Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME}...")
    conn = psql.connect(conn_string)
    conn.autocommit = False
    cur = conn.cursor()
    print("  ✓ Connected successfully")

    # Step 2: Create tables
    print("\n[2/5] Creating tables...")
    execute_sql_file(cur, os.path.join(SQL_DIR, "create_tables.sql"), "DDL (create_tables.sql)")
    conn.commit()

    # Step 3: Import CSV data via COPY
    csv_file = os.path.join(DATA_DIR, "chicago_crimes_raw.csv")
    if not os.path.exists(csv_file):
        print(f"\n  ERROR: Data file not found: {csv_file}")
        print("  Run scripts/data_collection.sh first to download the data.")
        sys.exit(1)

    print(f"\n[3/5] Importing data from {csv_file}...")
    print(f"  File size: {os.path.getsize(csv_file) / (1024**3):.2f} GB")

    with open(os.path.join(SQL_DIR, "import_data.sql"), "r") as f:
        copy_cmd = f.read()

    with open(csv_file, "r") as csv_data:
        cur.copy_expert(copy_cmd, csv_data)
    print("  ✓ Data import complete")

    # Step 4: Run quality stats
    print("\n[4/5] Running data quality statistics...")
    execute_sql_file(cur, os.path.join(SQL_DIR, "test_queries.sql"), "Quality checks")
    conn.commit()

    # Fetch and print test results
    cur.execute("SELECT * FROM crimes LIMIT 1")
    cols = [desc[0] for desc in cur.description]
    print(f"\n  Sample row columns: {cols}")
    print(f"  Total rows: {cur.execute('SELECT COUNT(*) FROM crimes').fetchone()[0]}")

    # Step 5: Clean up
    cur.close()
    conn.close()
    print(f"\n[5/5] Done! Database '{DB_NAME}' is ready.")
    print("=" * 60)


if __name__ == "__main__":
    main()