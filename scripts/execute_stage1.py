#!/usr/bin/env python3
"""
execute_stage1.py — Execute Stage I on IU Hadoop Cluster via Paramiko SSH
"""

import paramiko
import sys
import time
import os

# --- Configuration ---
HOST = "10.100.30.57"
PORT = 22
USER = "team2"
PASSWORD = "V2P1hy6zjPqWoXMm"

PROJECT_DIR = "/home/team2/project/chicago-crimes-pipeline"
LOCAL_DIR = r"C:\Users\язан\Desktop\folder\Big_Data_Project"


def ssh_connect():
    """Establish SSH connection."""
    print("Connecting to cluster...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        hostname=HOST, port=PORT, username=USER, password=PASSWORD,
        timeout=60, auth_timeout=60
    )
    print(f"✓ Connected to {HOST}\n")
    return client


def run_command(client, cmd, description, timeout=300, ignore_errors=False):
    """Run a single command and stream output."""
    print(f">>> {description}")
    print(f"    Command: {cmd[:100]}{'...' if len(cmd) > 100 else ''}")

    transport = client.get_transport()
    channel = transport.open_session()
    channel.settimeout(timeout)
    channel.exec_command(cmd)

    output_lines = []
    while True:
        if channel.exit_status_ready():
            break
        while channel.recv_ready():
            line = channel.recv(4096).decode('utf-8', errors='replace')
            if line:
                print(f"    {line}", end='', flush=True)
                output_lines.append(line)
        time.sleep(0.1)

    # Get remaining output
    remaining = channel.recv(9999).decode('utf-8', errors='replace')
    if remaining:
        print(f"    {remaining}", end='', flush=True)
        output_lines.append(remaining)

    exit_code = channel.recv_exit_status()
    print(f"\n    [Exit code: {exit_code}]")
    print()

    if exit_code != 0 and not ignore_errors:
        print(f"  ✗ FAILED: {description}")
        return False, ''.join(output_lines)

    print(f"  ✓ OK: {description}\n")
    return True, ''.join(output_lines)


def main():
    print("=" * 60)
    print("Stage I: Data Collection & Ingestion")
    print("Chicago Crimes Pipeline")
    print("=" * 60)
    print()

    # Connect
    client = ssh_connect()

    steps_passed = 0
    steps_failed = 0

    # --- Step 0: Environment setup ---
    print("=" * 60)
    print("STEP 0: Environment Setup")
    print("=" * 60)

    ok, _ = run_command(client, f"mkdir -p {PROJECT_DIR}/{{scripts,sql,data,secrets,notebooks,docs}}",
                        "Create project directories", timeout=30)
    steps_passed += 1 if ok else 0

    ok, _ = run_command(client,
                        f"echo '{PASSWORD}' > {PROJECT_DIR}/secrets/.psql.pass && chmod 600 {PROJECT_DIR}/secrets/.psql.pass",
                        "Save database password", timeout=30)
    steps_passed += 1 if ok else 0

    ok, _ = run_command(client, "pip install --quiet psycopg2-binary",
                        "Install psycopg2-binary", timeout=60)
    steps_passed += 1 if ok else 0

    # --- Step 1: Copy files to cluster ---
    print("=" * 60)
    print("STEP 1: Transfer Files to Cluster")
    print("=" * 60)

    sftp = client.open_sftp()

    # Upload all script files
    upload_files = [
        (f"{LOCAL_DIR}/scripts/data_collection.sh", f"{PROJECT_DIR}/scripts/data_collection.sh"),
        (f"{LOCAL_DIR}/scripts/build_db.py", f"{PROJECT_DIR}/scripts/build_db.py"),
        (f"{LOCAL_DIR}/scripts/sqoop_import.sh", f"{PROJECT_DIR}/scripts/sqoop_import.sh"),
        (f"{LOCAL_DIR}/scripts/verify.sh", f"{PROJECT_DIR}/scripts/verify.sh"),
        (f"{LOCAL_DIR}/sql/create_tables.sql", f"{PROJECT_DIR}/sql/create_tables.sql"),
        (f"{LOCAL_DIR}/sql/import_data.sql", f"{PROJECT_DIR}/sql/import_data.sql"),
        (f"{LOCAL_DIR}/sql/test_queries.sql", f"{PROJECT_DIR}/sql/test_queries.sql"),
        (f"{LOCAL_DIR}/sql/hive_create.sql", f"{PROJECT_DIR}/sql/hive_create.sql"),
    ]

    for local_path, remote_path in upload_files:
        if os.path.exists(local_path):
            try:
                sftp.put(local_path, remote_path)
                print(f"  ✓ Uploaded: {os.path.basename(local_path)}")
            except Exception as e:
                print(f"  ✗ Failed: {os.path.basename(local_path)} - {e}")
        else:
            print(f"  ⚠ Skipped (not found): {local_path}")

    sftp.close()
    print()

    # Make scripts executable
    run_command(client, f"chmod +x {PROJECT_DIR}/scripts/*.sh",
                "Make shell scripts executable", timeout=30)

    # --- Step 2: Download data ---
    print("=" * 60)
    print("STEP 2: Download Chicago Crimes Dataset")
    print("=" * 60)
    print("  ⏳ This may take 20-30 minutes...")
    print()

    ok, _ = run_command(client,
                        f"cd {PROJECT_DIR} && bash scripts/data_collection.sh",
                        "Run data_collection.sh", timeout=1800)
    if ok:
        steps_passed += 1
    else:
        steps_failed += 1

    # --- Step 3: Build PostgreSQL ---
    print("=" * 60)
    print("STEP 3: Build PostgreSQL + Import Data")
    print("=" * 60)
    print("  ⏳ This may take 5-10 minutes for 8.5M rows...")
    print()

    ok, _ = run_command(client,
                        f"cd {PROJECT_DIR} && python3 scripts/build_db.py",
                        "Run build_db.py", timeout=600)
    if ok:
        steps_passed += 1
    else:
        steps_failed += 1

    # --- Step 4: Sqoop Import (with benchmarking) ---
    print("=" * 60)
    print("STEP 4: Sqoop Import + Compression Benchmark")
    print("=" * 60)
    print("  ⏳ This may take 20-30 minutes (3 format combos)...")
    print()

    ok, _ = run_command(client,
                        f"bash {PROJECT_DIR}/scripts/sqoop_import.sh",
                        "Run sqoop_import.sh", timeout=1800)
    if ok:
        steps_passed += 1
    else:
        steps_failed += 1

    # --- Step 5: Hive Tables ---
    print("=" * 60)
    print("STEP 5: Create Hive Tables & Views")
    print("=" * 60)

    ok, _ = run_command(client,
                        f"beeline -u 'jdbc:hive2://hadoop-01.uni.innopolis.ru:10000/team2' -f {PROJECT_DIR}/sql/hive_create.sql",
                        "Run hive_create.sql", timeout=120)
    if ok:
        steps_passed += 1
    else:
        steps_failed += 1

    # --- Step 6: Verification ---
    print("=" * 60)
    print("STEP 6: Data Verification")
    print("=" * 60)

    ok, _ = run_command(client,
                        f"bash {PROJECT_DIR}/scripts/verify.sh",
                        "Run verify.sh", timeout=60)
    if ok:
        steps_passed += 1
    else:
        steps_failed += 1

    # --- Summary ---
    print("=" * 60)
    print("STAGE I EXECUTION SUMMARY")
    print("=" * 60)
    print(f"  Steps Passed: {steps_passed}")
    print(f"  Steps Failed: {steps_failed}")
    print()
    print("  Deliverables:")
    print(f"    PostgreSQL:  team2_projectdb.crimes")
    print(f"    HDFS:        /user/team2/project/warehouse/")
    print(f"    Hive:        team2.crimes (partitioned by year)")
    print(f"    Sample:      team2.crimes_sample")
    print(f"    Views:       team2.crimes_features, team2.arrest_balance")
    print(f"    Benchmark:   docs/benchmark_results.csv")
    print()
    print("  Next: Stage II — ML Pipeline (notebooks/ml_pipeline.ipynb)")
    print("=" * 60)

    client.close()


if __name__ == "__main__":
    try:
        main()
    except paramiko.AuthenticationException:
        print("\n✗ AUTHENTICATION FAILED — Check username/password")
        print("  Username: team2")
        print("  Password: from readme.txt")
    except paramiko.SSHException as e:
        print(f"\n✗ SSH ERROR: {e}")
    except Exception as e:
        print(f"\n✗ ERROR: {e}")