#!/usr/bin/env python3
"""
Connect_and_run.py — Attempt to connect to cluster and run Stage 1
"""
import paramiko
import time
import sys
import os

sys.path.insert(0, "scripts")

HOST = "10.100.30.57"
USER = "team2"
PASS = "V2P1hy6zjPqWoXMm"
PROJECT = "/home/team2/project/chicago-crimes-pipeline"
LOCAL = r"C:\Users\язан\Desktop\folder\Big_Data_Project"

print("=" * 60)
print("Attempting cluster connection and Stage I execution")
print("=" * 60)

# Step 1: Connect
print(f"\n[1] Connecting to {HOST}...")
client = paramiko.SSHClient()
client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

try:
    client.connect(
        hostname=HOST, port=22, username=USER, password=PASS,
        timeout=30, auth_timeout=30, banner_timeout=30
    )
    print("   CONNECTED!")
except Exception as e:
    print(f"   FAILED: {e}")
    sys.exit(1)

# Step 2: Create dirs
stdin, out, err = client.exec_command(f"mkdir -p {PROJECT}/{{scripts,sql,data,secrets,notebooks,docs}}", timeout=10)
print(out.read().decode())
print("   [2] Directories created")

# Step 3: Upload
print("   [3] Uploading files via SFTP...")
sftp = client.open_sftp()
files = [
    ("scripts/data_collection.sh", f"{PROJECT}/scripts/"),
    ("scripts/build_db.py", f"{PROJECT}/scripts/"),
    ("scripts/sqoop_import.sh", f"{PROJECT}/scripts/"),
    ("scripts/verify.sh", f"{PROJECT}/scripts/"),
    ("scripts/remote_stage1.sh", f"{PROJECT}/scripts/"),
    ("sql/create_tables.sql", f"{PROJECT}/sql/"),
    ("sql/import_data.sql", f"{PROJECT}/sql/"),
    ("sql/test_queries.sql", f"{PROJECT}/sql/"),
    ("sql/hive_create.sql", f"{PROJECT}/sql/"),
]
for local, remote in files:
    src = os.path.join(LOCAL, local)
    dst = remote + os.path.basename(local)
    if os.path.exists(src):
        sftp.put(src, dst)
        print(f"      ✓ {local}")
    else:
        print(f"      ✗ Not found: {src}")

sftp.close()

# Make executable + set password
cmds = f"""
chmod +x {PROJECT}/scripts/*.sh
echo '{PASS}' > {PROJECT}/secrets/.psql.pass
chmod 600 {PROJECT}/secrets/.psql.pass
echo 'Setup complete'
"""
stdin, out, err = client.exec_command(cmds, timeout=10)
print(out.read().decode())
print("   [4] Permissions set, password saved")

# Step 5: Run Stage I
print("   [5] Running Stage I (this will take 60-120 min)...")
cmd = f"bash {PROJECT}/scripts/remote_stage1.sh"
stdin, out, err = client.exec_command(cmd, timeout=7200)
# Stream output
while not out.channel.exit_status_ready():
    line = out.readline()
    if line:
        print(line, end='')

print("\n   STAGE I COMPLETE!")
client.close()