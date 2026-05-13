#!/usr/bin/env python3
"""Quick SSH test to cluster"""
import paramiko, sys, socket

HOST = "10.100.30.57"
USER = "team2"
PASS = "V2P1hy6zjPqWoXMm"

print(f"Connecting to {HOST}...")
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # Try with explicit timeout values
    client.connect(
        hostname=HOST,
        port=22,
        username=USER,
        password=PASS,
        timeout=15,
        auth_timeout=15,
        banner_timeout=15,
        sock_timeout=15,
    )
    print("CONNECTED!")
    stdin, out, err = client.exec_command("hostname && whoami && date", timeout=10)
    print("OUTPUT:", out.read().decode())
    client.close()
except socket.timeout:
    print("SOCKET TIMEOUT - Firewall blocking")
except paramiko.AuthenticationException as e:
    print("AUTH FAILED:", e)
except paramiko.SSHException as e:
    print("SSH ERROR:", e)
except Exception as e:
    print("ERROR:", type(e).__name__, e)