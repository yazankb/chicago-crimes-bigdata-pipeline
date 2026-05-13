#!/usr/bin/env python3
"""SSH test with multiple approaches"""
import paramiko, socket, sys

HOST = "10.100.30.57"
USER = "team2"
PASS = "V2P1hy6zjPqWoXMm"

# Try multiple ports
for port in [22, 2222, 8022, 222]:
    print(f"\nTrying port {port}...")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((HOST, port))
        sock.close()
        if result == 0:
            print(f"  Port {port} is OPEN")
        else:
            print(f"  Port {port} is CLOSED (err={result})")
    except Exception as e:
        print(f"  Port {port}: {e}")

# Try Paramiko with verbose output
print("\n--- Paramiko SSH attempt ---")
try:
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(HOST, 22, USER, PASS, timeout=10)
    print("SSH CONNECTED!")
    stdin, out, err = client.exec_command("echo 'SUCCESS'")
    print("Output:", out.read().decode())
    client.close()
except Exception as e:
    print(f"SSH Failed: {type(e).__name__}: {e}")