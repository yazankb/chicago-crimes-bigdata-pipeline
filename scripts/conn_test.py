#!/usr/bin/env python3
import os, sys, socket, paramiko

os.chdir(r"C:\Users\язан\Desktop\folder\Big_Data_Project")
f = open("docs/connection_test_" + str(int(__import__('time').time())) + ".txt", "w")

def log(msg):
    print(msg); f.write(msg + "\n"); f.flush()

log("=== Connection Test ===")

# Test 1: TCP socket
log("\n[Test 1] TCP socket to port 22...")
try:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(10)
    r = s.connect_ex(("10.100.30.57", 22))
    s.close()
    log("  Result: %s (0=open)" % r)
except Exception as e:
    log("  Error: " + str(e))

# Test 2: Direct SSH command via subprocess
log("\n[Test 2] subprocess ssh...")
import subprocess
try:
    p = subprocess.Popen(
        ["ssh", "-o", "StrictHostKeyChecking=no", "-o", "BatchMode=no",
         "-o", "ConnectTimeout=5", "team2@10.100.30.57", "echo OK"],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    try:
        out, err = p.communicate(input=b"V2P1hy6zjPqWoXMm\n", timeout=15)
        log("  stdout: " + out.decode())
        log("  stderr: " + err.decode())
    except subprocess.TimeoutExpired:
        p.kill()
        log("  TIMEOUT after 15s")
except Exception as e:
    log("  Error: " + str(e))

# Test 3: Paramiko with keyboard-interactive
log("\n[Test 3] Paramiko...")
try:
    import paramiko
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect("10.100.30.57", 22, "team2", "V2P1hy6zjPqWoXMm", timeout=10)
    stdin, out, err = client.exec_command("echo SUCCESS && hostname && whoami && pwd")
    result = out.read().decode()
    log("  Result: " + result)
    client.close()
except Exception as e:
    log("  Error: %s: %s" % (type(e).__name__, e))

log("\n=== Done ===")
f.close()
print("Results written to file.")