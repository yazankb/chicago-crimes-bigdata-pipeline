import socket
import paramiko

results = []
results.append("=== SSH Diagnostic ===")

for port in [22, 2222, 8022]:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)
        r = s.connect_ex(("10.100.30.57", port))
        s.close()
        results.append(f"Port {port}: {'OPEN' if r == 0 else 'CLOSED'}")
    except Exception as e:
        results.append(f"Port {port}: ERROR {e}")

results.append("")
results.append("Paramiko:")
try:
    c = paramiko.SSHClient()
    c.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    c.connect("10.100.30.57", 22, "team2", "V2P1hy6zjPqWoXMm", timeout=15)
    results.append("CONNECTED!")
    stdin, out, err = c.exec_command("hostname")
    results.append("Hostname: " + out.read().decode())
    c.close()
except Exception as e:
    results.append(f"FAILED: {type(e).__name__}: {e}")

with open("C:\\Users\\азан\\Desktop\\folder\\Big_Data_Project\\docs\\ssh_test_results.txt", "w") as f:
    f.write("\n".join(results))
print("\n".join(results))