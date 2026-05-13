"""pexpect SSH with file output"""
import pexpect
import sys

HOST = "10.100.30.57"
USER = "team2"
PASS = "V2P1hy6zjPqWoXMm"
OUT = "C:\\Users\\язан\\Desktop\\folder\\Big_Data_Project\\docs\\pexpect_out.txt"

with open(OUT, "w") as f:
    f.write(f"Connecting to {HOST} via pexpect SSH...\n")
    try:
        child = pexpect.spawn(
            f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=30 {USER}@{HOST} hostname && whoami && date",
            timeout=30,
            encoding='utf-8',
        )
        index = child.expect_exact(["password:", "Password:", pexpect.TIMEOUT, pexpect.EOF], timeout=20)
        f.write(f"Expect result: {index}\n")
        if index == 0 or index == 1:
            child.sendline(PASS)
            child.expect(pexpect.EOF, timeout=30)
            f.write("OUTPUT:\n")
            f.write(child.before)
            f.write("\n")
        else:
            f.write(f"Before: {child.before}\n")
            f.write(f"After: {child.after}\n")
    except Exception as e:
        f.write(f"Error: {type(e).__name__}: {e}\n")
        f.write(f"Before: {child.before}\n")

print(f"Results written to {OUT}")
sys.exit(0)