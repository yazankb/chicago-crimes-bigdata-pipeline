"""pexpect SSH connection to cluster"""
import pexpect
import sys

HOST = "10.100.30.57"
USER = "team2"
PASS = "V2P1hy6zjPqWoXMm"

print(f"Connecting to {HOST} via pexpect SSH...")
child = pexpect.spawn(
    f"ssh -o StrictHostKeyChecking=no -o ConnectTimeout=30 {USER}@{HOST} hostname && whoami && date",
    timeout=30,
    encoding='utf-8',
    logfile=sys.stdout
)

try:
    index = child.expect_exact(["password:", "Password:", pexpect.TIMEOUT, pexpect.EOF], timeout=20)
    if index == 0 or index == 1:
        child.sendline(PASS)
        result = child.expect([pexpect.EOF, pexpect.TIMEOUT], timeout=20)
        if result == 0:
            print("SUCCESS! Output above.")
        else:
            print("Connected but output may be incomplete")
    else:
        print(f"pexpect got: {index} (TIMEOUT={2}, EOF={3})")
        print("Before:", child.before[:500])
except Exception as e:
    print(f"pexpect error: {e}")
    print("Before:", child.before[:500])
    print("After:", child.after[:500])