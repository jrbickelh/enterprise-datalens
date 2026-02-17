import subprocess
import os
from deltalake import DeltaTable

def run_test(name, command):
    print(f"--> [TEST] {name}...")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ {name} Passed.")
        return True
    else:
        print(f"❌ {name} Failed.")
        print(result.stderr)
        return False

if __name__ == "__main__":
    print("=== DATALENS LOCAL CI RUNNER ===")

    # 1. Environment Check
    if not run_test("Dependency Sync", "uv sync"):
        exit(1)

    # 2. Build & Quality Gate Check
    if not run_test("Lakehouse Build", "python build_lakehouse.py"):
        exit(1)

    # 3. Data Integrity Check
    try:
        dt = DeltaTable("./lakehouse/transactions")
        version_count = len(dt.history())
        if version_count > 0:
            print(f"✅ History Integrity: {version_count} versions found.")
        else:
            print("❌ History Integrity: No versions found.")
            exit(1)
    except Exception as e:
        print(f"❌ DeltaTable Error: {e}")
        exit(1)

    print("\n🚀 ALL SYSTEMS GO. Your code and data are certified for push.")
