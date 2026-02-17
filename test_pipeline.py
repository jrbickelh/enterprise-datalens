import subprocess
import sys

def run_step(cmd):
    print(f"Running: {cmd}")
    res = subprocess.run(cmd, shell=True)
    if res.returncode != 0:
        print(f"FAILED: {cmd}")
        sys.exit(1)

if __name__ == "__main__":
    run_step("python build_lakehouse.py")
    run_step("python semantic_indexer.py")
    run_step('python datalens_engine.py --sql "SELECT count(*) FROM transactions"')
    print("\n✨ ALL SYSTEMS GREEN.")
