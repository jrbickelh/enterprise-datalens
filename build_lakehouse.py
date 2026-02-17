import pandas as pd
import numpy as np
from deltalake.writer import write_deltalake
import duckdb
import os
import shutil

# --- Configuration ---
LAKEHOUSE_PATH = "./lakehouse"
TRANSACTIONS_PATH = f"{LAKEHOUSE_PATH}/transactions"
CUSTOMERS_PATH = f"{LAKEHOUSE_PATH}/customers"
DUCKDB_FILE = "lakehouse_serving.duckdb"

def clean_existing_lakehouse():
    print("-> Cleaning existing lakehouse artifacts...")
    if os.path.exists(LAKEHOUSE_PATH):
        shutil.rmtree(LAKEHOUSE_PATH)
    if os.path.exists(DUCKDB_FILE):
        os.remove(DUCKDB_FILE)

def generate_mock_data():
    print("-> Generating synthetic banking data...")
    # Customers
    customers = pd.DataFrame({
        "customer_id": range(1, 101),
        "name": [f"Customer_{i}" for i in range(1, 101)],
        "status": np.random.choice(["Active", "Inactive", "Pending"], 100, p=[0.7, 0.2, 0.1]),
        "signup_date": pd.to_datetime("2025-01-01") + pd.to_timedelta(np.random.randint(0, 365, 100), unit='D')
    })
    # Transactions
    transactions = pd.DataFrame({
        "transaction_id": range(1, 1001),
        "customer_id": np.random.randint(1, 101, 1000),
        "amount": np.round(np.random.uniform(10.0, 5000.0, 1000), 2),
        "timestamp": pd.to_datetime("2025-01-01") + pd.to_timedelta(np.random.randint(0, 365, 1000), unit='D')
    })
    return customers, transactions

def build_delta_tables(customers, transactions):
    print(f"-> Writing Delta tables to {LAKEHOUSE_PATH}...")
    os.makedirs(LAKEHOUSE_PATH, exist_ok=True)
    write_deltalake(CUSTOMERS_PATH, customers)
    write_deltalake(TRANSACTIONS_PATH, transactions)
    print("   [OK] Delta Lake Storage Layer Materialized.")

def initialize_duckdb_catalog():
    print(f"-> Initializing DuckDB serving layer: {DUCKDB_FILE}...")
    con = duckdb.connect(DUCKDB_FILE)
    con.execute("INSTALL delta; LOAD delta;") # Critical for reading Delta files
    con.execute(f"CREATE VIEW customers AS SELECT * FROM delta_scan('{CUSTOMERS_PATH}')")
    con.execute(f"CREATE VIEW transactions AS SELECT * FROM delta_scan('{TRANSACTIONS_PATH}')")
    print("   [OK] DuckDB Catalog Synced with Delta Lake.")
    con.close()

if __name__ == "__main__":
    print("=== ENTERPRISE DATALENS | LAKEHOUSE BUILDER v3.5 ===")
    clean_existing_lakehouse()
    c, t = generate_mock_data()
    build_delta_tables(c, t)
    initialize_duckdb_catalog()
    print("=== SUCCESS: Lakehouse Ready ===")
