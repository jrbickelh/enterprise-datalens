import pandas as pd
import numpy as np
from deltalake import DeltaTable, write_deltalake
import duckdb
import great_expectations as gx
from gx_config import get_documented_suite

# --- CONFIGURATION ---
LAKEHOUSE_PATH = "./lakehouse/transactions"
DUCKDB_PATH = "lakehouse_serving.duckdb"

def generate_banking_data(n=1000):
    """Generates synthetic banking data for the lakehouse."""
    print(f"--> Generating {n} synthetic banking records...")
    data = {
        "transaction_id": [f"TXN-{i:06d}" for i in range(n)],
        "customer_id": [f"CUST-{np.random.randint(1000, 9999)}" for _ in range(n)],
        "amount": np.round(np.random.uniform(5.0, 5000.0, n), 2),
        "status": np.random.choice(["Active", "Inactive"], n, p=[0.9, 0.1]),
        "timestamp": pd.date_range(start="2025-01-01", periods=n, freq="h")
    }
    return pd.DataFrame(data)

def build_delta_tables(df):
    """Writes the dataframe to a local Delta Lake."""
    print(f"--> Writing data to Delta Lake at: {LAKEHOUSE_PATH}")
    write_deltalake(LAKEHOUSE_PATH, df, mode="overwrite")

def validate_lakehouse_data():
    print("--> Commencing Professional Data Quality Validation...")
    
    # CHANGE: Initialize a permanent local context instead of an ephemeral one
    # This creates the /gx folder structure in your current directory
    context = gx.get_context(project_root_dir="./")
    
    # 1. Get the actual Suite OBJECT
    suite = get_documented_suite(context)
    
    # 2. Load the data
    dt = DeltaTable(LAKEHOUSE_PATH)
    pandas_df = dt.to_pandas()
    
    # 3. Fluent API Batch Setup
    datasource = context.data_sources.add_or_update_pandas(name="delta_delivery")
    data_asset = datasource.add_dataframe_asset(name="transactions")
    
    # Define the batch
    batch_definition = data_asset.add_batch_definition_whole_dataframe("all_data")
    batch = batch_definition.get_batch(batch_parameters={"dataframe": pandas_df})
    
    # 4. Validate using the object
    print(f"--> Validating batch against suite: {suite.name}...")
    results = batch.validate(suite)
    
    # 5. Build the Docs (This now saves them to your disk)
    print("--> Generating permanent Data Docs...")
    context.build_data_docs()
    
    # NEW: Automatically open the docs at the end of the run
    context.open_data_docs()
    
    return results.success

def initialize_duckdb_catalog():
    """Mounts the Delta Lake tables as views in DuckDB for sub-second querying."""
    print(f"--> Promoting to Serving Layer: {DUCKDB_PATH}")
    con = duckdb.connect(DUCKDB_PATH)
    
    # Create a view that points directly to the Delta Parquet files
    con.execute(f"""
        CREATE OR REPLACE VIEW transactions AS 
        SELECT * FROM delta_scan('{LAKEHOUSE_PATH}')
    """)
    
    print("✅ Serving Layer Online. View 'transactions' registered.")
    con.close()

if __name__ == "__main__":
    print("============================================================")
    print("   ENTERPRISE DATALENS v3.5 | LAKEHOUSE BUILDER")
    print("============================================================")
    
    # 1. Ingestion
    raw_data = generate_banking_data(5000)
    build_delta_tables(raw_data)
    
    # 2. Quality Gate (The Lead Scientist Step)
    quality_passed = validate_lakehouse_data()
    
    if quality_passed:
        # 3. Serving Layer Promotion
        print("✅ DATA QUALITY PASSED. Updating analytical catalog...")
        initialize_duckdb_catalog()
        print("\nBuild Complete. Ready for Neural Queries.")
    else:
        print("\n❌ DATA QUALITY FAILURE: Check Data Docs in bx/uncommitted/data_docs/")
        print("Serving Layer update aborted to prevent corruption.")
        exit(1)
