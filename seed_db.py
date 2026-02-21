import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

DB_PATH = "datalens_lakehouse.db"
NUM_RECORDS = 5000

def generate_mock_data():
    print(f"🧬 Generating {NUM_RECORDS} realistic enterprise transactions...")
    np.random.seed(42) # For reproducible demos

    # 1. Date Range: Past 24 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1200)
    dates = [start_date + timedelta(days=np.random.randint(0, 1200)) for _ in range(NUM_RECORDS)]

    # 2. Products & Categories with weighted probabilities
    products = [
        ("DataLens Pro", "Enterprise", 1200.00),
        ("DataLens Lite", "SMB", 299.00),
        ("LakeHouse Connector", "Add-on", 450.00),
        ("Semantic Engine API", "Enterprise", 850.00),
        ("Support Tier 1", "Services", 150.00)
    ]

    # Weighted choices to create realistic chart variance
    chosen_products = np.random.choice(len(products), NUM_RECORDS, p=[0.4, 0.25, 0.15, 0.1, 0.1])

    # 3. Build the DataFrame
    df = pd.DataFrame({
        "transaction_id": [f"TRX-{10000 + i}" for i in range(NUM_RECORDS)],
        "transaction_date": dates,
        "product_name": [products[i][0] for i in chosen_products],
        "category": [products[i][1] for i in chosen_products],
        "region": np.random.choice(["North America", "EMEA", "APAC", "LATAM"], NUM_RECORDS, p=[0.5, 0.3, 0.15, 0.05]),
        "status": np.random.choice(["Successful", "Pending", "Failed"], NUM_RECORDS, p=[0.85, 0.10, 0.05])
    })

    # Add base price with some random discounting/variance
    base_prices = np.array([products[i][2] for i in chosen_products])
    variance = np.random.normal(1.0, 0.05, NUM_RECORDS) # +/- 5% variance
    df["amount"] = np.round(base_prices * variance, 2)

    # Sort chronologically for better time-series queries
    df = df.sort_values("transaction_date").reset_index(drop=True)
    return df

def seed_database():
    df = generate_mock_data()

    print(f"💿 Writing to DuckDB LakeHouse at {DB_PATH}...")
    with duckdb.connect(DB_PATH) as con:
        # Drop if exists for clean resets
        con.execute("DROP TABLE IF EXISTS transactions;")

        # Explicitly register the DataFrame to satisfy the linter and DuckDB
        con.register("mock_data_view", df)
        con.execute("CREATE TABLE transactions AS SELECT * FROM mock_data_view;")

        # Verify
        count = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        print(f"✅ Success! {count} rows inserted into 'transactions' table.")

if __name__ == "__main__":
    seed_database()
