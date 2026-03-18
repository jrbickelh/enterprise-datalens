import os
import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "datalens_lakehouse.db"))
NUM_RECORDS = 5000


def generate_mock_data():
    print(f"🧬 Generating {NUM_RECORDS} realistic enterprise transactions...")
    np.random.seed(42)  # For reproducible demos

    # 1. Date Range: Past 24 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1200)
    dates = [
        start_date + timedelta(days=np.random.randint(0, 1200))
        for _ in range(NUM_RECORDS)
    ]

    # 2. Products & Categories with weighted probabilities
    products = [
        ("DataLens Pro", "Enterprise", 1200.00),
        ("DataLens Lite", "SMB", 299.00),
        ("LakeHouse Connector", "Add-on", 450.00),
        ("Semantic Engine API", "Enterprise", 850.00),
        ("Support Tier 1", "Services", 150.00),
    ]

    # Weighted choices to create realistic chart variance
    chosen_products = np.random.choice(
        len(products), NUM_RECORDS, p=[0.4, 0.25, 0.15, 0.1, 0.1]
    )

    # 3. Build the DataFrame
    df = pd.DataFrame(
        {
            "transaction_id": [f"TRX-{10000 + i}" for i in range(NUM_RECORDS)],
            "transaction_date": dates,
            "product_name": [products[i][0] for i in chosen_products],
            "category": [products[i][1] for i in chosen_products],
            "region": np.random.choice(
                ["North America", "EMEA", "APAC", "LATAM"],
                NUM_RECORDS,
                p=[0.5, 0.3, 0.15, 0.05],
            ),
            "status": np.random.choice(
                ["Successful", "Pending", "Failed"], NUM_RECORDS, p=[0.85, 0.10, 0.05]
            ),
        }
    )

    # Add base price with some random discounting/variance
    base_prices = np.array([products[i][2] for i in chosen_products])
    variance = np.random.normal(1.0, 0.05, NUM_RECORDS)  # +/- 5% variance
    df["amount"] = np.round(base_prices * variance, 2)

    # Sort chronologically for better time-series queries
    df = df.sort_values("transaction_date").reset_index(drop=True)
    return df


def generate_customers_dimension():
    """Generate 10K customer records with demographic and churn data."""
    print(f"🧬 Generating 10K customer dimension records...")
    np.random.seed(42)

    customer_ids = [f"CUST-{10000 + i}" for i in range(10000)]
    customer_names = [f"Customer-{i}" for i in range(10000)]
    region_ids = np.random.choice(["EMEA", "APAC", "AMERICAS", "LATAM"], 10000, p=[0.3, 0.15, 0.5, 0.05])
    segments = np.random.choice(
        ["Enterprise", "Mid-Market", "SMB"], 10000, p=[0.2, 0.3, 0.5]
    )
    lifetime_values = np.random.lognormal(9, 1.5, 10000)  # Realistic LTV distribution
    churn_flags = np.random.binomial(1, 0.15, 10000)  # 15% churn rate
    join_dates = [
        datetime.now() - timedelta(days=np.random.randint(0, 1200)) for _ in range(10000)
    ]

    return pd.DataFrame(
        {
            "customer_id": customer_ids,
            "customer_name": customer_names,
            "region_id": region_ids,
            "customer_segment": segments,
            "lifetime_value": lifetime_values.round(2),
            "churn_flag": churn_flags,
            "join_date": join_dates,
        }
    )


def generate_products_dimension():
    """Generate 50 product records with pricing and margins."""
    print(f"🧬 Generating 50 product records...")
    np.random.seed(42)

    product_names = [
        "DataLens Pro",
        "DataLens Lite",
        "LakeHouse Connector",
        "Semantic Engine API",
        "Support Tier 1",
    ]
    # Create variants
    products = []
    categories = ["Enterprise", "SMB", "Add-on", "Services"]

    for i in range(50):
        base = product_names[i % len(product_names)]
        variant = f"{base}-{i // len(product_names)}" if i >= len(product_names) else base
        category = np.random.choice(categories)
        gross_margin = np.random.uniform(0.4, 0.8)
        unit_price = np.random.uniform(100, 5000)

        products.append(
            {
                "product_name": variant,
                "product_id": f"PROD-{1000 + i}",
                "category": category,
                "gross_margin_pct": round(gross_margin, 2),
                "unit_price": round(unit_price, 2),
            }
        )

    return pd.DataFrame(products)


def generate_regions_dimension():
    """Generate 4 region records with sales tiers."""
    print(f"🧬 Generating 4 region records...")

    return pd.DataFrame(
        {
            "region_id": ["EMEA", "APAC", "AMERICAS", "LATAM"],
            "region_name": [
                "Europe, Middle East, Africa",
                "Asia Pacific",
                "North & South America",
                "Latin America",
            ],
            "sales_tier": ["Tier-1", "Tier-2", "Tier-1", "Tier-3"],
            "manager_id": [f"MGR-{i}" for i in range(1, 5)],
        }
    )


def seed_database():
    print(f"💿 Writing to DuckDB LakeHouse at {DB_PATH}...")

    with duckdb.connect(DB_PATH) as con:
        # Drop all tables for clean resets
        con.execute("DROP TABLE IF EXISTS transactions;")
        con.execute("DROP TABLE IF EXISTS customers;")
        con.execute("DROP TABLE IF EXISTS products;")
        con.execute("DROP TABLE IF EXISTS regions;")

        # Generate and seed transactions
        df_transactions = generate_mock_data()
        con.register("transactions_view", df_transactions)
        con.execute("CREATE TABLE transactions AS SELECT * FROM transactions_view;")
        count_trx = con.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
        print(f"✅ {count_trx} transaction records inserted.")

        # Generate and seed customers
        df_customers = generate_customers_dimension()
        con.register("customers_view", df_customers)
        con.execute("CREATE TABLE customers AS SELECT * FROM customers_view;")
        count_cust = con.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
        print(f"✅ {count_cust} customer records inserted.")

        # Generate and seed products
        df_products = generate_products_dimension()
        con.register("products_view", df_products)
        con.execute("CREATE TABLE products AS SELECT * FROM products_view;")
        count_prod = con.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        print(f"✅ {count_prod} product records inserted.")

        # Generate and seed regions
        df_regions = generate_regions_dimension()
        con.register("regions_view", df_regions)
        con.execute("CREATE TABLE regions AS SELECT * FROM regions_view;")
        count_reg = con.execute("SELECT COUNT(*) FROM regions").fetchone()[0]
        print(f"✅ {count_reg} region records inserted.")

        print(
            f"\n✅ Multi-table LakeHouse seeded: transactions, customers, products, regions"
        )


if __name__ == "__main__":
    seed_database()
