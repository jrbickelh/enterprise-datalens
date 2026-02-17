import lancedb
import pandas as pd
from sentence_transformers import SentenceTransformer
import os

# Configuration
LANCE_DB_PATH = "./lancedb"
TABLE_NAME = "column_metadata"
MODEL_NAME = "all-MiniLM-L6-v2"

def build_index():
    # Ensure the directory exists
    if not os.path.exists(LANCE_DB_PATH):
        os.makedirs(LANCE_DB_PATH)

    print(f"--> Initializing Vector Store at {LANCE_DB_PATH}...")
    db = lancedb.connect(LANCE_DB_PATH)

    # Force CPU for stability on your GTX 1060
    model = SentenceTransformer(MODEL_NAME, device="cpu")

    # 1. Define your Data Dictionary
    data_dict = [
        {"column": "transaction_id", "description": "Unique identifier for each banking record, receipt number"},
        {"column": "customer_id", "description": "Unique ID for the bank account holder or client"},
        {"column": "amount", "description": "The monetary value of the transaction, cost, price, or payment"},
        {"column": "status", "description": "Current state of the transaction: Active, Pending, or Closed"},
        {"column": "timestamp", "description": "The date and time the transaction occurred"}
    ]

    df = pd.DataFrame(data_dict)

    # 2. Generate Embeddings
    print(f"--> Generating embeddings on CPU...")
    df['vector'] = model.encode(df['description']).tolist()

    # 3. Create/Overwrite the Table
    # Using mode="overwrite" ensures it clears any ghost files
    tbl = db.create_table(TABLE_NAME, data=df, mode="overwrite")

    print(f"✅ Success! Table '{TABLE_NAME}' created.")
    print(f"--> Record count: {len(tbl)}")
    print(f"--> Files created in {LANCE_DB_PATH}: {os.listdir(LANCE_DB_PATH)}")

if __name__ == "__main__":
    build_index()
