import os
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

load_dotenv()

# 1. Initialize the Embedding Model
embeddings = AzureOpenAIEmbeddings(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_EMBEDDINGS"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION"),
)

# 2. Your Library of Verified SQL
golden_queries = [
    "Time-Series Aggregation: SELECT DATE_TRUNC('month', transaction_date) as month, SUM(amount) FROM transactions GROUP BY 1 ORDER BY 1;",
    "Finding Top Performers: SELECT product_name, SUM(amount) as total_revenue FROM transactions GROUP BY product_name ORDER BY total_revenue DESC LIMIT 5;",
    "Safe Casting for Anomalies: SELECT * FROM transactions WHERE amount > (SELECT AVG(amount) + (3 * STDDEV(amount)) FROM transactions);",
    "Preparing Data for Forecasting (Scientist): SELECT CAST(transaction_date AS DATE) as ds, amount as y FROM transactions WHERE region = 'EMEA' ORDER BY ds ASC;",
]

# 3. Build and Persist the Database locally
print("Embedding queries and building ChromaDB...")
vectorstore = Chroma.from_texts(
    texts=golden_queries,
    embedding=embeddings,
    persist_directory="./chroma_db",  # This creates a folder in your project
)

print("✅ Chroma DB Seeded Successfully! You can now search it.")
