import os
from dotenv import load_dotenv
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

load_dotenv()

# 1. Your Library of Verified SQL (15+ Golden Patterns)
golden_queries = [
    # Time-Series Aggregations (3 variants)
    "Monthly Revenue Trend: SELECT DATE_TRUNC('month', transaction_date) as month, SUM(amount) as monthly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",
    "Weekly Revenue Trend: SELECT DATE_TRUNC('week', transaction_date) as week, SUM(amount) as weekly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",
    "Quarterly Revenue Trend: SELECT DATE_TRUNC('quarter', transaction_date) as quarter, SUM(amount) as quarterly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",

    # Segment Analysis (3 variants)
    "Revenue by Product: SELECT product_name, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY product_name ORDER BY total_revenue DESC;",
    "Revenue by Region: SELECT region, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY region ORDER BY total_revenue DESC;",
    "Revenue by Category: SELECT category, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY category ORDER BY total_revenue DESC;",

    # Metrics Calculations (4 variants)
    "Total Revenue with Row Count: SELECT SUM(amount) as total_revenue, COUNT(*) as total_transactions, AVG(amount) as avg_transaction FROM transactions;",
    "Gross vs Successful Transactions: SELECT SUM(CASE WHEN status = 'Successful' THEN amount ELSE 0 END) as successful_revenue, COUNT(*) as total_count FROM transactions;",
    "Net Revenue (Exclude Failed): SELECT SUM(amount) as net_revenue FROM transactions WHERE status IN ('Successful', 'Pending');",
    "Recurring Revenue by Region: SELECT region, product_name, SUM(amount) as recurring_revenue FROM transactions WHERE category = 'Enterprise' GROUP BY region, product_name;",

    # Anomaly Detection Patterns (2 variants)
    "Transactions Above 3-Sigma Threshold: SELECT transaction_id, transaction_date, amount FROM transactions WHERE amount > (SELECT AVG(amount) + (3 * STDDEV(amount)) FROM transactions) ORDER BY amount DESC;",
    "Low-Value Outliers (Below 1-Sigma): SELECT transaction_id, amount FROM transactions WHERE amount < (SELECT AVG(amount) - STDDEV(amount) FROM transactions) ORDER BY amount ASC LIMIT 10;",

    # Forecasting Prep (2 variants)
    "Time-Series Data for Region Forecast: SELECT CAST(transaction_date AS DATE) as ds, SUM(amount) as y FROM transactions WHERE region = 'EMEA' GROUP BY CAST(transaction_date AS DATE) ORDER BY ds ASC;",
    "Monthly Time-Series for All Regions: SELECT DATE_TRUNC('month', transaction_date) as ds, region, SUM(amount) as y FROM transactions GROUP BY DATE_TRUNC('month', transaction_date), region ORDER BY ds, region;",

    # Performance Cohort (1 variant)
    "Top 10 Customers by Revenue (High-Value Cohort): SELECT transaction_id, product_name, region, SUM(amount) as lifetime_value FROM transactions GROUP BY transaction_id, product_name, region ORDER BY lifetime_value DESC LIMIT 10;",
]


def seed_chroma():
    """Initialize the Embedding Model and build ChromaDB."""
    embeddings = AzureOpenAIEmbeddings(
        azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_EMBEDDINGS"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
    )

    print("Embedding queries and building ChromaDB...")
    Chroma.from_texts(
        texts=golden_queries,
        embedding=embeddings,
        persist_directory="./chroma_db",
    )
    print("✅ Chroma DB Seeded Successfully! You can now search it.")


if __name__ == "__main__":
    seed_chroma()
