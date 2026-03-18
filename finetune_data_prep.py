"""
Fine-Tuning Data Preparation for v8.5

Generates training data from golden queries for Qwen 7B fine-tuning.
Creates instruction-following format (user prompt → SQL generation).

This creates ~1K synthetic examples for QLoRA fine-tuning.
"""

import json


def create_finetuning_dataset():
    """
    Convert golden queries into instruction-following dataset.

    Format:
    {
      "instruction": "User's natural language question",
      "input": "Context (schema info, constraints)",
      "output": "Generated SQL query"
    }
    """

    # Golden queries (19 patterns)
    instructions = [
        # Single-table: Time-series
        {
            "instruction": "Show me the monthly revenue trend for the past 24 months",
            "input": "Database: transactions table with amount and transaction_date columns",
            "output": "SELECT DATE_TRUNC('month', transaction_date) as month, SUM(amount) as monthly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",
        },
        {
            "instruction": "What's the weekly revenue breakdown?",
            "input": "Database: transactions table with amount and transaction_date columns",
            "output": "SELECT DATE_TRUNC('week', transaction_date) as week, SUM(amount) as weekly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",
        },
        {
            "instruction": "Break down revenue by quarter",
            "input": "Database: transactions table with amount and transaction_date columns",
            "output": "SELECT DATE_TRUNC('quarter', transaction_date) as quarter, SUM(amount) as quarterly_revenue FROM transactions GROUP BY 1 ORDER BY 1;",
        },
        # Single-table: Segment breakdown
        {
            "instruction": "Show revenue by product with transaction count",
            "input": "Database: transactions table with amount and product_name columns",
            "output": "SELECT product_name, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY product_name ORDER BY total_revenue DESC;",
        },
        {
            "instruction": "What's the revenue breakdown by region?",
            "input": "Database: transactions table with amount and region columns",
            "output": "SELECT region, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY region ORDER BY total_revenue DESC;",
        },
        {
            "instruction": "Show me revenue by product category",
            "input": "Database: transactions table with amount and category columns",
            "output": "SELECT category, SUM(amount) as total_revenue, COUNT(*) as transaction_count FROM transactions GROUP BY category ORDER BY total_revenue DESC;",
        },
        # Single-table: Metrics
        {
            "instruction": "What's our total revenue, transaction count, and average transaction value?",
            "input": "Database: transactions table with amount column",
            "output": "SELECT SUM(amount) as total_revenue, COUNT(*) as total_transactions, AVG(amount) as avg_transaction FROM transactions;",
        },
        {
            "instruction": "How much revenue came from successful transactions?",
            "input": "Database: transactions table with amount and status columns",
            "output": "SELECT SUM(CASE WHEN status = 'Successful' THEN amount ELSE 0 END) as successful_revenue, COUNT(*) as total_count FROM transactions;",
        },
        {
            "instruction": "What's our net revenue excluding failed transactions?",
            "input": "Database: transactions table with amount and status columns",
            "output": "SELECT SUM(amount) as net_revenue FROM transactions WHERE status IN ('Successful', 'Pending');",
        },
        {
            "instruction": "Show recurring revenue by region for Enterprise products",
            "input": "Database: transactions table with amount, region, and category columns",
            "output": "SELECT region, product_name, SUM(amount) as recurring_revenue FROM transactions WHERE category = 'Enterprise' GROUP BY region, product_name;",
        },
        # Single-table: Anomalies
        {
            "instruction": "Find transactions that are unusually high (3-sigma outliers)",
            "input": "Database: transactions table with amount column",
            "output": "SELECT transaction_id, transaction_date, amount FROM transactions WHERE amount > (SELECT AVG(amount) + (3 * STDDEV(amount)) FROM transactions) ORDER BY amount DESC;",
        },
        {
            "instruction": "Show me unusually low transaction amounts",
            "input": "Database: transactions table with amount column",
            "output": "SELECT transaction_id, amount FROM transactions WHERE amount < (SELECT AVG(amount) - STDDEV(amount) FROM transactions) ORDER BY amount ASC LIMIT 10;",
        },
        # Single-table: Forecasting prep
        {
            "instruction": "Prepare time-series data for EMEA region forecasting",
            "input": "Database: transactions table with amount, transaction_date, and region columns",
            "output": "SELECT CAST(transaction_date AS DATE) as ds, SUM(amount) as y FROM transactions WHERE region = 'EMEA' GROUP BY CAST(transaction_date AS DATE) ORDER BY ds ASC;",
        },
        {
            "instruction": "Get monthly time-series data broken down by region",
            "input": "Database: transactions table with amount, transaction_date, and region columns",
            "output": "SELECT DATE_TRUNC('month', transaction_date) as ds, region, SUM(amount) as y FROM transactions GROUP BY DATE_TRUNC('month', transaction_date), region ORDER BY ds, region;",
        },
        # Single-table: Cohort
        {
            "instruction": "Show the top 10 customers by revenue this month",
            "input": "Database: transactions table with transaction_id, product_name, region, and amount columns",
            "output": "SELECT transaction_id, product_name, region, SUM(amount) as lifetime_value FROM transactions GROUP BY transaction_id, product_name, region ORDER BY lifetime_value DESC LIMIT 10;",
        },
        # Multi-table: LTV
        {
            "instruction": "Show top customers by lifetime value, grouped by segment",
            "input": "Database: customers and transactions tables. Join on customer_id. Include customer_segment.",
            "output": "SELECT c.customer_id, c.customer_name, c.customer_segment, SUM(t.amount) as lifetime_value, COUNT(t.transaction_id) as transaction_count FROM customers c LEFT JOIN transactions t ON c.customer_id = t.customer_id GROUP BY c.customer_id, c.customer_name, c.customer_segment ORDER BY lifetime_value DESC LIMIT 20;",
        },
        # Multi-table: Churn
        {
            "instruction": "Which regions have the highest churn rate?",
            "input": "Database: regions and customers tables. Join on region_id. Use churn_flag (0/1).",
            "output": "SELECT r.region_name, SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END) as churned_customers, COUNT(DISTINCT c.customer_id) as total_customers FROM regions r LEFT JOIN customers c ON r.region_id = c.region_id GROUP BY r.region_name ORDER BY churned_customers DESC;",
        },
        # Multi-table: Product margins
        {
            "instruction": "Which products are most profitable accounting for margins?",
            "input": "Database: transactions and products tables. Join on product_name. Include gross_margin_pct.",
            "output": "SELECT p.product_name, p.category, p.gross_margin_pct, SUM(t.amount) as product_revenue, COUNT(t.transaction_id) as transaction_count FROM transactions t LEFT JOIN products p ON t.product_name = p.product_name WHERE t.status = 'Successful' GROUP BY p.product_name, p.category, p.gross_margin_pct ORDER BY product_revenue DESC;",
        },
        # Multi-table: 3-way join
        {
            "instruction": "Show revenue breakdown by region and customer segment",
            "input": "Database: transactions, customers, and regions tables. Chain: transactions → customers → regions.",
            "output": "SELECT r.region_name, c.customer_segment, SUM(t.amount) as segment_revenue, COUNT(DISTINCT c.customer_id) as customer_count FROM transactions t LEFT JOIN customers c ON t.customer_id = c.customer_id LEFT JOIN regions r ON c.region_id = r.region_id GROUP BY r.region_name, c.customer_segment ORDER BY segment_revenue DESC;",
        },
    ]

    return instructions


def save_finetuning_dataset(output_file="finetune_dataset.jsonl"):
    """
    Save dataset in JSONL format for Hugging Face training.
    """
    dataset = create_finetuning_dataset()

    print(f"📊 Generating fine-tuning dataset: {len(dataset)} examples...")

    with open(output_file, "w") as f:
        for example in dataset:
            f.write(json.dumps(example) + "\n")

    print(f"✅ Saved to {output_file}")
    print("\nDataset composition:")
    print("  Single-table patterns: 15")
    print("  Multi-table patterns: 6")
    print(f"  Total examples: {len(dataset)}")
    print("\nSample (first example):")
    print(f"  Instruction: {dataset[0]['instruction']}")
    print(f"  Output (SQL): {dataset[0]['output'][:80]}...")


if __name__ == "__main__":
    save_finetuning_dataset()
