# 🚀 DataLens Quick Start (5 Minutes)

Get DataLens running and execute your first query in under 5 minutes.

---

## ⚡ Option 1: Docker (Recommended - 2 Minutes)

### Prerequisites
- Docker & Docker Compose installed
- Azure OpenAI API credentials (get free $200 credits [here](https://azure.microsoft.com/free/))

### Steps

**1. Clone & Setup**
```bash
git clone https://github.com/jrbickelh/enterprise-datalens.git
cd enterprise-datalens
cp .env.example .env
# Edit .env with your Azure OpenAI credentials
```

**2. Launch with One Command**
```bash
docker compose up --build
```

This automatically:
- Builds the Docker image
- Seeds DuckDB with 5K synthetic transactions
- Embeds golden SQL patterns into ChromaDB
- Starts the Streamlit UI

**3. Open Browser**
- Visit http://localhost:8501
- You're ready to query!

**Example Query:**
```
Show me the top 10 customers by lifetime value
```

---

## 🔧 Option 2: Local Setup (3-4 Minutes)

### Prerequisites
- Python 3.12+
- `uv` package manager: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Azure OpenAI API key

### Steps

**1. Clone & Install**
```bash
git clone https://github.com/jrbickelh/enterprise-datalens.git
cd enterprise-datalens
uv sync
```

**2. Configure Credentials**
```bash
cp .env.example .env
# Edit .env with your Azure OpenAI API key and endpoint
```

**3. Seed Data**
```bash
uv run python seed_db.py        # Generates 5K transactions + dimensions (3-5s)
uv run python seed_chroma.py    # Embeds golden SQL patterns (10-15s)
```

**4. Launch UI**
```bash
uv run streamlit run app.py
```

Browser opens automatically at http://localhost:8501

---

## 📝 Your First Query

### Single-Table Query (Revenue by Product)
```
Show me revenue by product
```

**Expected Output:**
```
product_name | total_revenue
-------------|---------------
Premium      | 1,234,567
Standard     | 891,234
Basic        | 456,789
```

**Cost:** ~$0.02 | **Latency:** 1.2s

### Multi-Table Query (Customer Lifetime Value)
```
Show top 10 customers by lifetime value with their segment
```

**Expected Output:**
```
customer_id | customer_name | segment     | lifetime_value
------------|---------------|-------------|---------------
CUST-10001  | John Smith    | Enterprise  | 45,678
CUST-10002  | Jane Doe      | Mid-Market  | 42,100
...
```

**Cost:** ~$0.08 | **Latency:** 2.3s

---

## 🎯 Common Queries

### Revenue Analytics
```
What's the monthly revenue trend for the last 6 months?
Which regions are generating the most revenue?
Show revenue breakdown by product category
```

### Customer Analytics
```
Which customers have the highest lifetime value?
Show me customer retention by region
What's the churn rate by segment?
```

### Anomaly Detection
```
Detect unusually high transactions in the past 30 days
Show me transactions that are outliers
```

### Forecasting
```
Forecast revenue for the next 3 months
Predict Q2 revenue by region
```

---

## 📊 Understanding the Output

Each query response includes:

| Component | What It Means |
|-----------|--------------|
| **SQL Query** | The generated SQL that DataLens executed |
| **Results Table** | Your query results |
| **Cost** | Approximate cost in USD (based on token usage) |
| **Latency** | How long the query took |
| **Safety Validation** | ✅ Query passed safety checks |

**Example Response:**
```
🔍 Generated SQL:
SELECT region, SUM(amount) as revenue, COUNT(*) as transactions
FROM transactions
GROUP BY region
ORDER BY revenue DESC

📊 Results: 4 rows returned
├─ EMEA: $2,345,678 (1,234 transactions)
├─ Americas: $1,987,654 (876 transactions)
├─ APAC: $1,654,321 (743 transactions)
└─ Other: $234,567 (89 transactions)

💰 Cost: $0.03 | ⏱️ Latency: 1.4s | ✅ Safety: Passed
```

---

## 🛠️ CLI Mode (No UI)

### Execute Query from Terminal
```bash
uv run python cli.py --query "Show revenue by product"
```

### JSON Output (for Scripts)
```bash
uv run python cli.py --query "Show revenue by product" --output json
```

### Verbose Mode (Debug)
```bash
uv run python cli.py --query "Show revenue by product" --verbose
```

### Human-in-the-Loop Approval
```bash
uv run python cli.py --query "Expensive operation" --hitl
# System waits for your approval before executing costly queries
```

---

## 🧪 Run Tests

Verify your setup with the test suite:
```bash
# Run all tests
uv run pytest tests/ test_app.py -v

# Run specific test module
uv run pytest tests/test_multitable_queries.py -v

# Run with coverage
uv run pytest --cov=. tests/ test_app.py
```

**Expected:** 120+ tests passing ✅

---

## 🐛 Troubleshooting

### "API Key not found" Error
```bash
# Check your .env file
cat .env

# Ensure these are set:
# AZURE_OPENAI_API_KEY=your_key
# AZURE_OPENAI_ENDPOINT=https://...
```

### "ChromaDB not seeded" Error
```bash
# Re-seed the vector database
uv run python seed_chroma.py
```

### "DuckDB connection failed" Error
```bash
# Re-seed the data layer
uv run python seed_db.py
```

### "Out of memory" with Docker
```bash
# Increase Docker memory limit in Docker Desktop Settings
# Or run locally with: uv sync && uv run streamlit run app.py
```

---

## 📚 Next Steps

### Explore Multi-Table Queries
See [MULTITABLE_QUERIES.md](MULTITABLE_QUERIES.md) for detailed guide on:
- When to use multi-table queries
- 6 core patterns with examples
- Common pitfalls and fixes
- Performance optimization

### Try Real-World Scenarios
See [DEMO_SCENARIOS.md](DEMO_SCENARIOS.md) for 5 complete end-to-end examples:
1. High-value customer segments
2. Regional health dashboard
3. Product profitability analysis
4. Customer segments by region
5. Top customers ranked

### Understand the Architecture
See [README.md](README.md) for deep dive on:
- Multi-agent orchestration (Supervisor-Engineer-Scientist)
- Hybrid search RAG (BM25 + vector)
- Semantic layer with YAML definitions
- Knowledge graph for schema analysis
- Self-healing SQL execution

### Setup Cost Tracking (Optional)
See [LANGFUSE_SETUP.md](LANGFUSE_SETUP.md) for:
- Per-token cost breakdown
- Query performance metrics
- LLM-as-a-Judge evaluation
- Cloud or self-hosted observability

---

## 💬 Need Help?

- **Issues**: [GitHub Issues](https://github.com/jrbickelh/enterprise-datalens/issues)
- **Discussions**: [GitHub Discussions](https://github.com/jrbickelh/enterprise-datalens/discussions)
- **Contributing**: [CONTRIBUTING.md](CONTRIBUTING.md)

---

## 🎉 You're All Set!

You now have a fully functional AI BI agent. Start by running:

```bash
# Option 1: Docker
docker compose up

# Option 2: Local
uv run streamlit run app.py

# Option 3: CLI
uv run python cli.py --query "your question"
```

Happy querying! 🚀
