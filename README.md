# Enterprise DataLens v4.0 | Neural Temporal Lakehouse

![CI Status](https://github.com/jrbickelh/enterprise-datalens/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11%2B-blue)
![DuckDB](https://img.shields.io/badge/DuckDB-Powered-yellow)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-orange)

**Enterprise DataLens** is a **local-first Lakehouse engine** showcasing modern data engineering patterns by unifying **ACID storage, time travel, data quality enforcement, and semantic vector search** behind a single query interface.

Built to simulate a high-scale financial analytics platform, DataLens enables:

- Natural language queries translated into SQL  
- Historical “time travel” queries across dataset versions  
- Automated data validation at ingestion  
- Fast, in-process analytical querying  

---

## 🏗 Architecture

Enterprise DataLens follows a **Medallion Architecture** compressed into a high-performance local execution model.

```mermaid
graph TD
    A[Raw Data Stream] -->|Ingest & Validate| B(Great Expectations Gate)
    B -->|Pass| C{Delta Lake Storage}
    B -->|Fail| D[Quarantine / Alert]
    C -->|Parquet + _delta_log| E[DuckDB Query Engine]
    
    subgraph "Semantic Layer (v4.0)"
        F[Data Dictionary] -->|Embeddings| G[LanceDB Vector Store]
        G <-->|RAG Lookup| H["Neural Engine (Phi-3)"]
    end
    
    H -->|SQL Generation| E
    E -->|Result Set| I[User Interface]
```

---

## ✨ Key Capabilities

| Feature            | Technology           | Description |
|--------------------|----------------------|-------------|
| Temporal Querying  | delta-rs             | Query historical versions of data with zero duplication (Time Travel). |
| Semantic Search    | LanceDB              | Vector-based schema discovery enabling fuzzy, natural-language queries. |
| Data Quality       | Great Expectations   | Automated validation gates preventing bad data from entering the lake. |
| Compute Engine     | DuckDB               | In-process OLAP engine delivering sub-second analytics on Parquet. |
| CI/CD Pipeline     | GitHub Actions       | Automated lakehouse builds and end-to-end query tests on every push. |

---

## 🚀 Getting Started

This project uses **uv** for fast dependency resolution and deterministic environments.

### Prerequisites

- Python **3.11+**
- **uv** (recommended) or pip

---

## Installation

Clone the repository:

```bash
git clone https://github.com/YOUR_USERNAME/enterprise-datalens.git
cd enterprise-datalens
```

Sync the environment:

```bash
uv sync --frozen --no-install-project --all-extras
```

Build the Lakehouse and Semantic Index:

```bash
# Generate synthetic data, validate it, and build the Delta table
uv run python build_lakehouse.py

# Index schema metadata into the vector store
uv run python semantic_indexer.py
```

---

## 🧠 Usage

### Interactive Mode (Neural Shell)

Launch the conversational query engine:

```bash
uv run python datalens_engine.py
```

Example prompts:

- `Show me the total active transactions`
- `What was the count of records for version 0?`
- `show history`

---

### Headless Mode (CI / Automation)

Run a single query and exit—ideal for automated reporting or tests:

```bash
uv run python datalens_engine.py --sql "SELECT COUNT(*) FROM transactions"
```

---

## 🛡 Quality Assurance

Enterprise DataLens follows strict **DataOps and Lakehouse best practices**:

- **Validation Gate**  
  Every write is validated against  
  `gx/expectations/banking_quality_suite.json`.

- **Continuous Integration**  
  The CI pipeline builds the environment from scratch, generates fresh data,
  and runs an end-to-end smoke test on every commit.

- **Deterministic Builds**  
  `uv.lock` guarantees identical dependency resolution across local development
  and CI environments.

---

## 📂 Project Structure

```text
├── .github/workflows/   # CI/CD automation (GitHub Actions)
├── gx/                  # Great Expectations data quality suites
├── lancedb/             # (Ignored) local vector store
├── lakehouse/           # (Ignored) Delta Lake storage (Parquet + logs)
├── build_lakehouse.py   # Data generator & ingestion pipeline
├── datalens_engine.py   # Main query engine & CLI
├── semantic_indexer.py  # Vector embedding & indexing
├── gx_config.py         # Data quality configuration
└── uv.lock              # Immutable dependency manifest
```

---

## 📌 Summary

**Enterprise DataLens v4.0** demonstrates how modern Lakehouse concepts—**ACID storage, time travel, semantic querying, and automated data quality**—can be combined into a **single, local-first analytics engine** suitable for enterprise-scale design discussions, experimentation, and CI-driven validation.


