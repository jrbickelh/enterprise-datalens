# Enterprise DataLens v3.5 | The Precision Interface

![Build Status](https://github.com/jrbickelh/enterprise-datalens/actions/workflows/ci.yml/badge.svg)  
![Python](https://img.shields.io/badge/Python-3.11-blue)  
![License](https://img.shields.io/badge/License-MIT-green)  
![Storage](https://img.shields.io/badge/Storage-Delta%20Lake%20%2B%20DuckDB-orange)

**DataLens v3.5** represents a paradigm shift from chat-based analytics to deterministic, engineering-grade SQL generation. It implements a **Local Lakehouse** architecture that combines the ACID compliance of Delta Lake with the high-performance analytical engine of DuckDB, all governed by a "Total Suppression" neural interface powered by Phi-3 that eliminates hallucination.

---

## 🏗 Architecture

The system operates on a **Zero-Copy principle** where data is materialized once in Delta Lake and queried directly via DuckDB's zero-copy readers.

1. **Storage Layer:**  
   Raw data is ingested and stored as versioned **Delta Tables** (Parquet + `_delta_log`).

2. **Serving Layer:**  
   DuckDB mounts these tables as Views, enabling sub-second analytical queries without data duplication.

3. **Neural Engine:**  
   A quantized Microsoft Phi-3 model (via ONNX) translates natural language into deterministic SQL, strictly bound to the lakehouse schema.

---

## 🚀 Quick Start

This project is optimized for **CachyOS** and uses **uv** for lightning-fast dependency management.

### 1️⃣ Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (Recommended) or pip

---

### 2️⃣ Installation

```bash
# Clone the repository
git clone https://github.com/jrbickelh/enterprise-datalens.git
cd enterprise-datalens

# Initialize Environment & Install Dependencies
uv venv .venv
source .venv/bin/activate.fish  # For Fish shell
uv pip install -r requirements.txt
```

---

### 3️⃣ Configuration

Create your `.env` file from the template:

```bash
cp .env.example .env
# Edit .env to add your HUGGINGFACE_TOKEN
```

---

## ⚡️ Usage

### Step 1: Build the Lakehouse

Initialize the local environment. This script generates synthetic banking data, writes it to Delta Lake, and registers it with DuckDB.

```bash
python build_lakehouse.py
```

**Output:**  
Creates `lakehouse/` directory and `lakehouse_serving.duckdb` catalog.

---

### Step 2: Run the Precision Interface

Launch the neural query engine in CLI mode.

```bash
python datalens_engine.py
```

---

## 📟 Live Demo Output

Below is an actual capture of the v3.5 engine running in **Total Suppression mode**.

```plaintext
============================================================
   ENTERPRISE DATALENS v3.5 | THE PRECISION INTERFACE
   Status: Online | Storage: Delta Lake + DuckDB | Model: Phi-3
============================================================
-> Applied Total Suppression: Reflection disabled. [OK]
Loading weights: 100%|██████████| 199/199 [00:01<00:00, Materializing param]
-> Connecting to Local Lakehouse (DuckDB)...
-> Syncing Delta Lake to Serving Layer... [OK]
-> Initializing Neural Query Engine... [OK]

System Ready. Precision Interface Loaded.
------------------------------------------------------------
User >> What is the total transaction amount for all Active customers?  

[GENERATED SQL]:
SELECT SUM(transactions.amount) AS total_transaction_amount   
FROM transactions   
JOIN customers ON transactions.customer_id = customers.customer_id   
WHERE customers.status = 'Active';

[INSIGHT]:
The total transaction amount for all Active customers is $1,612,241.93.
------------------------------------------------------------
```

---

## 📂 Project Structure

```plaintext
.
├── .github/workflows/         # CI/CD Pipeline (Build & Smoke Tests)
├── lakehouse/                 # Delta Lake Storage (GitIgnored)
├── .venv/                     # Virtual Environment (GitIgnored)
├── build_lakehouse.py         # Data Generation & ETL Script
├── datalens_engine.py         # Main CLI & Neural Logic
├── lakehouse_serving.duckdb   # Hot Serving Layer (GitIgnored)
├── metadata.yaml              # Versioning Configuration
├── requirements.txt           # Locked Dependencies (uv compatible)
├── LICENSE                    # MIT License
└── README.md                  # Documentation
```

---

## 🛡 License

Distributed under the **MIT License**. See `LICENSE` for more information.

---

**Built with Delta Lake, DuckDB, and Phi-3.**

