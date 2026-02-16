# Enterprise DataLens 🔍

**Semantic Data Mesh & RAG Architecture for Automated SQL Generation.**

## 🚀 Overview
Enterprise DataLens is an autonomous GenAI agent designed to democratize access to complex banking data. Unlike standard "Text-to-SQL" bots, it utilizes a **Semantic Layer** and **Vector Search (RAG)** to understand business context before generating queries.

## 🏗 Architecture
* **Orchestrator:** LlamaIndex
* **LLM:** Microsoft Phi-3 Mini (Local 3.8B quantized)
* **Vector Store:** ChromaDB (Schema RAG)
* **Governance:** Role-Based Access Control (RBAC) simulation via Metadata injection.

## 🛠 Quick Start
1.  **Clone the repo:**
    ```bash
    git clone [https://github.com/YOUR_USERNAME/enterprise-datalens.git](https://github.com/YOUR_USERNAME/enterprise-datalens.git)
    ```
2.  **Install dependencies (using uv):**
    ```bash
    uv pip install -r requirements.txt
    ```
3.  **Generate the Data Warehouse:**
    ```bash
    python generate_data.py
    ```
4.  **Run the Agent:**
    ```bash
    python datalens_engine.py
    ```

## 🔮 Roadmap
* [x] **Phase 1:** Local MVP with LlamaIndex & SQLite.
* [ ] **Phase 2:** Vector Search for Schema Scalability.
* [ ] **Phase 3:** Data Quality Framework with Apache Spark.
* [ ] **Phase 4:** Airflow Orchestration.
