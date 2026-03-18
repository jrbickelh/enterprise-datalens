# DataLens Roadmap: v7.0 → v10.0

## Current State: v7.0 ✅ (Complete)

**Completed Features:**
- ✅ LangGraph multi-agent architecture (Supervisor, Engineer, Scientist)
- ✅ Hybrid Search RAG (BM25 + vector via Reciprocal Rank Fusion)
- ✅ YAML Semantic Layer (metrics, dimensions, constraints)
- ✅ NetworkX Knowledge Graph (schema structure, column roles)
- ✅ Langfuse observability (cost tracking, evaluation, tracing)
- ✅ DuckDB lakehouse + ChromaDB vector store
- ✅ Self-healing SQL execution (autonomous retry on errors)
- ✅ Shadow Audit (Pydantic-based LLM evaluation)
- ✅ Human-in-the-Loop (HITL) security breakpoints
- ✅ Deterministic ML (Isolation Forest, Linear Regression, Plotly)
- ✅ 86 passing tests + GitHub Actions CI/CD
- ✅ Docker + docker-compose for easy deployment
- ✅ AGPL-3.0 open source

**Stats:**
- 1,200+ lines of core agent code
- 800+ lines of new infrastructure (hybrid search, semantic layer, knowledge graph)
- 4 comprehensive guides (Langfuse, Hybrid Search, Semantic Layer, Setup)

---

## v7.1 (Minor Polish) — 1 week

Quick wins to maximize current value without major refactoring.

### Features
- [ ] Expand golden SQL library from 4 → 15 patterns
  - Time-series aggregation variants (daily, weekly, quarterly)
  - Segment analysis (product, region, customer cohort)
  - Forecasting prep patterns
  - Anomaly detection variants
  - Revenue metrics (gross, net, recurring, lifetime)
- [ ] Add more dataset scenarios to seed_db.py
  - Customer dimension table (prep for future multi-table)
  - Product catalog table
  - Regional hierarchy
- [ ] CLI tool for testing agent without Streamlit
  - `uv run python -m datalens.cli --query "Show Q4 revenue by region"`
- [ ] Add rate-limiting config for Azure OpenAI (prevent cost overruns)

### Tests
- [ ] Add integration tests (end-to-end agent flows)
- [ ] Benchmark test: "Show total revenue by product" latency/cost

### Documentation
- [ ] Blog post: "5 Techniques to Reduce LLM Hallucinations in SQL" (using DataLens tech)
- [ ] Video demo: Docker setup → query example (5 min)

**Effort:** 1 week solo | **Impact:** Medium (expands demo value, better documentation)

---

## v7.2 (Performance & Monitoring) — 2 weeks

Focus on production readiness and observability.

### Features
- [ ] Query caching layer
  - Cache (question → SQL → results) in Redis or in-memory
  - Reuse identical/similar queries within session
  - Estimated cost savings: 20-30%
- [ ] Token usage alerts
  - Warn if single query exceeds $1, $5, etc.
  - Per-session budget enforcement
- [ ] Structured logging (JSON format)
  - Replace print() with logging module
  - Integrate with Langfuse
- [ ] Retry logic for transient API failures
  - Exponential backoff for Azure OpenAI rate limits

### Tests
- [ ] Cache hit/miss ratio benchmarks
- [ ] Cost tracking accuracy tests

**Effort:** 2 weeks | **Impact:** Medium (reduces costs, improves reliability)

---

## v8.0 (Multi-Source Analytics) — 3-4 weeks

**Major feature:** Multi-table queries + new data sources (via Semantic Layer expansion).

### Features
- [ ] Expand Semantic Layer to support JOIN definitions
  - `transactions.customer_id → customers.customer_id`
  - `customers.region_id → regions.region_id`
  - Hierarchy: transactions → customers → regions
- [ ] Seed multi-table dataset
  - customers table (10K rows)
  - products table (500 rows)
  - regions table (10 rows)
  - Foreign key relationships
- [ ] Update Engineer agent to handle JOINs
  - Leverage schema graph for valid joins
  - Generate multi-table SQL with confidence
- [ ] New query examples
  - "Show top 10 customers by lifetime value"
  - "Which regions have highest churn?"
  - "Product adoption by customer segment"

### Tests
- [ ] Multi-table join correctness tests
- [ ] Circular dependency detection (avoid infinite joins)

**Effort:** 3-4 weeks | **Impact:** High (major capability expansion)

---

## v8.5 (Model Compression & Distillation) — 2-3 weeks

**Major feature:** Reduce costs by distilling GPT-4o into smaller open models.

### Features (from Phase research)
- [ ] Fine-tune Qwen 2.5-Coder 7B on golden SQL patterns
  - QLoRA fine-tuning with 1K examples
  - Estimated cost: $50-100
- [ ] Set up vLLM inference server
  - Azure ML managed endpoint
  - Sub-100ms latency
- [ ] Routing optimization
  - Use 3B Phi-4 for supervisor (currently gpt-4o-mini)
  - Use Qwen 7B for engineer (currently gpt-4o)
  - Keep gpt-4o for scientist (math/reasoning-heavy)
- [ ] Cost comparison dashboard
  - Side-by-side: Azure OpenAI vs self-hosted models
  - Break-even analysis

### Tests
- [ ] Accuracy comparison: fine-tuned Qwen vs GPT-4o on SQL
- [ ] Latency benchmarks for vLLM

**Effort:** 2-3 weeks | **Impact:** High (post-free-credits sustainability)

---

## v9.0 (MCP: Model Context Protocol) — 3-4 weeks

**Major refactoring:** Standardize tool integrations via Anthropic's MCP standard.

### Current Pain Points
- Each new data source requires custom tool implementation
- Agent code tightly coupled to specific tools
- Hard to swap DuckDB for PostgreSQL/Snowflake

### MCP Solution
Replace custom tools with standardized MCP servers:

```
Engineer Agent
    ↓
MCP Client (auto-discovery)
    ↓
MCP Servers (pluggable)
    ├── duckdb-mcp-server
    ├── postgres-mcp-server
    ├── snowflake-mcp-server
    ├── api-mcp-server (REST/GraphQL)
    └── knowledge-base-mcp-server
```

### Implementation
- [ ] Implement MCP client in agent_service.py
- [ ] Migrate `execute_duckdb_query` → `duckdb-mcp-server`
- [ ] Migrate `explore_schema` → MCP tool discovery
- [ ] Add PostgreSQL MCP server support (optional)
- [ ] Add REST API MCP server support (optional)
- [ ] Update Engineer agent to dynamically select tools

### Benefits
- ✅ Swap DuckDB → PostgreSQL without touching agent code
- ✅ Add new data sources as MCP plugins
- ✅ Agents discover available tools dynamically
- ✅ Standardized interface (industry-standard protocol)

### Example Use Case
```python
# Before (hardcoded)
agent = create_react_agent(
    tools=[execute_duckdb_query, search_golden_queries],
    ...
)

# After (MCP-based)
agent = create_react_agent(
    mcp_client=mcp_client,  # Discovers tools automatically
    ...
)
# Agent can use: DuckDB, Postgres, Snowflake, APIs, etc.
```

### Tests
- [ ] MCP server lifecycle (start/stop/restart)
- [ ] Tool discovery accuracy
- [ ] Cross-source query tests (join DuckDB table with API data)

**Effort:** 3-4 weeks | **Impact:** Very High (enables enterprise multi-source)

---

## v10.0 (Enterprise Ready) — 2-3 weeks

Polish for production deployment at scale.

### Features
- [ ] Role-based access control (RBAC)
  - User roles: analyst, editor, admin
  - Query approval workflows
- [ ] Data governance
  - Audit log: who queried what, when, cost
  - Data lineage tracking
  - Query approval gates
- [ ] Advanced scheduling
  - Cron-based report generation
  - Email delivery of analytics
  - Slack/Teams integration
- [ ] Multi-tenant isolation
  - Separate ChromaDB indexes per tenant
  - Isolated DuckDB connections
- [ ] Secrets management
  - Remove Azure keys from .env files
  - Use Azure Key Vault / HashiCorp Vault
- [ ] Monitoring & alerting
  - Prometheus metrics (query latency, error rate)
  - PagerDuty/Opsgenie integration

### Tests
- [ ] Multi-tenant isolation tests
- [ ] RBAC permission enforcement

**Effort:** 2-3 weeks | **Impact:** High (enterprise readiness)

---

## Summary Timeline

| Version | Focus | Effort | Timeline |
|---------|-------|--------|----------|
| **v7.0** | ✅ Complete | Done | Live |
| **v7.1** | Polish + docs | 1 week | Next sprint |
| **v7.2** | Perf + monitoring | 2 weeks | Sprint 2 |
| **v8.0** | Multi-table queries | 3-4 weeks | Sprint 3-4 |
| **v8.5** | Model compression | 2-3 weeks | Sprint 5 |
| **v9.0** | MCP integration | 3-4 weeks | Sprint 6-7 |
| **v10.0** | Enterprise ready | 2-3 weeks | Sprint 8 |
| **Total** | → v10.0 | ~16-18 weeks | 4-5 months |

---

## Strategic Value at Each Milestone

| Version | Marketability | Use Cases |
|---------|---------------|-----------|
| **v7.0** | Portfolio/demo | Small-scale BI, education, research |
| **v7.2** | Job interviews | "Production-ready cost control" |
| **v8.0** | Mid-market demo | Real-world multi-table analytics |
| **v8.5** | Cost story | "Runs on commodity hardware post-free-credits" |
| **v9.0** | Enterprise pitch | "Works with any data source via MCP" |
| **v10.0** | VCs/acquisition | "Multi-tenant SaaS ready" |

---

## Decision Points

**Go/No-Go criteria:**

- **v7.1:** Always do (low effort, high polish value)
- **v7.2:** Do if free Azure credits running low (cache + budgets save money)
- **v8.0:** Do if multi-source is in scope (major UX upgrade)
- **v8.5:** Do if planning to operate post-free-credits (essential for sustainability)
- **v9.0:** Do if targeting enterprise (unlocks multi-source as selling point)
- **v10.0:** Do if aiming for SaaS product (skip if portfolio-only)

---

## Notes

- **MCP readiness:** v9.0 requires understanding of [Anthropic's MCP spec](https://modelcontextprotocol.io)
- **Cost optimization:** v8.5 is where free credits are extended the furthest
- **Market positioning:** Each version targets a different audience (student → startup → enterprise)
- **Open source sustainability:** Model compression (v8.5) is critical for long-term viability

---

# Next Steps Decision Framework

## For Job Search (Recommended if on timeline)
**Goal:** Impressive portfolio + content for interviews

**v7.1 + Content** (1.5 weeks)
```
Week 1: Expand golden queries (4→15), create CLI tool
Week 1.5: Blog + video + LinkedIn post
Outcome: "Wow, this candidate built multi-agent LLM system with self-healing SQL"
```

Then optionally **v8.0** (if time permits) to show multi-table capability.

---

## For Startup/Product (Longer timeline)
**Goal:** Production-ready feature set for demo → fundraising

**v7.1 + v8.0 + v8.5** (12-14 weeks total)
```
Week 1-2: v7.1 polish
Week 3-6: v8.0 multi-table queries
Week 7-10: v8.5 model compression + cost optimization
Week 11-14: Benchmarking, documentation, demo scenarios
Outcome: "This AI BI agent works on real-world multi-table schemas AND costs 80% less to run"
```

---

## Key Decision: When to Commit to v8.0+?

Ask yourself:
- Do I want to demo complex multi-table scenarios? → YES = v8.0
- Will free credits last beyond month 3-4? → NO = do v8.5 by then
- Am I targeting enterprise sales? → YES = plan for v9.0 (MCP)
- Is this a startup/product play? → YES = commit to full roadmap

---

## Competitive Positioning at Each Stage

| Version | Pitch | Competitors |
|---------|-------|-------------|
| v7.0 | "Self-healing SQL agent with hybrid RAG" | Vanna AI, Text2SQL demos |
| v7.1 | "Agent that handles 15+ SQL patterns" | Semantic layer differentiates |
| v8.0 | "Multi-table queries with AI agents" | Unique: hybrid search + semantic layer |
| v8.5 | "Enterprise-grade SQL agent, 80% cost savings" | Only self-hosters offer this cost profile |
| v9.0 | "AI agent works with ANY data source (MCP)" | Industry-first interoperability |
| v10.0 | "Multi-tenant SaaS-ready AI BI platform" | Enterprise-grade product |

---

## Final Recommendation

**IF you're job searching:** Do v7.1 + content marketing (2 weeks) → dominate interviews
**IF you're building a startup:** Plan v7.1 → v8.0 → v8.5 (3 months) → pitch with unique tech
**IF you're going long-term:** Full roadmap to v10.0 (6 months) → viable SaaS product

**MCP (v9.0)** is valuable but secondary—only do after v8.0 + v8.5 prove the core value.
