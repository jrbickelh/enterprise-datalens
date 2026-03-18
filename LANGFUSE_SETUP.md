# Langfuse Observability Setup Guide

DataLens now integrates **Langfuse** for comprehensive LLM observability, tracing, and cost tracking. This guide walks you through setup.

## What Langfuse Provides

- **Tracing:** Full nested traces of every LLM call, tool execution, and agent decision
- **Cost Tracking:** Per-token and per-query cost breakdown by model and user
- **Evaluation:** LLM-as-a-Judge scoring of agent outputs
- **Session Management:** User-level attribution and conversation threading

## Quick Start (5 minutes)

### Option 1: Cloud (Free Tier)

1. Sign up at [langfuse.com](https://langfuse.com) (free tier = 50k observations/month)

2. Get your credentials from the dashboard:
   - `LANGFUSE_PUBLIC_KEY`
   - `LANGFUSE_SECRET_KEY`

3. Update your `.env` file:
   ```bash
   LANGFUSE_ENABLED=true
   LANGFUSE_PUBLIC_KEY=your_public_key
   LANGFUSE_SECRET_KEY=your_secret_key
   LANGFUSE_HOST=https://cloud.langfuse.com
   ```

4. Run DataLens and start querying:
   ```bash
   uv run streamlit run app.py
   ```

All agent interactions will now be traced to your Langfuse dashboard.

### Option 2: Self-Hosted (Development)

For unlimited traces without API limits:

```bash
# Start Langfuse with Docker Compose
docker compose -f langfuse-docker-compose.yml up -d

# Update .env
LANGFUSE_ENABLED=true
LANGFUSE_PUBLIC_KEY=pk-lf-dev
LANGFUSE_SECRET_KEY=sk-lf-dev
LANGFUSE_HOST=http://localhost:3000
```

**Note:** Self-hosted Langfuse requires PostgreSQL and ClickHouse. Use Cloud for simplicity.

## What Gets Traced

When `LANGFUSE_ENABLED=true`, Langfuse captures:

- **Supervisor routing decisions** → which agent to call next
- **Engineer SQL generation** → ChromaDB retrieval + DuckDB execution
- **Scientist predictions** → anomaly detection, forecasting
- **Token usage & cost** → per-call breakdown
- **Error traces** → failed queries with stack traces
- **Final audit scores** → Groundedness & Completeness metrics

Example trace path:
```
trace_id=abc123
├── node=supervisor
│   └── route_decision=engineer
├── node=engineer
│   ├── tool=search_golden_queries
│   │   └── retrieval_result="SELECT * FROM transactions WHERE..."
│   └── tool=execute_duckdb_query
│       └── execution_result="rows=150, duration=234ms"
└── audit
    └── groundedness=0.95, completeness=0.88
```

## Monitoring Costs

Visit your Langfuse dashboard to see:

- **Per-query costs:** Which user questions cost the most?
- **Model breakdown:** GPT-4o vs 4o-mini token usage
- **Trend analysis:** Are agent queries getting cheaper or more expensive?

This is critical for DataLens since Azure free credits are finite. Use Langfuse to optimize:
- When to use 4o-mini (routing, simple queries)
- When to use gpt-4o (complex SQL, math)

## Disabling Langfuse

```bash
LANGFUSE_ENABLED=false
```

Agent operations continue normally; no traces are sent.

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "Langfuse integration failed" | Ensure `LANGFUSE_ENABLED=true` and keys are set in `.env` |
| Missing traces in dashboard | Check network connectivity; traces batch every 5 seconds |
| High latency in UI | Langfuse batching adds ~200ms. Disable if this is unacceptable. |

## Next Steps

- [ ] Set up Langfuse Cloud account (free tier)
- [ ] Add credentials to `.env`
- [ ] Enable `LANGFUSE_ENABLED=true`
- [ ] Run a few queries and inspect the Langfuse dashboard
- [ ] Review cost trends after 1 week of usage

---

**References:**
- [Langfuse Documentation](https://langfuse.com/docs)
- [Langfuse LangChain Integration](https://langfuse.com/integrations/frameworks/langchain)
- [Langfuse Azure Deployment](https://langfuse.com/self-hosting/deployment/azure)
