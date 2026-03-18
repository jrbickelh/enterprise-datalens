# Security Policy

## Reporting Security Vulnerabilities

**We take security seriously.** If you discover a security vulnerability in DataLens, please report it **privately** instead of using public issue tracker.

### How to Report

**Email:** Send a detailed report to `security@datalens.ai` with:

1. **Vulnerability Title**: Brief description (e.g., "SQL Injection in semantic layer")
2. **Component Affected**: Which file/module (e.g., `agent_tools.py`, `semantic_layer.py`)
3. **Severity Level**: Critical | High | Medium | Low
4. **Description**: Technical details of the vulnerability
5. **Proof of Concept**: Steps to reproduce (if possible)
6. **Recommended Fix**: Your suggested solution (if applicable)

**Example Report:**
```
Subject: [SECURITY] SQL Injection in query generator

Component: agent_tools.py - execute_duckdb_query()
Severity: High

Description:
User input is not properly sanitized before being passed to DuckDB,
allowing potential SQL injection attacks via the --query flag.

Proof of Concept:
./cli.py --query "'; DROP TABLE transactions; --"

Recommended Fix:
Use parameterized queries with placeholders (@param syntax) instead of
string concatenation.
```

### Response Timeline

| Timeline | Action |
|----------|--------|
| **Day 1** | We acknowledge receipt of your report |
| **Day 3-7** | Initial assessment and reproduction attempt |
| **Day 7-14** | Fix development and testing |
| **Day 14-21** | Security patch released (coordinated disclosure) |
| **Day 21+** | Public vulnerability disclosure (after patch release) |

---

## Known Security Considerations

### 1. **Azure OpenAI API Keys**
DataLens requires Azure OpenAI credentials. **Never commit `.env` files to git.**

**Mitigations:**
- `.gitignore` excludes `.env` files
- Use `.env.example` as template
- Store credentials in GitHub Secrets for CI/CD
- Rotate keys quarterly

**Best Practices:**
```bash
# ✅ GOOD: Use .env file locally
cp .env.example .env
echo "AZURE_OPENAI_API_KEY=sk-..." >> .env

# ❌ BAD: Don't hardcode secrets
export AZURE_OPENAI_API_KEY=sk-... # Do not do this in scripts
```

### 2. **SQL Injection Prevention**
DataLens implements multi-layer protection:

**Layer 1: Semantic Layer Validation**
- Only allows pre-defined metrics and dimensions
- Whitelists JOIN paths and table references
- Rejects dynamic table/column names

**Layer 2: Query Execution Validation**
- `_validate_multi_table_query()` rejects dangerous patterns:
  - INNER JOINs on fact tables
  - Cartesian product detection
  - Unvalidated table references

**Layer 3: DuckDB Parameterization** (v8.5+)
- ⚠️ Currently uses string concatenation
- **Planned for v8.5+**: Migrate to parameterized queries

**Example Safe Query:**
```python
# ✅ Uses semantic layer (safe)
suggestion = suggest_joins("transactions", "customers")
result = execute_duckdb_query(
    "SELECT customer_id, SUM(amount) FROM transactions WHERE status='Successful'"
)

# ❌ Don't do this (vulnerable to injection)
# result = execute_duckdb_query(f"SELECT * FROM {table_name}")
```

### 3. **Sensitive Data Handling**

**What's Protected:**
- API keys and credentials (in `.env`, not committed to git)
- PII in query results (logged only in safe mode)
- Cost tracking data (Langfuse encrypted by default)

**What's Logged:**
- Query text (for debugging and cost tracking)
- Generated SQL (for audit trails)
- Error messages (for troubleshooting)

**Best Practices:**
- Don't include passwords or PII in query text
- Use region or department codes instead of customer names
- Review Langfuse logs regularly for exposed data
- Enable HITL approval for sensitive queries

**Example Safe Query:**
```
✅ SAFE:
"Show revenue by region code"
"Show customer segments by country_id"

❌ AVOID:
"Show revenue by customer_ssn"
"Show transactions for john.doe@company.com"
```

### 4. **Dependency Management**

DataLens uses `uv` for deterministic dependency locking:

```bash
# All dependencies frozen in uv.lock
uv sync --frozen --all-extras

# Regular updates (with testing)
uv pip compile --upgrade
uv sync
uv run pytest tests/ -v
```

**Supply Chain Security:**
- ✅ GitHub Actions uses pinned action versions
- ✅ Dependencies scanned for vulnerabilities (via Dependabot)
- ✅ Lock file committed to git for reproducibility
- ⚠️ Manual review required for new dependencies

**Report Dependency Vulnerabilities:**
```bash
# Check for known vulnerabilities
pip-audit

# Contact: security@datalens.ai
```

### 5. **LLM Prompt Injection**

LLM models are susceptible to prompt injection attacks. DataLens mitigates via:

**Mitigation 1: Structured Output**
- All LLM responses validated with Pydantic schemas
- Strict enum constraints for routing decisions
- No raw string parsing from LLM output

**Mitigation 2: Semantic Layer Constraints**
- Engineer agent only generates SQL from pre-defined patterns
- Supervisor router has 3 fixed choices (Engineer/Scientist/Halt)
- No user input directly embedded in system prompts

**Mitigation 3: Shadow Audit**
- Secondary LLM evaluates every response
- Groundedness check: "Is answer grounded in actual data?"
- Completeness check: "Does it fully address the question?"

**Example Defense:**
```python
# ✅ SAFE: Structured output validated
response_model = StructuredOutput
# Engineer can only generate SQL for pre-defined patterns

# ❌ VULNERABLE: Raw LLM output
# response = llm.invoke(f"User said: {user_input}")
# sql = response.split("```sql")[1]  # String parsing!
```

### 6. **Access Control**

**Current State:**
- No authentication (v8.0)
- Single-user assumption (local/Docker deployment)

**v10.0 Roadmap:**
- RBAC (Role-Based Access Control)
- Query approval workflows
- Audit logging with user attribution

**Interim Recommendations:**
- Deploy behind VPN or firewall
- Use Docker with network isolation
- Restrict API key to specific Azure regions
- Monitor cost limits with `AZURE_COST_LIMIT_USD`

### 7. **Data Validation**

**Input Validation:**
- CLI queries validated as strings (no injection via args)
- `.env` variables type-checked before use
- ChromaDB and DuckDB paths validated

**Output Validation:**
- Query results sanitized before JSON serialization
- CSV export escapes special characters
- Error messages don't leak internal paths

---

## Security Testing

### Running Security Checks

```bash
# Check dependencies for known vulnerabilities
pip-audit --desc

# Run linter (catches some security issues)
uv run ruff check . --select=S  # Security checks only

# Run full test suite (includes security-related tests)
uv run pytest tests/ -v -k "validation or security"
```

### Manual Security Review Checklist

Before deploying to production:

- [ ] `.env` file is in `.gitignore` (check with `git status`)
- [ ] No hardcoded API keys in source code
- [ ] `AZURE_COST_LIMIT_USD` is set to reasonable value
- [ ] All user inputs are validated
- [ ] Error messages don't expose internal details
- [ ] Langfuse is disabled unless logs are secured
- [ ] Database backups encrypted at rest
- [ ] API keys rotated quarterly

---

## Security Updates

### Notification Policy
- Security patches released as PATCH version bumps (e.g., 8.0.1)
- Announced in GitHub Security Advisories
- Email notification to security@datalens.ai subscribers

### Update Frequency
- Critical vulnerabilities: Patch within 7 days
- High severity: Patch within 14 days
- Medium/Low: Included in next regular release

### How to Update
```bash
# Pull latest code
git pull origin main

# Update dependencies
uv sync --all-extras

# Run tests to verify
uv run pytest tests/ -v

# Restart application
docker compose restart  # or
uv run streamlit run app.py
```

---

## Responsible Disclosure

We follow **Coordinated Vulnerability Disclosure (CVD)** principles:

1. **You report privately** to security@datalens.ai
2. **We confirm receipt** within 24 hours
3. **We develop fix** in private branch
4. **We test fix** to prevent regressions
5. **We release patch** to all users
6. **We disclose publicly** with credit to reporter (if you want)

**By reporting privately, you give us time to fix before exploits are published.**

---

## Compliance & Standards

DataLens aims to follow security best practices:

| Standard | Status | Notes |
|----------|--------|-------|
| **OWASP Top 10** | ✅ Partially | Addresses A01 (Injection), A02 (Auth), A03 (Injection), A07 (Identification) |
| **PCI DSS** | ⚠️ Not Yet | Data doesn't contain payment card data (v8.0) |
| **GDPR** | ⚠️ Not Yet | No user PII stored; future RBAC will enable audit trails |
| **SOC 2** | 🔄 Roadmap | v10.0 enterprise features will include audit logging |

---

## Third-Party Security Audits

Currently: **Not yet audited** (pre-v1.0 release)

We welcome independent security researchers to review DataLens. Contact security@datalens.ai to discuss audit arrangements.

---

## License & Attribution

DataLens is released under **AGPL 3.0** with security considerations:

- Source code must be disclosed if modified
- Commercial use requires license agreement
- See [LICENSE](LICENSE) for full terms

---

## Questions?

- **Report Vulnerability**: security@datalens.ai
- **General Security Questions**: GitHub Discussions
- **Policy Feedback**: Open an issue on GitHub

---

**Last Updated:** March 2026 | **Version:** 8.0.0
