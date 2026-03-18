# Contributing to DataLens

Welcome! DataLens is an open-source AI BI agent built on LangGraph, semantic layers, and hybrid search. We welcome contributions from engineers, data scientists, and researchers.

---

## Getting Started

### 1. Fork & Clone

```bash
git clone https://github.com/YOUR_USERNAME/datalens.git
cd datalens
```

### 2. Set Up Environment

```bash
# Install uv (fast Python package manager)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install dependencies
uv sync

# Create .env from template
cp .env.example .env
# Edit .env with your Azure OpenAI credentials
```

### 3. Seed Databases

```bash
uv run python seed_db.py        # Generate 5K transactions + dimensions
uv run python seed_chroma.py    # Embed golden SQL patterns
```

### 4. Run Tests

```bash
uv run pytest tests/ test_app.py -v
```

All tests should pass ✅

---

## Development Workflow

### Code Style

We follow **PEP 8** with some preferences:

```bash
# Run linter (enforced in CI)
uv run ruff check .

# Format code
uv run ruff format .
```

### Commit Messages

Use conventional commits:

```
feat(hybrid-search): Add BM25 retrieval for golden queries
fix(schema-graph): Correct cardinality calculation for identifiers
docs(readme): Update installation instructions
test(integration): Add multi-table query tests
chore: Update dependencies
```

### Creating a Pull Request

1. Create a feature branch:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes + commit:
   ```bash
   git add .
   git commit -m "feat(module): description"
   ```

3. Push and open PR:
   ```bash
   git push origin feature/your-feature-name
   ```

4. In the PR description, include:
   - What problem does this solve?
   - How do you test it?
   - Any breaking changes?

---

## Areas for Contribution

### 🚀 High Impact (Help Wanted)

**Multi-Source Support (v9.0)**
- Add PostgreSQL MCP server support
- Add Snowflake MCP server support
- Add REST API MCP server
- **Effort**: 2-4 weeks per source
- **Impact**: Enables multi-database analytics

**Model Compression (v8.5)**
- Fine-tune Qwen 7B on golden SQL patterns
- Set up vLLM inference server
- Benchmark cost savings
- **Effort**: 2-3 weeks
- **Impact**: 80% cost reduction post free-credits

**Enterprise Features (v10.0)**
- Implement RBAC (role-based access control)
- Add data lineage tracking
- Implement query approval workflows
- **Effort**: 3-5 weeks
- **Impact**: SaaS-ready product

### 🔧 Medium Impact

**Golden Query Expansion**
- Add 20+ SQL patterns for specific domains (e-commerce, SaaS, finance)
- Create domain-specific semantic layers
- **Effort**: 1-2 weeks
- **Impact**: Better SQL generation accuracy for your domain

**Documentation**
- Add video tutorials (setup, first query, multi-table analytics)
- Create benchmarking guides
- Write blog posts on hybrid search, semantic layers
- **Effort**: 1 week
- **Impact**: Attracts users and contributors

**Performance Optimization**
- Implement query caching (Redis/in-memory)
- Benchmark and optimize multi-table JOINs
- Profile LLM API calls
- **Effort**: 1-2 weeks
- **Impact**: 20-30% cost savings

### 📚 Lower Effort (Great for Starters)

- Add more test cases for edge cases
- Improve error messages
- Update documentation and examples
- Add CLI improvements (--output formats, --save queries)
- Fix typos and grammar in docstrings

---

## Testing Guidelines

### Run All Tests

```bash
uv run pytest tests/ test_app.py -v
```

### Add Tests for New Features

```python
# tests/test_your_feature.py
import pytest
from your_module import your_function

def test_basic_functionality():
    result = your_function("input")
    assert result == "expected_output"

def test_edge_case():
    with pytest.raises(ValueError):
        your_function(None)
```

### Test Coverage Targets

- **Core Agent**: 80%+ coverage
- **Tools**: 85%+ coverage
- **Infrastructure** (search, semantic layer, graph): 80%+ coverage

Current coverage: 120 tests passing, all major components covered ✅

---

## Debugging

### Enable Verbose Logging

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Run Agent with Debug Output

```bash
# In app.py or cli.py, set debug=True
uv run streamlit run app.py --logger.level=debug
```

### Test Individual Tools

```python
from agent_tools import execute_duckdb_query

# Test SQL execution
result = execute_duckdb_query("SELECT * FROM transactions LIMIT 1")
print(result)
```

---

## Reporting Issues

### Found a Bug?

1. **Check existing issues** to avoid duplicates
2. **Create a detailed report** including:
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment (Python version, GPU, OS)
   - Full error traceback

### Have a Feature Request?

1. **Describe the use case** (why do you need it?)
2. **Suggest an implementation** (if you have ideas)
3. **Check with maintainers** before large refactors

---

## Code Review Process

When you submit a PR:

1. **Automated checks** run (linting, tests)
2. **Maintainers review** within 48 hours
3. **Feedback & iteration** (usually 1-2 rounds)
4. **Merge** once approved ✅

### What We Look For

- ✅ Tests pass locally + in CI
- ✅ Code follows PEP 8 style
- ✅ Docstrings on public functions
- ✅ No breaking changes (or documented)
- ✅ Commit messages are clear

---

## Deployment & Release

### Versioning

We follow **semantic versioning** (MAJOR.MINOR.PATCH):

- **MAJOR**: Breaking changes (agent API changes)
- **MINOR**: New features (new agent capabilities, golden queries)
- **PATCH**: Bug fixes

### Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create git tag: `git tag v8.0.0`
4. Push tag: `git push origin v8.0.0`
5. GitHub Actions auto-creates release notes

---

## Resources

- **Architecture**: See `ARCHITECTURE.md` (coming soon)
- **Roadmap**: See `ROADMAP.md`
- **Multi-Table Guide**: See `MULTITABLE_QUERIES.md`
- **Demo Scenarios**: See `DEMO_SCENARIOS.md`

---

## Questions?

- **Issues**: Open a GitHub issue
- **Discussions**: Start a GitHub discussion
- **Email**: maintainers@datalens.ai (coming soon)

---

## Recognition

All contributors are listed in `CONTRIBUTORS.md`. Thank you for making DataLens better! 🎉

