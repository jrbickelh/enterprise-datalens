"""Core integration tests for DataLens agent infrastructure."""

import os
import sys
import json
import warnings

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = ""
os.environ["LANGFUSE_ENABLED"] = "false"

warnings.filterwarnings("ignore", message=".*Pydantic serializer warnings.*")


# ---------------------------------------------------------------------------
# Database & schema
# ---------------------------------------------------------------------------

class TestDatabaseInfrastructure:
    """Verify DuckDB lakehouse is seeded and accessible."""

    def test_schema_returns_transactions_table(self):
        from agent_tools import get_db_schema_string
        schema = get_db_schema_string()
        assert "transactions" in schema.lower()

    def test_schema_contains_expected_columns(self):
        from agent_tools import get_db_schema_string
        schema = get_db_schema_string()
        for col in ["amount", "product_name", "region", "transaction_date"]:
            assert col in schema.lower(), f"Missing column: {col}"

    def test_database_has_5000_rows(self):
        import duckdb
        from agent_tools import DB_PATH
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            count = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            assert count == 5000

    def test_database_has_expected_products(self):
        import duckdb
        from agent_tools import DB_PATH
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            products = conn.execute(
                "SELECT DISTINCT product_name FROM transactions ORDER BY 1"
            ).fetchall()
            names = [p[0] for p in products]
            assert "DataLens Pro" in names
            assert "DataLens Lite" in names

    def test_database_has_expected_regions(self):
        import duckdb
        from agent_tools import DB_PATH
        with duckdb.connect(DB_PATH, read_only=True) as conn:
            regions = conn.execute(
                "SELECT DISTINCT region FROM transactions ORDER BY 1"
            ).fetchall()
            names = [r[0] for r in regions]
            assert "EMEA" in names
            assert "North America" in names


# ---------------------------------------------------------------------------
# Agent tools
# ---------------------------------------------------------------------------

class TestAgentTools:
    """Verify LangChain tools execute correctly."""

    def test_execute_duckdb_query_returns_json(self):
        from agent_tools import execute_duckdb_query
        result = execute_duckdb_query.invoke(
            {"query": "SELECT COUNT(*) as cnt FROM transactions"}
        )
        data = json.loads(result)
        assert data[0]["cnt"] == 5000

    def test_execute_duckdb_query_enforces_row_limit(self):
        from agent_tools import execute_duckdb_query
        result = execute_duckdb_query.invoke(
            {"query": "SELECT * FROM transactions"}
        )
        assert "Error" in result
        assert "100 rows" in result

    def test_execute_duckdb_query_self_heals_on_error(self):
        from agent_tools import execute_duckdb_query
        result = execute_duckdb_query.invoke(
            {"query": "SELECT * FROM nonexistent_table"}
        )
        assert "DATABASE ERROR" in result
        assert "INSTRUCTION" in result

    def test_execute_duckdb_query_strips_quotes(self):
        from agent_tools import execute_duckdb_query
        result = execute_duckdb_query.invoke(
            {"query": '"SELECT COUNT(*) as cnt FROM transactions"'}
        )
        data = json.loads(result)
        assert data[0]["cnt"] == 5000

    def test_generate_chart_returns_plotly_json(self):
        from agent_tools import generate_chart
        data = json.dumps([
            {"region": "EMEA", "revenue": 100},
            {"region": "APAC", "revenue": 200},
        ])
        result = generate_chart.invoke({
            "data_json": data,
            "x_col": "region",
            "y_col": "revenue",
            "chart_type": "bar",
            "title": "Test Chart",
        })
        parsed = json.loads(result)
        assert "data" in parsed
        assert "layout" in parsed

    def test_generate_chart_supports_pie(self):
        from agent_tools import generate_chart
        data = json.dumps([
            {"name": "A", "value": 10},
            {"name": "B", "value": 20},
        ])
        result = generate_chart.invoke({
            "data_json": data,
            "x_col": "name",
            "y_col": "value",
            "chart_type": "pie",
            "title": "Pie Test",
        })
        parsed = json.loads(result)
        assert "data" in parsed

    def test_detect_anomalies_returns_results(self):
        from agent_tools import detect_anomalies
        result = detect_anomalies.invoke({
            "sql_query": "SELECT amount FROM transactions"
        })
        assert "Anomal" in result or "anomal" in result or "No significant" in result

    def test_forecast_returns_predictions(self):
        from agent_tools import forecast_data
        import json as _json
        test_data = _json.dumps([
            {"ds": "2025-01-01", "y": 100},
            {"ds": "2025-02-01", "y": 120},
            {"ds": "2025-03-01", "y": 140},
            {"ds": "2025-04-01", "y": 160},
        ])
        result = forecast_data.invoke({"data_json": test_data, "periods": 2})
        parsed = _json.loads(result)
        assert len(parsed) == 2
        assert "y_hat" in parsed[0]

    def test_search_golden_queries_returns_patterns(self):
        from agent_tools import search_golden_queries
        result = search_golden_queries.invoke({"search_term": "monthly revenue"})
        assert "Observation:" in result
        assert "EXAMPLE" in result


# ---------------------------------------------------------------------------
# Tool exports
# ---------------------------------------------------------------------------

class TestToolExports:
    """Verify tool lists are correctly configured for agents."""

    def test_engineer_tools_list(self):
        from agent_tools import engineer_tools
        tool_names = [t.name for t in engineer_tools]
        assert "execute_duckdb_query" in tool_names
        assert "search_golden_queries" in tool_names
        assert "explore_schema" in tool_names

    def test_scientist_tools_list(self):
        from agent_tools import scientist_tools
        tool_names = [t.name for t in scientist_tools]
        assert "generate_chart" in tool_names
        assert "detect_anomalies" in tool_names
        assert "forecast_data" in tool_names

    def test_no_tool_overlap_between_agents(self):
        from agent_tools import engineer_tools, scientist_tools
        eng_names = {t.name for t in engineer_tools}
        sci_names = {t.name for t in scientist_tools}
        overlap = eng_names & sci_names
        assert len(overlap) == 0, f"Tools shared between agents: {overlap}"


# ---------------------------------------------------------------------------
# Graph compilation
# ---------------------------------------------------------------------------

class TestGraphCompilation:
    """Verify LangGraph state machines compile correctly."""

    def test_auto_graph_compiles(self):
        from agent_graph import graph_auto
        assert graph_auto is not None

    def test_hitl_graph_compiles(self):
        from agent_graph import graph_hitl
        assert graph_hitl is not None

    def test_graphs_have_expected_nodes(self):
        from agent_graph import workflow
        node_names = list(workflow.nodes.keys())
        assert "supervisor" in node_names
        assert "ENGINEER" in node_names
        assert "SCIENTIST" in node_names


# ---------------------------------------------------------------------------
# Agent service
# ---------------------------------------------------------------------------

class TestAgentService:
    """Verify the streaming service layer."""

    def test_langfuse_handler_is_none_when_disabled(self):
        from agent_service import langfuse_handler
        # Should be None since LANGFUSE_ENABLED=false
        assert langfuse_handler is None

    def test_confidence_scorer_handles_failure(self):
        from agent_service import calculate_confidence_score
        # Without valid Azure credentials, it should return the fallback
        result = calculate_confidence_score("test", "history", "answer")
        assert "groundedness" in result
        assert "completeness" in result
