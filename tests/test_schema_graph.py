"""Tests for the Schema Knowledge Graph (Phase 4)."""

import os
import sys

# Ensure project root is importable
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from schema_graph import (
    build_schema_graph,
    get_schema_graph,
    get_table_context,
    get_valid_joins_for_table,
    get_column_profile,
    get_graph_summary,
    explore_schema,
)


# ---------------------------------------------------------------------------
# Graph construction
# ---------------------------------------------------------------------------

class TestSchemaGraphConstruction:
    """Verify the graph builds correctly from DuckDB information_schema."""

    def test_graph_builds_without_error(self):
        G = build_schema_graph()
        assert G is not None
        assert G.number_of_nodes() > 0

    def test_graph_has_table_nodes(self):
        G = build_schema_graph()
        tables = [n for n, d in G.nodes(data=True) if d.get("node_type") == "table"]
        assert "transactions" in tables

    def test_graph_has_column_nodes(self):
        G = build_schema_graph()
        columns = [n for n, d in G.nodes(data=True) if d.get("node_type") == "column"]
        assert "transactions.amount" in columns
        assert "transactions.product_name" in columns
        assert "transactions.region" in columns

    def test_table_node_has_row_count(self):
        G = build_schema_graph()
        assert G.nodes["transactions"]["row_count"] == 5000

    def test_column_nodes_have_metadata(self):
        G = build_schema_graph()
        amount = G.nodes["transactions.amount"]
        assert amount["data_type"] == "DOUBLE"
        assert amount["node_type"] == "column"
        assert "role" in amount
        assert "distinct_values" in amount

    def test_ownership_edges_exist(self):
        G = build_schema_graph()
        has_col_edges = [
            (u, v)
            for u, v, d in G.edges(data=True)
            if d.get("edge_type") == "has_column"
        ]
        # v8.0: Multi-table schema: transactions(7) + customers(7) + products(5) + regions(4) = 23
        assert len(has_col_edges) == 23


# ---------------------------------------------------------------------------
# Column role classification
# ---------------------------------------------------------------------------

class TestColumnRoleClassification:
    """Verify the graph correctly classifies columns as dimension/metric/identifier."""

    def test_amount_is_metric(self):
        G = build_schema_graph()
        assert G.nodes["transactions.amount"]["role"] == "metric"

    def test_product_name_is_dimension(self):
        G = build_schema_graph()
        assert G.nodes["transactions.product_name"]["role"] == "dimension"

    def test_region_is_dimension(self):
        G = build_schema_graph()
        assert G.nodes["transactions.region"]["role"] == "dimension"

    def test_transaction_id_is_identifier(self):
        G = build_schema_graph()
        assert G.nodes["transactions.transaction_id"]["role"] == "identifier"

    def test_status_is_dimension(self):
        G = build_schema_graph()
        assert G.nodes["transactions.status"]["role"] == "dimension"


# ---------------------------------------------------------------------------
# Context generation for LLM prompts
# ---------------------------------------------------------------------------

class TestContextGeneration:
    """Verify context strings are well-formed for prompt injection."""

    def test_table_context_contains_columns(self):
        ctx = get_table_context("transactions")
        assert "amount" in ctx
        assert "product_name" in ctx
        assert "region" in ctx

    def test_table_context_contains_row_count(self):
        ctx = get_table_context("transactions")
        assert "5000 rows" in ctx

    def test_table_context_contains_roles(self):
        ctx = get_table_context("transactions")
        assert "role=metric" in ctx
        assert "role=dimension" in ctx

    def test_nonexistent_table_returns_error(self):
        ctx = get_table_context("nonexistent_table")
        assert "not found" in ctx.lower()

    def test_graph_summary_contains_table_info(self):
        summary = get_graph_summary()
        assert "transactions" in summary
        assert "Dimensions:" in summary
        assert "Metrics:" in summary

    def test_column_profile_contains_metadata(self):
        profile = get_column_profile("transactions", "amount")
        assert "DOUBLE" in profile
        assert "metric" in profile
        assert "Distinct values" in profile

    def test_joins_message_for_single_table(self):
        joins = get_valid_joins_for_table("transactions")
        assert "no detected join paths" in joins.lower() or "JOIN" in joins


# ---------------------------------------------------------------------------
# Schema caching
# ---------------------------------------------------------------------------

class TestCaching:
    """Verify the module-level cache works correctly."""

    def test_cached_graph_is_same_object(self):
        g1 = get_schema_graph()
        g2 = get_schema_graph()
        assert g1 is g2


# ---------------------------------------------------------------------------
# LangChain tool interface
# ---------------------------------------------------------------------------

class TestExploreSchemaool:
    """Verify the explore_schema LangChain tool works correctly."""

    def test_tool_summary_command(self):
        result = explore_schema.invoke({"query": "tables"})
        assert "transactions" in result

    def test_tool_table_command(self):
        result = explore_schema.invoke({"query": "transactions"})
        assert "amount" in result
        assert "product_name" in result

    def test_tool_column_command(self):
        result = explore_schema.invoke({"query": "column transactions amount"})
        assert "DOUBLE" in result

    def test_tool_joins_command(self):
        result = explore_schema.invoke({"query": "joins transactions"})
        assert "transactions" in result.lower()

    def test_tool_empty_query_returns_summary(self):
        result = explore_schema.invoke({"query": ""})
        assert "SCHEMA GRAPH SUMMARY" in result
