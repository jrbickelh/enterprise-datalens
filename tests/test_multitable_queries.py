"""
Multi-Table Query Tests: v8.0 Week 2-3 Validation

Tests verify that complex multi-table queries execute correctly
against the v8.0 schema (transactions, customers, products, regions).
"""

import pytest
import duckdb
from pathlib import Path


DB_PATH = str(Path(__file__).parent.parent / "datalens_lakehouse.db")


class TestMultiTableExecution:
    """Test that multi-table queries run successfully."""

    def test_customer_lifetime_value_query(self):
        """Test: Customer Lifetime Value across transactions."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT c.customer_id, c.customer_segment,
               SUM(t.amount) as lifetime_value,
               COUNT(t.transaction_id) as transaction_count
        FROM customers c
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id, c.customer_segment
        ORDER BY lifetime_value DESC
        LIMIT 10;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) > 0, "Query should return results"
        assert len(result[0]) == 4, "Should have 4 columns"
        assert result[0][2] > 0, "Lifetime value should be positive"

    def test_regional_churn_analysis(self):
        """Test: Churn rate calculation by region."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT r.region_name, r.sales_tier,
               SUM(CASE WHEN c.churn_flag = 1 THEN 1 ELSE 0 END) as churned_customers,
               COUNT(DISTINCT c.customer_id) as total_customers
        FROM regions r
        LEFT JOIN customers c ON r.region_id = c.region_id
        GROUP BY r.region_name, r.sales_tier
        ORDER BY churned_customers DESC;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) == 4, "Should have 4 regions"
        assert len(result[0]) == 4, "Should have 4 columns"
        # Verify churn count <= total count
        for row in result:
            assert row[2] <= row[3], "Churned customers should not exceed total"

    def test_product_revenue_with_margins(self):
        """Test: Product revenue aggregation with margin analysis."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT p.product_name, p.category, p.gross_margin_pct,
               SUM(t.amount) as product_revenue,
               COUNT(t.transaction_id) as transaction_count,
               AVG(t.amount) as avg_transaction_value
        FROM transactions t
        LEFT JOIN products p ON t.product_name = p.product_name
        GROUP BY p.product_name, p.category, p.gross_margin_pct
        ORDER BY product_revenue DESC;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) > 0, "Query should return results"
        assert len(result[0]) == 6, "Should have 6 columns"

    def test_three_table_join_transactions_customers_regions(self):
        """Test: Multi-hop join (transactions → customers → regions)."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT r.region_name, c.customer_segment,
               SUM(t.amount) as regional_segment_revenue,
               COUNT(DISTINCT c.customer_id) as customer_count
        FROM transactions t
        LEFT JOIN customers c ON t.customer_id = c.customer_id
        LEFT JOIN regions r ON c.region_id = r.region_id
        GROUP BY r.region_name, c.customer_segment
        ORDER BY regional_segment_revenue DESC;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) > 0, "Query should return results"
        assert len(result[0]) == 4, "Should have 4 columns"

    def test_customer_cohort_analysis(self):
        """Test: Segment customers by region and tenure."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT r.region_name, c.customer_segment, COUNT(DISTINCT c.customer_id) as customer_count,
               AVG(c.lifetime_value) as avg_ltv, SUM(c.churn_flag) as churned_count
        FROM customers c
        LEFT JOIN regions r ON c.region_id = r.region_id
        GROUP BY r.region_name, c.customer_segment
        ORDER BY avg_ltv DESC;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) > 0, "Query should return results"
        assert len(result[0]) == 5, "Should have 5 columns"

    def test_top_customers_by_region(self):
        """Test: Rank customers within each region by spending."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT r.region_name, c.customer_name, c.customer_segment,
               SUM(t.amount) as region_revenue,
               COUNT(t.transaction_id) as transaction_count
        FROM regions r
        LEFT JOIN customers c ON r.region_id = c.region_id
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        GROUP BY r.region_name, c.customer_id, c.customer_name, c.customer_segment
        ORDER BY r.region_name, region_revenue DESC
        LIMIT 100;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) > 0, "Query should return results"
        assert len(result[0]) == 5, "Should have 5 columns"


class TestJoinValidation:
    """Test the multi-table query validation logic."""

    def test_left_join_is_approved(self):
        """LEFT JOIN should pass validation."""
        from agent_tools import _validate_multi_table_query

        query = "SELECT * FROM customers c LEFT JOIN transactions t ON c.customer_id = t.customer_id"
        result = _validate_multi_table_query(query)

        assert result["valid"] is True

    def test_inner_join_on_fact_table_is_rejected(self):
        """INNER JOIN on fact table (transactions) should be rejected."""
        from agent_tools import _validate_multi_table_query

        query = "SELECT * FROM transactions t INNER JOIN customers c ON t.customer_id = c.customer_id"
        result = _validate_multi_table_query(query)

        assert result["valid"] is False
        assert "INNER JOIN" in result["reason"]

    def test_missing_from_clause_rejected(self):
        """Query without FROM clause should be rejected."""
        from agent_tools import _validate_multi_table_query

        query = "SELECT customer_id, amount"
        result = _validate_multi_table_query(query)

        assert result["valid"] is False


class TestSemanticLayerIntegration:
    """Test semantic layer functionality for multi-table queries."""

    def test_get_valid_joins(self):
        """Semantic layer should provide valid join paths."""
        from semantic_layer import get_valid_joins

        joins = get_valid_joins()

        assert "transactions_to_customers" in joins
        assert "customers_to_regions" in joins
        assert joins["transactions_to_customers"]["from_table"] == "transactions"
        assert joins["transactions_to_customers"]["to_table"] == "customers"

    def test_get_join_chains(self):
        """Semantic layer should recommend safe multi-table chains."""
        from semantic_layer import get_join_chains

        chains = get_join_chains()

        assert "fact_to_all_dimensions" in chains
        assert "fact_with_product" in chains
        assert "customer_regional_cohort" in chains

    def test_validate_join_transactions_to_customers(self):
        """Should approve transactions → customers join."""
        from semantic_layer import validate_join

        is_valid = validate_join("transactions", "customers")
        assert is_valid is True

    def test_validate_join_customers_to_regions(self):
        """Should approve customers → regions join."""
        from semantic_layer import validate_join

        is_valid = validate_join("customers", "regions")
        assert is_valid is True

    def test_validate_invalid_join_rejects(self):
        """Should reject invalid/undefined joins."""
        from semantic_layer import validate_join

        is_valid = validate_join("products", "regions")
        assert is_valid is False


class TestSuggestJoinsTool:
    """Test the suggest_joins tool functionality."""

    def test_valid_joins_via_semantic_layer(self):
        """Semantic layer should provide valid join suggestions."""
        from semantic_layer import get_valid_joins

        joins = get_valid_joins()
        trans_joins = [j for j in joins.keys() if "transactions" in j]

        assert len(trans_joins) > 0, "Should have joins from transactions"

    def test_join_chains_availability(self):
        """Semantic layer should recommend safe multi-table chains."""
        from semantic_layer import get_join_chains

        chains = get_join_chains()

        assert "fact_to_all_dimensions" in chains
        assert len(chains) > 0, "Should have recommended chains"

    def test_join_chain_descriptions_present(self):
        """Join chains should have descriptions and use cases."""
        from semantic_layer import get_join_chain_description

        desc = get_join_chain_description("fact_to_all_dimensions")

        assert "Customer lifetime value" in desc or "churn" in desc.lower()
        assert "Use case" in desc


class TestMultiTableErrorRecovery:
    """Test self-healing for multi-table query errors."""

    def test_column_mismatch_error_message(self):
        """Execution should fail gracefully on bad column names."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = "SELECT nonexistent_column FROM transactions"

        try:
            con.execute(query).fetchall()
            assert False, "Should raise error for nonexistent column"
        except Exception as e:
            assert "nonexistent" in str(e).lower() or "not found" in str(e).lower()

        con.close()

    def test_valid_multi_table_join_executes(self):
        """Valid multi-table joins should execute without error."""
        con = duckdb.connect(DB_PATH, read_only=True)

        query = """
        SELECT c.customer_id, SUM(t.amount) as total
        FROM customers c
        LEFT JOIN transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_id LIMIT 1;
        """

        result = con.execute(query).fetchall()
        con.close()

        assert len(result) >= 0, "Query should execute without error"
