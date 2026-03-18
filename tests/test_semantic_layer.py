"""Tests for the YAML Semantic Layer (Phase 3)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from semantic_layer import (
    load_semantic_layer,
    get_semantic_context,
    get_cached_semantic_context,
    get_metric_definition,
    validate_metric_dimensions,
)


# ---------------------------------------------------------------------------
# YAML loading
# ---------------------------------------------------------------------------

class TestYAMLLoading:
    """Verify semantic_layer.yaml loads correctly."""

    def test_loads_without_error(self):
        data = load_semantic_layer()
        assert isinstance(data, dict)

    def test_has_required_sections(self):
        data = load_semantic_layer()
        for section in ["tables", "metrics", "dimensions", "constraints", "common_patterns"]:
            assert section in data, f"Missing section: {section}"

    def test_tables_section_has_transactions(self):
        data = load_semantic_layer()
        assert "transactions" in data["tables"]

    def test_transactions_has_columns(self):
        data = load_semantic_layer()
        columns = data["tables"]["transactions"]["columns"]
        assert "amount" in columns
        assert "product_name" in columns
        assert "region" in columns
        assert "transaction_date" in columns


# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------

class TestMetrics:
    """Verify metric definitions are correct and retrievable."""

    def test_total_revenue_definition(self):
        definition = get_metric_definition("total_revenue")
        assert definition == "SUM(amount)"

    def test_transaction_count_definition(self):
        definition = get_metric_definition("transaction_count")
        assert definition == "COUNT(*)"

    def test_average_transaction_value_definition(self):
        definition = get_metric_definition("average_transaction_value")
        assert definition == "AVG(amount)"

    def test_anomaly_threshold_definition(self):
        definition = get_metric_definition("anomaly_threshold")
        assert "STDDEV" in definition

    def test_nonexistent_metric_raises_key_error(self):
        with pytest.raises(KeyError):
            get_metric_definition("nonexistent_metric")

    def test_metrics_have_descriptions(self):
        data = load_semantic_layer()
        for name, metric in data["metrics"].items():
            assert "description" in metric, f"Metric '{name}' missing description"
            assert "definition" in metric, f"Metric '{name}' missing definition"


# ---------------------------------------------------------------------------
# Dimensions
# ---------------------------------------------------------------------------

class TestDimensions:
    """Verify dimension definitions and validation."""

    def test_valid_dimensions_for_total_revenue(self):
        assert validate_metric_dimensions("total_revenue", ["product_name", "region"])

    def test_valid_single_dimension(self):
        assert validate_metric_dimensions("total_revenue", ["product_name"])

    def test_empty_dimensions_is_valid(self):
        assert validate_metric_dimensions("total_revenue", [])

    def test_nonexistent_metric_validation_raises(self):
        with pytest.raises(KeyError):
            validate_metric_dimensions("fake_metric", ["product_name"])

    def test_region_has_valid_values(self):
        data = load_semantic_layer()
        region = data["dimensions"]["region"]
        assert "valid_values" in region
        assert "EMEA" in region["valid_values"]
        assert "APAC" in region["valid_values"]

    def test_time_period_has_granularities(self):
        data = load_semantic_layer()
        time = data["dimensions"]["time_period"]
        assert "supported_granularities" in time


# ---------------------------------------------------------------------------
# Context generation
# ---------------------------------------------------------------------------

class TestSemanticContext:
    """Verify the formatted context string for prompt injection."""

    def test_context_contains_metrics(self):
        ctx = get_semantic_context()
        assert "AVAILABLE METRICS:" in ctx
        assert "total_revenue" in ctx
        assert "SUM(amount)" in ctx

    def test_context_contains_dimensions(self):
        ctx = get_semantic_context()
        assert "AVAILABLE DIMENSIONS:" in ctx
        assert "product_name" in ctx
        assert "region" in ctx

    def test_context_contains_constraints(self):
        ctx = get_semantic_context()
        assert "CRITICAL SQL CONSTRAINTS:" in ctx
        assert "DATE_TRUNC" in ctx

    def test_context_contains_patterns(self):
        ctx = get_semantic_context()
        assert "COMMON SQL PATTERNS" in ctx

    def test_context_is_reasonable_size(self):
        ctx = get_semantic_context()
        # Should be between 500 and 5000 chars for prompt injection
        assert 500 < len(ctx) < 5000

    def test_cached_context_matches_fresh(self):
        fresh = get_semantic_context()
        cached = get_cached_semantic_context()
        assert fresh == cached


# ---------------------------------------------------------------------------
# Common patterns
# ---------------------------------------------------------------------------

class TestCommonPatterns:
    """Verify SQL patterns are valid and complete."""

    def test_patterns_contain_sql_keywords(self):
        data = load_semantic_layer()
        valid_tables = ["transactions", "customers", "products", "regions"]
        for name, pattern in data["common_patterns"].items():
            assert "SELECT" in pattern, f"Pattern '{name}' missing SELECT"
            assert "FROM" in pattern, f"Pattern '{name}' missing FROM"
            # Check that pattern references at least one valid table
            has_table = any(table in pattern for table in valid_tables)
            assert has_table, f"Pattern '{name}' missing valid table reference"
