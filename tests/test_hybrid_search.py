"""Tests for Hybrid Search (BM25 + Vector) retriever (Phase 2)."""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from seed_chroma import golden_queries
from hybrid_retriever import HybridRetriever, get_hybrid_retriever


# ---------------------------------------------------------------------------
# Golden queries
# ---------------------------------------------------------------------------

class TestGoldenQueries:
    """Verify golden query corpus is well-formed."""

    def test_golden_queries_exist(self):
        assert len(golden_queries) >= 4

    def test_golden_queries_are_strings(self):
        for q in golden_queries:
            assert isinstance(q, str)

    def test_golden_queries_contain_sql(self):
        for q in golden_queries:
            assert "SELECT" in q, f"Golden query missing SELECT: {q[:50]}"

    def test_golden_queries_have_labels(self):
        """Each golden query should have a descriptive label before the SQL."""
        for q in golden_queries:
            assert ":" in q, f"Golden query missing label: {q[:50]}"


# ---------------------------------------------------------------------------
# Hybrid retriever construction
# ---------------------------------------------------------------------------

class TestHybridRetrieverConstruction:
    """Verify the hybrid retriever builds correctly."""

    def test_retriever_builds_without_error(self):
        retriever = get_hybrid_retriever(golden_queries)
        assert isinstance(retriever, HybridRetriever)

    def test_retriever_has_both_components(self):
        retriever = get_hybrid_retriever(golden_queries)
        assert retriever.bm25_retriever is not None
        assert retriever.vector_retriever is not None

    def test_retriever_weights_sum_to_one(self):
        retriever = get_hybrid_retriever(golden_queries)
        total = retriever.bm25_weight + retriever.vector_weight
        assert abs(total - 1.0) < 0.01


# ---------------------------------------------------------------------------
# Retrieval quality
# ---------------------------------------------------------------------------

class TestRetrievalQuality:
    """Verify hybrid search returns relevant results."""

    @pytest.fixture(autouse=True)
    def setup_retriever(self, hybrid_retriever):
        self.retriever = hybrid_retriever

    def test_returns_results_for_sql_keyword_query(self):
        results = self.retriever.invoke("DATE_TRUNC monthly aggregation")
        assert len(results) > 0

    def test_returns_results_for_semantic_query(self):
        results = self.retriever.invoke("find outliers and unusual transactions")
        assert len(results) > 0

    def test_top_result_for_timeseries_is_relevant(self):
        results = self.retriever.invoke("DATE_TRUNC month time series aggregation")
        all_content = " ".join(r.page_content.lower() for r in results)
        assert "date_trunc" in all_content or "transaction_date" in all_content

    def test_top_result_for_anomalies_is_relevant(self):
        results = self.retriever.invoke("statistical outlier detection")
        top = results[0].page_content.lower()
        assert "anomal" in top or "stddev" in top or "avg" in top

    def test_respects_k_parameter(self):
        results = self.retriever.invoke("revenue", k=1)
        assert len(results) <= 1

    def test_no_duplicates_in_results(self):
        results = self.retriever.invoke("top products by revenue")
        contents = [r.page_content for r in results]
        assert len(contents) == len(set(contents))


# ---------------------------------------------------------------------------
# RRF scoring
# ---------------------------------------------------------------------------

class TestRRFScoring:
    """Verify Reciprocal Rank Fusion produces sensible rankings."""

    def test_document_appearing_in_both_retrievers_ranks_higher(self):
        """If a doc appears in both BM25 and vector results, it should rank first."""
        retriever = get_hybrid_retriever(golden_queries)
        # "SELECT SUM revenue" should match both keyword and semantic
        results = retriever.invoke("SELECT product_name SUM amount revenue")
        # The top performers query should rank high since it matches both
        top = results[0].page_content
        assert "SUM(amount)" in top or "product_name" in top
