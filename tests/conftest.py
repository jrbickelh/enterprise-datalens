"""Shared pytest fixtures for DataLens tests."""

import os
import sys
import warnings

import pytest

# Setup path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

# Silence telemetry
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = ""
os.environ["LANGFUSE_ENABLED"] = "false"

warnings.filterwarnings("ignore", message=".*Pydantic serializer warnings.*")


@pytest.fixture
def hybrid_retriever():
    """Fixture providing a pre-initialized hybrid retriever."""
    from seed_chroma import golden_queries
    from hybrid_retriever import get_hybrid_retriever

    return get_hybrid_retriever(golden_queries)


@pytest.fixture
def schema_graph():
    """Fixture providing the schema knowledge graph."""
    from schema_graph import get_schema_graph

    return get_schema_graph()


@pytest.fixture
def semantic_context():
    """Fixture providing the semantic layer context."""
    from semantic_layer import get_cached_semantic_context

    return get_cached_semantic_context()


@pytest.fixture
def agent_graphs():
    """Fixture providing both compiled agent graphs."""
    from agent_graph import graph_auto, graph_hitl

    return {"auto": graph_auto, "hitl": graph_hitl}
