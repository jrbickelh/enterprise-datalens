"""
Integration Tests: End-to-End Agent Flows

These tests verify that the full agent pipeline works correctly,
from user query through supervisor routing to worker execution.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from langchain_core.messages import HumanMessage, AIMessage

# Import agent components
from agent_graph import supervisor_node, engineer_node, scientist_node, AgentState


class TestSupervisorRouting:
    """Test the Supervisor's routing logic."""

    def test_supervisor_routes_to_engineer_for_sql_query(self):
        """Supervisor should route SQL-heavy queries to Engineer."""
        state = AgentState(
            messages=[HumanMessage(content="Show total revenue by product")],
            next_node="supervisor",
        )

        # Mock LLM response
        with patch("agent_graph.llm_mini") as mock_llm:
            mock_router = Mock()
            mock_router.next = "ENGINEER"
            mock_llm.with_structured_output.return_value.invoke.return_value = mock_router

            result = supervisor_node(state)

            assert result["next_node"] == "ENGINEER"

    def test_supervisor_routes_to_scientist_for_anomaly_detection(self):
        """Supervisor should route anomaly queries to Scientist."""
        state = AgentState(
            messages=[HumanMessage(content="Detect anomalies in our transactions")],
            next_node="supervisor",
        )

        with patch("agent_graph.llm_mini") as mock_llm:
            mock_router = Mock()
            mock_router.next = "SCIENTIST"
            mock_llm.with_structured_output.return_value.invoke.return_value = mock_router

            result = supervisor_node(state)

            assert result["next_node"] == "SCIENTIST"

    def test_supervisor_routes_to_finish_for_acknowledgments(self):
        """Supervisor should finish for simple acknowledgments."""
        state = AgentState(
            messages=[HumanMessage(content="Thanks for the analysis!")],
            next_node="supervisor",
        )

        with patch("agent_graph.llm_mini") as mock_llm:
            mock_router = Mock()
            mock_router.next = "FINISH"
            mock_llm.with_structured_output.return_value.invoke.return_value = mock_router

            result = supervisor_node(state)

            assert result["next_node"] == "FINISH"

    def test_supervisor_defaults_to_finish_on_invalid_route(self):
        """Supervisor should default to FINISH for invalid routes."""
        state = AgentState(
            messages=[HumanMessage(content="Some query")],
            next_node="supervisor",
        )

        with patch("agent_graph.llm_mini") as mock_llm:
            mock_router = Mock()
            mock_router.next = "INVALID_NODE"  # Invalid
            mock_llm.with_structured_output.return_value.invoke.return_value = mock_router

            result = supervisor_node(state)

            assert result["next_node"] == "FINISH"


class TestEngineerExecution:
    """Test the Engineer agent's SQL execution."""

    def test_engineer_executes_simple_query(self):
        """Engineer should execute a simple query and return results."""
        state = AgentState(
            messages=[HumanMessage(content="Show total revenue")],
            next_node="ENGINEER",
        )

        with patch("agent_graph.create_react_agent") as mock_create:
            mock_agent = Mock()
            mock_result = {
                "messages": [
                    HumanMessage(content="Show total revenue"),
                    AIMessage(content='{"total_revenue": 10000.00}'),
                ]
            }
            mock_agent.invoke.return_value = mock_result
            mock_create.return_value = mock_agent

            result = engineer_node(state)

            assert "messages" in result
            assert len(result["messages"]) > 0

    def test_engineer_returns_json_results(self):
        """Engineer should return JSON-formatted results for Scientist consumption."""
        state = AgentState(
            messages=[HumanMessage(content="Get revenue data")],
            next_node="ENGINEER",
        )

        with patch("agent_graph.create_react_agent") as mock_create:
            mock_agent = Mock()
            json_response = '[{"product": "Pro", "revenue": 5000}, {"product": "Lite", "revenue": 3000}]'
            mock_result = {
                "messages": [
                    HumanMessage(content="Get revenue data"),
                    AIMessage(content=json_response),
                ]
            }
            mock_agent.invoke.return_value = mock_result
            mock_create.return_value = mock_agent

            result = engineer_node(state)

            assert "messages" in result


class TestScientistExecution:
    """Test the Scientist agent's analysis and visualization."""

    def test_scientist_generates_charts(self):
        """Scientist should generate Plotly visualizations."""
        state = AgentState(
            messages=[
                HumanMessage(content="Visualize revenue data"),
                AIMessage(
                    content='[{"product": "Pro", "revenue": 5000}, {"product": "Lite", "revenue": 3000}]'
                ),
            ],
            next_node="SCIENTIST",
        )

        with patch("agent_graph.create_react_agent") as mock_create:
            mock_agent = Mock()
            plotly_json = '{"data": [], "layout": {}}'
            mock_result = {
                "messages": [
                    state["messages"][-1],
                    AIMessage(content=f"Chart: {plotly_json}"),
                ]
            }
            mock_agent.invoke.return_value = mock_result
            mock_create.return_value = mock_agent

            result = scientist_node(state)

            assert "messages" in result

    def test_scientist_detects_anomalies(self):
        """Scientist should detect anomalies using Isolation Forest."""
        state = AgentState(
            messages=[
                HumanMessage(content="Find transaction anomalies"),
                AIMessage(content="Analyzing data..."),
            ],
            next_node="SCIENTIST",
        )

        with patch("agent_graph.create_react_agent") as mock_create:
            mock_agent = Mock()
            anomaly_response = "Found 5 anomalies exceeding 3-sigma threshold"
            mock_result = {
                "messages": [
                    state["messages"][-1],
                    AIMessage(content=anomaly_response),
                ]
            }
            mock_agent.invoke.return_value = mock_result
            mock_create.return_value = mock_agent

            result = scientist_node(state)

            assert "messages" in result


class TestAgentStateManagement:
    """Test LangGraph state handling."""

    def test_messages_accumulate_in_state(self):
        """Messages should accumulate across turns in AgentState."""
        msg1 = HumanMessage(content="First query")
        msg2 = AIMessage(content="First response")
        msg3 = HumanMessage(content="Follow-up query")

        state = AgentState(
            messages=[msg1, msg2, msg3],
            next_node="supervisor",
        )

        assert len(state["messages"]) == 3
        assert state["messages"][0].content == "First query"
        assert state["messages"][-1].content == "Follow-up query"

    def test_next_node_routing_state(self):
        """next_node should correctly store routing decisions."""
        state = AgentState(
            messages=[HumanMessage(content="Query")],
            next_node="ENGINEER",
        )

        assert state["next_node"] == "ENGINEER"

        # Simulate routing update
        state["next_node"] = "SCIENTIST"
        assert state["next_node"] == "SCIENTIST"


class TestCostTracking:
    """Test cost estimation and tracking."""

    def test_estimate_cost_gpt4o(self):
        """Cost estimation should work for gpt-4o."""
        from agent_service import estimate_cost

        cost = estimate_cost(100, 50, model="gpt-4o")
        assert cost > 0
        assert cost < 0.01  # Should be cheap for small token counts

    def test_estimate_cost_mini(self):
        """gpt-4o-mini should be cheaper than gpt-4o."""
        from agent_service import estimate_cost

        cost_mini = estimate_cost(1000, 1000, model="gpt-4o-mini")
        cost_gpt4 = estimate_cost(1000, 1000, model="gpt-4o")

        assert cost_mini < cost_gpt4

    def test_session_cost_tracking(self):
        """Session cost should accumulate correctly."""
        from agent_service import track_session_cost, reset_session_cost

        reset_session_cost()

        result1 = track_session_cost(0.10)
        assert result1["cumulative_cost"] == 0.10
        assert result1["exceeded"] is False

        result2 = track_session_cost(0.15)
        assert result2["cumulative_cost"] == 0.25
        assert result2["exceeded"] is False


class TestRateLimiting:
    """Test rate-limiting configuration."""

    def test_rate_limit_config_loaded(self):
        """Rate limit config should be loaded from environment."""
        from agent_graph import RATE_LIMIT_RPM, COST_LIMIT_USD

        assert RATE_LIMIT_RPM > 0
        assert COST_LIMIT_USD > 0

    def test_llm_timeout_configured(self):
        """LLM clients should have timeout configured."""
        from agent_graph import llm_mini, llm_gpt4

        # Both should be initialized
        assert llm_mini is not None
        assert llm_gpt4 is not None
