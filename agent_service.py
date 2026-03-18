import os

# Disable LangSmith telemetry
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = ""

import re
import json
from pydantic import BaseModel, Field
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from langchain_openai import AzureChatOpenAI

# Import both graphs
from agent_graph import graph_auto, graph_hitl, COST_LIMIT_USD

load_dotenv()

# Cost tracking per session
_session_cost = 0.0
_session_token_count = 0


class AuditScore(BaseModel):
    """Structured audit evaluation output."""
    groundedness: float = Field(..., ge=0.0, le=1.0, description="Groundedness score (0-1)")
    completeness: float = Field(..., ge=0.0, le=1.0, description="Completeness score (0-1)")
    reasoning: str = Field(..., description="Explanation of the evaluation")


# Initialize Langfuse callback if enabled
langfuse_handler = None
if os.getenv("LANGFUSE_ENABLED", "false").lower() == "true":
    try:
        from langfuse.langchain import CallbackHandler
        langfuse_handler = CallbackHandler()
    except ImportError as e:
        print(f"⚠️  Langfuse integration failed: {e}")
        langfuse_handler = None

llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
    temperature=0,
)


def estimate_cost(input_tokens: int, output_tokens: int, model: str = "gpt-4o") -> float:
    """
    Estimate cost based on token usage.
    Uses approximate Azure OpenAI pricing (as of March 2026).
    """
    pricing = {
        "gpt-4o": {"input": 0.000005, "output": 0.000015},  # $5/$15 per 1M
        "gpt-4o-mini": {"input": 0.00000015, "output": 0.0000006},  # $0.15/$0.6 per 1M
        "text-embedding-3-small": {"input": 0.00000002, "output": 0},  # $0.02 per 1M
    }
    rates = pricing.get(model, pricing["gpt-4o"])
    return (input_tokens * rates["input"]) + (output_tokens * rates["output"])


def track_session_cost(cost: float) -> dict:
    """
    Track cumulative cost and check against budget limit.
    """
    global _session_cost
    _session_cost += cost

    exceeded = _session_cost > COST_LIMIT_USD
    if exceeded:
        print(f"⚠️  Cost limit exceeded: ${_session_cost:.2f} > ${COST_LIMIT_USD:.2f}")

    return {
        "cost": cost,
        "cumulative_cost": _session_cost,
        "limit_usd": COST_LIMIT_USD,
        "exceeded": exceeded,
    }


def reset_session_cost():
    """Reset cost tracker for a new session."""
    global _session_cost
    _session_cost = 0.0


def calculate_confidence_score(question: str, history: str, final_answer: str) -> dict:
    """Evaluate agent output using structured LLM response."""
    judge_prompt = f"""You are an LLMOps Evaluation Judge.
Evaluate the agent's Final Answer based strictly on the retrieved data (History).

USER QUESTION: {question}
AGENT HISTORY: {history}
AGENT FINAL ANSWER: {final_answer}

Score from 0.0 to 1.0:
- Groundedness: Is the answer grounded in the data history?
- Completeness: Does it fully address the user's question?"""

    try:
        scorer = llm.with_structured_output(AuditScore)
        result = scorer.invoke(judge_prompt)
        return {
            "groundedness": result.groundedness,
            "completeness": result.completeness,
            "reasoning": result.reasoning,
        }
    except Exception:
        return {
            "groundedness": 0.0,
            "completeness": 0.0,
            "reasoning": "Evaluation failed (LLM unavailable).",
        }


def stream_datalens_query(
    question: str | None, thread_id: str = "default_session", hitl_enabled: bool = True
):
    active_graph = graph_hitl if hitl_enabled else graph_auto
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 50}

    # Add Langfuse callback if enabled
    if langfuse_handler:
        config["callbacks"] = [langfuse_handler]

    input_data = {"messages": [HumanMessage(content=question)]} if question else None

    execution_history = ""
    final_output = ""

    # THE FIX: We deleted num_processed entirely.

    for output in active_graph.stream(input_data, config=config, stream_mode="updates"):
        for node_name, node_state in output.items():
            if node_name == "supervisor":
                yield {
                    "type": "routing",
                    "route": node_state.get("next_node", "FINISH"),
                }
                continue

            yield {"type": "node_start", "node": node_name}

            if "messages" in node_state:
                # THE FIX: Stop slicing the messages! Take them exactly as LangGraph yields them.
                new_msgs = node_state["messages"]

                for msg in new_msgs:
                    if isinstance(msg, AIMessage):
                        if msg.content:
                            yield {"type": "thought", "text": msg.content}

                        if hasattr(msg, "tool_calls") and msg.tool_calls:
                            for tc in msg.tool_calls:
                                t_args = tc["args"]
                                code_str = t_args.get(
                                    "query",
                                    t_args.get(
                                        "code",
                                        t_args.get("data_json", json.dumps(t_args)),
                                    ),
                                )
                                lang = (
                                    "sql"
                                    if "sql" in tc["name"] or "duckdb" in tc["name"]
                                    else "python"
                                )
                                yield {
                                    "type": "action",
                                    "name": tc["name"],
                                    "input": code_str,
                                    "lang": lang,
                                }

                    elif isinstance(msg, ToolMessage):
                        obs = msg.content
                        if getattr(msg, "name", "") == "generate_chart" or (
                            '"layout"' in obs and '"data"' in obs
                        ):
                            try:
                                json.loads(obs)
                                yield {"type": "chart", "json": obs}
                                obs = "✅ Interactive Chart successfully generated and captured by the UI Engine."
                            except Exception:
                                pass
                        yield {"type": "observation", "text": obs}

                # Just grab the last content string for the final output
                if new_msgs:
                    final_output = new_msgs[-1].content
                execution_history += f"\n[{node_name.upper()}]: {final_output}"

    # --- HITL CHECK ---
    post_state = active_graph.get_state(config)
    if post_state.next:
        yield {"type": "interrupt", "node": post_state.next[0]}
        return

    # --- FINAL AUDIT ---
    final_output = re.sub(r"\{.*?\"layout\".*?\}", "", final_output, flags=re.DOTALL)
    final_output = final_output.replace("<chart>", "").replace("</chart>", "").strip()

    yield {"type": "audit_start"}
    metrics = calculate_confidence_score(
        question if question else "Resumed execution.", execution_history, final_output
    )
    yield {"type": "final", "text": final_output, "metrics": metrics}


def run_agent(query: str, hitl: bool = False, verbose: bool = False) -> dict:
    """
    Non-streaming agent execution for CLI/batch use.

    Args:
        query: User's natural language query
        hitl: Use Human-in-the-Loop mode
        verbose: Print debug info

    Returns:
        dict with response, messages, cost, latency
    """
    reset_session_cost()
    final_response = ""
    all_messages = []

    for event in stream_datalens_query(query, hitl_enabled=hitl):
        if event["type"] == "final":
            final_response = event.get("text", "")
        if verbose and event["type"] in ["thought", "action"]:
            print(f"[{event['type'].upper()}] {event.get('text', event.get('input', ''))[:100]}")

    return {
        "response": final_response,
        "messages": all_messages,
        "cost": _session_cost,
    }
