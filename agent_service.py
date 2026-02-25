import os

# Disable LangSmith telemetry
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = ""

import re
import json
from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage, ToolMessage
from langchain_openai import AzureChatOpenAI

# Import both graphs
from agent_graph import graph_auto, graph_hitl

load_dotenv()

llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
    temperature=0,
)


def calculate_confidence_score(question: str, history: str, final_answer: str) -> dict:
    judge_prompt = f"""You are an LLMOps Evaluation Judge.
Evaluate the agent's Final Answer based strictly on the retrieved data (History).
USER QUESTION: {question}
AGENT HISTORY: {history}
AGENT FINAL ANSWER: {final_answer}

Evaluate on two metrics (0.0 to 1.0). Return ONLY valid JSON: {{"groundedness": 1.0, "completeness": 0.9, "reasoning": "text"}}"""
    try:
        response = llm.invoke(judge_prompt).content
        return json.loads(response.replace("```json", "").replace("```", "").strip())
    except Exception:
        return {
            "groundedness": 0.0,
            "completeness": 0.0,
            "reasoning": "Evaluation failed.",
        }


def stream_datalens_query(
    question: str | None, thread_id: str = "default_session", hitl_enabled: bool = True
):
    active_graph = graph_hitl if hitl_enabled else graph_auto
    config = {"configurable": {"thread_id": thread_id}, "recursion_limit": 50}

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
