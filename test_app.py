import sys
import os
import warnings
from agent_service import stream_datalens_query
from agent_graph import graph_auto, graph_hitl

# Silence telemetry and warnings
os.environ["LANGCHAIN_TRACING_V2"] = "false"
warnings.filterwarnings("ignore", message=".*Pydantic serializer warnings.*")

# --- 🧪 PYTEST SUITE (Automated) ---

def test_engine_initialization():
    """Verify that both graph modes are correctly compiled."""
    assert graph_auto is not None
    assert graph_hitl is not None

def test_supervisor_routing_logic():
    """Verify the supervisor can handle a basic routing request."""
    thread_id = "test_routing_session"
    events = list(stream_datalens_query(
        "Show me total sales",
        thread_id=thread_id,
        hitl_enabled=False
    ))

    # Check if a routing event occurred
    routes = [e["route"] for e in events if e["type"] == "routing"]
    assert len(routes) > 0
    assert "ENGINEER" in routes or "FINISH" in routes

def test_db_schema_accessibility():
    """Ensure the engine can access the schema via the toolset."""
    from agent_tools import get_db_schema_string
    schema = get_db_schema_string()
    assert "transactions" in schema.lower()
    assert "amount" in schema.lower()

# --- 🧬 HEADLESS TESTER (Manual CLI) ---

def run_headless_tester(hitl_enabled=False):
    mode = "Safe Mode / HITL" if hitl_enabled else "Autonomous Mode"
    print(f"\n--- 🧬 DataLens Headless Tester ({mode}) ---")

    thread_id = "cli_test_session"
    question = input("🤖 Query: ")
    if not question.strip():
        return

    active_graph = graph_hitl if hitl_enabled else graph_auto

    def process_stream(query_text):
        last_route = ""
        for event in stream_datalens_query(
            query_text, thread_id=thread_id, hitl_enabled=hitl_enabled
        ):
            if event["type"] == "routing" and event["route"] != last_route:
                print(f"📍 Supervisor routing to: {event['route']}")
                last_route = event["route"]
            elif event["type"] == "node_start" and event["node"] != "__interrupt__":
                print(f"\n⚡ {event['node'].upper()} ACTIVE")
            elif event["type"] == "thought":
                print(f"🧠 Thought: {event['text']}")
            elif event["type"] == "action":
                print(f"🛠️ Tool: {event['name']} | Input: {event['input'][:100]}...")
            elif event["type"] == "chart":
                print("📊 [CHART JSON INTERCEPTED]")
            elif event["type"] == "final":
                print(f"\n✅ FINAL ANSWER:\n{event['text']}")
                m = event.get("metrics", {})
                print(f"🛡️ AUDIT: Groundedness: {m.get('groundedness', 0)*100}%")

    process_stream(question)

    if hitl_enabled:
        while True:
            state = active_graph.get_state({"configurable": {"thread_id": thread_id}})
            if state.next:
                choice = input(f"\n👉 Approve {state.next[0].upper()}? (y/n): ").lower()
                if choice == "y":
                    process_stream(None)
                else:
                    break
            else:
                break

if __name__ == "__main__":
    # If run directly, launch the CLI tool
    hitl = "--hitl" in sys.argv
    run_headless_tester(hitl_enabled=hitl)
