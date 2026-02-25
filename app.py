import uuid
import streamlit as st
import plotly.io as pio
from agent_tools import get_db_schema_string
from agent_service import stream_datalens_query
from agent_graph import graph_auto, graph_hitl
import warnings

# Suppress harmless Pydantic serialization warnings from LangChain structured output
warnings.filterwarnings("ignore", message=".*Pydantic serializer warnings.*")

# 1. PAGE CONFIG
st.set_page_config(page_title="DataLens AI", page_icon="🧬", layout="wide")

# --- STATE INITIALIZATION ---
if "thread_id" not in st.session_state:
    st.session_state.thread_id = str(uuid.uuid4())

# We track the currently active engine mode
if "engine_mode" not in st.session_state:
    st.session_state.engine_mode = False

# 2. SIDEBAR & HARD STOP TOGGLE
with st.sidebar:
    st.title("🧬 DataLens v6.0")
    st.subheader("Persistent Multi-Agent UI")
    st.divider()

    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.session_state.thread_id = str(uuid.uuid4())
        st.rerun()

    st.divider()
    with st.expander("📊 Database Schema"):
        st.code(get_db_schema_string(), language="sql")

    st.markdown("### ⚙️ Engine Settings")

    # Render the toggle
    hitl_toggle = st.toggle(
        "🛡️ Require Agent Approval",
        value=st.session_state.engine_mode,
        help="Pause the graph before agents execute tools.",
    )

    # THE HARD STOP FIX: If the toggle doesn't match the engine, freeze the app!
    if hitl_toggle != st.session_state.engine_mode:
        st.error(
            "⚠️ **Engine Swap Detected**\n\nChanging this setting mid-conversation causes memory corruption. You must reset the conversation to apply this change."
        )
        if st.button("🚨 Reset Memory & Apply", use_container_width=True):
            st.session_state.engine_mode = hitl_toggle
            st.session_state.messages = []
            st.session_state.thread_id = str(uuid.uuid4())
            st.rerun()
        st.stop()  # This prevents the rest of the app from running!

# 3. MESSAGE HISTORY RENDERING
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg_idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        if "steps" in message and message["steps"]:
            with st.expander("🛸 View Analysis Logs", expanded=False):
                for step in message["steps"]:
                    if step["type"] == "routing":
                        st.caption(f"📍 Supervisor routed to: {step['route']}")
                    elif step["type"] == "node_start":
                        st.markdown(f"**⚡ {step['node'].upper()} Node**")
                    elif step["type"] == "thought":
                        st.info(f"🧠 {step['text']}")
                    elif step["type"] == "action":
                        st.markdown(f"🛠️ **Tool:** `{step['name']}`")
                        st.code(step["input"], language=step.get("lang", "python"))
                    elif step["type"] == "observation":
                        st.success("👀 **Observation:**")
                        st.code(step["text"], language="json")
                    elif step["type"] == "metric":
                        m = step["data"]
                        st.divider()
                        col1, col2 = st.columns(2)
                        col1.metric(
                            "Groundedness", f"{int(m.get('groundedness', 0) * 100)}%"
                        )
                        col2.metric(
                            "Completeness", f"{int(m.get('completeness', 0) * 100)}%"
                        )
                        st.info(f"**Judge Verdict:** {m.get('reasoning')}")

        st.markdown(message["content"])

        if "figures" in message:
            for fig_idx, fig_json in enumerate(message["figures"]):
                try:
                    st.plotly_chart(
                        pio.from_json(fig_json),
                        width="stretch",
                        key=f"hist_{msg_idx}_{fig_idx}",
                    )
                except Exception as e:
                    st.error(f"Failed to render chart: {e}")


# 4. CORE EXECUTION WRAPPER
def execute_and_render_stream(query_text=None):
    current_steps = []
    figures_log = []
    final_text = "*(Execution Paused for Approval)*"
    last_route = ""

    with st.status("🛸 Team is orchestrating analysis...", expanded=True) as status:
        for event in stream_datalens_query(
            query_text,
            thread_id=st.session_state.thread_id,
            hitl_enabled=st.session_state.engine_mode,
        ):
            current_steps.append(event)

            if event["type"] == "routing":
                if event["route"] != last_route:
                    st.write(f"📍 **Supervisor routed to:** `{event['route']}`")
                    last_route = event["route"]
            elif event["type"] == "node_start":
                st.write(f"### ⚡ **{event['node'].upper()} Active**")
            elif event["type"] == "thought":
                st.info(f"🧠 **Thought:** {event['text']}")
            elif event["type"] == "action":
                st.write(f"🛠️ **Executing Tool:** `{event['name']}`")
                with st.expander("View Code Execution", expanded=False):
                    st.code(event["input"], language=event["lang"])
            elif event["type"] == "observation":
                with st.expander("👀 View Observation", expanded=False):
                    obs = event["text"]
                    if len(obs) > 1500:
                        obs = obs[:1500] + "\n... [Data Truncated]"
                    st.code(obs, language="json")
            elif event["type"] == "chart":
                figures_log.append(event["json"])
            elif event["type"] == "interrupt":
                status.update(
                    label="⚠️ Awaiting User Approval", state="error", expanded=True
                )
                return current_steps, figures_log, final_text
            elif event["type"] == "audit_start":
                st.write("⚖️ **Running Shadow Audit...**")
            elif event["type"] == "final":
                final_text = event["text"]
                current_steps.append({"type": "metric", "data": event["metrics"]})

                st.write("---")
                st.caption("🛡️ **Internal Quality Audit**")
                m = event["metrics"]
                col1, col2 = st.columns(2)
                col1.metric("Groundedness", f"{int(m.get('groundedness', 0) * 100)}%")
                col2.metric("Completeness", f"{int(m.get('completeness', 0) * 100)}%")
                st.info(f"**Judge Verdict:** {m.get('reasoning')}")

                status.update(
                    label="✅ Analysis Complete", state="complete", expanded=False
                )

    return current_steps, figures_log, final_text


# 5. CHAT INPUT & HITL LOGIC
active_graph = graph_hitl if st.session_state.engine_mode else graph_auto
config = {"configurable": {"thread_id": st.session_state.thread_id}}

current_state = active_graph.get_state(config)

if current_state.next:
    with st.chat_message("assistant"):
        target_node = current_state.next[0].upper()

        # Extract the Supervisor's reasoning from the last message in the state
        state_messages = current_state.values.get("messages", [])
        delegation_reason = "No explicit reasoning provided."

        if (
            state_messages
            and hasattr(state_messages[-1], "content")
            and state_messages[-1].content
        ):
            delegation_reason = state_messages[-1].content

        st.warning(
            f"⚠️ **Security Breakpoint:** The Supervisor is delegating execution to the `{target_node}` node."
        )

        # Display the context to the user
        st.info(f"**🧠 Task Context & Reasoning:**\n\n{delegation_reason}")
        st.write("**Do you approve this action?**")

        col1, col2 = st.columns([1, 5])

        if col1.button("✅ Approve Action"):
            st.session_state.messages.append(
                {
                    "role": "user",
                    "content": f"*(User Approved {target_node} Execution)*",
                }
            )
            steps, figs, text = execute_and_render_stream(query_text=None)

            if text != "*(Execution Paused for Approval)*":
                st.markdown(f"### Result\n{text}")
                for f_idx, f_json in enumerate(figs):
                    st.plotly_chart(
                        pio.from_json(f_json),
                        width="stretch",
                        key=f"resume_{len(st.session_state.messages)}_{f_idx}",
                    )

                st.session_state.messages.append(
                    {
                        "role": "assistant",
                        "content": text,
                        "figures": figs,
                        "steps": steps,
                    }
                )
            st.rerun()

        if col2.button("❌ Reject"):
            st.error(
                "Execution Cancelled. Please clear the conversation to reset the engine."
            )

else:
    if user_input := st.chat_input("Query transactions..."):
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)

        with st.chat_message("assistant"):
            steps, figs, text = execute_and_render_stream(query_text=user_input)

            if text != "*(Execution Paused for Approval)*":
                st.markdown(f"### Result\n{text}")
                for f_idx, f_json in enumerate(figs):
                    st.plotly_chart(
                        pio.from_json(f_json),
                        width="stretch",
                        key=f"live_{len(st.session_state.messages)}_{f_idx}",
                    )

            st.session_state.messages.append(
                {"role": "assistant", "content": text, "figures": figs, "steps": steps}
            )

            if post_run_state := active_graph.get_state(config):
                if post_run_state.next:
                    st.rerun()
