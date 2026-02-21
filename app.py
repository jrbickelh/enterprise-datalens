import os
import re
import json
import ast

# 1. SILENCE TELEMETRY IMMEDIATELY (Must happen before Langchain imports)
os.environ["LANGCHAIN_TRACING_V2"] = "false"

import streamlit as st
import plotly.io as pio
from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from agent_tools import tools, get_db_schema_string

# 2. LOAD ENVIRONMENT
load_dotenv()

st.set_page_config(page_title="DataLens AI", page_icon="🧬", layout="wide")

# 3. INITIALIZE AZURE ENGINE
llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
    temperature=0
)

tool_map = {tool.name: tool for tool in tools}

# 4. SIDEBAR AESTHETICS & CONTROLS
with st.sidebar:
    st.title("🧬 DataLens")
    st.subheader("The ReAct LakeHouse Agent")
    st.divider()

    if st.button("🗑️ Clear Conversation", use_container_width=True):
        st.session_state.messages = []
        st.rerun()

    st.divider()
    with st.expander("📊 Database Schema"):
        st.code(get_db_schema_string(), language="sql")

    st.markdown("### 💡 Example Prompts")
    st.caption("• 'What are our current stock levels?'")
    st.caption("• 'Calculate revenue per product and show a bar chart.'")
    st.caption("• 'Forecast next month sales with a 15% increase.'")

# 5. THE ORCHESTRATOR
def run_streamlit_agent(question):
    db_schema = get_db_schema_string()

    system_prompt = f"""You are a Senior Data Orchestrator.
    You manage a local DuckDB lakehouse and a Python sandbox.

    {db_schema}

    DUCKDB SYNTAX TIPS:
    - Case Insensitivity: ALWAYS use ILIKE or LOWER() for string filtering (e.g., WHERE LOWER(status) = 'successful').
    - Monthly Aggregation: Use DATE_TRUNC('month', column_name) to group by month.
    - Date Casting: Use CAST(column_name AS DATE) for daily groupings.
    - Nulls: Use COALESCE(SUM(amount), 0) to handle missing values.

    GUIDELINES:
    1. Query Data: Use 'execute_duckdb_query' with raw SQL.
    2. Math: Use 'python_analyst' with raw Python code (use print()).
    3. Visualize: Use 'generate_chart'. You MUST pass a SINGLE JSON dictionary:
       {{"data_json": "[...]", "x_col": "col", "y_col": "col", "chart_type": "bar", "title": "Chart"}}

    FORMAT:
    Thought: reasoning
    Action: tool_name
    Action Input: tool_input
    Observation: (result)
    ...
    Final Answer: conclusion
    """

    current_prompt = system_prompt + f"\n\nQuestion: {question}"
    steps_log = []
    figures_log = []

    with st.status("🛸 Orchestrating Analysis...", expanded=True) as status:
        for step_idx in range(10):
            response = llm.invoke(current_prompt, stop=["\nObservation:", "Observation:"]).content
            current_prompt += response

            # A. EXPOSE THOUGHTS
            thought_match = re.search(r"Thought: (.*?)(?:\nAction:|\Z)", response, re.S)
            if thought_match:
                thought_text = thought_match.group(1).strip()
                st.info(f"🧠 **Thought:** {thought_text}")
                steps_log.append({"type": "thought", "text": thought_text})

            if "Final Answer:" in response:
                final_text = response.split("Final Answer:")[-1].strip()
                status.update(label="✅ Analysis Complete", state="complete", expanded=False)
                return {"final_answer": final_text, "steps": steps_log, "figures": figures_log}

            # B. PARSE ACTION
            action_match = re.search(r"Action: (.*?)(?:\n|$)", response)
            input_match = re.search(r"Action Input:\s*(.*)", response, re.S)

            if action_match and input_match:
                raw_action = action_match.group(1).strip().strip("`").strip("'")
                action_input = input_match.group(1).strip()

                # PEP-8 Compliant Fuzzy Matcher
                if "sql" in raw_action.lower():
                    action = "execute_duckdb_query"
                elif "python" in raw_action.lower():
                    action = "python_analyst"
                elif "chart" in raw_action.lower():
                    action = "generate_chart"
                else:
                    action = raw_action

                st.write(f"🛠️ **Executing:** `{action}`")

                clean_input = action_input.replace("```json", "").replace("```python", "").replace("```sql", "").replace("```", "").strip()

                # Quick polish for the UI renderer
                display_input = clean_input.strip('"').strip("'").replace("; ", "\n")

                with st.expander("View Code Execution", expanded=False):
                    st.code(display_input, language="python" if "python" in action else "sql")

                steps_log.append({"type": "action", "name": action, "input": clean_input})

                if action in tool_map:
                    try:
                        # Clean nested Exception handling for tool parsing
                        if action == "generate_chart":
                            try:
                                final_input = json.loads(clean_input)
                            except Exception:
                                try:
                                    final_input = ast.literal_eval(clean_input)
                                except Exception:
                                    final_input = clean_input

                            # Ensure data_json is a string if parsed as a list
                            if isinstance(final_input, dict) and isinstance(final_input.get("data_json"), list):
                                final_input["data_json"] = json.dumps(final_input["data_json"])
                        else:
                            final_input = clean_input

                        observation = str(tool_map[action].invoke(final_input))

                        if action == "generate_chart" and "Error" not in observation:
                            figures_log.append(observation)
                            observation = "Chart generated successfully."

                        st.success("👀 **Observation:**")
                        st.code(observation, language="json")
                        steps_log.append({"type": "observation", "text": observation})
                        current_prompt += f"\nObservation: {observation}\n"

                    except Exception as e:
                        obs_error = f"Execution Error: {e}"
                        st.error(obs_error)
                        steps_log.append({"type": "error", "text": obs_error})
                        current_prompt += f"\nObservation: {obs_error}\n"
            else:
                current_prompt += "\nObservation: Format error. Use Thought/Action/Action Input.\n"

    return {"final_answer": "Timeout reached.", "steps": steps_log, "figures": figures_log}

# 6. PERSISTENT HISTORY DISPLAY
if "messages" not in st.session_state:
    st.session_state.messages = []



for msg_idx, message in enumerate(st.session_state.messages):
    with st.chat_message(message["role"]):
        if message["role"] == "assistant" and "steps" in message:
            with st.expander("🛸 View Analysis Logs", expanded=False):
                for step in message["steps"]:
                    if step["type"] == "thought":
                        st.info(f"🧠 **Thought:** {step['text']}")
                    elif step["type"] == "action":
                        st.markdown(f"**Action:** `{step['name']}`")
                        st.code(step['input'], language="python" if "python" in step['name'] else "sql")
                    elif step["type"] == "observation":
                        st.success("👀 **Observation:**")
                        st.code(step['text'], language="json")
                    elif step["type"] == "error":
                        st.error(step['text'])

        st.markdown(message["content"])

        if "figures" in message:
            for fig_idx, fig_json in enumerate(message["figures"]):
                st.plotly_chart(pio.from_json(fig_json), width="stretch", key=f"hist_{msg_idx}_{fig_idx}")

# 7. NEW INPUT HANDLING
if user_input := st.chat_input("Query transactions, inventory, or forecasts..."):
    st.session_state.messages.append({"role": "user", "content": user_input})
    with st.chat_message("user"):
        st.markdown(user_input)

    with st.chat_message("assistant"):
        result = run_streamlit_agent(user_input)
        st.markdown(f"### Result\n{result['final_answer']}")

        msg_count = len(st.session_state.messages)
        for f_idx, f_json in enumerate(result['figures']):
            st.plotly_chart(pio.from_json(f_json), width="stretch", key=f"live_{msg_count}_{f_idx}")

        st.session_state.messages.append({
            "role": "assistant",
            "content": result['final_answer'],
            "steps": result['steps'],
            "figures": result['figures']
        })
