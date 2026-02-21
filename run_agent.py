import os
import re
import json

# 1. SILENCE TELEMETRY IMMEDIATELY
os.environ["LANGCHAIN_TRACING_V2"] = "false"

from dotenv import load_dotenv
from langchain_openai import AzureChatOpenAI
from agent_tools import tools, get_db_schema_string

# 2. LOAD ENVIRONMENT & INITIALIZE
load_dotenv()

llm = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION", "2024-12-01-preview"),
    temperature=0
)

# Tool Map for Orchestration
tool_map = {tool.name: tool for tool in tools}
tool_names = ", ".join(tool_map.keys())

# 3. DYNAMIC PROMPT CONTEXT
def get_system_prompt():
    schema = get_db_schema_string()

    duckdb_tips = """
    DUCKDB SYNTAX TIPS:
    - Monthly Aggregation: Use DATE_TRUNC('month', column_name) to group by month.
    - Date Casting: Use CAST(column_name AS DATE) for daily groupings.
    - Nulls: Use COALESCE(SUM(amount), 0) to handle missing values.
    """
    database_tips = """
    DATABASE TIPS:
    - The 'status' column is capitalized: Use 'Successful', not 'successful'.
    - Products include: 'DataLens Pro', 'DataLens Lite', 'LakeHouse Connector'.
    - Dates are stored as DATE objects."""

    return f"""You are a senior data science AI orchestrating a local DuckDB lakehouse.

{schema}

{duckdb_tips}

{database_tips}

TOOL GUIDELINES:
1. 'execute_duckdb_query': Pass ONLY valid, raw SQL.
2. 'python_analyst': Pass ONLY raw Python code. You MUST use print() to output the result. Do not pass JSON.
3. 'generate_chart': You MUST pass a SINGLE JSON dictionary containing exactly these keys:
   {{"data_json": "[{{\\"col1\\": \\"val1\\", \\"col2\\": 10}}]", "x_col": "col1", "y_col": "col2", "chart_type": "bar", "title": "Chart Title"}}
   Never use the key "data". It MUST be "data_json" containing a stringified list of dictionaries.

STRICT FORMAT:
Thought: your reasoning
Action: tool_name (Must be one of [{tool_names}])
Action Input: the raw string or JSON dictionary to pass to the tool
Observation: the result
... (repeat)
Final Answer: your conclusion
"""

# 4. THE REACT ORCHESTRATOR
def run_agent(question, max_steps=10):
    system_prompt = get_system_prompt()
    current_prompt = system_prompt + f"\n\nQuestion: {question}\n"

    for step in range(max_steps):
        print(f"\n--- 🧠 Agent Step {step + 1} ---")

        # Invoke LLM with stop sequences to prevent hallucinated observations
        response = llm.invoke(current_prompt, stop=["\nObservation:", "Observation:"]).content
        print(response)
        current_prompt += response

        if "Final Answer:" in response:
            print("\n✅ Agent finished successfully.")
            return

        # Parse Action and Input
        action_match = re.search(r"Action: (.*?)(?:\n|$)", response)
        input_match = re.search(r"Action Input:\s*(.*)", response, re.S)

        if action_match and input_match:
            raw_action = action_match.group(1).strip().replace("`", "").replace("'", "")
            action_input = input_match.group(1).strip()

            # --- FUZZY TOOL MATCHER (PEP-8 Compliant) ---
            if "sql" in raw_action.lower():
                action = "execute_duckdb_query"
            elif "python" in raw_action.lower():
                action = "python_analyst"
            elif "chart" in raw_action.lower():
                action = "generate_chart"
            else:
                action = raw_action

            # Sanitize stray markdown blocks
            clean_input = action_input.replace("```sql", "").replace("```python", "").replace("```json", "").replace("```", "").strip()

            if action in tool_map:
                print(f"🛠️  Executing: [{action}]")
                try:
                    # Multi-arg tool handling with clean Exception chaining
                    if action == "generate_chart":
                        import ast
                        try:
                            final_input = json.loads(clean_input)
                        except Exception:
                            try:
                                final_input = ast.literal_eval(clean_input)
                            except Exception:
                                final_input = clean_input
                    else:
                        final_input = clean_input

                    observation = str(tool_map[action].invoke(final_input))
                except Exception as e:
                    observation = f"Error executing {action}: {e}"

                print(f"👀 Observation: {observation}")
                current_prompt += f"\nObservation: {observation}\n"
            else:
                error_msg = f"Tool '{action}' not found. Use one of: {tool_names}"
                print(f"⚠️  {error_msg}")
                current_prompt += f"\nObservation: {error_msg}\n"
        else:
            warning = "Format error. Please use Thought/Action/Action Input format."
            print(f"⚠️  {warning}")
            current_prompt += f"\nObservation: {warning}\n"

    print("\n❌ Max steps reached.")

# 5. ENTRY POINT
if __name__ == "__main__":
    print("🚀 Running DataLens: The ReAct LakeHouse Agent (CLI)...")
    query = (
        """What was the total successful revenue for 'DataLens Pro' in Q4 of 2025? Then, use the python_analyst tool to calculate a 20% 'Stretch Goal' for that same period in 2026."""
    )
    run_agent(query)
