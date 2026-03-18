import os

# Disable LangSmith telemetry
os.environ["LANGCHAIN_TRACING_V2"] = "false"
os.environ["LANGCHAIN_API_KEY"] = ""

from pydantic import BaseModel, Field
from typing import Annotated, Sequence, TypedDict
from langchain_openai import AzureChatOpenAI
from langchain_core.messages import BaseMessage, SystemMessage
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from dotenv import load_dotenv
from langgraph.prebuilt import create_react_agent
from agent_tools import engineer_tools, scientist_tools, get_db_schema_string
from semantic_layer import get_cached_semantic_context
from schema_graph import get_graph_summary
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
load_dotenv()


# 1. THE SHARED STATE
class AgentState(TypedDict):
    """The shared memory of the team."""

    messages: Annotated[Sequence[BaseMessage], add_messages]
    next_node: str  # Dictates the 'Route'


# 2. INITIALIZE LLM WITH RATE LIMITING
# Rate limit config (optional, via environment variables)
RATE_LIMIT_RPM = int(os.getenv("AZURE_RATE_LIMIT_RPM", "60"))  # Requests per minute
COST_LIMIT_USD = float(os.getenv("AZURE_COST_LIMIT_USD", "10.0"))  # Per-session budget

# TIER 1: The Supervisor (Fast, Cheap)
llm_mini = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_MINI"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION"),
    temperature=0,
    timeout=30,  # Prevent hanging requests
)

# TIER 2: The Workers (High-Reasoning, Powerful)
llm_gpt4 = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_GPT4"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION"),
    temperature=0,
    timeout=30,  # Prevent hanging requests
)


# 1. Define the Strict Output Structure
class Router(BaseModel):
    """Worker to route to next. If no workers are needed, route to FINISH."""

    next: str = Field(
        description="Next node to route to: 'ENGINEER', 'SCIENTIST', or 'FINISH'"
    )


# 2. Update the Supervisor Node
def supervisor_node(state: AgentState) -> dict:
    """The Supervisor stays on the Mini model but uses strict routing logic."""
    schema = get_db_schema_string()

    # THE FIX: Isolate the user's exact latest question
    latest_user_message = (
        state["messages"][-1].content if state["messages"] else "No input"
    )

    system_instructions = f"""You are the DataLens Team Supervisor.
    Your team:
    - ENGINEER: Expert in DuckDB SQL and data extraction.
    - SCIENTIST: Expert in Anomaly Detection, Python Math, and Plotly Charts.

    DATABASE SCHEMA:
    {schema}

    CRITICAL ROUTING RULES:
    1. Look EXACTLY at the user's LATEST request below.
    2. Does the chat history already contain the EXACT answer to this specific new request? If NO, you MUST route to a worker.
    3. If the user asks for new data, a new region, a new metric, or a different time period -> Route to ENGINEER.
    4. If the user asks for anomalies, forecasting, or charts -> Route to SCIENTIST.
    5. ONLY route to FINISH if the user is just saying "thanks" or the EXACT requested data and chart for the LATEST prompt have already been generated in the previous turn.

    LATEST USER REQUEST: "{latest_user_message}"
    """

    # Pass the system prompt and the chat history
    messages = [SystemMessage(content=system_instructions)] + state["messages"]

    # THE FIX: Force strict structured output instead of regex/string parsing
    router_chain = llm_mini.with_structured_output(Router)

    try:
        route_result = router_chain.invoke(messages)
        route = route_result.next
    except Exception as e:
        print(f"Router Exception: {e}")
        route = "FINISH"

    # Failsafe: Ensure it only routes to valid nodes
    if route not in ["ENGINEER", "SCIENTIST", "FINISH"]:
        route = "FINISH"

    return {"next_node": route}


def create_worker_agent(llm, tools, system_prompt):
    """Helper to create a specialized worker with their own tools."""
    prompt = ChatPromptTemplate.from_messages(
        [
            ("system", system_prompt),
            MessagesPlaceholder(variable_name="messages"),
        ]
    )
    # We use create_react_agent to give them reasoning power
    return create_react_agent(llm, tools, prompt=prompt)

# Note: Golden queries are now centralized in seed_chroma.py
# and injected via hybrid_retriever.search_golden_queries()


def engineer_node(state: AgentState) -> dict:
    """The Data Engineer Worker Node - Powered by GPT-4o"""
    schema = get_db_schema_string()
    semantic_context = get_cached_semantic_context()
    graph_summary = get_graph_summary()

    agent = create_react_agent(
        model=llm_gpt4,
        tools=engineer_tools,
        prompt=f"""You are the Data Engineer. Expert in DuckDB and SQL.

        DATABASE SCHEMA:
        {schema}

        {graph_summary}

        {semantic_context}

        ═══════════════════════════════════════════════════════════════
        DECISION TREE: Single-Table vs Multi-Table Query
        ═══════════════════════════════════════════════════════════════

        IF user asks for a SINGLE metric from ONE table (e.g., "total revenue"):
          → Use single-table rules (below)

        IF user asks for a METRIC ACROSS RELATED TABLES (e.g., "customer lifetime value", "regional churn"):
          → Use multi-table rules (below)

        IF you're unsure:
          → Use search_golden_queries FIRST (covers both single and multi-table patterns)

        ═══════════════════════════════════════════════════════════════
        SINGLE-TABLE QUERY RULES
        ═══════════════════════════════════════════════════════════════

        1. Use `search_golden_queries` to find verified patterns FIRST
        2. Use `explore_schema` to inspect column roles (dimension/metric/identifier)
        3. Reference SEMANTIC LAYER metrics and dimensions by name
        4. If error occurs, rewrite using feedback from execute_duckdb_query

        ═══════════════════════════════════════════════════════════════
        MULTI-TABLE QUERY RULES (Required for cross-table analysis)
        ═══════════════════════════════════════════════════════════════

        STEP 1: Detect if multi-table is needed
          - Question mentions customer, region, product data → multi-table likely
          - Keywords: "by region", "by customer", "lifetime value", "churn", "segments"

        STEP 2: Get approved joins
          - Call suggest_joins(primary_table) to see valid paths
          - Example: If about customers → suggest_joins('customers')

        STEP 3: Build query following semantic layer chains
          - Use ONLY joins explicitly listed in suggest_joins output
          - Follow "RECOMMENDED MULTI-TABLE CHAINS" from suggest_joins
          - Example chain: transactions → customers → regions

        STEP 4: Join validation rules
          - ALWAYS use LEFT JOIN (preserve all fact rows)
          - Never INNER JOIN fact tables (data loss)
          - Fact table goes in FROM, dimensions in JOIN
          - Join condition must be explicit (no fuzzy matching)

        STEP 5: Execute and validate
          - Call execute_duckdb_query with your multi-table SQL
          - If validation error, use suggest_joins again to verify path
          - If data error (e.g., circular reference), try alternative chain

        ═══════════════════════════════════════════════════════════════
        MULTI-TABLE EXAMPLE WORKFLOW
        ═══════════════════════════════════════════════════════════════

        USER: "Show top 10 customers by lifetime value"

        Step 1: Recognize → multi-table (customer + transaction data)
        Step 2: suggest_joins('customers') → learn valid paths
        Step 3: Output suggests: "transactions → customers → regions"
        Step 4: Write query following this chain:

          SELECT c.customer_id, c.customer_name, c.customer_segment,
                 SUM(t.amount) as lifetime_value, COUNT(t.transaction_id) as transaction_count
          FROM customers c
          LEFT JOIN transactions t ON c.customer_id = t.customer_id
          GROUP BY c.customer_id, c.customer_name, c.customer_segment
          ORDER BY lifetime_value DESC
          LIMIT 10;

        Step 5: Call execute_duckdb_query with this SQL

        ═══════════════════════════════════════════════════════════════
        OUTPUT FORMAT
        ═══════════════════════════════════════════════════════════════

        Always return RAW JSON ARRAY, never formatted text:
        [{{'customer_id': 'CUST-10000', 'lifetime_value': 500000}}, ...]

        The Scientist will ingest this JSON for visualization and analysis.
        """,
    )
    result = agent.invoke(
        {"messages": state["messages"]},
        config={"recursion_limit": 50},  # Increased headroom
    )
    new_messages = result["messages"][len(state["messages"]) :]
    return {"messages": new_messages}


# --- Define the Scientist Node ---
def scientist_node(state: AgentState) -> dict:
    """The ML and Vis Worker Node - Powered by GPT-4o"""
    schema = get_db_schema_string()
    semantic_context = get_cached_semantic_context()

    agent = create_react_agent(
        model=llm_gpt4,  # High-reasoning model
        tools=scientist_tools,
        prompt=f"""### ROLE
            You are the Lead Data Scientist for DataLens. Your expertise lies in Statistical Analysis, Time-Series Forecasting, and Narrative Data Visualization. If you have called a tool and received a valid observation, immediately synthesize the answer. Do not call the same tool again with the same parameters.

            ### KNOWLEDGE BASE
            DATABASE SCHEMA:
            {schema}

            {semantic_context}

            ### OPERATIONAL GUIDELINES

            #### 1. ANOMALY DETECTION (Diagnostic Analytics)
            - **Tool Trigger:** Use `detect_anomalies`.
            - **Narrative:** Explain the 'WHY' by comparing outlier amounts to the Baseline Statistics (mean/std).

            #### 2. PREDICTIVE ANALYTICS (Forecasting)
            - **Tool Constraint:** You MUST use the `forecast_data` tool for all predictions. DO NOT write custom forecasting scripts using `python_analyst` (e.g., Prophet, statsmodels, ARIMA).
            - **Data Acquisition:** If a forecast is requested, you MUST have historical data. If missing, ask the ENGINEER for "historical time-series data grouped by date." Do not mock data.
            - **Data Preparation:** Format input for `forecast_data` as JSON with 'ds' (date) and 'y' (value).
            - **Interpretation:** Explain the slope/trend direction clearly.

            #### 3. VISUALIZATION STANDARDS
            - **Integration:** Use `generate_chart` for every analysis.
            - **CRITICAL:** DO NOT output raw JSON or <chart> tags. Simply state: "I have rendered a visualization below for your review."

            ### RESPONSE FORMAT
            1. **Executive Summary:** 1-2 sentence finding.
            2. **Technical Deep Dive:** Statistical breakdown or Forecast Trend analysis.
            3. **Visual Confirmation:** "Chart rendered below."
            """,
    )
    result = agent.invoke(
        {"messages": state["messages"]},
        config={"recursion_limit": 50},  # Increased headroom
    )
    new_messages = result["messages"][len(state["messages"]) :]
    return {"messages": new_messages}


workflow = StateGraph(AgentState)

# 1. Add our Nodes
workflow.add_node("supervisor", supervisor_node)
workflow.add_node("ENGINEER", engineer_node)
workflow.add_node("SCIENTIST", scientist_node)

# 2. Define the Routing Logic
workflow.set_entry_point("supervisor")

# Conditional edges: The Supervisor decides where to go
workflow.add_conditional_edges(
    "supervisor",
    lambda x: x["next_node"],
    {"ENGINEER": "ENGINEER", "SCIENTIST": "SCIENTIST", "FINISH": END},
)

# After a worker finishes, they ALWAYS go back to the Supervisor for review
workflow.add_edge("ENGINEER", "supervisor")
workflow.add_edge("SCIENTIST", "supervisor")

# 3. Compile the Graph
memory = MemorySaver()

# 1. The Autonomous Graph
graph_auto = workflow.compile(checkpointer=memory)

# 2. The HITL (Human-in-the-Loop) Graph
# (Make sure "ENGINEER" and "SCIENTIST" match the exact casing of your add_node names)
graph_hitl = workflow.compile(
    checkpointer=memory, interrupt_before=["ENGINEER", "SCIENTIST"]
)
