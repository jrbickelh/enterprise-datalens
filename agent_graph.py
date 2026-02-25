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
from langchain_core.prompts import ChatPromptTemplate, MessagesPlaceholder
load_dotenv()


# 1. THE SHARED STATE
class AgentState(TypedDict):
    """The shared memory of the team."""

    messages: Annotated[Sequence[BaseMessage], add_messages]
    next_node: str  # Dictates the 'Route'


# 2. INITIALIZE LLM
# TIER 1: The Supervisor (Fast, Cheap)
llm_mini = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_MINI"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION"),
    temperature=0,
)

# TIER 2: The Workers (High-Reasoning, Powerful)
llm_gpt4 = AzureChatOpenAI(
    azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_GPT4"),
    azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
    api_key=os.getenv("AZURE_OPENAI_API_KEY"),
    api_version=os.getenv("AZURE_API_VERSION"),
    temperature=0,
)


# 1. Define the Strict Output Structure
class Router(BaseModel):
    """Worker to route to next. If no workers are needed, route to FINISH."""

    next: str = Field(
        description="Next node to route to: 'ENGINEER', 'SCIENTIST', or 'FINISH'"
    )


# 2. Update the Supervisor Node
def supervisor_node(state: AgentState):
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

# --- Define the Engineer Node ---
GOLDEN_QUERIES = """
--- GOLDEN SQL EXAMPLES ---
Use these verified patterns when writing your queries:

1. Time-Series Aggregation:
SELECT DATE_TRUNC('month', transaction_date) as month, SUM(amount)
FROM transactions GROUP BY 1 ORDER BY 1;

2. Finding Top Performers:
SELECT product_name, SUM(amount) as total_revenue
FROM transactions GROUP BY product_name ORDER BY total_revenue DESC LIMIT 5;

3. Safe Casting for Anomalies:
SELECT * FROM transactions WHERE amount > (SELECT AVG(amount) + (3 * STDDEV(amount)) FROM transactions);

4. Preparing Data for Forecasting (Scientist):
SELECT CAST(transaction_date AS DATE) as ds, amount as y
FROM transactions WHERE region = 'EMEA' ORDER BY ds ASC;
---------------------------
"""


def engineer_node(state: AgentState):
    """The Data Engineer Worker Node - Powered by GPT-4o"""
    schema = get_db_schema_string()

    agent = create_react_agent(
        model=llm_gpt4,
        tools=engineer_tools,
        prompt=f"""You are the Data Engineer. Expert in DuckDB and SQL.

        DATABASE SCHEMA:
        {schema}

        CRITICAL RULES:
        1. BEFORE writing complex queries, use `search_golden_queries` to find verified enterprise SQL patterns.
        2. Only write queries that match the exact tables and columns in the schema.
        3. If the database returns an error, YOU MUST REWRITE THE QUERY AND TRY AGAIN using the error feedback.
        4. Provide the final retrieved data to the Supervisor as a RAW JSON ARRAY so the Scientist can easily ingest it. Do not format it as a text list.

        """,
    )
    result = agent.invoke(
        {"messages": state["messages"]},
        config={"recursion_limit": 50},  # Increased headroom
    )
    new_messages = result["messages"][len(state["messages"]) :]
    return {"messages": new_messages}


# --- Define the Scientist Node ---
def scientist_node(state: AgentState):
    """The ML and Vis Worker Node - Powered by GPT-4o"""
    schema = get_db_schema_string()

    agent = create_react_agent(
        model=llm_gpt4,  # High-reasoning model
        tools=scientist_tools,
        prompt=f"""### ROLE
            You are the Lead Data Scientist for DataLens. Your expertise lies in Statistical Analysis, Time-Series Forecasting, and Narrative Data Visualization. If you have called a tool and received a valid observation, immediately synthesize the answer. Do not call the same tool again with the same parameters.

            ### KNOWLEDGE BASE
            DATABASE SCHEMA:
            {schema}

            {GOLDEN_QUERIES}

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
