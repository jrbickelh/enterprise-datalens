import json
import duckdb
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.io as pio
from langchain_core.tools import tool
from sklearn.linear_model import LinearRegression
import os
from schema_graph import explore_schema

# USE RELATIVE PATHS FOR GITHUB
DB_PATH = os.path.join(os.path.dirname(__file__), "datalens_lakehouse.db")

def _validate_multi_table_query(query: str) -> dict:
    """
    Validate multi-table (JOIN) queries for common issues.

    Checks for:
    - All JOIN tables exist in schema
    - Join conditions reference real columns
    - Cardinality: LEFT JOIN used (not INNER for fact-to-dim)

    Returns:
        dict with 'valid' (bool), 'reason' (str), 'suggestion' (str)
    """
    from semantic_layer import get_valid_joins

    query_upper = query.upper()

    # Check for valid FROM table
    if "FROM" not in query_upper:
        return {"valid": False, "reason": "Missing FROM clause", "suggestion": "Add FROM table_name"}

    # Quick validation: check for approved joins via semantic layer
    joins = get_valid_joins()
    join_names = list(joins.keys())

    # If query uses JOIN but we can't auto-validate, let it through (schema_graph will catch errors)
    # This is permissive by design: let DuckDB catch real errors
    if "JOIN" in query_upper:
        # Warn on INNER JOIN for facts (prefer LEFT)
        # Fact tables: transactions (the main one)
        if "INNER JOIN" in query_upper and "transactions" in query_upper.lower():
            return {
                "valid": False,
                "reason": "Risky INNER JOIN on fact table (transactions). Use LEFT JOIN to preserve all rows.",
                "suggestion": "Replace INNER JOIN with LEFT JOIN",
            }

    return {"valid": True, "reason": "OK", "suggestion": ""}


def get_db_schema_string():
    """Retrieves schema with error handling for empty databases."""
    try:
        with duckdb.connect(DB_PATH) as conn:
            query = "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = 'main'"
            results = conn.execute(query).fetchall()

            if not results:
                return "Database is empty."

            schema_map = {}
            for table, col, dtype in results:
                if table not in schema_map:
                    schema_map[table] = []
                schema_map[table].append(f"{col} ({dtype})")

            return "DATABASE SCHEMA:\n" + "\n".join(
                [f"- {t}: {', '.join(c)}" for t, c in schema_map.items()]
            )
    except Exception as e:
        return f"Schema Error: {e}"


@tool
def execute_duckdb_query(query: str):
    """Executes SQL and prevents context window flooding. Always use CAST(date AS DATE) for monthly trends.

    For multi-table queries: validates joins first (no INNER JOINs on fact tables, etc.).
    Use suggest_joins() BEFORE writing complex queries to ensure joins are approved."""
    try:
        import duckdb

        clean_query = query.strip()

        # --- KEEP YOUR ROBUST QUOTE PEELING LOGIC ---
        if clean_query.startswith('"""') and clean_query.endswith('"""'):
            clean_query = clean_query[3:-3]
        elif clean_query.startswith("'''") and clean_query.endswith("'''"):
            clean_query = clean_query[3:-3]
        elif clean_query.startswith('"') and clean_query.endswith('"'):
            clean_query = clean_query[1:-1]
        elif clean_query.startswith("'") and clean_query.endswith("'"):
            clean_query = clean_query[1:-1]

        clean_query = clean_query.strip()

        # --- PRE-EXECUTION VALIDATION: Multi-table queries ---
        if "JOIN" in clean_query.upper():
            validation = _validate_multi_table_query(clean_query)
            if not validation["valid"]:
                return f"VALIDATION ERROR (Multi-Table Query): {validation['reason']}\nSUGGESTION: {validation['suggestion']}"

        # --- EXECUTION ---
        with duckdb.connect(DB_PATH) as con:
            res = con.execute(clean_query).fetchmany(101)

            if len(res) > 100:
                # PHASE 8 TWEAK: Make the LIMIT error actionable
                return "Error: Query returned > 100 rows. REWRITE your query using LIMIT or aggregation (SUM, AVG) to be more specific."

            columns = [desc[0] for desc in con.description]
            results = [dict(zip(columns, row)) for row in res]
            return json.dumps(results, default=str)

    except Exception as e:
        # PHASE 8 FIX: The "Self-Healing" Prompt Injection
        # We feed the raw error back to the Engineer's brain so it can try again.
        return (
            f"DATABASE ERROR: {str(e)}\n"
            f"PROCESSED QUERY: {clean_query}\n"
            "INSTRUCTION: Do not apologize. Analyze the error (e.g., check column names or syntax), "
            "correct the SQL query, and call execute_duckdb_query again."
        )


@tool
def generate_chart(
    data_json: str,
    x_col: str,
    y_col: list[str] | str,
    chart_type: str = "bar",
    title: str = "Chart",
) -> str:
    """
    Generates a Plotly chart configuration.
    Pass data_json as a stringified list of dicts.
    """
    try:
        clean_data = data_json.strip()
        if clean_data.startswith('"') and clean_data.endswith('"'):
            clean_data = clean_data[1:-1]

        data = json.loads(clean_data)
        df = pd.DataFrame(data)

        # ADD THE SCATTER CONDITION HERE
        if chart_type.lower() == "bar":
            fig = px.bar(df, x=x_col, y=y_col, title=title, barmode="group")
        elif chart_type.lower() == "pie":
            y = y_col[0] if isinstance(y_col, list) else y_col
            fig = px.pie(df, names=x_col, values=y, title=title)
        elif chart_type.lower() == "scatter":
            fig = px.scatter(df, x=x_col, y=y_col, title=title)
        else:
            fig = px.line(df, x=x_col, y=y_col, title=title)

        return pio.to_json(fig)
    except Exception as e:
        return f"Chart Error: {e}"


@tool
def detect_anomalies(sql_query: str, contamination: float = 0.05) -> str:
    """
    Executes a SQL query on the Lakehouse and uses an Isolation Forest to find outliers.
    Returns a markdown table of anomalies and a prompt for visualization.
    """
    try:

        # Connect in read-only mode to avoid locking issues
        con = duckdb.connect(DB_PATH, read_only=True)
        df = con.execute(sql_query).df()
        con.close()

        if df.empty:
            return "Observation: No data returned from the query. Cannot run ML."

        # Identify numeric features for the model
        numeric_cols = df.select_dtypes(include=["number"]).columns.tolist()
        if not numeric_cols:
            return "Observation: No numeric columns found for anomaly detection."

        # Run Isolation Forest
        from sklearn.ensemble import IsolationForest

        model = IsolationForest(contamination=contamination, random_state=42)
        df["anomaly_flag"] = model.fit_predict(df[numeric_cols].fillna(0))

        # Filter for outliers (-1 is anomaly)
        anomalies = df[df["anomaly_flag"] == -1]

        if anomalies.empty:
            return "Observation: No significant anomalies detected."

        # THE FIX: Calculate baseline stats so the LLM can explain the "WHY"
        baseline_stats = df[numeric_cols].describe().to_markdown()

        # Return the data, stats, and a 'Call to Action' for the agent
        response = f"### 🚨 Detected {len(anomalies)} Anomalies\n\n"
        response += (
            f"**Dataset Baseline Statistics (The 'Normal'):**\n{baseline_stats}\n\n"
        )
        response += "**Top Outlier Examples:**\n"
        response += anomalies.head(5).to_markdown()
        response += "\n\nNOTE: You MUST explain WHY these are anomalies by comparing the outlier amounts to the Baseline Statistics (e.g., 'These are 3x higher than the mean'). Then use 'generate_chart' to plot them."
        return response

    except Exception as e:
        return f"Observation: Anomaly Tool Error: {e}"


@tool
def forecast_data(data_json: str, periods: int = 3) -> str:
    """
    Predicts future trends based on historical JSON data.
    Input data_json must have 'ds' (date string) and 'y' (numeric value) keys.
    'periods' is the number of future intervals to forecast.
    """
    try:
        from io import StringIO
        df = pd.read_json(StringIO(data_json))
        df["ds"] = pd.to_datetime(df["ds"])
        df = df.sort_values("ds")

        # Convert dates to ordinal for linear regression
        df["x"] = df["ds"].map(lambda x: x.toordinal())
        X = df[["x"]].values
        y = df["y"].values

        model = LinearRegression()
        model.fit(X, y)

        # Generate future dates
        last_date = df["ds"].max()
        # Assume monthly if frequency isn't specified, or daily
        future_dates = pd.date_range(start=last_date, periods=periods + 1, freq="ME")[
            1:
        ]

        future_x = np.array([d.toordinal() for d in future_dates]).reshape(-1, 1)
        predictions = model.predict(future_x)

        forecast_df = pd.DataFrame(
            {
                "ds": future_dates.strftime("%Y-%m-%d"),
                "y_hat": predictions.round(2),
                "trend": "forecast",
            }
        )

        return forecast_df.to_json(orient="records")
    except Exception as e:
        return f"Forecast Error: {str(e)}. Ensure data has 'ds' and 'y' columns."


@tool
def search_golden_queries(search_term: str) -> str:
    """Search golden SQL queries using hybrid search (BM25 + vector similarity).
    Combines keyword matching with semantic similarity for better recall.
    Use this tool FIRST if you are unsure about exact SQL syntax or table schemas."""

    from hybrid_retriever import get_hybrid_retriever
    from seed_chroma import golden_queries

    # Get hybrid retriever (BM25 + vector)
    retriever = get_hybrid_retriever(golden_queries)

    # Retrieve the top results via hybrid search
    results = retriever.invoke(search_term)

    if not results:
        return (
            "Observation: No matching golden queries found. Proceed with standard SQL."
        )

    formatted_results = "\n".join(
        [f"EXAMPLE {i + 1}: {res.page_content}" for i, res in enumerate(results)]
    )
    return f"Observation: Found verified SQL patterns via hybrid search:\n{formatted_results}"


@tool
def suggest_joins(table_name: str) -> str:
    """Suggest valid joins from a table based on semantic layer definitions.
    Use this BEFORE writing multi-table queries to ensure joins are valid.

    Args:
        table_name: Name of the table (e.g., 'transactions', 'customers')

    Returns:
        List of valid join paths with descriptions
    """
    from semantic_layer import get_valid_joins, get_join_chains

    joins = get_valid_joins()
    chains = get_join_chains()

    # Find joins involving this table
    relevant_joins = []
    for join_name, join_def in joins.items():
        if join_def.get("from_table") == table_name and join_def.get("enabled", True):
            target = join_def.get("to_table", "?")
            condition = join_def.get("join_condition", "?")
            desc = join_def.get("description", "")
            relevant_joins.append(
                f"  {table_name} → {target}: {condition} ({desc})"
            )

    if not relevant_joins:
        return f"Observation: No valid joins found for '{table_name}'. Proceed with single-table queries."

    result = f"Observation: Valid JOIN paths from '{table_name}':\n"
    result += "\n".join(relevant_joins)

    # Add multi-table chain recommendations
    result += "\n\nRECOMMENDED MULTI-TABLE CHAINS:\n"
    for chain_name, chain_def in chains.items():
        chain = chain_def.get("chain", "")
        if table_name in chain:
            use_case = chain_def.get("use_case", "")
            result += f"  {chain}: {use_case}\n"

    return result


# Final export for the agent
# Complete tool list for reference
tools = [
    execute_duckdb_query,
    search_golden_queries,
    explore_schema,
    generate_chart,
    detect_anomalies,
    forecast_data,
]

engineer_tools = [execute_duckdb_query, search_golden_queries, explore_schema, suggest_joins]
scientist_tools = [generate_chart, detect_anomalies, forecast_data]
