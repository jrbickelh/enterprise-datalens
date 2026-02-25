import json
import duckdb
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.io as pio
from langchain_core.tools import tool
from langchain_experimental.utilities import PythonREPL
from sklearn.linear_model import LinearRegression
import os
from langchain_chroma import Chroma
from langchain_openai import AzureOpenAIEmbeddings

# USE RELATIVE PATHS FOR GITHUB
DB_PATH = os.path.join(os.path.dirname(__file__), "datalens_lakehouse.db")

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
    """Executes SQL and prevents context window flooding. Always use CAST(date AS DATE) for monthly trends."""
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


# Persistent REPL instance for the tool
python_repl = PythonREPL()


@tool
def python_analyst(code: str):
    """Executes python code and captures stdout. Use print() to see results."""
    clean_code = code.strip()

    # THE FIX: Safely un-quote and unescape newlines for execution
    if clean_code.startswith('"') and clean_code.endswith('"'):
        clean_code = clean_code[1:-1].replace("\\n", "\n")
    elif clean_code.startswith("'") and clean_code.endswith("'"):
        clean_code = clean_code[1:-1].replace("\\n", "\n")

    result = python_repl.run(clean_code)
    return (
        result
        if result.strip()
        else "Success (but no output was printed. Use print() to see results)."
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
        df = pd.read_json(data_json)
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
    """Search the vector database for 'Golden SQL Queries' matching the user's request.
    Use this tool FIRST if you are unsure about exact SQL syntax or table schemas."""

    embeddings = AzureOpenAIEmbeddings(
        azure_deployment=os.getenv("AZURE_DEPLOYMENT_NAME_EMBEDDINGS"),
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version=os.getenv("AZURE_API_VERSION"),
    )

    # Connect to the local database we just built
    vectorstore = Chroma(persist_directory="./chroma_db", embedding_function=embeddings)

    # Retrieve the top 2 most relevant queries
    results = vectorstore.similarity_search(search_term, k=2)

    if not results:
        return (
            "Observation: No matching golden queries found. Proceed with standard SQL."
        )

    formatted_results = "\n".join(
        [f"EXAMPLE {i + 1}: {res.page_content}" for i, res in enumerate(results)]
    )
    return f"Observation: Found verified SQL patterns:\n{formatted_results}"


# Final export for the agent
# Updated Tool List
tools = [
    execute_duckdb_query,
    python_analyst,
    generate_chart,
    detect_anomalies,
    forecast_data,
]

engineer_tools = [execute_duckdb_query, search_golden_queries]
scientist_tools = [generate_chart, detect_anomalies, forecast_data]
