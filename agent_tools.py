import json
import duckdb
import pandas as pd
import plotly.express as px
import plotly.io as pio
from langchain_core.tools import tool
from langchain_experimental.utilities import PythonREPL

# USE RELATIVE PATHS FOR GITHUB
DB_PATH = "datalens_lakehouse.db"

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

            return "DATABASE SCHEMA:\n" + "\n".join([f"- {t}: {', '.join(c)}" for t, c in schema_map.items()])
    except Exception as e:
        return f"Schema Error: {e}"

@tool
def execute_duckdb_query(query: str):
    """Executes SQL and prevents context window flooding. Always use CAST(date AS DATE) for monthly trends."""
    try:
        import duckdb
        clean_query = query.strip()

        # THE FIX: Safely peel off triple quotes first, then single quotes
        if clean_query.startswith('"""') and clean_query.endswith('"""'):
            clean_query = clean_query[3:-3]
        elif clean_query.startswith("'''") and clean_query.endswith("'''"):
            clean_query = clean_query[3:-3]
        elif clean_query.startswith('"') and clean_query.endswith('"'):
            clean_query = clean_query[1:-1]
        elif clean_query.startswith("'") and clean_query.endswith("'"):
            clean_query = clean_query[1:-1]

        # Clean up any leftover whitespace after peeling quotes
        clean_query = clean_query.strip()

        with duckdb.connect("datalens_lakehouse.db") as con:
            res = con.execute(clean_query).fetchmany(101)

            if len(res) > 100:
                return "Error: Query returned > 100 rows. Please use LIMIT or aggregate your data (SUM, AVG) to avoid flooding the context."

            columns = [desc[0] for desc in con.description]
            results = [dict(zip(columns, row)) for row in res]
            return json.dumps(results, default=str)
    except Exception as e:
        return f"SQL Error: {e} | Processed Query: {clean_query}"

# Persistent REPL instance for the tool
python_repl = PythonREPL()

@tool
def python_analyst(code: str):
    """Executes python code and captures stdout. Use print() to see results."""
    clean_code = code.strip()

    # THE FIX: Safely un-quote and unescape newlines for execution
    if clean_code.startswith('"') and clean_code.endswith('"'):
        clean_code = clean_code[1:-1].replace('\\n', '\n')
    elif clean_code.startswith("'") and clean_code.endswith("'"):
        clean_code = clean_code[1:-1].replace('\\n', '\n')

    result = python_repl.run(clean_code)
    return result if result.strip() else "Success (but no output was printed. Use print() to see results)."

@tool
def generate_chart(data_json: str, x_col: str, y_col: list[str] | str, chart_type: str = "bar", title: str = "Chart") -> str:
    """
    Generates a Plotly chart configuration.
    Pass data_json as a stringified list of dicts.
    """
    try:
        # THE FIX: Clean input string before JSON loading
        clean_data = data_json.strip()
        if clean_data.startswith('"') and clean_data.endswith('"'):
            clean_data = clean_data[1:-1]

        data = json.loads(clean_data)
        df = pd.DataFrame(data)

        if chart_type.lower() == "bar":
            fig = px.bar(df, x=x_col, y=y_col, title=title, barmode='group')
        elif chart_type.lower() == "pie":
            y = y_col[0] if isinstance(y_col, list) else y_col
            fig = px.pie(df, names=x_col, values=y, title=title)
        else:
            fig = px.line(df, x=x_col, y=y_col, title=title)

        return pio.to_json(fig)
    except Exception as e:
        return f"Chart Error: {e}"

# Final export for the agent
tools = [execute_duckdb_query, python_analyst, generate_chart]
