"""
Schema Knowledge Graph for DataLens.

Builds a NetworkX directed graph from DuckDB's information_schema to provide
agents with structural awareness of the database. This eliminates join
hallucinations by giving agents a graph-traversal tool instead of forcing
them to guess table relationships.

Graph Structure:
    - Nodes: tables and columns (with metadata: type, nullable, etc.)
    - Edges: table→column ownership, column→column foreign key relationships

Usage:
    from schema_graph import get_schema_graph, get_table_context, get_valid_joins

    graph = get_schema_graph()
    context = get_table_context("transactions")
    joins = get_valid_joins("transactions")
"""

import os
import duckdb
import networkx as nx
from langchain_core.tools import tool

DB_PATH = os.path.join(os.path.dirname(__file__), "datalens_lakehouse.db")

# Module-level cache
_schema_graph = None


def build_schema_graph() -> nx.DiGraph:
    """
    Build a directed graph from DuckDB's information_schema.

    Returns:
        NetworkX DiGraph with tables, columns, and relationships as nodes/edges
    """
    G = nx.DiGraph()

    with duckdb.connect(DB_PATH, read_only=True) as conn:
        # 1. Add table and column nodes
        columns = conn.execute("""
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'main'
            ORDER BY table_name, ordinal_position
        """).fetchall()

        for table_name, column_name, data_type, is_nullable in columns:
            # Add table node (if not exists)
            if not G.has_node(table_name):
                G.add_node(table_name, node_type="table")

            # Add column node with fully qualified name
            col_id = f"{table_name}.{column_name}"
            G.add_node(col_id, node_type="column", data_type=data_type,
                       nullable=is_nullable == "YES", table=table_name)

            # Add ownership edge: table → column
            G.add_edge(table_name, col_id, edge_type="has_column")

        # 2. Detect implicit foreign key relationships via naming conventions
        # Columns ending in _id that share names across tables suggest FK relationships
        id_columns = {}
        for table_name, column_name, data_type, _ in columns:
            if column_name.endswith("_id") or column_name == "id":
                base_name = column_name.replace("_id", "") if column_name != "id" else table_name
                if base_name not in id_columns:
                    id_columns[base_name] = []
                id_columns[base_name].append((table_name, column_name))

        # Create FK edges between tables that share ID column patterns
        for base_name, col_list in id_columns.items():
            if len(col_list) > 1:
                for i, (t1, c1) in enumerate(col_list):
                    for t2, c2 in col_list[i + 1:]:
                        src = f"{t1}.{c1}"
                        dst = f"{t2}.{c2}"
                        G.add_edge(src, dst, edge_type="foreign_key",
                                   join_condition=f"{t1}.{c1} = {t2}.{c2}")
                        G.add_edge(dst, src, edge_type="foreign_key",
                                   join_condition=f"{t2}.{c2} = {t1}.{c1}")

        # 3. Add table-level statistics as node attributes
        for table_name, _, _, _ in columns:
            if G.nodes[table_name].get("row_count") is None:
                try:
                    count = conn.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    ).fetchone()[0]
                    G.nodes[table_name]["row_count"] = count
                except Exception:
                    G.nodes[table_name]["row_count"] = -1

        # 4. Detect column cardinality for dimensions vs metrics classification
        for table_name, column_name, data_type, _ in columns:
            col_id = f"{table_name}.{column_name}"
            try:
                distinct = conn.execute(
                    f"SELECT COUNT(DISTINCT {column_name}) FROM {table_name}"
                ).fetchone()[0]
                total = G.nodes[table_name].get("row_count", 1)
                cardinality_ratio = distinct / max(total, 1)

                G.nodes[col_id]["distinct_values"] = distinct
                G.nodes[col_id]["cardinality_ratio"] = round(cardinality_ratio, 4)

                # Classify: low cardinality = dimension, high = metric/identifier
                if data_type in ("DOUBLE", "FLOAT", "DECIMAL", "BIGINT", "INTEGER"):
                    G.nodes[col_id]["role"] = "metric" if cardinality_ratio > 0.5 else "dimension"
                elif cardinality_ratio < 0.01:
                    G.nodes[col_id]["role"] = "dimension"
                elif cardinality_ratio > 0.9:
                    G.nodes[col_id]["role"] = "identifier"
                else:
                    G.nodes[col_id]["role"] = "attribute"

            except Exception:
                G.nodes[col_id]["role"] = "unknown"

    return G


def get_schema_graph() -> nx.DiGraph:
    """Get cached schema graph (builds once, reuses)."""
    global _schema_graph
    if _schema_graph is None:
        _schema_graph = build_schema_graph()
    return _schema_graph


def get_table_context(table_name: str) -> str:
    """
    Get rich context about a table including columns, types, roles, and statistics.

    Args:
        table_name: Name of the table

    Returns:
        Formatted string with table context for LLM consumption
    """
    G = get_schema_graph()

    if table_name not in G:
        return f"Table '{table_name}' not found in schema graph."

    row_count = G.nodes[table_name].get("row_count", "unknown")
    context = f"TABLE: {table_name} ({row_count} rows)\n"
    context += "COLUMNS:\n"

    for _, col_id in G.edges(table_name):
        if G[table_name][col_id].get("edge_type") == "has_column":
            col_data = G.nodes[col_id]
            col_name = col_id.split(".")[-1]
            dtype = col_data.get("data_type", "?")
            role = col_data.get("role", "?")
            distinct = col_data.get("distinct_values", "?")
            context += f"  - {col_name} ({dtype}) [role={role}, distinct={distinct}]\n"

    return context


def get_valid_joins_for_table(table_name: str) -> str:
    """
    Get valid join paths from a table based on the schema graph.

    Args:
        table_name: Source table name

    Returns:
        Formatted string listing valid JOIN conditions
    """
    G = get_schema_graph()

    if table_name not in G:
        return f"Table '{table_name}' not found in schema graph."

    joins = []
    # Walk through all columns of this table
    for _, col_id in G.edges(table_name):
        if G[table_name][col_id].get("edge_type") != "has_column":
            continue
        # Check if this column has FK edges to other tables
        for _, target_col in G.edges(col_id):
            edge_data = G[col_id][target_col]
            if edge_data.get("edge_type") == "foreign_key":
                target_table = G.nodes[target_col].get("table", "?")
                join_cond = edge_data.get("join_condition", "?")
                joins.append(f"  JOIN {target_table} ON {join_cond}")

    if not joins:
        return f"Table '{table_name}' has no detected join paths (single-table queries only)."

    return f"VALID JOINS FROM {table_name}:\n" + "\n".join(joins)


def get_column_profile(table_name: str, column_name: str) -> str:
    """
    Get detailed profile of a specific column.

    Args:
        table_name: Table name
        column_name: Column name

    Returns:
        Formatted column profile
    """
    G = get_schema_graph()
    col_id = f"{table_name}.{column_name}"

    if col_id not in G:
        return f"Column '{col_id}' not found in schema graph."

    data = G.nodes[col_id]
    profile = f"COLUMN: {col_id}\n"
    profile += f"  Type: {data.get('data_type', '?')}\n"
    profile += f"  Role: {data.get('role', '?')}\n"
    profile += f"  Distinct values: {data.get('distinct_values', '?')}\n"
    profile += f"  Cardinality ratio: {data.get('cardinality_ratio', '?')}\n"
    profile += f"  Nullable: {data.get('nullable', '?')}\n"

    return profile


def get_graph_summary() -> str:
    """
    Get a high-level summary of the schema graph for prompt injection.

    Returns:
        Formatted string summarizing the database structure
    """
    G = get_schema_graph()

    tables = [n for n, d in G.nodes(data=True) if d.get("node_type") == "table"]
    columns = [n for n, d in G.nodes(data=True) if d.get("node_type") == "column"]
    fk_edges = [(u, v) for u, v, d in G.edges(data=True) if d.get("edge_type") == "foreign_key"]

    summary = "SCHEMA GRAPH SUMMARY:\n"
    summary += f"  Tables: {len(tables)} | Columns: {len(columns)} | Foreign Keys: {len(fk_edges) // 2}\n\n"

    for table in tables:
        row_count = G.nodes[table].get("row_count", "?")
        dims = []
        metrics = []
        identifiers = []

        for _, col_id in G.edges(table):
            if G[table][col_id].get("edge_type") != "has_column":
                continue
            col_name = col_id.split(".")[-1]
            role = G.nodes[col_id].get("role", "unknown")
            if role == "dimension":
                dims.append(col_name)
            elif role == "metric":
                metrics.append(col_name)
            elif role == "identifier":
                identifiers.append(col_name)

        summary += f"  {table} ({row_count} rows):\n"
        if identifiers:
            summary += f"    Identifiers: {', '.join(identifiers)}\n"
        if dims:
            summary += f"    Dimensions: {', '.join(dims)}\n"
        if metrics:
            summary += f"    Metrics: {', '.join(metrics)}\n"

    return summary


# --- LangChain Tool for Agent Use ---

@tool
def explore_schema(query: str) -> str:
    """Explore the database schema graph to understand table structure, column roles,
    and valid join paths. Use this when you need to understand the data model before
    writing SQL.

    Args:
        query: What to explore. Examples:
            - "tables" → list all tables with column roles
            - "transactions" → detailed context for the transactions table
            - "joins transactions" → valid join paths from transactions
            - "column transactions amount" → profile of a specific column
    """
    parts = query.strip().lower().split()

    if not parts:
        return get_graph_summary()

    command = parts[0]

    if command in ("tables", "summary", "all"):
        return get_graph_summary()

    if command == "joins" and len(parts) > 1:
        return get_valid_joins_for_table(parts[1])

    if command == "column" and len(parts) > 2:
        return get_column_profile(parts[1], parts[2])

    # Default: treat as table name
    table_context = get_table_context(command)
    joins = get_valid_joins_for_table(command)
    return f"{table_context}\n{joins}"
