"""
Semantic Layer for DataLens.

Provides a formal definition of metrics, dimensions, and join paths to constrain
LLM SQL generation and reduce hallucinations. Inject into agent prompts via
get_semantic_context().

Key benefits:
- Metrics are defined once with their SQL definition
- Dimensions are named and validated
- Join paths are pre-defined to prevent invalid joins
- LLM sees metric names rather than inventing column queries
"""

import yaml
from pathlib import Path


def load_semantic_layer() -> dict:
    """
    Load semantic layer YAML definition.

    Returns:
        Dictionary with tables, metrics, dimensions, join_paths, constraints, and common_patterns
    """
    layer_path = Path(__file__).parent / "semantic_layer.yaml"

    if not layer_path.exists():
        raise FileNotFoundError(f"Semantic layer not found at {layer_path}")

    with open(layer_path, "r") as f:
        return yaml.safe_load(f)


def get_semantic_context() -> str:
    """
    Format semantic layer for injection into agent prompts.

    Returns:
        Formatted string with metrics, dimensions, and constraints
    """
    semantic = load_semantic_layer()

    # Build metrics reference
    metrics_ref = "AVAILABLE METRICS:\n"
    for metric_name, metric_def in semantic.get("metrics", {}).items():
        metrics_ref += f"  - {metric_name}: {metric_def['definition']} ({metric_def['description']})\n"
        if "valid_dimensions" in metric_def:
            metrics_ref += f"    Valid dimensions: {', '.join(metric_def['valid_dimensions'])}\n"

    # Build dimensions reference
    dimensions_ref = "AVAILABLE DIMENSIONS:\n"
    for dim_name, dim_def in semantic.get("dimensions", {}).items():
        dimensions_ref += f"  - {dim_name}: {dim_def.get('description', 'N/A')}\n"
        if "valid_values" in dim_def:
            dimensions_ref += f"    Valid values: {', '.join(dim_def['valid_values'])}\n"

    # Build constraints reference
    constraints_ref = "CRITICAL SQL CONSTRAINTS:\n"
    for constraint_name, constraint_value in semantic.get("constraints", {}).items():
        constraints_ref += f"  - {constraint_name}: {constraint_value}\n"

    # Build common patterns reference
    patterns_ref = "COMMON SQL PATTERNS (Use as templates):\n"
    for pattern_name, pattern_sql in semantic.get("common_patterns", {}).items():
        patterns_ref += f"\n{pattern_name}:\n{pattern_sql}\n"

    # Combine all sections
    context = f"""--- SEMANTIC LAYER ---

{metrics_ref}

{dimensions_ref}

{constraints_ref}

{patterns_ref}

INSTRUCTION: When generating SQL, prefer metrics and dimensions by name (e.g., "total_revenue", "product_name")
over inventing column names. Always validate joins against the schema before executing.
"""

    return context


def get_metric_definition(metric_name: str) -> str:
    """
    Get SQL definition for a specific metric.

    Args:
        metric_name: Name of the metric (e.g., 'total_revenue')

    Returns:
        SQL definition of the metric

    Raises:
        KeyError: If metric not found in semantic layer
    """
    semantic = load_semantic_layer()
    metric = semantic.get("metrics", {}).get(metric_name)

    if not metric:
        raise KeyError(f"Metric '{metric_name}' not found in semantic layer")

    return metric["definition"]


def validate_metric_dimensions(metric_name: str, dimensions: list[str]) -> bool:
    """
    Validate that provided dimensions are valid for a metric.

    Args:
        metric_name: Name of the metric
        dimensions: List of dimension names

    Returns:
        True if all dimensions are valid for this metric

    Raises:
        KeyError: If metric not found
    """
    semantic = load_semantic_layer()
    metric = semantic.get("metrics", {}).get(metric_name)

    if not metric:
        raise KeyError(f"Metric '{metric_name}' not found")

    if "valid_dimensions" not in metric:
        return True  # Metric has no dimension restrictions

    valid_dims = set(metric["valid_dimensions"])
    provided_dims = set(dimensions)

    return provided_dims.issubset(valid_dims)


# Pre-load semantic context for performance
_SEMANTIC_CONTEXT = None


def get_cached_semantic_context() -> str:
    """
    Get semantic context with caching for performance.

    Returns:
        Formatted semantic layer string
    """
    global _SEMANTIC_CONTEXT

    if _SEMANTIC_CONTEXT is None:
        _SEMANTIC_CONTEXT = get_semantic_context()

    return _SEMANTIC_CONTEXT


def get_valid_joins() -> dict:
    """
    Get all valid join paths defined in semantic layer.

    Returns:
        Dictionary of join_name → join_definition
    """
    semantic = load_semantic_layer()
    return semantic.get("join_paths", {})


def get_join_condition(join_name: str) -> str:
    """
    Get the JOIN condition SQL for a specific join.

    Args:
        join_name: Name of the join (e.g., 'transactions_to_customers')

    Returns:
        JOIN condition string (e.g., 'transactions.customer_id = customers.customer_id')

    Raises:
        KeyError: If join not found
    """
    joins = get_valid_joins()
    join_def = joins.get(join_name)

    if not join_def:
        raise KeyError(f"Join '{join_name}' not found in semantic layer")

    # Support both simple join_condition and join_chain
    if "join_condition" in join_def:
        return join_def["join_condition"]
    elif "join_chain" in join_def:
        return " AND ".join(join_def["join_chain"])
    else:
        raise KeyError(f"Join '{join_name}' has no join_condition or join_chain")


def validate_join(from_table: str, to_table: str) -> bool:
    """
    Validate if a join between two tables is allowed.

    Args:
        from_table: Source table name
        to_table: Target table name

    Returns:
        True if join is defined and enabled, False otherwise
    """
    joins = get_valid_joins()

    for join_name, join_def in joins.items():
        if (
            join_def.get("from_table") == from_table
            and join_def.get("to_table") == to_table
            and join_def.get("enabled", True)
        ):
            return True

    return False


def get_join_chains() -> dict:
    """
    Get valid multi-table join chains (e.g., fact → dim1 → dim2).

    Returns:
        Dictionary of chain_name → chain_definition
    """
    semantic = load_semantic_layer()
    return semantic.get("join_chains", {})


def get_join_chain_description(chain_name: str) -> str:
    """
    Get description and use case for a join chain.

    Args:
        chain_name: Name of the chain (e.g., 'fact_to_all_dimensions')

    Returns:
        Description string with use cases

    Raises:
        KeyError: If chain not found
    """
    chains = get_join_chains()
    chain_def = chains.get(chain_name)

    if not chain_def:
        raise KeyError(f"Join chain '{chain_name}' not found")

    return f"{chain_def['description']} | Use case: {chain_def['use_case']}"


def get_multi_table_metrics() -> dict:
    """
    Get metrics that require multi-table joins.

    Returns:
        Dictionary of metric_name → metric_definition for multi-table metrics
    """
    semantic = load_semantic_layer()
    metrics = semantic.get("metrics", {})

    multi_table = {}
    for metric_name, metric_def in metrics.items():
        if "joins_required" in metric_def:
            multi_table[metric_name] = metric_def

    return multi_table
