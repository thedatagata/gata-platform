"""
BSL Semantic Model Builder.

Reads our existing semantic YAML configs and builds BSL v2 SemanticTable
objects backed by Ibis/MotherDuck connections. This bridges our config-driven
architecture with BSL's Python-native semantic layer.

Each tenant gets their own set of SemanticTable objects connected to their
star schema tables in MotherDuck.
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional

import ibis

logger = logging.getLogger(__name__)

# BSL imports
try:
    from boring_semantic_layer import to_semantic_table, Dimension, Measure
    BSL_AVAILABLE = True
except ImportError:
    BSL_AVAILABLE = False
    logger.warning("[BSL] boring-semantic-layer not installed")


def _get_ibis_connection() -> ibis.BaseBackend:
    """Create an Ibis connection to MotherDuck or local DuckDB."""
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if md_token:
        # MotherDuck connection via Ibis
        con = ibis.duckdb.connect(f"md:my_db?motherduck_token={md_token}")
    elif os.environ.get("GATA_ENV") == "local":
        sandbox_path = str(
            Path(__file__).parent.parent.parent
            / "warehouse" / "sandbox.duckdb"
        )
        con = ibis.duckdb.connect(sandbox_path)
    else:
        con = ibis.duckdb.connect("md:my_db")
    return con


def _load_tenant_config(tenant_slug: str) -> dict:
    """Load the semantic YAML config for a tenant."""
    config_path = (
        Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    )
    if not config_path.exists():
        raise FileNotFoundError(f"No semantic config for tenant: {tenant_slug}")
    with open(config_path) as f:
        return yaml.safe_load(f)


def _build_dimension_expr(dim_config: dict):
    """
    Convert a dimension config dict to a BSL Dimension.

    Our YAML format: { name: "campaign_id", type: "string" }
    """
    name = dim_config["name"]
    dim_type = dim_config.get("type", "string")
    description = dim_config.get("description", f"{name} ({dim_type})")

    return Dimension(
        expr=lambda t, _n=name: getattr(t, _n),
        description=description,
    )


def _build_measure_expr(measure_config: dict):
    """
    Convert a measure config dict to a BSL Measure.

    Our YAML format: { name: "spend", type: "number", agg: "sum" }
    """
    name = measure_config["name"]
    agg = measure_config.get("agg", "sum")
    label = measure_config.get("label", name)
    description = f"{label} (aggregation: {agg})"

    # Map our agg strings to Ibis aggregation methods
    if agg == "sum":
        expr = lambda t, _n=name: getattr(t, _n).sum()
    elif agg == "avg":
        expr = lambda t, _n=name: getattr(t, _n).mean()
    elif agg == "count":
        expr = lambda t, _n=name: getattr(t, _n).count()
    elif agg == "count_distinct":
        expr = lambda t, _n=name: getattr(t, _n).nunique()
    elif agg == "max":
        expr = lambda t, _n=name: getattr(t, _n).max()
    elif agg == "min":
        expr = lambda t, _n=name: getattr(t, _n).min()
    else:
        # Default to sum
        expr = lambda t, _n=name: getattr(t, _n).sum()

    return Measure(expr=expr, description=description)


def build_tenant_semantic_tables(
    tenant_slug: str,
    con: Optional[ibis.BaseBackend] = None,
) -> dict:
    """
    Build BSL SemanticTable objects for all models in a tenant's config.

    Returns a dict mapping model_name -> SemanticTable.

    Args:
        tenant_slug: The tenant identifier (e.g., "tyrell_corp")
        con: Optional Ibis connection. Creates one if not provided.
    """
    if not BSL_AVAILABLE:
        raise RuntimeError(
            "boring-semantic-layer not installed. "
            "Run: pip install 'boring-semantic-layer[agent]'"
        )

    config = _load_tenant_config(tenant_slug)
    if con is None:
        con = _get_ibis_connection()

    semantic_tables = {}

    for model_config in config.get("models", []):
        model_name = model_config["name"]

        try:
            # Get the Ibis table reference from the warehouse
            ibis_table = con.table(model_name)

            # Start building the SemanticTable
            st = to_semantic_table(ibis_table, name=model_name)

            # Add dimensions
            dim_kwargs = {}
            for dim in model_config.get("dimensions", []):
                dim_kwargs[dim["name"]] = _build_dimension_expr(dim)

            if dim_kwargs:
                st = st.with_dimensions(**dim_kwargs)

            # Add measures
            measure_kwargs = {}
            for m in model_config.get("measures", []):
                key = m["name"]
                measure_kwargs[key] = _build_measure_expr(m)

            if measure_kwargs:
                st = st.with_measures(**measure_kwargs)

            # Note: calculated_measures and joins are handled at query time
            # by the existing QueryBuilder. BSL handles the semantic layer,
            # while complex calculated measures stay in SQL.

            semantic_tables[model_name] = st
            logger.info(
                f"[BSL] Built SemanticTable '{model_name}' "
                f"({len(dim_kwargs)} dims, {len(measure_kwargs)} measures)"
            )

        except Exception as e:
            logger.warning(
                f"[BSL] Failed to build SemanticTable '{model_name}': {e}"
            )
            continue

    return semantic_tables


# Per-tenant cache of semantic tables
_tenant_cache: dict[str, dict] = {}


def get_tenant_semantic_tables(
    tenant_slug: str,
    force_refresh: bool = False,
) -> dict:
    """
    Get cached BSL SemanticTable objects for a tenant.

    Caches results in memory. Call with force_refresh=True after
    config changes.
    """
    if tenant_slug not in _tenant_cache or force_refresh:
        _tenant_cache[tenant_slug] = build_tenant_semantic_tables(tenant_slug)
    return _tenant_cache[tenant_slug]
