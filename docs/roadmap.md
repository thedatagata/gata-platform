# BSL + Ollama Implementation Plan

## GATA Platform — Boring Semantic Layer Integration with Ollama Qwen2.5-Coder 14B

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│  CURRENT STATE                                                      │
│                                                                     │
│  semantic_configs/*.yaml → QueryBuilder → raw SQL string → DuckDB   │
│  (static YAML files)       (custom class)  (hand-built)             │
│                                                                     │
│  Problems:                                                          │
│  - QueryBuilder is a custom SQL string builder (fragile, no Ibis)   │
│  - boring-semantic-layer installed but completely unused             │
│  - No LLM integration for natural language queries                  │
│  - Semantic configs are static YAML, disconnected from BSL library  │
│  - No join execution (joins defined in YAML but QueryBuilder does   │
│    simple string concatenation, not semantic joins)                  │
└─────────────────────────────────────────────────────────────────────┘

                              ↓ BECOMES ↓

┌─────────────────────────────────────────────────────────────────────┐
│  TARGET STATE                                                       │
│                                                                     │
│  tenants.yaml + INFORMATION_SCHEMA                                  │
│       ↓                                                             │
│  BSL SemanticModel (per tenant, built at startup)                   │
│       ├── .get_dimensions() → /catalog/dimensions endpoint          │
│       ├── .get_measures()   → /catalog/measures endpoint            │
│       ├── .group_by().aggregate() → /query endpoint (structured)    │
│       └── Ollama agent → /ask endpoint (natural language)           │
│           ├── BSLTools (list_models, query_model, get_documentation)│
│           └── Qwen2.5-Coder 14B via langchain-ollama               │
│                                                                     │
│  Data flow: MotherDuck → Ibis backend → BSL SemanticModel → API    │
└─────────────────────────────────────────────────────────────────────┘
```

## What We're Replacing vs Keeping

| Component                 | Current                        | Target                                                  | Action                               |
| ------------------------- | ------------------------------ | ------------------------------------------------------- | ------------------------------------ |
| `QueryBuilder` class      | Custom SQL string builder      | BSL `SemanticModel.group_by().aggregate()`              | **REPLACE**                          |
| `semantic_configs/*.yaml` | Static model definitions       | Auto-generated from MotherDuck schema + tenant config   | **REPLACE** (keep as cache/fallback) |
| `models.py` (Pydantic)    | `SemanticQueryRequest` etc     | Keep request models, add BSL-native response types      | **EXTEND**                           |
| `main.py` endpoints       | 10 endpoints                   | Keep observability endpoints, rework semantic endpoints | **REFACTOR**                         |
| LLM integration           | None                           | Ollama Qwen2.5-Coder 14B + BSLTools agent               | **NEW**                              |
| Catalog endpoints         | `GET /config` returns raw YAML | Dynamic from BSL `.get_dimensions()/.get_measures()`    | **REPLACE**                          |

## Physical Schema Reference (from MotherDuck)

All analytics tables live in `main.*` schema. Per tenant, 6 tables:

**fct_{slug}__ad_performance**:
`tenant_slug, source_platform, report_date(DATE), campaign_id, ad_group_id, ad_id, spend(DOUBLE), impressions(BIGINT), clicks(BIGINT), conversions(DOUBLE)`

**fct_{slug}__orders**:
`tenant_slug, source_platform, order_id(BIGINT), order_date(TIMESTAMP), total_price(DOUBLE), currency, financial_status, customer_email, customer_id, line_items_json(JSON)`

**fct_{slug}__sessions**:
`tenant_slug, source_platform, session_id, user_pseudo_id, user_id, session_start_ts(BIGINT), session_end_ts(BIGINT), session_duration_seconds(DOUBLE), events_in_session(BIGINT), traffic_source, traffic_medium, traffic_campaign, geo_country, device_category, is_conversion_session(BOOLEAN), session_revenue(DOUBLE), transaction_id`

**fct_{slug}__events**:
`tenant_slug, source_platform, event_name, event_timestamp(BIGINT), user_pseudo_id, user_id, session_id, order_id, order_total(DOUBLE), traffic_source, traffic_medium, traffic_campaign, geo_country, device_category`

**dim_{slug}__campaigns**:
`tenant_slug, source_platform, campaign_id, campaign_name, campaign_status`

**dim_{slug}__users**:
`tenant_slug, source_platform, user_pseudo_id, user_id, customer_email, customer_id, is_customer(BOOLEAN), first_seen_at(BIGINT), last_seen_at(BIGINT), total_events(BIGINT), total_sessions(BIGINT), first_geo_country, first_device_category`

**Active Tenants**: tyrell_corp, wayne_enterprises, stark_industries (all have
same 6-table star schema)

---

## Implementation Prompts (8 Prompts, Sequential Execution)

---

### PROMPT 1: Dependencies + Project Structure

**Goal**: Add required packages, create the `services/platform-api/bsl/` module
directory.

**Files to create/modify**:

- `services/platform-api/pyproject.toml` — add dependencies
- `services/platform-api/bsl/__init__.py` — package init
- `services/platform-api/bsl/connection.py` — MotherDuck/DuckDB connection
  factory

**Instructions**:

1. Edit `services/platform-api/pyproject.toml` to add these dependencies:

```toml
dependencies = [
    "fastapi",
    "uvicorn",
    "duckdb",
    "pyyaml",
    "pydantic",
    "httpx",
    "boring-semantic-layer>=0.3.7",
    "ibis-framework[duckdb]>=9.0.0",
    "langchain-ollama>=0.3.0",
    "langchain-core>=0.3.0",
]
```

2. Create `services/platform-api/bsl/__init__.py`:

```python
"""BSL (Boring Semantic Layer) integration for GATA Platform."""
```

3. Create `services/platform-api/bsl/connection.py`:

```python
"""
MotherDuck/DuckDB connection factory for Ibis backend.

BSL uses Ibis under the hood. Ibis needs a DuckDB connection.
We reuse the same connection logic as the existing API but
return an ibis.duckdb.connect() instead of raw duckdb.connect().
"""
import os
import ibis
from pathlib import Path
from functools import lru_cache


@lru_cache(maxsize=1)
def get_ibis_connection() -> ibis.BaseBackend:
    """
    Returns a cached Ibis DuckDB backend connection.
    
    Routes to MotherDuck (prod) or local sandbox (dev) based on env vars.
    The connection is cached because Ibis backends are stateless query compilers
    that hold a single DuckDB connection — safe to reuse across requests.
    """
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    
    if md_token:
        conn_str = f"md:my_db?motherduck_token={md_token}"
    elif os.environ.get("GATA_ENV") == "local":
        conn_str = str(
            Path(__file__).parent.parent.parent.parent / "warehouse" / "sandbox.duckdb"
        )
    else:
        conn_str = "md:my_db"
    
    return ibis.duckdb.connect(conn_str)


def get_raw_duckdb_connection():
    """
    Returns a raw duckdb connection for endpoints that don't use Ibis
    (observability queries, etc). Kept for backward compat.
    """
    import duckdb
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if md_token:
        conn_str = f"md:my_db?motherduck_token={md_token}"
    elif os.environ.get("GATA_ENV") == "local":
        conn_str = str(
            Path(__file__).parent.parent.parent.parent / "warehouse" / "sandbox.duckdb"
        )
    else:
        conn_str = "md:my_db"
    return duckdb.connect(conn_str)
```

**Verification**: Run `cd services/platform-api && uv pip install -e .` — should
install without errors.

---

### PROMPT 2: Tenant Semantic Model Builder

**Goal**: Create the core module that reads MotherDuck table schemas + tenant
YAML configs and constructs BSL `SemanticModel` objects per tenant. This is the
heart of the integration — equivalent to what `model_llm.py` does in the dlthub
demo but driven by our existing configs instead of LLM inference.

**Files to create**:

- `services/platform-api/bsl/model_builder.py` — builds BSL SemanticModel per
  tenant
- `services/platform-api/bsl/table_definitions.py` — canonical star schema
  table/column definitions

**Reference**: The dlthub demo's `model_llm.py` pattern of building
`to_semantic_table()` with `.with_dimensions()` and `.with_measures()` then
recursively joining. Our version is simpler because we don't need alias
deduplication (no diamond joins in our star schema — facts join to dims, dims
don't join to each other).

**Instructions**:

1. Create `services/platform-api/bsl/table_definitions.py`:

This file defines the canonical semantic metadata for each of the 6 star schema
table types. It reads from the existing `semantic_configs/*.yaml` files to stay
in sync, but could also be hardcoded since the star schema is stable.

```python
"""
Canonical semantic definitions for the GATA star schema.

Each analytics table type (fct_ad_performance, fct_orders, etc.) has a fixed
set of dimensions and measures. This module defines them in a format that
model_builder.py can consume to construct BSL SemanticModel objects.

The definitions are derived from the semantic_configs/*.yaml files but
expressed as typed Python structures for BSL integration.
"""
from dataclasses import dataclass, field
from typing import Literal


@dataclass
class DimensionDef:
    """A column that can be used for GROUP BY / filtering."""
    name: str
    description: str = ""


@dataclass
class MeasureDef:
    """A column that can be aggregated."""
    name: str
    source_column: str  # physical column name in the table
    aggregation: str  # sum, count, nunique, mean, max, min
    description: str = ""


@dataclass
class JoinDef:
    """How a fact table joins to a dimension table."""
    to_table_type: str  # e.g. "campaigns", "users"
    left_columns: list[str] = field(default_factory=list)
    right_columns: list[str] = field(default_factory=list)
    how: str = "left"


@dataclass
class TableTypeDef:
    """Complete semantic definition for a star schema table type."""
    table_type: str  # "fct_ad_performance", "dim_campaigns", etc.
    label: str
    description: str
    dimensions: list[DimensionDef] = field(default_factory=list)
    measures: list[MeasureDef] = field(default_factory=list)
    joins: list[JoinDef] = field(default_factory=list)


# ── Canonical Definitions ──────────────────────────────────────────

AD_PERFORMANCE = TableTypeDef(
    table_type="fct_ad_performance",
    label="Ad Performance",
    description="Daily ad spend and engagement metrics across all ad platforms.",
    dimensions=[
        DimensionDef("source_platform", "Ad platform (facebook_ads, google_ads, etc.)"),
        DimensionDef("report_date", "Date of the ad performance report"),
        DimensionDef("campaign_id", "Campaign identifier"),
        DimensionDef("ad_group_id", "Ad group/ad set identifier"),
        DimensionDef("ad_id", "Individual ad identifier"),
    ],
    measures=[
        MeasureDef("total_spend", "spend", "sum", "Total advertising spend"),
        MeasureDef("total_impressions", "impressions", "sum", "Total ad impressions"),
        MeasureDef("total_clicks", "clicks", "sum", "Total ad clicks"),
        MeasureDef("total_conversions", "conversions", "sum", "Total conversions attributed to ads"),
    ],
    joins=[
        JoinDef("campaigns", ["campaign_id", "source_platform"], ["campaign_id", "source_platform"]),
    ],
)

ORDERS = TableTypeDef(
    table_type="fct_orders",
    label="Orders",
    description="Ecommerce transactions with customer and financial details.",
    dimensions=[
        DimensionDef("source_platform", "Ecommerce platform (shopify, bigcommerce, woocommerce)"),
        DimensionDef("order_date", "Timestamp of the order"),
        DimensionDef("currency", "Order currency code"),
        DimensionDef("financial_status", "Payment status (paid, pending, refunded)"),
        DimensionDef("customer_email", "Customer email address"),
        DimensionDef("customer_id", "Customer identifier from ecommerce platform"),
    ],
    measures=[
        MeasureDef("total_revenue", "total_price", "sum", "Total order revenue"),
        MeasureDef("order_count", "order_id", "nunique", "Count of unique orders"),
    ],
    joins=[
        JoinDef("users", ["customer_email"], ["customer_email"]),
    ],
)

SESSIONS = TableTypeDef(
    table_type="fct_sessions",
    label="Sessions",
    description="Sessionized web analytics with attribution, duration, and conversion flags.",
    dimensions=[
        DimensionDef("source_platform", "Analytics platform (google_analytics, mixpanel)"),
        DimensionDef("user_pseudo_id", "Anonymous user identifier"),
        DimensionDef("session_id", "Unique session identifier"),
        DimensionDef("traffic_source", "Traffic source (google, facebook, direct)"),
        DimensionDef("traffic_medium", "Traffic medium (organic, cpc, referral)"),
        DimensionDef("traffic_campaign", "UTM campaign name"),
        DimensionDef("geo_country", "User country"),
        DimensionDef("device_category", "Device type (desktop, mobile, tablet)"),
        DimensionDef("is_conversion_session", "Whether session included a conversion event"),
    ],
    measures=[
        MeasureDef("avg_session_duration", "session_duration_seconds", "mean", "Average session duration in seconds"),
        MeasureDef("avg_events_per_session", "events_in_session", "mean", "Average events per session"),
        MeasureDef("total_session_revenue", "session_revenue", "sum", "Total revenue from sessions"),
        MeasureDef("session_count", "session_id", "nunique", "Count of unique sessions"),
    ],
    joins=[
        JoinDef("users", ["user_pseudo_id"], ["user_pseudo_id"]),
        JoinDef("campaigns", ["traffic_campaign"], ["campaign_name"]),
    ],
)

EVENTS = TableTypeDef(
    table_type="fct_events",
    label="Events",
    description="Raw analytics events with attribution and optional ecommerce data.",
    dimensions=[
        DimensionDef("source_platform", "Analytics platform"),
        DimensionDef("event_name", "Event type (session_start, view_item, purchase, etc.)"),
        DimensionDef("user_pseudo_id", "Anonymous user identifier"),
        DimensionDef("session_id", "Session identifier"),
        DimensionDef("order_id", "Order ID (for purchase events)"),
        DimensionDef("traffic_source", "Traffic source"),
        DimensionDef("traffic_medium", "Traffic medium"),
        DimensionDef("traffic_campaign", "UTM campaign name"),
        DimensionDef("geo_country", "User country"),
        DimensionDef("device_category", "Device type"),
    ],
    measures=[
        MeasureDef("event_count", "event_timestamp", "count", "Total event count"),
        MeasureDef("total_order_value", "order_total", "sum", "Total order value from purchase events"),
    ],
    joins=[
        JoinDef("users", ["user_pseudo_id"], ["user_pseudo_id"]),
    ],
)

CAMPAIGNS_DIM = TableTypeDef(
    table_type="dim_campaigns",
    label="Campaigns",
    description="Campaign dimension with name and status across ad platforms.",
    dimensions=[
        DimensionDef("source_platform", "Ad platform"),
        DimensionDef("campaign_id", "Campaign identifier"),
        DimensionDef("campaign_name", "Campaign display name"),
        DimensionDef("campaign_status", "Campaign status (active, paused, archived)"),
    ],
    measures=[],
    joins=[],
)

USERS_DIM = TableTypeDef(
    table_type="dim_users",
    label="Users",
    description="Unified user dimension combining analytics and ecommerce identities.",
    dimensions=[
        DimensionDef("source_platform", "Platform where user was first seen"),
        DimensionDef("user_pseudo_id", "Anonymous analytics user ID"),
        DimensionDef("user_id", "Authenticated user ID"),
        DimensionDef("customer_email", "Customer email from ecommerce"),
        DimensionDef("customer_id", "Customer ID from ecommerce"),
        DimensionDef("is_customer", "Whether user has made a purchase"),
        DimensionDef("first_geo_country", "Country from first seen event"),
        DimensionDef("first_device_category", "Device from first seen event"),
    ],
    measures=[
        MeasureDef("total_user_events", "total_events", "sum", "Total events across all users"),
        MeasureDef("total_user_sessions", "total_sessions", "sum", "Total sessions across all users"),
    ],
    joins=[],
)


# ── Registry ───────────────────────────────────────────────────────

# Maps table_type suffix → definition
TABLE_TYPE_REGISTRY: dict[str, TableTypeDef] = {
    "ad_performance": AD_PERFORMANCE,
    "orders": ORDERS,
    "sessions": SESSIONS,
    "events": EVENTS,
    "campaigns": CAMPAIGNS_DIM,
    "users": USERS_DIM,
}


def get_table_def(table_type_suffix: str) -> TableTypeDef:
    """Look up a table type definition by its suffix (e.g. 'ad_performance')."""
    if table_type_suffix not in TABLE_TYPE_REGISTRY:
        raise ValueError(
            f"Unknown table type '{table_type_suffix}'. "
            f"Valid types: {list(TABLE_TYPE_REGISTRY.keys())}"
        )
    return TABLE_TYPE_REGISTRY[table_type_suffix]
```

2. Create `services/platform-api/bsl/model_builder.py`:

```python
"""
Builds BSL SemanticModel objects per tenant from MotherDuck tables.

This is the GATA equivalent of the dlthub demo's model_llm.py.
Instead of using an LLM to infer schema structure, we read our
known star schema definitions from table_definitions.py and wire
them into BSL's SemanticModel using Ibis expressions.

Architecture:
  1. Connect to MotherDuck via Ibis
  2. For each tenant, discover their analytics tables (fct_* and dim_*)
  3. For each table, create a BSL SemanticTable with dimensions and measures
  4. Join fact tables to dimension tables using known foreign keys
  5. Cache the resulting SemanticModel per tenant

The result is a live BSL SemanticModel per tenant that supports:
  - .get_dimensions() → list of available dimensions
  - .get_measures() → list of available measures
  - .group_by("dim").aggregate("measure") → Ibis query
  - .filter(lambda t: t.col == value) → filtered query
"""
from boring_semantic_layer import to_semantic_table, SemanticModel, Dimension, Measure
from bsl.connection import get_ibis_connection
from bsl.table_definitions import (
    TABLE_TYPE_REGISTRY,
    TableTypeDef,
    DimensionDef,
    MeasureDef,
    JoinDef,
)
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)


# ── In-memory cache of tenant models ──────────────────────────────

_tenant_models: Dict[str, Dict[str, SemanticModel]] = {}
# Structure: { "tyrell_corp": { "ad_performance": SemanticModel, ... } }


def _physical_table_name(tenant_slug: str, table_type_suffix: str) -> str:
    """
    Resolve the physical table name in MotherDuck.
    
    Convention:
      fct_{tenant_slug}__{suffix}  (for fact tables)
      dim_{tenant_slug}__{suffix}  (for dimension tables)
    """
    if table_type_suffix in ("campaigns", "users"):
        return f"dim_{tenant_slug}__{table_type_suffix}"
    return f"fct_{tenant_slug}__{table_type_suffix}"


def _build_dimension_kwargs(
    table_def: TableTypeDef,
) -> dict:
    """
    Build BSL Dimension kwargs from table definition.
    
    Returns dict of {dimension_name: Dimension(expr=lambda, description=str)}
    
    Each dimension is a simple column reference — no aliasing needed because
    we build one SemanticModel per star schema model (not one giant joined model
    like the dlthub demo). Joins are handled at query time by BSL.
    """
    kwargs = {}
    for dim_def in table_def.dimensions:
        col_name = dim_def.name
        kwargs[col_name] = Dimension(
            expr=lambda t, c=col_name: t[c],
            description=dim_def.description,
        )
    return kwargs


def _build_measure_kwargs(
    table_def: TableTypeDef,
) -> dict:
    """
    Build BSL Measure kwargs from table definition.
    
    Returns dict of {measure_name: Measure(expr=lambda, description=str)}
    
    Aggregation types map to Ibis column methods:
      sum → t[col].sum()
      count → t[col].count()
      nunique → t[col].nunique()
      mean → t[col].mean()
      max → t[col].max()
      min → t[col].min()
    """
    kwargs = {}
    for measure_def in table_def.measures:
        src_col = measure_def.source_column
        agg = measure_def.aggregation
        
        kwargs[measure_def.name] = Measure(
            expr=lambda t, c=src_col, a=agg: getattr(t[c], a)(),
            description=measure_def.description,
        )
    return kwargs


def _build_single_table_model(
    tenant_slug: str,
    table_type_suffix: str,
) -> Optional[SemanticModel]:
    """
    Build a standalone BSL SemanticModel for one analytics table.
    
    This creates the base model WITHOUT joins. Joins are wired up
    separately in build_tenant_models() because BSL's .join() method
    requires both the left and right SemanticModel to already exist.
    """
    con = get_ibis_connection()
    physical_name = _physical_table_name(tenant_slug, table_type_suffix)
    table_def = TABLE_TYPE_REGISTRY[table_type_suffix]
    
    try:
        ibis_table = con.table(physical_name, schema="main")
    except Exception as e:
        logger.warning(f"Table {physical_name} not found: {e}")
        return None
    
    dim_kwargs = _build_dimension_kwargs(table_def)
    measure_kwargs = _build_measure_kwargs(table_def)
    
    # Create BSL semantic table
    st = to_semantic_table(ibis_table)
    
    if dim_kwargs:
        st = st.with_dimensions(**dim_kwargs)
    if measure_kwargs:
        st = st.with_measures(**measure_kwargs)
    
    return st


def _wire_joins(
    fact_model: SemanticModel,
    dim_models: Dict[str, SemanticModel],
    fact_def: TableTypeDef,
) -> SemanticModel:
    """
    Wire up joins from a fact table to its dimension tables.
    
    Uses BSL's .join() method with Ibis lambda on-clauses.
    Our star schema is simple (facts → dims, no dim → dim chains),
    so we don't need the recursive join traversal from the dlthub demo.
    """
    model = fact_model
    
    for join_def in fact_def.joins:
        dim_suffix = join_def.to_table_type
        if dim_suffix not in dim_models:
            logger.warning(f"Dimension '{dim_suffix}' not found for join, skipping")
            continue
        
        dim_model = dim_models[dim_suffix]
        
        # Build on-clause lambda
        left_cols = join_def.left_columns
        right_cols = join_def.right_columns
        
        def _on_clause(left, right, lc=left_cols, rc=right_cols):
            cond = None
            for l_col, r_col in zip(lc, rc):
                c = left[l_col] == right[r_col]
                cond = c if cond is None else (cond & c)
            return cond
        
        model = model.join(
            dim_model,
            on=_on_clause,
            how=join_def.how,
        )
    
    return model


def build_tenant_models(tenant_slug: str) -> Dict[str, SemanticModel]:
    """
    Build all BSL SemanticModels for a tenant.
    
    Returns a dict keyed by table_type_suffix:
      {
        "ad_performance": SemanticModel (fact, with dim joins),
        "orders": SemanticModel (fact, with dim joins),
        "sessions": SemanticModel (fact, with dim joins),
        "events": SemanticModel (fact, with dim joins),
        "campaigns": SemanticModel (dim, standalone),
        "users": SemanticModel (dim, standalone),
      }
    
    The fact models include joins to their dimension tables.
    The dimension models are standalone (queryable independently).
    """
    # Check cache first
    if tenant_slug in _tenant_models:
        return _tenant_models[tenant_slug]
    
    logger.info(f"Building BSL models for tenant: {tenant_slug}")
    
    # Step 1: Build standalone models for all tables
    standalone: Dict[str, SemanticModel] = {}
    for suffix in TABLE_TYPE_REGISTRY:
        model = _build_single_table_model(tenant_slug, suffix)
        if model is not None:
            standalone[suffix] = model
    
    # Step 2: Wire joins from facts to dims
    dim_models = {
        k: v for k, v in standalone.items()
        if k in ("campaigns", "users")
    }
    
    result: Dict[str, SemanticModel] = {}
    
    for suffix, model in standalone.items():
        table_def = TABLE_TYPE_REGISTRY[suffix]
        if table_def.joins and suffix not in dim_models:
            # This is a fact table with joins
            result[suffix] = _wire_joins(model, dim_models, table_def)
        else:
            # Dimension table or fact with no joins
            result[suffix] = model
    
    # Cache
    _tenant_models[tenant_slug] = result
    logger.info(f"Built {len(result)} BSL models for {tenant_slug}")
    
    return result


def get_tenant_model(tenant_slug: str, model_name: str) -> SemanticModel:
    """
    Get a specific BSL SemanticModel for a tenant.
    
    model_name can be:
      - Full table name: "fct_tyrell_corp__ad_performance"
      - Short suffix: "ad_performance"
    """
    models = build_tenant_models(tenant_slug)
    
    # Try direct suffix match first
    if model_name in models:
        return models[model_name]
    
    # Try stripping prefix
    for suffix in models:
        full_name = _physical_table_name(tenant_slug, suffix)
        if model_name == full_name:
            return models[suffix]
    
    raise ValueError(
        f"Model '{model_name}' not found for tenant '{tenant_slug}'. "
        f"Available: {list(models.keys())}"
    )


def invalidate_tenant_cache(tenant_slug: str):
    """Clear cached models for a tenant (call after config changes)."""
    _tenant_models.pop(tenant_slug, None)


def invalidate_all_caches():
    """Clear all cached models."""
    _tenant_models.clear()


def list_available_tenants() -> list[str]:
    """
    Discover tenants by scanning MotherDuck for fct_*__ad_performance tables.
    """
    con = get_ibis_connection()
    try:
        # Use raw SQL through the Ibis DuckDB backend
        result = con.raw_sql(
            "SELECT DISTINCT REPLACE(REPLACE(table_name, 'fct_', ''), '__ad_performance', '') as tenant "
            "FROM information_schema.columns "
            "WHERE table_schema = 'main' AND table_name LIKE 'fct_%__ad_performance'"
        ).fetchall()
        return [row[0] for row in result]
    except Exception as e:
        logger.error(f"Failed to discover tenants: {e}")
        return []
```

**Verification**:

```python
# Quick smoke test (run from services/platform-api/)
from bsl.model_builder import build_tenant_models
models = build_tenant_models("tyrell_corp")
print(list(models.keys()))  # Should show 6 model names
print(models["ad_performance"].get_dimensions())
print(models["ad_performance"].get_measures())
```

---

### PROMPT 3: Ollama LLM Agent Integration

**Goal**: Create the agent module that uses Ollama Qwen2.5-Coder 14B with BSL's
tool-calling capabilities for natural language queries.

**Files to create**:

- `services/platform-api/bsl/agent.py` — Ollama agent with BSLTools
- `services/platform-api/bsl/config.py` — Configuration constants

**Reference**: The dlthub demo's `chat.py` uses OpenAI + MCP. We use Ollama +
BSLTools (BSL's native LangChain tools) which is simpler and free.

**Instructions**:

1. Create `services/platform-api/bsl/config.py`:

```python
"""
BSL integration configuration.

All configurable values for the semantic layer and LLM agent.
Override via environment variables.
"""
import os


# ── Ollama Configuration ──────────────────────────────────────────
OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5-coder:14b")
OLLAMA_TEMPERATURE = float(os.environ.get("OLLAMA_TEMPERATURE", "0"))
OLLAMA_REQUEST_TIMEOUT = int(os.environ.get("OLLAMA_REQUEST_TIMEOUT", "120"))

# ── Agent Configuration ───────────────────────────────────────────
AGENT_MAX_ITERATIONS = int(os.environ.get("AGENT_MAX_ITERATIONS", "10"))
AGENT_SYSTEM_PROMPT = """You are an analytics assistant for the GATA Platform.
You help users explore their marketing and ecommerce data using the semantic layer tools.

Available tools:
- list_models: See all available data models and their descriptions
- get_documentation: Get detailed dimension and measure definitions for a model
- query_model: Execute a query with specific dimensions, measures, and filters

When answering questions:
1. First use get_documentation to understand what dimensions and measures are available
2. Then use query_model to fetch the data
3. Present results clearly with context about what the numbers mean

Always prefer specific measures over raw column queries.
Format numbers appropriately (currency with $, percentages with %).
If unsure which model to query, use list_models first.
"""
```

2. Create `services/platform-api/bsl/agent.py`:

```python
"""
Ollama-powered agent for natural language analytics queries.

Uses BSL's native tool-calling interface (BSLTools) with LangChain's
ChatOllama to enable natural language → structured query → results.

Architecture:
  User question → Ollama Qwen2.5-Coder 14B → BSLTools function calls
    ├── list_models → returns available models + descriptions
    ├── get_documentation → returns dimensions/measures for a model
    └── query_model → executes group_by().aggregate() and returns data

The agent runs a ReAct-style loop: it decides which tool to call,
interprets the result, and either calls another tool or returns
the final answer. BSLTools handles all the BSL ↔ Ibis ↔ DuckDB
query compilation internally.

Fallback: If Ollama is unavailable, agent endpoints return a clear
error telling the user to start Ollama or use structured queries instead.
"""
from langchain_ollama import ChatOllama
from langchain_core.messages import HumanMessage, SystemMessage, AIMessage
from boring_semantic_layer import SemanticModel
from bsl.config import (
    OLLAMA_BASE_URL,
    OLLAMA_MODEL,
    OLLAMA_TEMPERATURE,
    OLLAMA_REQUEST_TIMEOUT,
    AGENT_MAX_ITERATIONS,
    AGENT_SYSTEM_PROMPT,
)
from typing import Optional, Dict, Any
import logging
import json

logger = logging.getLogger(__name__)


def _get_ollama_llm() -> Optional[ChatOllama]:
    """
    Create a ChatOllama instance connected to the local Ollama server.
    
    Returns None if Ollama is not available (graceful degradation).
    """
    try:
        llm = ChatOllama(
            model=OLLAMA_MODEL,
            temperature=OLLAMA_TEMPERATURE,
            base_url=OLLAMA_BASE_URL,
            timeout=OLLAMA_REQUEST_TIMEOUT,
        )
        return llm
    except Exception as e:
        logger.warning(f"Ollama not available: {e}")
        return None


def check_ollama_health() -> dict:
    """Check if Ollama server is running and the model is available."""
    import httpx
    try:
        resp = httpx.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5.0)
        if resp.status_code == 200:
            models = resp.json().get("models", [])
            model_names = [m["name"] for m in models]
            has_model = any(OLLAMA_MODEL in name for name in model_names)
            return {
                "status": "healthy",
                "ollama_url": OLLAMA_BASE_URL,
                "target_model": OLLAMA_MODEL,
                "model_available": has_model,
                "available_models": model_names,
            }
        return {"status": "unhealthy", "detail": f"HTTP {resp.status_code}"}
    except Exception as e:
        return {"status": "unavailable", "detail": str(e)}


def _build_bsl_tools(models: Dict[str, SemanticModel]) -> list:
    """
    Build BSL tool functions that the agent can call.
    
    We implement the same 3 tools that BSLTools provides, but adapted
    for our multi-model-per-tenant architecture (BSLTools expects a
    single SemanticModel, we have 6 per tenant).
    """
    
    def list_models_tool() -> str:
        """List all available data models with their descriptions."""
        result = []
        for name, model in models.items():
            dims = model.get_dimensions()
            measures = model.get_measures()
            result.append({
                "name": name,
                "dimensions": list(dims.keys()) if isinstance(dims, dict) else [str(d) for d in dims],
                "measures": list(measures.keys()) if isinstance(measures, dict) else [str(m) for m in measures],
            })
        return json.dumps(result, indent=2)
    
    def get_documentation_tool(model_name: str) -> str:
        """Get detailed documentation for a specific model's dimensions and measures."""
        if model_name not in models:
            return json.dumps({"error": f"Model '{model_name}' not found. Available: {list(models.keys())}"})
        
        model = models[model_name]
        dims = model.get_dimensions()
        measures = model.get_measures()
        
        doc = {
            "model": model_name,
            "dimensions": {},
            "measures": {},
        }
        
        if isinstance(dims, dict):
            for name, dim_obj in dims.items():
                doc["dimensions"][name] = {
                    "description": getattr(dim_obj, 'description', '') or name,
                }
        
        if isinstance(measures, dict):
            for name, measure_obj in measures.items():
                doc["measures"][name] = {
                    "description": getattr(measure_obj, 'description', '') or name,
                }
        
        return json.dumps(doc, indent=2)
    
    def query_model_tool(
        model_name: str,
        dimensions: list[str] = None,
        measures: list[str] = None,
        filters: list[dict] = None,
        limit: int = 100,
    ) -> str:
        """
        Execute a semantic query on a model.
        
        Args:
            model_name: Which model to query
            dimensions: Columns to group by
            measures: Aggregations to compute
            filters: List of {"field": str, "op": str, "value": any}
            limit: Max rows to return
        """
        if model_name not in models:
            return json.dumps({"error": f"Model '{model_name}' not found"})
        
        model = models[model_name]
        query = model
        
        # Apply filters
        if filters:
            for f in filters:
                field_name = f.get("field")
                op = f.get("op", "=")
                value = f.get("value")
                
                dim_obj = model.get_dimensions().get(field_name)
                if dim_obj:
                    if op == "=":
                        query = query.filter(lambda t, fn=field_name, v=value: t[fn] == v)
                    elif op == "!=":
                        query = query.filter(lambda t, fn=field_name, v=value: t[fn] != v)
                    elif op == ">":
                        query = query.filter(lambda t, fn=field_name, v=value: t[fn] > v)
                    elif op == "<":
                        query = query.filter(lambda t, fn=field_name, v=value: t[fn] < v)
        
        # Apply aggregation
        if dimensions and measures:
            query = query.group_by(*dimensions).aggregate(*measures)
        elif measures:
            query = query.aggregate(*measures)
        
        try:
            # Execute and convert to records
            result_table = query.as_table()
            # Limit rows
            if hasattr(result_table, 'limit'):
                result_table = result_table.limit(limit)
            
            df = result_table.execute()
            records = df.to_dict(orient="records") if hasattr(df, 'to_dict') else []
            
            return json.dumps({
                "model": model_name,
                "row_count": len(records),
                "data": records[:limit],
            }, default=str)
        except Exception as e:
            return json.dumps({"error": str(e)})
    
    return [
        {
            "name": "list_models",
            "description": "List all available data models with their dimensions and measures",
            "function": list_models_tool,
        },
        {
            "name": "get_documentation",
            "description": "Get detailed documentation for a model's dimensions and measures. Requires model_name parameter.",
            "function": get_documentation_tool,
            "parameters": {"model_name": "string"},
        },
        {
            "name": "query_model",
            "description": "Execute a semantic query on a model with optional dimensions, measures, and filters",
            "function": query_model_tool,
            "parameters": {
                "model_name": "string",
                "dimensions": "list[string]",
                "measures": "list[string]",
                "filters": "list[dict]",
                "limit": "integer",
            },
        },
    ]


async def run_agent_query(
    question: str,
    models: Dict[str, SemanticModel],
    conversation_history: list[dict] = None,
) -> dict:
    """
    Run a natural language query through the Ollama agent.
    
    The agent uses a simple ReAct loop:
    1. Send question + tool descriptions to Ollama
    2. If Ollama returns a tool call → execute it, append result, loop
    3. If Ollama returns text → that's the final answer
    
    Returns:
        {
            "answer": str,           # Natural language answer
            "tool_calls": list,      # Tools called during execution
            "data": dict | None,     # Raw data from last query_model call
            "error": str | None,     # Error message if failed
        }
    """
    llm = _get_ollama_llm()
    if llm is None:
        return {
            "answer": None,
            "tool_calls": [],
            "data": None,
            "error": "Ollama is not available. Start Ollama with `ollama serve` and ensure "
                     f"'{OLLAMA_MODEL}' is pulled (`ollama pull {OLLAMA_MODEL}`).",
        }
    
    tools = _build_bsl_tools(models)
    tool_map = {t["name"]: t["function"] for t in tools}
    
    # Build tool descriptions for the prompt
    tool_descriptions = "\n".join(
        f"- {t['name']}: {t['description']}" for t in tools
    )
    
    # Build conversation messages
    messages = [
        SystemMessage(content=AGENT_SYSTEM_PROMPT + f"\n\nAvailable tools:\n{tool_descriptions}"),
    ]
    
    # Add conversation history if provided
    if conversation_history:
        for msg in conversation_history:
            if msg["role"] == "user":
                messages.append(HumanMessage(content=msg["content"]))
            elif msg["role"] == "assistant":
                messages.append(AIMessage(content=msg["content"]))
    
    messages.append(HumanMessage(content=question))
    
    tool_calls_log = []
    last_data = None
    
    # Simple ReAct loop
    for iteration in range(AGENT_MAX_ITERATIONS):
        try:
            response = llm.invoke(messages)
        except Exception as e:
            return {
                "answer": None,
                "tool_calls": tool_calls_log,
                "data": None,
                "error": f"LLM invocation failed: {str(e)}",
            }
        
        content = response.content if hasattr(response, 'content') else str(response)
        
        # Check if response contains a tool call pattern
        # Ollama with tool calling returns structured tool_calls
        if hasattr(response, 'tool_calls') and response.tool_calls:
            for tool_call in response.tool_calls:
                tool_name = tool_call.get("name") or tool_call.get("function", {}).get("name")
                tool_args = tool_call.get("args") or tool_call.get("function", {}).get("arguments", {})
                
                if isinstance(tool_args, str):
                    tool_args = json.loads(tool_args)
                
                if tool_name in tool_map:
                    try:
                        result = tool_map[tool_name](**tool_args) if tool_args else tool_map[tool_name]()
                        tool_calls_log.append({
                            "tool": tool_name,
                            "args": tool_args,
                            "result_preview": result[:500] if len(result) > 500 else result,
                        })
                        
                        # Track data from query_model calls
                        if tool_name == "query_model":
                            try:
                                last_data = json.loads(result)
                            except:
                                pass
                        
                        messages.append(AIMessage(content=f"Tool call: {tool_name}({json.dumps(tool_args)})"))
                        messages.append(HumanMessage(content=f"Tool result:\n{result}"))
                    except Exception as e:
                        messages.append(HumanMessage(content=f"Tool error: {str(e)}"))
                continue
        
        # No tool calls — this is the final answer
        return {
            "answer": content,
            "tool_calls": tool_calls_log,
            "data": last_data,
            "error": None,
        }
    
    return {
        "answer": "Agent reached maximum iterations without a final answer.",
        "tool_calls": tool_calls_log,
        "data": last_data,
        "error": "max_iterations_reached",
    }
```

**Verification**:

```python
from bsl.agent import check_ollama_health
print(check_ollama_health())
# Should return status + model availability
```

---

### PROMPT 4: Pydantic Models Update

**Goal**: Add new request/response models for BSL endpoints and the agent `/ask`
endpoint. Keep existing models for backward compatibility.

**File to modify**: `services/platform-api/models.py`

**Instructions**:

Append the following new models to the existing `models.py` file (do NOT remove
existing models — they're still used by observability endpoints):

```python
# --- BSL Catalog Models ---

class BSLDimensionInfo(BaseModel):
    name: str
    description: str = ""


class BSLMeasureInfo(BaseModel):
    name: str
    description: str = ""


class BSLModelCatalog(BaseModel):
    """Catalog of a single semantic model's dimensions and measures."""
    model_name: str
    label: str
    description: str
    dimensions: list[BSLDimensionInfo]
    measures: list[BSLMeasureInfo]


class BSLTenantCatalog(BaseModel):
    """Full catalog for a tenant — all models with their dims/measures."""
    tenant_slug: str
    models: list[BSLModelCatalog]


# --- BSL Query Models ---

class BSLQueryFilter(BaseModel):
    field: str
    op: str = "="
    value: str | int | float | bool | None = None


class BSLQueryRequest(BaseModel):
    """Structured query request using BSL semantic model."""
    model: str  # model suffix like "ad_performance" or full name
    dimensions: list[str] = []
    measures: list[str] = []
    filters: list[BSLQueryFilter] = []
    limit: int = 1000

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int) -> int:
        if v > 10000:
            raise ValueError("Limit cannot exceed 10000")
        return v


class BSLQueryResponse(BaseModel):
    """Response from a BSL structured query."""
    model: str
    data: list[dict]
    row_count: int
    dimensions_used: list[str]
    measures_used: list[str]


# --- Agent Models ---

class AgentMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str


class AskRequest(BaseModel):
    """Natural language query sent to the Ollama agent."""
    question: str
    conversation_history: list[AgentMessage] = []


class ToolCallInfo(BaseModel):
    tool: str
    args: dict = {}
    result_preview: str = ""


class AskResponse(BaseModel):
    """Response from the Ollama agent."""
    answer: str | None
    tool_calls: list[ToolCallInfo] = []
    data: dict | None = None
    error: str | None = None


# --- Health Models ---

class OllamaHealthResponse(BaseModel):
    status: str
    ollama_url: str = ""
    target_model: str = ""
    model_available: bool = False
    available_models: list[str] = []
    detail: str = ""
```

---

### PROMPT 5: FastAPI Endpoint Refactor

**Goal**: Refactor `main.py` to add BSL-powered endpoints alongside (not
replacing) the existing endpoints. The new endpoints live under `/bsl/` prefix
to avoid conflicts during migration.

**File to modify**: `services/platform-api/main.py`

**Instructions**:

Add the following new endpoint groups to `main.py`. Keep ALL existing endpoints
intact — they still serve the old frontend. The new `/bsl/` endpoints will be
the target for the upgraded frontend.

Add these imports at the top:

```python
from bsl.model_builder import (
    build_tenant_models,
    get_tenant_model,
    invalidate_tenant_cache,
    list_available_tenants,
)
from bsl.agent import run_agent_query, check_ollama_health
from bsl.table_definitions import TABLE_TYPE_REGISTRY
from models import (
    # ... existing imports ...
    BSLTenantCatalog, BSLModelCatalog, BSLDimensionInfo, BSLMeasureInfo,
    BSLQueryRequest, BSLQueryResponse, BSLQueryFilter,
    AskRequest, AskResponse, ToolCallInfo,
    OllamaHealthResponse,
)
```

Add these endpoint groups:

```python
# ═══════════════════════════════════════════════════════════════════
# BSL CATALOG ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.get("/bsl/{tenant_slug}/catalog", response_model=BSLTenantCatalog)
def get_bsl_catalog(tenant_slug: str):
    """
    Returns the full semantic catalog for a tenant.
    
    Lists all available models with their dimensions and measures.
    This endpoint powers the frontend's model/dimension/measure dropdowns.
    """
    try:
        models = build_tenant_models(tenant_slug)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build models: {e}")
    
    catalog_models = []
    for suffix, model in models.items():
        table_def = TABLE_TYPE_REGISTRY.get(suffix)
        if not table_def:
            continue
        
        dims = model.get_dimensions()
        measures = model.get_measures()
        
        dim_list = []
        if isinstance(dims, dict):
            for name, dim_obj in dims.items():
                dim_list.append(BSLDimensionInfo(
                    name=name,
                    description=getattr(dim_obj, 'description', '') or '',
                ))
        
        measure_list = []
        if isinstance(measures, dict):
            for name, measure_obj in measures.items():
                measure_list.append(BSLMeasureInfo(
                    name=name,
                    description=getattr(measure_obj, 'description', '') or '',
                ))
        
        catalog_models.append(BSLModelCatalog(
            model_name=suffix,
            label=table_def.label,
            description=table_def.description,
            dimensions=dim_list,
            measures=measure_list,
        ))
    
    return BSLTenantCatalog(
        tenant_slug=tenant_slug,
        models=catalog_models,
    )


@app.get("/bsl/{tenant_slug}/models/{model_name}/dimensions")
def get_bsl_dimensions(tenant_slug: str, model_name: str):
    """Get dimensions for a specific model."""
    try:
        model = get_tenant_model(tenant_slug, model_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    dims = model.get_dimensions()
    if isinstance(dims, dict):
        return {name: {"description": getattr(d, 'description', '') or ''} for name, d in dims.items()}
    return {}


@app.get("/bsl/{tenant_slug}/models/{model_name}/measures")
def get_bsl_measures(tenant_slug: str, model_name: str):
    """Get measures for a specific model."""
    try:
        model = get_tenant_model(tenant_slug, model_name)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    measures = model.get_measures()
    if isinstance(measures, dict):
        return {name: {"description": getattr(m, 'description', '') or ''} for name, m in measures.items()}
    return {}


# ═══════════════════════════════════════════════════════════════════
# BSL QUERY ENDPOINT
# ═══════════════════════════════════════════════════════════════════

@app.post("/bsl/{tenant_slug}/query", response_model=BSLQueryResponse)
def execute_bsl_query(tenant_slug: str, request: BSLQueryRequest):
    """
    Execute a structured semantic query using BSL.
    
    This replaces the old QueryBuilder SQL string approach with
    BSL's native Ibis-based query compilation. Queries go through:
    
      BSL SemanticModel → Ibis expression → DuckDB SQL → results
    
    The frontend sends dimension/measure names (from the catalog endpoint),
    BSL handles all SQL generation, type casting, and join resolution.
    """
    try:
        model = get_tenant_model(tenant_slug, request.model)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    
    query = model
    
    # Apply filters
    if request.filters:
        dims = model.get_dimensions()
        for f in request.filters:
            if isinstance(dims, dict) and f.field in dims:
                dim_obj = dims[f.field]
                if f.op == "=":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) == v)
                elif f.op == "!=":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) != v)
                elif f.op == ">":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) > v)
                elif f.op == ">=":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) >= v)
                elif f.op == "<":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) < v)
                elif f.op == "<=":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t) <= v)
                elif f.op == "contains":
                    query = query.filter(lambda t, d=dim_obj, v=f.value: d.expr(t).contains(v))
    
    # Apply aggregation
    try:
        if request.measures:
            if request.dimensions:
                query = query.group_by(*request.dimensions).aggregate(*request.measures)
            else:
                query = query.aggregate(*request.measures)
        
        # Execute
        result_table = query.as_table()
        if request.limit:
            result_table = result_table.limit(request.limit)
        
        df = result_table.execute()
        records = df.to_dict(orient="records") if hasattr(df, 'to_dict') else []
        
        return BSLQueryResponse(
            model=request.model,
            data=records,
            row_count=len(records),
            dimensions_used=request.dimensions,
            measures_used=request.measures,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Query execution error: {e}")


# ═══════════════════════════════════════════════════════════════════
# AGENT ENDPOINTS (Natural Language)
# ═══════════════════════════════════════════════════════════════════

@app.post("/bsl/{tenant_slug}/ask", response_model=AskResponse)
async def ask_agent(tenant_slug: str, request: AskRequest):
    """
    Ask a natural language question about a tenant's data.
    
    Uses Ollama Qwen2.5-Coder 14B with BSL tools to:
    1. Understand the question
    2. Discover available models/dimensions/measures
    3. Build and execute the right query
    4. Return a natural language answer with the data
    
    Requires: Ollama running locally with qwen2.5-coder:14b pulled.
    Gracefully returns an error if Ollama is unavailable.
    """
    try:
        models = build_tenant_models(tenant_slug)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to build models: {e}")
    
    history = [
        {"role": msg.role, "content": msg.content}
        for msg in request.conversation_history
    ]
    
    result = await run_agent_query(
        question=request.question,
        models=models,
        conversation_history=history,
    )
    
    return AskResponse(
        answer=result.get("answer"),
        tool_calls=[ToolCallInfo(**tc) for tc in result.get("tool_calls", [])],
        data=result.get("data"),
        error=result.get("error"),
    )


# ═══════════════════════════════════════════════════════════════════
# HEALTH / ADMIN ENDPOINTS
# ═══════════════════════════════════════════════════════════════════

@app.get("/bsl/health/ollama", response_model=OllamaHealthResponse)
def ollama_health():
    """Check Ollama server connectivity and model availability."""
    result = check_ollama_health()
    return OllamaHealthResponse(**result)


@app.get("/bsl/tenants")
def list_tenants():
    """List all tenants with analytics data in the warehouse."""
    return {"tenants": list_available_tenants()}


@app.post("/bsl/{tenant_slug}/cache/invalidate")
def invalidate_cache(tenant_slug: str):
    """
    Invalidate cached BSL models for a tenant.
    
    Call this after tenant config changes or dbt runs to force
    model rebuilding on the next request.
    """
    invalidate_tenant_cache(tenant_slug)
    return {"status": "ok", "message": f"Cache invalidated for {tenant_slug}"}
```

**Also update** the existing `_get_db_connection()` helper to use the shared
connection factory:

```python
# At the top of main.py, replace the existing _get_db_connection with:
from bsl.connection import get_raw_duckdb_connection

def _get_db_connection():
    return get_raw_duckdb_connection()
```

**Verification**: Start the API and test each endpoint group:

```bash
cd services/platform-api
uvicorn main:app --port 8001 --reload

# Test catalog
curl http://localhost:8001/bsl/tyrell_corp/catalog

# Test structured query
curl -X POST http://localhost:8001/bsl/tyrell_corp/query \
  -H "Content-Type: application/json" \
  -d '{"model": "ad_performance", "dimensions": ["source_platform"], "measures": ["total_spend", "total_clicks"]}'

# Test Ollama health
curl http://localhost:8001/bsl/health/ollama

# Test agent (requires Ollama running)
curl -X POST http://localhost:8001/bsl/tyrell_corp/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "What was the total ad spend by platform?"}'
```

---

### PROMPT 6: Tests

**Goal**: Write tests for the BSL integration covering model building, query
execution, and agent fallback behavior.

**Files to create**:

- `services/platform-api/test_bsl.py`

**Instructions**:

```python
"""
Tests for BSL (Boring Semantic Layer) integration.

Tests are organized by layer:
1. Connection tests — verify Ibis backend connects
2. Model builder tests — verify SemanticModel construction per tenant
3. Query tests — verify structured query execution
4. Catalog tests — verify dimension/measure discovery
5. Agent tests — verify Ollama health check and graceful degradation

Tests use the same MotherDuck connection as the API (set MOTHERDUCK_TOKEN env var)
or local sandbox (set GATA_ENV=local).
"""
import pytest
from fastapi.testclient import TestClient
from main import app

client = TestClient(app)

TENANT = "tyrell_corp"  # Known test tenant with data


# ── Catalog Tests ──────────────────────────────────────────────────

class TestCatalog:
    def test_get_catalog_returns_all_models(self):
        resp = client.get(f"/bsl/{TENANT}/catalog")
        assert resp.status_code == 200
        data = resp.json()
        assert data["tenant_slug"] == TENANT
        assert len(data["models"]) == 6  # 4 facts + 2 dims
        model_names = {m["model_name"] for m in data["models"]}
        assert "ad_performance" in model_names
        assert "orders" in model_names
        assert "sessions" in model_names
        assert "events" in model_names
        assert "campaigns" in model_names
        assert "users" in model_names

    def test_each_model_has_dimensions(self):
        resp = client.get(f"/bsl/{TENANT}/catalog")
        for model in resp.json()["models"]:
            assert len(model["dimensions"]) > 0, f"{model['model_name']} has no dimensions"

    def test_fact_models_have_measures(self):
        resp = client.get(f"/bsl/{TENANT}/catalog")
        fact_models = [m for m in resp.json()["models"] if m["model_name"].startswith(("ad_", "order", "session", "event"))]
        for model in fact_models:
            assert len(model["measures"]) > 0, f"{model['model_name']} has no measures"

    def test_get_dimensions_for_model(self):
        resp = client.get(f"/bsl/{TENANT}/models/ad_performance/dimensions")
        assert resp.status_code == 200
        dims = resp.json()
        assert "source_platform" in dims
        assert "report_date" in dims

    def test_get_measures_for_model(self):
        resp = client.get(f"/bsl/{TENANT}/models/ad_performance/measures")
        assert resp.status_code == 200
        measures = resp.json()
        assert "total_spend" in measures
        assert "total_clicks" in measures

    def test_unknown_model_returns_404(self):
        resp = client.get(f"/bsl/{TENANT}/models/nonexistent/dimensions")
        assert resp.status_code == 404

    def test_unknown_tenant_returns_error(self):
        resp = client.get("/bsl/fake_tenant/catalog")
        # Should return 200 with empty models or 500 — depends on MotherDuck
        assert resp.status_code in (200, 500)


# ── Structured Query Tests ─────────────────────────────────────────

class TestQuery:
    def test_basic_aggregation(self):
        resp = client.post(f"/bsl/{TENANT}/query", json={
            "model": "ad_performance",
            "dimensions": ["source_platform"],
            "measures": ["total_spend"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] > 0
        assert "source_platform" in data["data"][0]
        assert "total_spend" in data["data"][0]

    def test_measures_only_no_dimensions(self):
        resp = client.post(f"/bsl/{TENANT}/query", json={
            "model": "ad_performance",
            "measures": ["total_spend", "total_clicks"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["row_count"] == 1  # Single aggregate row

    def test_query_with_filter(self):
        resp = client.post(f"/bsl/{TENANT}/query", json={
            "model": "ad_performance",
            "dimensions": ["source_platform"],
            "measures": ["total_spend"],
            "filters": [{"field": "source_platform", "op": "=", "value": "facebook_ads"}],
        })
        assert resp.status_code == 200
        data = resp.json()
        for row in data["data"]:
            assert row["source_platform"] == "facebook_ads"

    def test_query_respects_limit(self):
        resp = client.post(f"/bsl/{TENANT}/query", json={
            "model": "events",
            "dimensions": ["event_name"],
            "measures": ["event_count"],
            "limit": 3,
        })
        assert resp.status_code == 200
        assert resp.json()["row_count"] <= 3

    def test_unknown_model_returns_404(self):
        resp = client.post(f"/bsl/{TENANT}/query", json={
            "model": "nonexistent",
            "measures": ["total_spend"],
        })
        assert resp.status_code == 404


# ── Agent/Ollama Tests ─────────────────────────────────────────────

class TestAgent:
    def test_ollama_health_endpoint(self):
        resp = client.get("/bsl/health/ollama")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] in ("healthy", "unhealthy", "unavailable")

    def test_ask_returns_response_structure(self):
        resp = client.post(f"/bsl/{TENANT}/ask", json={
            "question": "What is the total ad spend?",
        })
        assert resp.status_code == 200
        data = resp.json()
        # Should have either an answer or an error (if Ollama not running)
        assert "answer" in data
        assert "error" in data
        assert "tool_calls" in data


# ── Cache Management Tests ─────────────────────────────────────────

class TestCache:
    def test_invalidate_cache(self):
        resp = client.post(f"/bsl/{TENANT}/cache/invalidate")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_list_tenants(self):
        resp = client.get("/bsl/tenants")
        assert resp.status_code == 200
        tenants = resp.json()["tenants"]
        assert TENANT in tenants


# ── Backward Compatibility Tests ───────────────────────────────────

class TestBackwardCompat:
    """Ensure old endpoints still work after BSL additions."""
    
    def test_old_semantic_config_endpoint(self):
        resp = client.get(f"/semantic-layer/{TENANT}/config")
        assert resp.status_code == 200

    def test_old_models_endpoint(self):
        resp = client.get(f"/semantic-layer/{TENANT}/models")
        assert resp.status_code == 200
```

**Verification**: `cd services/platform-api && pytest test_bsl.py -v`

---

### PROMPT 7: Docker + Environment Setup

**Goal**: Add Ollama to the docker-compose and update environment configuration.

**Files to modify**:

- `docker-compose.yml` (project root) — add Ollama service
- `.env` (project root) — add Ollama env vars
- `services/platform-api/pyproject.toml` — final dependency check

**Instructions**:

1. Add Ollama service to `docker-compose.yml`:

```yaml
  ollama:
    image: ollama/ollama:latest
    container_name: gata-ollama
    ports:
      - "11434:11434"
    volumes:
      - ollama_data:/root/.ollama
    # GPU support (uncomment if NVIDIA GPU available):
    # deploy:
    #   resources:
    #     reservations:
    #       devices:
    #         - driver: nvidia
    #           count: 1
    #           capabilities: [gpu]

volumes:
  ollama_data:
```

2. Add to `.env`:

```bash
# Ollama (Semantic Layer Agent)
OLLAMA_BASE_URL=http://localhost:11434
OLLAMA_MODEL=qwen2.5-coder:14b
```

3. Create `scripts/setup_ollama.sh` (or `.ps1` for Windows):

```bash
#!/bin/bash
# Setup script for Ollama + Qwen2.5-Coder 14B
# Run this once after installing Ollama

echo "Pulling Qwen2.5-Coder 14B model (~9GB download)..."
ollama pull qwen2.5-coder:14b

echo "Verifying model is available..."
ollama list | grep qwen2.5-coder

echo "Testing model responds..."
echo '{"model":"qwen2.5-coder:14b","messages":[{"role":"user","content":"SELECT 1"}],"stream":false}' | \
  curl -s http://localhost:11434/api/chat -d @- | head -c 200

echo ""
echo "Setup complete! Model ready for BSL agent."
```

4. Create `scripts/setup_ollama.ps1` (Windows equivalent):

```powershell
# Setup script for Ollama + Qwen2.5-Coder 14B (Windows)
Write-Host "Pulling Qwen2.5-Coder 14B model (~9GB download)..."
ollama pull qwen2.5-coder:14b

Write-Host "Verifying model is available..."
ollama list

Write-Host "Testing model responds..."
$body = '{"model":"qwen2.5-coder:14b","messages":[{"role":"user","content":"SELECT 1"}],"stream":false}'
Invoke-RestMethod -Uri "http://localhost:11434/api/chat" -Method Post -Body $body -ContentType "application/json"

Write-Host "Setup complete! Model ready for BSL agent."
```

---

### PROMPT 8: CLAUDE.md Update + Cleanup

**Goal**: Update project documentation to reflect the BSL integration, remove
references to the old QueryBuilder as primary approach, and document the new
endpoint structure.

**File to modify**: `CLAUDE.md`

**Instructions**:

Add a new section after the "BSL Semantic Configs" section:

```markdown
## BSL Integration (Boring Semantic Layer)

The platform uses the `boring-semantic-layer` library for semantic query
compilation. Instead of the custom QueryBuilder that generates raw SQL strings,
BSL compiles queries through Ibis expressions → DuckDB SQL, with proper type
handling and join resolution.

### Architecture
```

Tenant YAML + MotherDuck Schema ↓ BSL SemanticModel (per tenant, 6 models each,
cached in memory) ├── .get_dimensions() → catalog API ├── .get_measures() →
catalog API ├── .group_by().aggregate() → structured query API └── Ollama agent
→ natural language query API

````
### Key Files

| What | Where |
|------|-------|
| BSL package | `services/platform-api/bsl/` |
| Connection factory | `services/platform-api/bsl/connection.py` |
| Model builder | `services/platform-api/bsl/model_builder.py` |
| Table definitions | `services/platform-api/bsl/table_definitions.py` |
| Ollama agent | `services/platform-api/bsl/agent.py` |
| Agent config | `services/platform-api/bsl/config.py` |
| BSL tests | `services/platform-api/test_bsl.py` |

### API Endpoints (BSL)

| Method | Path | Description |
|--------|------|-------------|
| GET | `/bsl/{tenant}/catalog` | Full semantic catalog (all models, dims, measures) |
| GET | `/bsl/{tenant}/models/{model}/dimensions` | Dimensions for a model |
| GET | `/bsl/{tenant}/models/{model}/measures` | Measures for a model |
| POST | `/bsl/{tenant}/query` | Structured query execution |
| POST | `/bsl/{tenant}/ask` | Natural language query (Ollama) |
| GET | `/bsl/health/ollama` | Ollama server health check |
| GET | `/bsl/tenants` | List tenants with analytics data |
| POST | `/bsl/{tenant}/cache/invalidate` | Clear cached models |

### LLM Configuration

Uses Ollama with Qwen2.5-Coder 14B for natural language queries.
The agent is optional — structured query endpoints work without Ollama.

```bash
# Install Ollama (one-time)
# Windows: Download from https://ollama.com/download
# macOS: brew install ollama
# Linux: curl -fsSL https://ollama.com/install.sh | sh

# Pull model (one-time, ~9GB)
ollama pull qwen2.5-coder:14b

# Start server (must be running for /ask endpoint)
ollama serve
````

Environment variables (in `.env`):

- `OLLAMA_BASE_URL` — Ollama server URL (default: `http://localhost:11434`)
- `OLLAMA_MODEL` — Model name (default: `qwen2.5-coder:14b`)

```
---

## Execution Order & Dependencies
```

PROMPT 1 (Dependencies) ↓ PROMPT 2 (Model Builder) ← Core: BSL SemanticModel
construction ↓ PROMPT 3 (Agent) ← Depends on: model_builder for SemanticModel
objects ↓ PROMPT 4 (Pydantic Models) ← Standalone, can run in parallel with 2-3
↓ PROMPT 5 (Endpoints) ← Depends on: ALL of 1-4 ↓ PROMPT 6 (Tests) ← Depends on:
5 ↓ PROMPT 7 (Docker/Env) ← Standalone infra setup ↓ PROMPT 8 (Documentation) ←
Final cleanup

```
## Risk Areas & Mitigations

1. **BSL API surface uncertainty**: The `boring-semantic-layer` library's exact API for `.get_dimensions()`, `.get_measures()`, `Dimension()`, `Measure()` may differ from what the demo code shows. Prompt 2 should start with `from boring_semantic_layer import *; help(SemanticModel)` to verify the exact API.

2. **Ibis + MotherDuck connection pooling**: The cached `@lru_cache` Ibis connection may have issues with long-running connections. Mitigation: add a health check that recreates the connection if stale.

3. **Ollama tool calling reliability**: Qwen2.5-Coder 14B may not reliably produce structured tool calls. Mitigation: the agent in Prompt 3 uses a simple parse-based approach rather than relying on LangChain's native tool calling, and falls back gracefully.

4. **Join column name collisions**: When BSL joins fact + dim tables, column names may collide (e.g., both have `source_platform`). The dlthub demo handles this with column prefixing. We may need the same — test in Prompt 2.

5. **Calculated measures gap**: The existing YAML configs define calculated measures (CTR, CPC, AOV) as raw SQL expressions. BSL doesn't support raw SQL in measures — these need to be expressed as Ibis lambdas that combine multiple measures. This is a follow-up after the core 8 prompts work.
```

# BSL Integration — Code Examples for Claude Code

These are copy-paste-ready code examples based on the actual
`boring-semantic-layer==0.3.7` source code. Do NOT attempt to read the BSL
GitHub repo — everything you need is here.

---

## 1. BSL YAML Format (What a Generated Config Looks Like)

This is what `bsl_configs/tyrell_corp.bsl.yaml` should look like. BSL's
`from_yaml()` parses this format directly. **Order matters: dimension models
must come before fact models that join to them.**

```yaml
# bsl_configs/tyrell_corp.bsl.yaml

# --- Dimension models first ---

campaigns:
    table: dim_tyrell_corp__campaigns
    description: "Campaign dimension with name and status across ad platforms."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source ad platform"
        campaign_id:
            expr: "_.campaign_id"
            description: "Campaign identifier"
            is_entity: true
        campaign_name:
            expr: "_.campaign_name"
            description: "Campaign name"
        campaign_status:
            expr: "_.campaign_status"
            description: "Campaign status"
    measures: {}

users:
    table: dim_tyrell_corp__users
    description: "Unified user dimension combining analytics and ecommerce identities."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source platform"
        user_pseudo_id:
            expr: "_.user_pseudo_id"
            description: "Anonymous user ID"
            is_entity: true
        customer_email:
            expr: "_.customer_email"
            description: "Customer email"
        customer_id:
            expr: "_.customer_id"
            description: "Customer ID"
        is_customer:
            expr: "_.is_customer"
            description: "Whether user has made a purchase"
        first_seen_at:
            expr: "_.first_seen_at"
            description: "First seen timestamp"
        last_seen_at:
            expr: "_.last_seen_at"
            description: "Last seen timestamp"
        first_geo_country:
            expr: "_.first_geo_country"
            description: "First observed country"
        first_device_category:
            expr: "_.first_device_category"
            description: "First observed device type"
    measures:
        total_events:
            expr: "_.total_events.sum()"
            description: "Total events across all users"
        total_sessions:
            expr: "_.total_sessions.sum()"
            description: "Total sessions across all users"

# --- Fact models (reference dimension models above) ---

ad_performance:
    table: fct_tyrell_corp__ad_performance
    description: "Daily ad spend and engagement metrics across all ad platforms."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source ad platform"
        report_date:
            expr: "_.report_date"
            description: "Report date"
            is_time_dimension: true
            smallest_time_grain: "TIME_GRAIN_DAY"
        campaign_id:
            expr: "_.campaign_id"
            description: "Campaign identifier"
        ad_group_id:
            expr: "_.ad_group_id"
            description: "Ad group identifier"
        ad_id:
            expr: "_.ad_id"
            description: "Ad identifier"
    measures:
        total_spend:
            expr: "_.spend.sum()"
            description: "Total ad spend"
        total_impressions:
            expr: "_.impressions.sum()"
            description: "Total impressions"
        total_clicks:
            expr: "_.clicks.sum()"
            description: "Total clicks"
        total_conversions:
            expr: "_.conversions.sum()"
            description: "Total conversions"
    joins:
        campaigns:
            model: campaigns
            type: many
            left_on: campaign_id
            right_on: campaign_id

orders:
    table: fct_tyrell_corp__orders
    description: "Ecommerce transactions with customer and financial details."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source ecommerce platform"
        order_date:
            expr: "_.order_date"
            description: "Order timestamp"
            is_time_dimension: true
            smallest_time_grain: "TIME_GRAIN_DAY"
        currency:
            expr: "_.currency"
            description: "Order currency"
        financial_status:
            expr: "_.financial_status"
            description: "Payment status"
        customer_email:
            expr: "_.customer_email"
            description: "Customer email"
        customer_id:
            expr: "_.customer_id"
            description: "Customer ID"
    measures:
        total_revenue:
            expr: "_.total_price.sum()"
            description: "Total revenue"
        order_count:
            expr: "_.order_id.nunique()"
            description: "Unique order count"
    joins:
        users:
            model: users
            type: many
            left_on: customer_email
            right_on: customer_email

sessions:
    table: fct_tyrell_corp__sessions
    description: "Sessionized web analytics with attribution, duration, and conversion flags."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source analytics platform"
        user_pseudo_id:
            expr: "_.user_pseudo_id"
            description: "Anonymous user ID"
        session_start_ts:
            expr: "_.session_start_ts"
            description: "Session start (epoch)"
        session_end_ts:
            expr: "_.session_end_ts"
            description: "Session end (epoch)"
        traffic_source:
            expr: "_.traffic_source"
            description: "Traffic source"
        traffic_medium:
            expr: "_.traffic_medium"
            description: "Traffic medium"
        traffic_campaign:
            expr: "_.traffic_campaign"
            description: "Traffic campaign name"
        geo_country:
            expr: "_.geo_country"
            description: "Country"
        device_category:
            expr: "_.device_category"
            description: "Device type"
        is_conversion_session:
            expr: "_.is_conversion_session"
            description: "Whether session included a conversion"
    measures:
        avg_session_duration:
            expr: "_.session_duration_seconds.mean()"
            description: "Average session duration in seconds"
        avg_events_per_session:
            expr: "_.events_in_session.mean()"
            description: "Average events per session"
        total_session_revenue:
            expr: "_.session_revenue.sum()"
            description: "Total session revenue"
        session_count:
            expr: "_.session_id.nunique()"
            description: "Unique session count"
    joins:
        users:
            model: users
            type: many
            left_on: user_pseudo_id
            right_on: user_pseudo_id

events:
    table: fct_tyrell_corp__events
    description: "Raw analytics events with attribution and optional ecommerce data."
    filter: "_.tenant_slug == 'tyrell_corp'"
    dimensions:
        source_platform:
            expr: "_.source_platform"
            description: "Source analytics platform"
        event_name:
            expr: "_.event_name"
            description: "Event name"
        event_timestamp:
            expr: "_.event_timestamp"
            description: "Event timestamp (epoch)"
        user_pseudo_id:
            expr: "_.user_pseudo_id"
            description: "Anonymous user ID"
        session_id:
            expr: "_.session_id"
            description: "Session ID"
        order_id:
            expr: "_.order_id"
            description: "Associated order ID"
        traffic_source:
            expr: "_.traffic_source"
            description: "Traffic source"
        traffic_medium:
            expr: "_.traffic_medium"
            description: "Traffic medium"
        traffic_campaign:
            expr: "_.traffic_campaign"
            description: "Traffic campaign name"
        geo_country:
            expr: "_.geo_country"
            description: "Country"
        device_category:
            expr: "_.device_category"
            description: "Device type"
    measures:
        event_count:
            expr: "_.event_timestamp.count()"
            description: "Total event count"
        total_order_value:
            expr: "_.order_total.sum()"
            description: "Total order value from events"
    joins:
        users:
            model: users
            type: many
            left_on: user_pseudo_id
            right_on: user_pseudo_id
```

---

## 2. BSL Profile File

```yaml
# services/platform-api/profiles.yml

motherduck:
    type: duckdb
    database: "md:my_db"

sandbox:
    type: duckdb
    database: "../../warehouse/sandbox.duckdb"
```

BSL's profile loader calls `ibis.duckdb.connect(database=...)` under the hood.
For MotherDuck, `MOTHERDUCK_TOKEN` must be set as an env var — Ibis DuckDB
auto-detects it.

---

## 3. Config Generator (`bsl/config_generator.py`)

```python
"""
Convert semantic_configs/{slug}.yaml → BSL-native config dict.

BSL's from_config() and from_yaml() expect this format:
{
    "model_name": {
        "table": "actual_table_name",
        "description": "...",
        "filter": "_.tenant_slug == 'slug'",
        "dimensions": {
            "dim_name": {
                "expr": "_.column_name",
                "description": "...",
                "is_time_dimension": True,        # optional
                "smallest_time_grain": "TIME_GRAIN_DAY",  # optional
                "is_entity": True,                 # optional
            }
        },
        "measures": {
            "measure_name": {
                "expr": "_.column.sum()",
                "description": "..."
            }
        },
        "joins": {
            "alias": {
                "model": "other_model_name",
                "type": "many",           # "one" or "many"
                "left_on": "fk_column",
                "right_on": "pk_column"
            }
        }
    }
}
"""

from pathlib import Path
from collections import OrderedDict
import yaml


# Map from our custom agg names → Ibis method names
AGG_MAP = {
    "sum": "sum",
    "avg": "mean",       # Ibis uses .mean() not .avg()
    "count": "count",
    "count_distinct": "nunique",
}

# Types that should be flagged as time dimensions
TIME_TYPES = {"date", "timestamp", "timestamp_epoch"}

# Entity columns (primary keys / identifiers)
ENTITY_COLUMNS = {
    "campaign_id", "ad_group_id", "ad_id", "user_pseudo_id",
    "customer_id", "customer_email", "session_id", "order_id",
}


def _clean_model_name(table_name: str, tenant_slug: str) -> str:
    """
    Strip tenant prefix from table name to get clean BSL model name.
    fct_tyrell_corp__ad_performance → ad_performance
    dim_tyrell_corp__campaigns → campaigns
    """
    prefix = f"fct_{tenant_slug}__"
    if table_name.startswith(prefix):
        return table_name[len(prefix):]
    prefix = f"dim_{tenant_slug}__"
    if table_name.startswith(prefix):
        return table_name[len(prefix):]
    return table_name


def _build_dimension(dim: dict) -> dict:
    """Convert a dimension spec {name, type} → BSL dimension config."""
    result = {
        "expr": f"_.{dim['name']}",
        "description": dim.get("label", dim["name"].replace("_", " ").title()),
    }

    dim_type = dim.get("type", "string")

    if dim_type in TIME_TYPES:
        result["is_time_dimension"] = True
        result["smallest_time_grain"] = "TIME_GRAIN_DAY"

    if dim["name"] in ENTITY_COLUMNS:
        result["is_entity"] = True

    return result


def _build_measure(measure: dict) -> dict:
    """Convert a measure spec {name, type, agg} → BSL measure config."""
    agg = measure.get("agg", "sum")
    ibis_method = AGG_MAP.get(agg, "sum")
    col_name = measure["name"]

    return {
        "expr": f"_.{col_name}.{ibis_method}()",
        "description": measure.get("label", f"Total {col_name.replace('_', ' ')}"),
    }


def _build_measure_name(measure: dict) -> str:
    """Generate a descriptive measure name to avoid collision with dimension names."""
    agg = measure.get("agg", "sum")
    col = measure["name"]

    # Prefix with aggregation type
    prefix_map = {
        "sum": "total",
        "avg": "avg",
        "count": "total",
        "count_distinct": "unique",
    }
    prefix = prefix_map.get(agg, "total")

    # Avoid stuttering like "total_total_price"
    if col.startswith(prefix):
        return col
    return f"{prefix}_{col}"


def generate_bsl_config(semantic_config_path: Path, tenant_slug: str) -> OrderedDict:
    """
    Read a semantic_configs/{slug}.yaml and produce a BSL-compatible
    config dict. Returns an OrderedDict with dimension models first.
    """
    with open(semantic_config_path) as f:
        config = yaml.safe_load(f)

    models_list = config.get("models", [])

    # Separate dims and facts (dims must come first for join resolution)
    dim_models = []
    fact_models = []
    for m in models_list:
        if m["name"].startswith("dim_"):
            dim_models.append(m)
        else:
            fact_models.append(m)

    # Build name mapping for join resolution
    # Original table name → clean BSL name
    name_map = {}
    for m in models_list:
        clean = _clean_model_name(m["name"], tenant_slug)
        name_map[m["name"]] = clean

    bsl_config = OrderedDict()

    for model_spec in dim_models + fact_models:
        table_name = model_spec["name"]
        clean_name = name_map[table_name]

        # Dimensions
        dimensions = {}
        for dim in model_spec.get("dimensions", []):
            dimensions[dim["name"]] = _build_dimension(dim)

        # Measures
        measures = {}
        for measure in model_spec.get("measures", []):
            measure_key = _build_measure_name(measure)
            measures[measure_key] = _build_measure(measure)

        # Joins
        joins = {}
        for join_spec in model_spec.get("joins", []):
            join_table = join_spec["to"]  # e.g., "dim_tyrell_corp__campaigns"
            join_clean = name_map.get(join_table, join_table)

            on_mapping = join_spec.get("on", {})
            on_keys = list(on_mapping.items())

            if len(on_keys) == 1:
                left_col, right_col = on_keys[0]
                joins[join_clean] = {
                    "model": join_clean,
                    "type": "many",  # fact→dim is always many-to-one
                    "left_on": left_col,
                    "right_on": right_col,
                }
            elif len(on_keys) > 1:
                # BSL supports single-key joins only
                # Use first key for join, document the limitation
                left_col, right_col = on_keys[0]
                joins[join_clean] = {
                    "model": join_clean,
                    "type": "many",
                    "left_on": left_col,
                    "right_on": right_col,
                }

        model_config = OrderedDict()
        model_config["table"] = table_name
        if model_spec.get("description"):
            model_config["description"] = model_spec["description"]
        model_config["filter"] = f"_.tenant_slug == '{tenant_slug}'"
        model_config["dimensions"] = dimensions
        model_config["measures"] = measures
        if joins:
            model_config["joins"] = joins

        bsl_config[clean_name] = model_config

    return bsl_config


def write_bsl_yaml(config: OrderedDict, output_path: Path) -> None:
    """Write BSL config to YAML file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Use default_flow_style=False for readable YAML
    # sort_keys=False preserves our OrderedDict ordering
    with open(output_path, "w") as f:
        yaml.safe_dump(
            dict(config),  # yaml.safe_dump needs regular dict
            f,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
```

---

## 4. Tenant Model Loader (`bsl/tenant_models.py`)

```python
"""
Load and cache BSL SemanticModel objects per tenant.

Key API:
  get_tenant_models("tyrell_corp")
    → {"ad_performance": SemanticModel, "campaigns": SemanticModel, ...}

BSL's from_yaml() does:
  1. Read YAML config
  2. Connect to database via profile
  3. Load tables as Ibis table expressions
  4. Parse dimension/measure expressions (Ibis deferred: _ syntax)
  5. Apply filters
  6. Wire up joins
  7. Return dict[str, SemanticModel]
"""

import os
import yaml
from pathlib import Path

from boring_semantic_layer import from_yaml, from_config
from boring_semantic_layer.profile import get_connection

from bsl.config_generator import generate_bsl_config, write_bsl_yaml


_model_cache: dict[str, dict] = {}
_connection = None


def _get_profile_name() -> str:
    """Determine which BSL profile to use."""
    if os.environ.get("GATA_ENV") == "local":
        return "sandbox"
    return "motherduck"


def _get_profile_path() -> Path:
    return Path(__file__).parent.parent / "profiles.yml"


def get_db_connection():
    """Get or create the shared Ibis DuckDB/MotherDuck connection."""
    global _connection
    if _connection is None:
        _connection = get_connection(
            _get_profile_name(),
            profile_file=str(_get_profile_path()),
        )
    return _connection


def get_tenant_models(tenant_slug: str) -> dict:
    """
    Get BSL SemanticModel objects for a tenant.

    Returns dict mapping clean model names to SemanticModel instances.
    Example: {"ad_performance": <SemanticModel>, "campaigns": <SemanticModel>}

    Models are cached — call invalidate_cache() to force reload.
    """
    if tenant_slug in _model_cache:
        return _model_cache[tenant_slug]

    # Step 1: Generate BSL config from our semantic config
    semantic_path = Path(__file__).parent.parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not semantic_path.exists():
        raise FileNotFoundError(f"No semantic config for tenant: {tenant_slug}")

    bsl_config = generate_bsl_config(semantic_path, tenant_slug)

    # Step 2: Write BSL YAML (for agent/debugging use)
    bsl_yaml_path = Path(__file__).parent.parent / "bsl_configs" / f"{tenant_slug}.bsl.yaml"
    write_bsl_yaml(bsl_config, bsl_yaml_path)

    # Step 3: Load models via BSL
    # Option A: from_yaml with profile (lets BSL handle connection)
    models = from_yaml(
        str(bsl_yaml_path),
        profile=_get_profile_name(),
        profile_path=str(_get_profile_path()),
    )

    _model_cache[tenant_slug] = models
    return models


def invalidate_cache(tenant_slug: str | None = None):
    """Clear model cache. Call when tenant config changes."""
    if tenant_slug:
        _model_cache.pop(tenant_slug, None)
    else:
        _model_cache.clear()


def list_tenants() -> list[str]:
    """Get active tenant slugs from tenants.yaml."""
    tenants_path = Path(__file__).parent.parent.parent.parent / "tenants.yaml"
    with open(tenants_path) as f:
        config = yaml.safe_load(f)
    return [t["slug"] for t in config.get("tenants", [])]
```

---

## 5. Structured Query (`bsl/structured_query.py`)

```python
"""
Execute structured queries against BSL SemanticModel objects.

BSL's SemanticModel has a .query() method that accepts:
  - dimensions: list[str] - column names to group by
  - measures: list[str] - measure names to aggregate
  - filters: list[dict] - each dict has {dimension, operator, value}
  - order_by: list[list[str]] - each is [column, "asc"/"desc"]
  - limit: int
  - time_grain: str - "day", "week", "month", "year"
  - time_range: dict - {"start": "2024-01-01", "end": "2024-12-31"}

The .query() method returns an Ibis query expression.
Call .execute() on it to get a pandas DataFrame.
"""

import json


# Calculated measure formulas (applied post-query in Python)
CALCULATED_MEASURES = {
    "ctr": {
        "label": "Click-Through Rate",
        "requires": ["total_clicks", "total_impressions"],
        "compute": lambda r: (
            r["total_clicks"] / r["total_impressions"]
            if r.get("total_impressions", 0) > 0 else 0
        ),
        "format": "percent",
    },
    "cpc": {
        "label": "Cost Per Click",
        "requires": ["total_spend", "total_clicks"],
        "compute": lambda r: (
            r["total_spend"] / r["total_clicks"]
            if r.get("total_clicks", 0) > 0 else 0
        ),
        "format": "currency",
    },
    "cpm": {
        "label": "Cost Per Mille",
        "requires": ["total_spend", "total_impressions"],
        "compute": lambda r: (
            r["total_spend"] * 1000 / r["total_impressions"]
            if r.get("total_impressions", 0) > 0 else 0
        ),
        "format": "currency",
    },
    "aov": {
        "label": "Average Order Value",
        "requires": ["total_revenue", "order_count"],
        "compute": lambda r: (
            r["total_revenue"] / r["order_count"]
            if r.get("order_count", 0) > 0 else 0
        ),
        "format": "currency",
    },
    "conversion_rate": {
        "label": "Session Conversion Rate",
        "requires": ["session_count"],
        "compute": lambda r: (
            r.get("conversion_sessions", 0) / r["session_count"]
            if r.get("session_count", 0) > 0 else 0
        ),
        "format": "percent",
    },
}


def execute_structured_query(
    model,
    dimensions: list[str] | None = None,
    measures: list[str] | None = None,
    filters: list[dict] | None = None,
    order_by: list[list[str]] | None = None,
    limit: int | None = None,
    time_grain: str | None = None,
    time_range: dict | None = None,
) -> dict:
    """
    Execute a structured query against a BSL SemanticModel.

    Args:
        model: BSL SemanticModel instance
        dimensions: Column names to group by
        measures: Measure names to aggregate
        filters: List of {dimension, operator, value} dicts
        order_by: List of [column, direction] pairs
        limit: Max rows
        time_grain: Truncation grain for time dimensions
        time_range: Date range filter

    Returns:
        {
            "records": [{"dim1": val, "measure1": val}, ...],
            "columns": [{"name": "dim1", "type": "string"}, ...],
            "row_count": 42,
        }
    """
    # BSL's model.query() builds an Ibis expression
    query_result = model.query(
        dimensions=dimensions,
        measures=measures,
        filters=filters or [],
        order_by=order_by,
        limit=limit,
        time_grain=time_grain,
        time_range=time_range,
    )

    # Execute against the database → pandas DataFrame
    df = query_result.execute()

    # Convert to records
    # Handle special types (dates, timestamps, etc.)
    records = []
    for _, row in df.iterrows():
        record = {}
        for col in df.columns:
            val = row[col]
            # Convert numpy/pandas types to JSON-serializable Python types
            if hasattr(val, "isoformat"):
                record[col] = val.isoformat()
            elif hasattr(val, "item"):
                record[col] = val.item()  # numpy scalar → python scalar
            else:
                record[col] = val
        records.append(record)

    # Build column metadata
    columns = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        if "int" in dtype:
            col_type = "integer"
        elif "float" in dtype:
            col_type = "number"
        elif "bool" in dtype:
            col_type = "boolean"
        elif "datetime" in dtype:
            col_type = "timestamp"
        else:
            col_type = "string"
        columns.append({"name": col, "type": col_type})

    return {
        "records": records,
        "columns": columns,
        "row_count": len(records),
    }


def apply_calculated_measures(
    result: dict,
    requested_measures: list[str],
) -> dict:
    """
    Post-compute calculated measures on query results.

    Modifies result["records"] in place, adding computed fields.
    Also adds column metadata for new fields.
    """
    for measure_name in requested_measures:
        formula = CALCULATED_MEASURES.get(measure_name)
        if not formula:
            continue

        # Check required base measures exist in records
        if not result["records"]:
            continue

        sample = result["records"][0]
        missing = [r for r in formula["requires"] if r not in sample]
        if missing:
            continue  # Skip if base measures weren't requested

        # Compute for each row
        for record in result["records"]:
            record[measure_name] = round(formula["compute"](record), 6)

        # Add column metadata
        result["columns"].append({
            "name": measure_name,
            "type": "number",
        })

    return result
```

---

## 6. Ollama Agent (`bsl/agent.py`)

```python
"""
BSL agent using Ollama Qwen2.5-Coder-14B for natural language queries.

Architecture:
  1. BSLTools wraps the tenant's SemanticModel objects
  2. BSLTools.get_callable_tools() returns LangChain StructuredTool objects
  3. ChatOllama binds those tools and runs a tool-calling loop
  4. Agent autonomously calls list_models → get_model → query_model

BSLTools provides 4 tools:
  - list_models(): Returns model names + descriptions
  - get_model(model_name): Returns dimensions, measures, descriptions
  - query_model(query): Executes an Ibis expression string
  - get_documentation(topic): Returns BSL query syntax docs

The query_model tool accepts Ibis DSL strings like:
  "ad_performance.group_by('source_platform').aggregate('total_spend')"

The LLM learns the syntax from get_documentation and constructs queries.
"""

import json
import os
from pathlib import Path

import httpx
from boring_semantic_layer.agents.tools import BSLTools


# --- Agent cache ---
_agent_cache: dict[str, "TenantAgent"] = {}


class TenantAgent:
    """BSL agent per tenant using Ollama Qwen2.5-Coder-14B."""

    def __init__(
        self,
        tenant_slug: str,
        bsl_yaml_path: Path,
        profile_path: Path,
    ):
        self.tenant_slug = tenant_slug
        profile_name = "sandbox" if os.environ.get("GATA_ENV") == "local" else "motherduck"

        # BSLTools loads models from YAML and provides tool definitions
        self.bsl_tools = BSLTools(
            model_path=bsl_yaml_path,
            profile=profile_name,
            profile_file=profile_path,
            chart_backend="echarts",  # Returns ECharts JSON specs
        )

        # LangChain-compatible callable tools
        self.tools = self.bsl_tools.get_callable_tools()

        # Conversation history for multi-turn
        self.conversation_history: list = []

    def ask(self, question: str) -> dict:
        """
        Process a natural language question using Ollama + BSLTools.

        Returns:
            {
                "answer": "Based on the data...",
                "records": [...],       # if query_model was called
                "chart": {...},          # ECharts JSON spec if applicable
                "tool_calls": [...]      # transparency log
            }
        """
        from langchain_ollama import ChatOllama
        from langchain_core.messages import HumanMessage, AIMessage, ToolMessage

        llm = ChatOllama(
            model="qwen2.5-coder:14b",
            temperature=0,
            base_url=os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434"),
        )

        # Bind tools to the LLM
        llm_with_tools = llm.bind_tools(self.tools)

        # Build message history
        messages = []

        # System prompt from BSLTools
        messages.append({"role": "system", "content": self.bsl_tools.system_prompt})

        # Conversation history
        for msg in self.conversation_history:
            messages.append(msg)

        # Current question
        messages.append(HumanMessage(content=question))

        tool_outputs = []
        tool_call_log = []

        # Tool-calling loop (max 5 iterations)
        for iteration in range(5):
            response = llm_with_tools.invoke(messages)

            # Check if LLM wants to call tools
            if not hasattr(response, "tool_calls") or not response.tool_calls:
                # No tool calls → this is the final answer
                break

            # Add LLM response to messages
            messages.append(response)

            # Execute each tool call
            for tool_call in response.tool_calls:
                tool_name = tool_call["name"]
                tool_args = tool_call["args"]
                tool_call_log.append({
                    "name": tool_name,
                    "args": tool_args,
                    "iteration": iteration,
                })

                try:
                    # BSLTools.execute() dispatches to the right handler
                    result = self.bsl_tools.execute(tool_name, tool_args)
                except Exception as e:
                    result = f"Error: {str(e)}"

                if tool_name == "query_model":
                    tool_outputs.append(result)

                # Feed tool result back to LLM
                messages.append(
                    ToolMessage(
                        content=str(result),
                        tool_call_id=tool_call.get("id", f"call_{iteration}"),
                    )
                )

        # Extract final answer
        answer = ""
        if hasattr(response, "content"):
            if isinstance(response.content, str):
                answer = response.content
            elif isinstance(response.content, list):
                # Claude-style content blocks
                answer = " ".join(
                    block.get("text", "")
                    for block in response.content
                    if isinstance(block, dict) and block.get("type") == "text"
                )

        # Parse records and chart from tool outputs
        records = None
        chart = None
        for output in tool_outputs:
            if isinstance(output, str):
                try:
                    parsed = json.loads(output)
                    if isinstance(parsed, dict):
                        records = parsed.get("records", records)
                        chart = parsed.get("chart", chart)
                except (json.JSONDecodeError, TypeError):
                    pass

        # Update conversation history (keep bounded)
        self.conversation_history.append({"role": "user", "content": question})
        self.conversation_history.append({"role": "assistant", "content": answer})
        if len(self.conversation_history) > 20:
            self.conversation_history = self.conversation_history[-20:]

        return {
            "answer": answer,
            "records": records,
            "chart": chart,
            "tool_calls": tool_call_log,
        }

    def reset_history(self):
        """Clear conversation history."""
        self.conversation_history = []


def get_tenant_agent(tenant_slug: str) -> TenantAgent:
    """Get or create a cached TenantAgent for the given tenant."""
    if tenant_slug not in _agent_cache:
        bsl_yaml = Path(__file__).parent.parent / "bsl_configs" / f"{tenant_slug}.bsl.yaml"
        profile = Path(__file__).parent.parent / "profiles.yml"

        if not bsl_yaml.exists():
            raise FileNotFoundError(
                f"BSL config not found: {bsl_yaml}. "
                f"Run /bsl/{tenant_slug}/refresh first."
            )

        _agent_cache[tenant_slug] = TenantAgent(tenant_slug, bsl_yaml, profile)
    return _agent_cache[tenant_slug]


def invalidate_agent_cache(tenant_slug: str | None = None):
    """Clear agent cache."""
    if tenant_slug:
        _agent_cache.pop(tenant_slug, None)
    else:
        _agent_cache.clear()


async def check_ollama_health() -> dict:
    """Check if Ollama is running and qwen2.5-coder:14b is available."""
    base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.get(f"{base_url}/api/tags")
            if resp.status_code != 200:
                return {"status": "error", "message": f"HTTP {resp.status_code}"}
            models = resp.json().get("models", [])
            model_names = [m.get("name", "") for m in models]
            has_model = any("qwen2.5-coder" in name and "14b" in name for name in model_names)
            return {
                "status": "ok",
                "model_available": has_model,
                "models": model_names,
            }
    except httpx.ConnectError:
        return {
            "status": "error",
            "model_available": False,
            "message": "Cannot connect to Ollama. Run: ollama serve",
        }
    except Exception as e:
        return {
            "status": "error",
            "model_available": False,
            "message": str(e),
        }
```

---

## 7. Pydantic Models (additions to `models.py`)

```python
# Add these to the existing models.py file

from typing import Literal

# --- BSL Query Models ---

class BSLFilter(BaseModel):
    dimension: str
    operator: Literal[
        "eq", "neq", "gt", "gte", "lt", "lte",
        "in", "not_in", "like", "not_like",
        "between", "is_null", "is_not_null",
    ]
    value: str | int | float | list | None = None


class BSLQueryRequest(BaseModel):
    model: str                            # Clean name: "ad_performance"
    dimensions: list[str] = []
    measures: list[str] = []
    calculated_measures: list[str] = []   # Post-computed (CTR, CPC, etc.)
    filters: list[BSLFilter] = []
    order_by: list[list[str]] = []        # [["total_spend", "desc"]]
    limit: int | None = 100
    time_grain: str | None = None         # "day", "week", "month", "year"
    time_range: dict | None = None        # {"start": "...", "end": "..."}


class BSLQueryResponse(BaseModel):
    records: list[dict]
    columns: list[ColumnInfo]
    row_count: int


class BSLAskRequest(BaseModel):
    question: str
    reset_history: bool = False


class BSLAskResponse(BaseModel):
    answer: str
    records: list[dict] | None = None
    chart: dict | None = None
    tool_calls: list[dict] = []


class BSLModelSummary(BaseModel):
    name: str
    description: str | None = None
    dimension_count: int
    measure_count: int


class BSLModelDetail(BaseModel):
    name: str
    description: str | None = None
    dimensions: dict
    measures: dict
    calculated_measures: list[str] = []


class BSLCatalogResponse(BaseModel):
    tenant_slug: str
    models: list[BSLModelDetail]


class OllamaHealthResponse(BaseModel):
    status: str
    model_available: bool = False
    models: list[str] = []
    message: str | None = None
```

---

## 8. FastAPI Endpoints (new BSL routes in `main.py`)

```python
# Add these imports at the top of main.py
from bsl.tenant_models import get_tenant_models, invalidate_cache, list_tenants
from bsl.structured_query import execute_structured_query, apply_calculated_measures
from bsl.agent import get_tenant_agent, invalidate_agent_cache, check_ollama_health
from bsl.config_generator import generate_bsl_config, write_bsl_yaml
from models import (
    # ... existing imports ...
    BSLQueryRequest, BSLQueryResponse, BSLAskRequest, BSLAskResponse,
    BSLModelSummary, BSLModelDetail, BSLCatalogResponse, OllamaHealthResponse,
    BSLFilter,
)

# --- BSL Endpoints ---

@app.get("/tenants")
def get_active_tenants():
    return {"tenants": list_tenants()}


@app.get("/bsl/{tenant_slug}/models", response_model=list[BSLModelSummary])
def bsl_list_models(tenant_slug: str):
    try:
        models = get_tenant_models(tenant_slug)
    except FileNotFoundError:
        raise HTTPException(404, f"No config for tenant: {tenant_slug}")

    result = []
    for name, model in models.items():
        result.append(BSLModelSummary(
            name=name,
            description=model.description,
            dimension_count=len(model.get_dimensions()),
            measure_count=len(model.get_measures()),
        ))
    return result


@app.get("/bsl/{tenant_slug}/models/{model_name}", response_model=BSLModelDetail)
def bsl_get_model(tenant_slug: str, model_name: str):
    try:
        models = get_tenant_models(tenant_slug)
    except FileNotFoundError:
        raise HTTPException(404, f"No config for tenant: {tenant_slug}")

    if model_name not in models:
        available = ", ".join(models.keys())
        raise HTTPException(404, f"Model '{model_name}' not found. Available: {available}")

    model = models[model_name]

    dimensions = {}
    for name, dim in model.get_dimensions().items():
        dim_info = {}
        if dim.description:
            dim_info["description"] = dim.description
        if dim.is_time_dimension:
            dim_info["is_time_dimension"] = True
        if dim.smallest_time_grain:
            dim_info["smallest_time_grain"] = dim.smallest_time_grain
        dimensions[name] = dim_info if dim_info else "dimension"

    measures = {}
    for name, meas in model.get_measures().items():
        measures[name] = meas.description if meas.description else "measure"

    return BSLModelDetail(
        name=model_name,
        description=model.description,
        dimensions=dimensions,
        measures=measures,
        calculated_measures=list(model.get_calculated_measures().keys()),
    )


@app.get("/bsl/{tenant_slug}/catalog", response_model=BSLCatalogResponse)
def bsl_get_catalog(tenant_slug: str):
    """Full catalog for frontend WebLLM context injection."""
    try:
        models = get_tenant_models(tenant_slug)
    except FileNotFoundError:
        raise HTTPException(404, f"No config for tenant: {tenant_slug}")

    model_details = []
    for name, model in models.items():
        dims = {}
        for dname, dim in model.get_dimensions().items():
            dim_info = {}
            if dim.description:
                dim_info["description"] = dim.description
            if dim.is_time_dimension:
                dim_info["is_time_dimension"] = True
            dims[dname] = dim_info if dim_info else "dimension"

        measures = {}
        for mname, meas in model.get_measures().items():
            measures[mname] = meas.description if meas.description else "measure"

        model_details.append(BSLModelDetail(
            name=name,
            description=model.description,
            dimensions=dims,
            measures=measures,
            calculated_measures=list(model.get_calculated_measures().keys()),
        ))

    return BSLCatalogResponse(tenant_slug=tenant_slug, models=model_details)


@app.post("/bsl/{tenant_slug}/query", response_model=BSLQueryResponse)
def bsl_execute_query(tenant_slug: str, request: BSLQueryRequest):
    """Execute a structured query against a BSL model."""
    try:
        models = get_tenant_models(tenant_slug)
    except FileNotFoundError:
        raise HTTPException(404, f"No config for tenant: {tenant_slug}")

    if request.model not in models:
        available = ", ".join(models.keys())
        raise HTTPException(404, f"Model '{request.model}' not found. Available: {available}")

    # Convert BSLFilter objects to dicts for BSL
    filters = None
    if request.filters:
        filters = [f.model_dump() for f in request.filters]

    try:
        result = execute_structured_query(
            model=models[request.model],
            dimensions=request.dimensions or None,
            measures=request.measures or None,
            filters=filters,
            order_by=request.order_by or None,
            limit=request.limit,
            time_grain=request.time_grain,
            time_range=request.time_range,
        )
    except Exception as e:
        raise HTTPException(400, f"Query error: {str(e)}")

    # Post-compute calculated measures if requested
    if request.calculated_measures:
        result = apply_calculated_measures(result, request.calculated_measures)

    return BSLQueryResponse(**result)


@app.get("/bsl/health/ollama", response_model=OllamaHealthResponse)
async def bsl_ollama_health():
    """Check Ollama connectivity and model availability."""
    health = await check_ollama_health()
    return OllamaHealthResponse(**health)


@app.post("/bsl/{tenant_slug}/ask", response_model=BSLAskResponse)
async def bsl_ask(tenant_slug: str, request: BSLAskRequest):
    """Natural language query via Ollama Qwen2.5-Coder-14B + BSLTools."""
    # Check Ollama first
    health = await check_ollama_health()
    if health.get("status") != "ok" or not health.get("model_available"):
        raise HTTPException(
            503,
            "Ollama not available or qwen2.5-coder:14b not pulled. "
            "Run: ollama serve && ollama pull qwen2.5-coder:14b",
        )

    try:
        agent = get_tenant_agent(tenant_slug)
    except FileNotFoundError as e:
        raise HTTPException(404, str(e))

    if request.reset_history:
        agent.reset_history()

    try:
        result = agent.ask(request.question)
        return BSLAskResponse(**result)
    except Exception as e:
        raise HTTPException(500, f"Agent error: {str(e)}")


@app.post("/bsl/{tenant_slug}/refresh")
def bsl_refresh_tenant(tenant_slug: str):
    """Regenerate BSL config and reload models for a tenant."""
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        raise HTTPException(404, f"No semantic config for tenant: {tenant_slug}")

    # Invalidate caches
    invalidate_cache(tenant_slug)
    invalidate_agent_cache(tenant_slug)

    # Regenerate
    bsl_config = generate_bsl_config(config_path, tenant_slug)
    bsl_yaml_path = Path(__file__).parent / "bsl_configs" / f"{tenant_slug}.bsl.yaml"
    write_bsl_yaml(bsl_config, bsl_yaml_path)

    # Reload
    models = get_tenant_models(tenant_slug)

    return {
        "status": "refreshed",
        "tenant_slug": tenant_slug,
        "models": list(models.keys()),
    }


@app.post("/bsl/refresh-all")
def bsl_refresh_all():
    """Regenerate all tenant configs and reload."""
    invalidate_cache()
    invalidate_agent_cache()

    results = {}
    for slug in list_tenants():
        try:
            results[slug] = bsl_refresh_tenant(slug)
        except Exception as e:
            results[slug] = {"status": "error", "message": str(e)}
    return results
```

---

## 9. Updated `pyproject.toml`

```toml
[project]
name = "platform-api"
version = "0.2.0"
description = "GATA Platform API with BSL semantic layer"
requires-python = ">=3.11"
dependencies = [
    "fastapi",
    "uvicorn",
    "duckdb",
    "pyyaml",
    "pydantic",
    "httpx",
    "boring-semantic-layer>=0.3.7",
    "ibis-framework[duckdb]>=9.0.0",
    "langchain-core>=0.3.0",
    "langchain-ollama>=0.2.0",
    "langchain>=0.3.0",
    "sse-starlette>=1.6.0",
]
```

---

## 10. Key BSL API Reference (What Claude Code Needs to Know)

### SemanticModel methods:

```python
model.get_dimensions()       # → dict[str, Dimension]
model.get_measures()         # → dict[str, Measure]
model.get_calculated_measures()  # → dict[str, ...]
model.description            # → str | None
model.table                  # → Ibis table expression

# Query API:
model.query(
    dimensions=["col1", "col2"],
    measures=["measure1"],
    filters=[{"dimension": "col1", "operator": "eq", "value": "x"}],
    order_by=[["measure1", "desc"]],
    limit=100,
    time_grain="day",
    time_range={"start": "2024-01-01", "end": "2024-12-31"},
)  # → Ibis expression, call .execute() for DataFrame
```

### Dimension object:

```python
dim.description           # → str | None
dim.is_time_dimension     # → bool
dim.smallest_time_grain   # → str | None
dim.is_entity             # → bool
dim.expr                  # → Ibis deferred expression
```

### Measure object:

```python
meas.description          # → str | None
meas.expr                 # → Ibis deferred expression
```

### BSLTools methods:

```python
bsl_tools = BSLTools(model_path=Path("config.yaml"), profile="name", profile_file=Path("profiles.yml"))
bsl_tools.models           # → dict[str, SemanticModel]
bsl_tools.system_prompt    # → str (system prompt for LLM)
bsl_tools.tools            # → list[dict] (OpenAI tool definitions)
bsl_tools.get_callable_tools()  # → list[StructuredTool] (LangChain tools)
bsl_tools.execute(name, args)   # → str (tool result as string)
```

### from_yaml / from_config:

```python
from boring_semantic_layer import from_yaml, from_config

# From YAML file:
models = from_yaml("config.yaml", profile="motherduck", profile_path="profiles.yml")

# From dict (skip file I/O):
models = from_config(config_dict, profile="motherduck", profile_path="profiles.yml")

# With pre-loaded tables:
models = from_config(config_dict, tables={"table_name": ibis_table_expr})
```

### Profile format:

```yaml
# profiles.yml
profile_name:
    type: duckdb # backend type (duckdb, postgres, etc.)
    database: "md:my_db" # connection string
```

### BSL YAML dimension format:

```yaml
dim_name:
    expr: "_.column_name" # Ibis deferred expression (required)
    description: "Human readable" # Optional
    is_time_dimension: true # Optional, for date/time columns
    smallest_time_grain: "TIME_GRAIN_DAY" # Optional
    is_entity: true # Optional, for PK/FK columns

# Short form (no metadata):
dim_name: "_.column_name"
```

### BSL YAML measure format:

```yaml
measure_name:
    expr: "_.column.sum()" # Ibis aggregation expression (required)
    description: "Human readable" # Optional

# Short form:
measure_name: "_.column.sum()"
```

### BSL YAML join format:

```yaml
joins:
    alias_name:
        model: target_model_name # Must exist in same config file
        type: many # "one" or "many"
        left_on: fk_column # Column in this model
        right_on: pk_column # Column in target model
```

### BSL YAML filter format:

```yaml
# Applied as WHERE clause on every query
filter: "_.tenant_slug == 'tyrell_corp'"

# Can use any Ibis predicate:
filter: "_.status.isin(['active', 'pending'])"
filter: "_.amount > 0"
```
