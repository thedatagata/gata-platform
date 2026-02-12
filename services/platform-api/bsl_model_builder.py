"""
BSL Semantic Model Builder — Config-Driven Auto-Population

Builds BSL SemanticModel objects per tenant by reading the dbt-generated
`platform_ops__bsl_column_catalog` table, which contains pre-classified
column metadata for all star schema (fct_*/dim_*) tables.

The flow (zero YAML required):

  1. Read pre-classified columns from platform_ops__bsl_column_catalog
     (semantic_role, bsl_type, inferred_agg already computed in SQL)
  2. Auto-infer calculated measures from column patterns (CTR, CPC, AOV, etc.)
  3. Auto-infer joins from matching column names across fact/dim tables
  4. Optionally overlay YAML enrichments (descriptions, custom overrides)
  5. Call BSL's from_config() to build SemanticModel objects with Ibis expressions
  6. Build column metadata dict for API consumption

New tenants get full BSL functionality (dims, measures, calc measures, joins)
purely from their star schema — no manual config needed.
"""

import os
import json
import yaml
import logging
from pathlib import Path
from typing import Optional

import ibis

logger = logging.getLogger(__name__)

# BSL imports
try:
    from boring_semantic_layer import from_config, SemanticModel
    BSL_AVAILABLE = True
except ImportError:
    BSL_AVAILABLE = False
    logger.warning("[BSL] boring-semantic-layer not installed")


# ───────────────────────────────────────────────────────────
# Column type → semantic role classification
# Mirrors bsl_mapper.py logic but generates BSL config format
# ───────────────────────────────────────────────────────────

# Data types that are ALWAYS dimensions (group-by candidates)
DIMENSION_TYPES = {"VARCHAR", "TEXT", "DATE", "TIMESTAMP", "BOOLEAN", "BOOL"}

# Data types that are ALWAYS measures (aggregatable)
MEASURE_TYPES = {"DOUBLE", "FLOAT", "DECIMAL", "REAL"}

# Integer types need context — could be either
# (counts/totals = measures, IDs = dimensions)
INTEGER_TYPES = {"BIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT", "HUGEINT"}

# Column name patterns that indicate a column is a measure even if integer
MEASURE_NAME_PATTERNS = {
    "total_", "count_", "num_", "sum_", "events_in_", "session_duration",
    "revenue", "spend", "impressions", "clicks", "conversions",
    "price", "amount", "cost",
}

# Column name patterns that indicate a column is a dimension even if integer
DIMENSION_NAME_PATTERNS = {
    "_id", "_key", "_slug", "_name", "_status", "_type", "_category",
    "_email", "_source", "_medium", "_campaign", "_country", "_device",
}

# Columns to always skip (internal/partition columns)
SKIP_COLUMNS = {"tenant_slug"}

# Default aggregation by data type
DEFAULT_AGG_MAP = {
    "DOUBLE": "sum",
    "FLOAT": "sum",
    "DECIMAL": "sum",
    "REAL": "sum",
    "BIGINT": "sum",
    "INTEGER": "sum",
}


def _classify_column(col_name: str, data_type: str) -> str:
    """Classify a column as 'dimension', 'measure', or 'skip'.

    Uses data type as primary signal, column name patterns as tiebreaker
    for ambiguous integer types.
    """
    if col_name in SKIP_COLUMNS:
        return "skip"

    # JSON columns are skipped (not queryable via BSL)
    if data_type in ("JSON", "BLOB"):
        return "skip"

    # Clear dimension types
    if data_type in DIMENSION_TYPES:
        return "dimension"

    # Clear measure types
    if data_type in MEASURE_TYPES:
        return "measure"

    # Integer types: use column name patterns to disambiguate
    if data_type in INTEGER_TYPES:
        col_lower = col_name.lower()

        # Check dimension patterns first (IDs, keys, etc.)
        for pattern in DIMENSION_NAME_PATTERNS:
            if pattern in col_lower:
                return "dimension"

        # Check measure patterns (counts, totals, etc.)
        for pattern in MEASURE_NAME_PATTERNS:
            if pattern in col_lower:
                return "measure"

        # Default: integers with no clear pattern → measure
        return "measure"

    # Unknown type → dimension (safe default, won't break queries)
    return "dimension"


def _infer_aggregation(col_name: str, data_type: str) -> str:
    """Infer the best aggregation for a measure column."""
    col_lower = col_name.lower()

    # Duration/time columns → average
    if "duration" in col_lower or "avg" in col_lower:
        return "avg"

    # Counts and session/event counts → sum (they're pre-aggregated)
    if "count" in col_lower or "events_in" in col_lower:
        return "sum"

    # IDs used as measures → count_distinct
    if col_lower.endswith("_id"):
        return "count_distinct"

    # Default by data type
    return DEFAULT_AGG_MAP.get(data_type, "sum")


def _ibis_agg_expr(col_name: str, agg: str) -> str:
    """Generate an Ibis expression string for a measure aggregation."""
    if agg == "sum":
        return f"_.{col_name}.sum()"
    elif agg == "avg":
        return f"_.{col_name}.mean()"
    elif agg == "count":
        return f"_.{col_name}.count()"
    elif agg == "count_distinct":
        return f"_.{col_name}.nunique()"
    elif agg == "max":
        return f"_.{col_name}.max()"
    elif agg == "min":
        return f"_.{col_name}.min()"
    return f"_.{col_name}.sum()"


# ───────────────────────────────────────────────────────────
# Calculated measure patterns → Ibis expressions
# ───────────────────────────────────────────────────────────

CALC_MEASURE_PATTERNS = {
    "ctr": "ibis.ifelse(_.impressions.sum() > 0, _.clicks.sum().cast('float64') / _.impressions.sum(), 0)",
    "cpc": "ibis.ifelse(_.clicks.sum() > 0, _.spend.sum() / _.clicks.sum(), 0)",
    "cpm": "ibis.ifelse(_.impressions.sum() > 0, _.spend.sum() * 1000.0 / _.impressions.sum(), 0)",
    "aov": "ibis.ifelse(_.order_id.nunique() > 0, _.total_price.sum() / _.order_id.nunique(), 0)",
    "conversion_rate": "ibis.ifelse(_.session_id.nunique() > 0, _.is_conversion_session.cast('int64').sum().cast('float64') / _.session_id.nunique(), 0)",
}


def _convert_calculated_measure(calc_config: dict, model_config: dict) -> str | None:
    """Convert a YAML calculated measure to an Ibis expression string.

    Uses pattern matching for common formulas (CTR, CPC, AOV, etc.).
    Falls back to None for complex custom SQL that can't be auto-converted.
    """
    name = calc_config.get("name", "").lower()

    # Check known patterns
    if name in CALC_MEASURE_PATTERNS:
        return CALC_MEASURE_PATTERNS[name]

    # For unknown patterns, log and skip
    logger.info(
        f"[BSL] Calculated measure '{name}' has custom SQL that "
        f"cannot be auto-converted to Ibis. Skipping."
    )
    return None


# ───────────────────────────────────────────────────────────
# Auto-inference: calculated measures from column patterns
# ───────────────────────────────────────────────────────────

# Maps calc measure name → required columns that must exist in the model
CALC_MEASURE_REQUIREMENTS = {
    "ctr": {
        "requires": {"clicks", "impressions"},
        "label": "CTR",
        "sql": "CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END",
        "format": "percent",
    },
    "cpc": {
        "requires": {"spend", "clicks"},
        "label": "CPC",
        "sql": "CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END",
        "format": "currency",
    },
    "cpm": {
        "requires": {"spend", "impressions"},
        "label": "CPM",
        "sql": "CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END",
        "format": "currency",
    },
    "aov": {
        "requires": {"total_price", "order_id"},
        "label": "AOV",
        "sql": "CASE WHEN COUNT(DISTINCT order_id) > 0 THEN SUM(total_price) / COUNT(DISTINCT order_id) ELSE 0 END",
        "format": "currency",
    },
    "conversion_rate": {
        "requires": {"is_conversion_session", "session_id"},
        "label": "Conversion Rate",
        "sql": "CASE WHEN COUNT(DISTINCT session_id) > 0 THEN SUM(CASE WHEN is_conversion_session THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT session_id) ELSE 0 END",
        "format": "percent",
    },
}


def _auto_infer_calculated_measures(columns_by_subject: dict) -> dict:
    """Auto-infer calculated measures from column patterns per model.

    For each model, checks if the required columns for known calculated
    measures are present. Returns dict: subject → list of calc measure defs.
    """
    result = {}
    for subject, columns in columns_by_subject.items():
        col_names = {col["column_name"] for col in columns}
        inferred = []
        for calc_name, spec in CALC_MEASURE_REQUIREMENTS.items():
            if spec["requires"].issubset(col_names):
                inferred.append({
                    "name": calc_name,
                    "label": spec["label"],
                    "sql": spec["sql"],
                    "format": spec.get("format"),
                })
        if inferred:
            result[subject] = inferred
    return result


# Columns to exclude from join detection (too common, not meaningful as join keys)
_JOIN_EXCLUDE_COLUMNS = {"tenant_slug", "source_platform"}


def _auto_infer_joins(catalog: list[dict]) -> dict:
    """Auto-infer joins from matching column names across fact/dim tables.

    For each fact table, finds dimension tables with matching column names
    (excluding common columns like tenant_slug, source_platform).
    Returns dict: subject → list of join defs.
    """
    # Separate facts and dims
    facts = [e for e in catalog if e["table_type"] == "fact"]
    dims = [e for e in catalog if e["table_type"] == "dimension"]

    if not dims:
        return {}

    # Build dim column sets: subject → set of column names
    dim_columns = {}
    for dim in dims:
        cols = {
            col["column_name"]
            for col in dim["columns"]
            if col["column_name"] not in _JOIN_EXCLUDE_COLUMNS
        }
        dim_columns[dim["subject"]] = cols

    result = {}
    for fact in facts:
        fact_cols = {
            col["column_name"]
            for col in fact["columns"]
            if col["column_name"] not in _JOIN_EXCLUDE_COLUMNS
        }

        joins = []
        for dim in dims:
            matching = fact_cols & dim_columns[dim["subject"]]
            if matching:
                # Use the first matching column as join key
                # Prefer _id columns as they're more specific
                id_matches = [c for c in matching if c.endswith("_id")]
                join_col = id_matches[0] if id_matches else sorted(matching)[0]
                joins.append({
                    "to": dim["subject"],
                    "type": "left",
                    "on": {join_col: join_col},
                })

        if joins:
            result[fact["subject"]] = joins

    return result


# ───────────────────────────────────────────────────────────
# Enriched catalog reader (from dbt-classified columns)
# ───────────────────────────────────────────────────────────

def _read_enriched_catalog(con: ibis.BaseBackend, tenant_slug: str) -> list[dict]:
    """Read pre-classified columns from platform_ops__bsl_column_catalog.

    Returns list of dicts grouped by table, same structure as _read_catalog()
    but with extra fields: semantic_role, bsl_type, is_time_dimension, inferred_agg.

    Falls back to _read_catalog() + Python classification if the enriched
    catalog table doesn't exist yet.
    """
    try:
        # Use underlying DuckDB connection for parameterized queries
        # (ibis raw_sql() doesn't support bind parameters)
        result = con.con.execute("""
            SELECT table_name, table_type, subject, column_name, data_type,
                   semantic_role, bsl_type, is_time_dimension, inferred_agg,
                   ordinal_position
            FROM main.platform_ops__bsl_column_catalog
            WHERE tenant_slug = ?
            ORDER BY table_name, ordinal_position
        """, [tenant_slug]).fetchall()
    except Exception as e:
        logger.info(f"[BSL] Enriched catalog not available ({e}), falling back to raw catalog")
        # Fall back: read raw catalog and classify in Python
        raw_catalog = _read_catalog(con, tenant_slug)
        for entry in raw_catalog:
            for col in entry["columns"]:
                col_name = col["column_name"]
                data_type = col["data_type"]
                role = _classify_column(col_name, data_type)
                col["semantic_role"] = role
                if role == "measure":
                    col["inferred_agg"] = _infer_aggregation(col_name, data_type)
                else:
                    col["inferred_agg"] = None
                col["is_time_dimension"] = data_type in ("DATE", "TIMESTAMP")
                # bsl_type mapping
                if data_type in ("VARCHAR", "TEXT"):
                    col["bsl_type"] = "string"
                elif data_type == "DATE":
                    col["bsl_type"] = "date"
                elif data_type == "TIMESTAMP":
                    col["bsl_type"] = "timestamp"
                elif data_type in ("BOOLEAN", "BOOL"):
                    col["bsl_type"] = "boolean"
                else:
                    col["bsl_type"] = "number"
            # Filter out skipped columns
            entry["columns"] = [c for c in entry["columns"] if c.get("semantic_role") != "skip"]
        return raw_catalog

    # Group flat rows by table
    tables = {}
    for row in result:
        (table_name, table_type, subject, col_name, data_type,
         role, bsl_type, is_time, agg, ordinal) = row
        if subject not in tables:
            tables[subject] = {
                "table_name": table_name,
                "table_type": table_type,
                "subject": subject,
                "columns": [],
            }
        tables[subject]["columns"].append({
            "column_name": col_name,
            "data_type": data_type,
            "ordinal_position": ordinal,
            "semantic_role": role,
            "bsl_type": bsl_type,
            "is_time_dimension": bool(is_time),
            "inferred_agg": agg,
        })

    return list(tables.values())


# ───────────────────────────────────────────────────────────
# Column metadata builder (for API consumption)
# ───────────────────────────────────────────────────────────

def _build_column_metadata(
    catalog: list[dict],
    auto_calc_measures: dict,
    auto_joins: dict,
    enrichments: dict,
) -> dict:
    """Build column metadata dict for API endpoints.

    Combines enriched catalog data, auto-inferred calc measures/joins,
    and optional YAML enrichment overrides.

    Returns dict: subject → {table, description, label, columns, calculated_measures, joins, has_joins}
    """
    metadata = {}

    for entry in catalog:
        subject = entry["subject"]
        table_name = entry["table_name"]
        table_type = entry["table_type"]

        # YAML enrichment overrides
        enrich = enrichments.get(table_name, {})
        description = enrich.get("description") or f"{table_type.title()}: {subject}"
        label = enrich.get("label") or subject.replace("_", " ").title()

        # Build column metadata
        columns = {}
        for col in entry["columns"]:
            role = col.get("semantic_role", "dimension")
            if role == "skip":
                continue

            col_meta = {
                "bsl_type": col.get("bsl_type", "string"),
                "role": role,
                "is_time_dimension": col.get("is_time_dimension", False),
            }
            if role == "measure":
                col_meta["agg"] = col.get("inferred_agg", "sum")

            # Apply YAML per-column overrides
            dim_overrides = enrich.get("dimension_overrides", {})
            measure_overrides = enrich.get("measure_overrides", {})
            col_name = col["column_name"]

            if col_name in dim_overrides:
                override = dim_overrides[col_name]
                if "type" in override:
                    col_meta["bsl_type"] = override["type"]
                    if override["type"] in ("date", "timestamp", "timestamp_epoch"):
                        col_meta["is_time_dimension"] = True
            elif col_name in measure_overrides:
                override = measure_overrides[col_name]
                if "agg" in override:
                    col_meta["agg"] = override["agg"]
                if "type" in override:
                    col_meta["bsl_type"] = override["type"]

            columns[col_name] = col_meta

        # Calculated measures: auto-inferred, then YAML overrides
        calc_measures = auto_calc_measures.get(subject, [])
        yaml_calcs = enrich.get("calculated_measures", [])
        if yaml_calcs:
            # YAML calc measures replace auto-inferred ones
            existing_names = {cm["name"] for cm in calc_measures}
            for yc in yaml_calcs:
                if yc["name"] not in existing_names:
                    calc_measures.append({
                        "name": yc["name"],
                        "label": yc.get("label", yc["name"]),
                        "sql": yc.get("sql", ""),
                        "format": yc.get("format"),
                    })

        # Joins: auto-inferred, then YAML overrides
        joins = auto_joins.get(subject, [])
        yaml_joins = enrich.get("joins", [])
        if yaml_joins:
            # Build lookup: physical table_name → subject
            table_to_subject = {e["table_name"]: e["subject"] for e in catalog}
            existing_targets = {j["to"] for j in joins}
            for yj in yaml_joins:
                target_table = yj.get("to", "")
                target_subject = table_to_subject.get(target_table, target_table)
                if target_subject not in existing_targets:
                    on_clause = yj.get("on") or yj.get(True, {})
                    joins.append({
                        "to": target_subject,
                        "type": yj.get("type", "left"),
                        "on": on_clause if isinstance(on_clause, dict) else {},
                    })

        metadata[subject] = {
            "table": table_name,
            "description": description,
            "label": label,
            "columns": columns,
            "calculated_measures": calc_measures,
            "joins": joins,
            "has_joins": bool(joins),
        }

    return metadata


# ───────────────────────────────────────────────────────────
# Connection helpers
# ───────────────────────────────────────────────────────────

def _get_ibis_connection() -> ibis.BaseBackend:
    """Create an Ibis connection to MotherDuck or local DuckDB."""
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if md_token:
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


# ───────────────────────────────────────────────────────────
# Read dbt catalog → BSL config generation
# ───────────────────────────────────────────────────────────

def _read_catalog(con: ibis.BaseBackend, tenant_slug: str) -> list[dict]:
    """Read star schema table catalog from platform_ops__boring_semantic_layer.

    Returns list of dicts: [{table_name, table_type, subject, columns: [...]}]
    """
    # Use underlying DuckDB connection for parameterized queries
    result = con.con.execute("""
        SELECT table_name, table_type, subject, semantic_manifest::VARCHAR as manifest
        FROM main.platform_ops__boring_semantic_layer
        WHERE tenant_slug = ?
        ORDER BY table_type DESC, table_name
    """, [tenant_slug]).fetchall()

    catalog = []
    for row in result:
        table_name, table_type, subject, manifest_str = row
        columns = json.loads(manifest_str) if manifest_str else []
        catalog.append({
            "table_name": table_name,
            "table_type": table_type,  # 'fact' or 'dimension'
            "subject": subject,         # e.g. 'ad_performance', 'users'
            "columns": columns,         # [{column_name, data_type, ordinal_position}]
        })

    return catalog


def _load_yaml_enrichments(tenant_slug: str) -> dict:
    """Load hand-written YAML config for a tenant if it exists.

    Returns a dict keyed by table_name with enrichment metadata:
    descriptions, labels, calculated_measures, joins, custom agg overrides.
    """
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        return {}

    with open(config_path) as f:
        raw = yaml.safe_load(f) or {}

    enrichments = {}
    for model_cfg in raw.get("models", []):
        name = model_cfg["name"]
        enrichments[name] = {
            "label": model_cfg.get("label", ""),
            "description": model_cfg.get("description", ""),
            "calculated_measures": model_cfg.get("calculated_measures", []),
            "joins": model_cfg.get("joins", []),
            # Build a lookup for per-column overrides
            "dimension_overrides": {
                d["name"]: d for d in model_cfg.get("dimensions", [])
            },
            "measure_overrides": {
                m["name"]: m for m in model_cfg.get("measures", [])
            },
        }

    return enrichments


def _generate_bsl_config(
    catalog: list[dict],
    enrichments: dict,
    con: ibis.BaseBackend,
) -> dict:
    """Generate BSL from_config() compatible config dict.

    Plain strings for auto-classified items, dict format only when metadata
    is needed (descriptions, is_time_dimension, etc.).
    """
    bsl_config = {}

    for entry in catalog:
        table_name = entry["table_name"]
        subject = entry["subject"]
        columns = entry["columns"]

        enrich = enrichments.get(table_name, {})
        dim_overrides = enrich.get("dimension_overrides", {})
        measure_overrides = enrich.get("measure_overrides", {})
        description = enrich.get("description", f"{entry['table_type'].title()}: {subject}")

        model_config = {
            "table": table_name,  # physical table name — matches tables dict key
            "description": description,
            "dimensions": {},
            "measures": {},
        }

        for col in columns:
            col_name = col["column_name"]
            data_type = col["data_type"]
            role = _classify_column(col_name, data_type)

            if role == "skip":
                continue

            # --- YAML override check ---
            if col_name in measure_overrides:
                override = measure_overrides[col_name]
                agg = override.get("agg", _infer_aggregation(col_name, data_type))
                label = override.get("label", col_name)
                desc = override.get("description", f"{label} ({agg})")
                model_config["measures"][col_name] = {
                    "expr": _ibis_agg_expr(col_name, agg),
                    "description": desc,
                }
                continue

            if col_name in dim_overrides:
                override = dim_overrides[col_name]
                dim_config = {"expr": f"_.{col_name}"}
                if override.get("description"):
                    dim_config["description"] = override["description"]
                dtype = override.get("type", "")
                if dtype in ("date", "timestamp", "timestamp_epoch"):
                    dim_config["is_time_dimension"] = True
                model_config["dimensions"][col_name] = dim_config
                continue

            # --- Auto-classified ---
            if role == "dimension":
                if data_type in ("DATE", "TIMESTAMP"):
                    model_config["dimensions"][col_name] = {
                        "expr": f"_.{col_name}",
                        "is_time_dimension": True,
                    }
                else:
                    # Plain string format — no metadata needed
                    model_config["dimensions"][col_name] = f"_.{col_name}"
            elif role == "measure":
                agg = _infer_aggregation(col_name, data_type)
                # Plain string format
                model_config["measures"][col_name] = _ibis_agg_expr(col_name, agg)

        # NOTE: Calculated measures (CTR, CPC, AOV, etc.) use ibis.ifelse()
        # which requires `ibis` in the eval context. BSL's from_config() only
        # passes `_` to safe_eval, so we keep calc measures in metadata only
        # (for API display). The BSL agent injects `ibis` at query time.

        bsl_config[subject] = model_config

    return bsl_config


def _wire_joins(bsl_config: dict, enrichments: dict, catalog: list[dict]) -> dict:
    """Wire joins using subject-based model references.

    Maps from our YAML join format to BSL from_config() join format.
    Note: PyYAML parses `on:` as boolean True, so we check both keys.
    """
    # Build lookup: physical table_name → subject (BSL model key)
    table_to_subject = {e["table_name"]: e["subject"] for e in catalog}

    for entry in catalog:
        table_name = entry["table_name"]
        subject = entry["subject"]

        enrich = enrichments.get(table_name, {})
        yaml_joins = enrich.get("joins", [])

        if not yaml_joins or subject not in bsl_config:
            continue

        bsl_joins = {}
        for jdef in yaml_joins:
            target_table = jdef.get("to", "")
            target_subject = table_to_subject.get(target_table)

            if not target_subject or target_subject not in bsl_config:
                logger.warning(f"[BSL] Join target '{target_table}' not in catalog")
                continue

            # PyYAML parses `on:` as boolean True — check both keys
            on_clause = jdef.get("on") or jdef.get(True, {})
            if not on_clause:
                continue

            # Take first key pair (BSL from_config supports single-key joins)
            left_on, right_on = next(iter(on_clause.items()))

            join_type = "one"  # dim lookups are one-to-one
            if target_table.startswith("fct_"):
                join_type = "many"

            bsl_joins[target_subject] = {
                "model": target_subject,   # references another model KEY in bsl_config
                "type": join_type,
                "left_on": left_on,
                "right_on": right_on,
                "how": jdef.get("type", "left"),
            }

        if bsl_joins:
            bsl_config[subject]["joins"] = bsl_joins

    return bsl_config


# ───────────────────────────────────────────────────────────
# Public API — follows demo's create_semantic_model_llm(pipeline) pattern
# ───────────────────────────────────────────────────────────

def create_tenant_semantic_models(
    tenant_slug: str,
    con: Optional[ibis.BaseBackend] = None,
) -> dict[str, SemanticModel]:
    """Build BSL SemanticModel objects for a tenant from dbt metadata.

    Uses the enriched catalog (platform_ops__bsl_column_catalog) which has
    pre-classified columns. Auto-infers calculated measures and joins from
    the star schema structure. YAML configs are optional overrides.

    Also builds and caches column metadata for API consumption.

    Returns:
        dict mapping model_name (subject) → SemanticModel
    """
    if not BSL_AVAILABLE:
        raise RuntimeError(
            "boring-semantic-layer not installed. "
            "Run: pip install 'boring-semantic-layer[agent]'"
        )

    if con is None:
        con = _get_ibis_connection()

    # Step 1: Read enriched catalog (falls back to raw + Python classification)
    catalog = _read_enriched_catalog(con, tenant_slug)
    if not catalog:
        raise ValueError(
            f"No star schema tables found for tenant '{tenant_slug}' "
            f"in platform_ops__bsl_column_catalog. Run dbt first."
        )

    logger.info(f"[BSL] Catalog for '{tenant_slug}': {len(catalog)} tables")

    # Step 2: Auto-infer calculated measures from column patterns
    columns_by_subject = {e["subject"]: e["columns"] for e in catalog}
    auto_calc_measures = _auto_infer_calculated_measures(columns_by_subject)
    if auto_calc_measures:
        logger.info(
            f"[BSL] Auto-inferred calculated measures for '{tenant_slug}': "
            + ", ".join(f"{s}: [{', '.join(c['name'] for c in cs)}]"
                        for s, cs in auto_calc_measures.items())
        )

    # Step 3: Auto-infer joins from matching column names
    auto_joins = _auto_infer_joins(catalog)
    if auto_joins:
        logger.info(
            f"[BSL] Auto-inferred joins for '{tenant_slug}': "
            + ", ".join(f"{s} → [{', '.join(j['to'] for j in js)}]"
                        for s, js in auto_joins.items())
        )

    # Step 4: Load YAML enrichments (optional overrides)
    enrichments = _load_yaml_enrichments(tenant_slug)
    if enrichments:
        logger.info(
            f"[BSL] YAML enrichments loaded for '{tenant_slug}': "
            f"{len(enrichments)} models"
        )

    # Step 5: Build column metadata for API consumption
    metadata = _build_column_metadata(catalog, auto_calc_measures, auto_joins, enrichments)
    _tenant_metadata_cache[tenant_slug] = metadata

    # Step 6: Generate BSL config from catalog + enrichments
    bsl_config = _generate_bsl_config(catalog, enrichments, con)

    # Step 7: Wire joins — use auto-inferred joins merged with YAML
    # First wire auto-inferred joins
    _wire_auto_joins(bsl_config, auto_joins, catalog)
    # Then overlay YAML joins (may add more or override)
    bsl_config = _wire_joins(bsl_config, enrichments, catalog)

    # NOTE: Calculated measures (ibis.ifelse expressions) are kept in metadata
    # only — BSL's from_config() eval context doesn't include `ibis`.
    # The BSL agent injects `ibis` at query time via GATABSLTools._query_model().

    # Step 8: Build Ibis table references for from_config()
    tables = {}
    for entry in catalog:
        table_name = entry["table_name"]
        try:
            tables[table_name] = con.table(table_name)
            logger.info(f"[BSL] Loaded Ibis table: {table_name}")
        except Exception as e:
            logger.warning(f"[BSL] Could not load table '{table_name}': {e}")

    # Step 9: Call BSL from_config() to build SemanticModel objects
    try:
        models = from_config(bsl_config, tables=tables)
        logger.info(
            f"[BSL] Built {len(models)} SemanticModel objects for '{tenant_slug}'"
        )
        return models
    except Exception as e:
        logger.error(f"[BSL] from_config() failed for '{tenant_slug}': {e}")
        raise


def _wire_auto_joins(bsl_config: dict, auto_joins: dict, catalog: list[dict]):
    """Wire auto-inferred joins into BSL config.

    Similar to _wire_joins() but uses auto-inferred join definitions
    instead of YAML specs.
    """
    for subject, joins in auto_joins.items():
        if subject not in bsl_config:
            continue

        bsl_joins = bsl_config[subject].get("joins", {})
        for jdef in joins:
            target_subject = jdef["to"]
            if target_subject not in bsl_config or target_subject in bsl_joins:
                continue

            on_clause = jdef.get("on", {})
            if not on_clause:
                continue

            left_on, right_on = next(iter(on_clause.items()))
            join_type = "one"  # dim lookups are one-to-one by default

            bsl_joins[target_subject] = {
                "model": target_subject,
                "type": join_type,
                "left_on": left_on,
                "right_on": right_on,
                "how": jdef.get("type", "left"),
            }

        if bsl_joins:
            bsl_config[subject]["joins"] = bsl_joins


# ───────────────────────────────────────────────────────────
# Caching
# ───────────────────────────────────────────────────────────

_tenant_cache: dict[str, dict[str, SemanticModel]] = {}
_tenant_metadata_cache: dict[str, dict] = {}


def get_tenant_semantic_models(
    tenant_slug: str,
    force_refresh: bool = False,
) -> dict[str, SemanticModel]:
    """Get cached BSL SemanticModel objects for a tenant.

    Also populates the metadata cache. Call with force_refresh=True
    after config changes or dbt runs.
    """
    if tenant_slug not in _tenant_cache or force_refresh:
        _tenant_cache[tenant_slug] = create_tenant_semantic_models(tenant_slug)
    return _tenant_cache[tenant_slug]


def get_tenant_metadata(
    tenant_slug: str,
    force_refresh: bool = False,
) -> dict:
    """Get cached column metadata for a tenant's semantic models.

    Returns dict: subject → {table, description, label, columns, calculated_measures, joins, has_joins}

    Ensures BSL models are built first (which populates the metadata cache).
    """
    if tenant_slug not in _tenant_metadata_cache or force_refresh:
        get_tenant_semantic_models(tenant_slug, force_refresh=force_refresh)
    return _tenant_metadata_cache.get(tenant_slug, {})
