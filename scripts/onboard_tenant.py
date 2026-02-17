"""
Tenant Onboarding Pipeline — Single Entry Point

Consolidates mock data generation, dbt scaffolding, and pipeline execution
into one script. Replaces the need to run services/mock-data-engine/main.py
and root main.py separately.

Flow:
    Phase 1 — Generate mock data (MockOrchestrator)
        Lands raw data in the warehouse via dlt pipeline.
        Returns dlt schema dict for hash-based routing.

    Phase 2 — Create dbt scaffolding
        2a: Source YAMLs, staging pushers, master model files
        2b: Intermediate models (JSON extraction from master models)
        2c: Analytics shell models (factory macro one-liners)
        2d: Update dbt_project.yml tenant_configs
        2e: Generate BSL semantic config YAML
        2f: Update selectors.yml with tenant selector

    Phase 3 — Run dbt pipeline
        Full run + reporting refresh. BSL column catalog auto-populates.

    Phase 4 — Activate tenant
        Flips tenant status from 'onboarding' to 'active' in tenants.yaml.
"""
import pathlib
import argparse
import sys
import subprocess
import duckdb
import os
import hashlib
import yaml

# --- Path & Service Setup ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "warehouse" / "gata_transformation"
MASTER_MODELS_DIR = DBT_PROJECT_DIR / "models" / "platform" / "master_models"
MASTER_MODEL_TEMPLATE = "{{ generate_master_model() }}\n"

sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

from orchestrator import MockOrchestrator
from config import load_manifest

# Standard connector names for table name parsing
REGISTRY_KEYS = [
    'facebook_ads', 'google_ads', 'linkedin_ads', 'bing_ads', 'amazon_ads',
    'tiktok_ads', 'instagram_ads', 'shopify', 'woocommerce', 'bigcommerce',
    'amplitude', 'mixpanel', 'google_analytics'
]

# Analytics connectors that need conversion_events logic in dbt_project.yml
ANALYTICS_CONNECTORS = {'google_analytics', 'mixpanel', 'amplitude'}


# ═══════════════════════════════════════════════════════════════
# INTERMEDIATE MODEL SPECS
# ═══════════════════════════════════════════════════════════════
# Each connector maps to a list of intermediate models to generate.
# 'macro' type uses generate_intermediate_unpacker; 'raw_sql' uses a template.

def _fmt_col(col):
    """Format a single column spec dict as Jinja-compatible string."""
    parts = []
    for k, v in col.items():
        if "'" in str(v):
            parts.append(f"'{k}': \"{v}\"")
        else:
            parts.append(f"'{k}': '{v}'")
    return '{' + ', '.join(parts) + '}'


def _macro_model(tenant_slug, source_platform, master_model_id, columns):
    """Generate a generate_intermediate_unpacker macro call."""
    col_lines = ',\n'.join(f'        {_fmt_col(c)}' for c in columns)
    return (
        f"{{{{ generate_intermediate_unpacker(\n"
        f"    tenant_slug='{tenant_slug}',\n"
        f"    source_platform='{source_platform}',\n"
        f"    master_model_id='{master_model_id}',\n"
        f"    columns=[\n{col_lines}\n"
        f"    ]\n"
        f") }}}}"
    )


def _raw_sql_model(tenant_slug, source_platform, master_model_id, select_body):
    """Generate a raw SQL intermediate model."""
    return (
        f"{{{{ config(materialized='table') }}}}\n\n"
        f"SELECT\n"
        f"    tenant_slug,\n"
        f"    source_platform,\n"
        f"    tenant_skey,\n"
        f"    loaded_at,\n\n"
        f"{select_body}\n\n"
        f"    raw_data_payload\n\n"
        f"FROM {{{{ ref('platform_mm__{master_model_id}') }}}}\n"
        f"WHERE tenant_slug = '{tenant_slug}'\n"
        f"  AND source_platform = '{source_platform}'"
    )


# --- Column specs (shared across connectors) ---

_FB_INSIGHTS_COLS = [
    {'json_key': 'date_start', 'alias': 'report_date', 'cast_to': 'DATE'},
    {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
    {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
    {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
    {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'},
    {'json_key': 'campaign_id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'adset_id', 'alias': 'adset_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'ad_id', 'alias': 'ad_id', 'cast_to': 'VARCHAR'},
]

_CAMPAIGNS_COLS = [
    {'json_key': 'id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'name', 'alias': 'campaign_name', 'cast_to': 'VARCHAR'},
    {'json_key': 'status', 'alias': 'status', 'cast_to': 'VARCHAR'},
]

_BING_PERF_COLS = [
    {'json_key': 'time_period', 'alias': 'report_date', 'cast_to': 'DATE'},
    {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
    {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
    {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
]

_GA_EVENTS_COLS = [
    {'json_key': 'event_name', 'alias': 'event_name', 'cast_to': 'VARCHAR'},
    {'json_key': 'event_date', 'alias': 'event_date', 'cast_to': 'VARCHAR'},
    {'json_key': 'event_timestamp', 'alias': 'event_timestamp', 'cast_to': 'BIGINT'},
    {'json_key': 'user_pseudo_id', 'alias': 'user_pseudo_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'user_id', 'alias': 'user_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'geo_country', 'alias': 'geo_country', 'cast_to': 'VARCHAR'},
    {'json_key': 'geo_city', 'alias': 'geo_city', 'cast_to': 'VARCHAR'},
    {'json_key': 'traffic_source_source', 'alias': 'traffic_source', 'cast_to': 'VARCHAR'},
    {'json_key': 'traffic_source_medium', 'alias': 'traffic_medium', 'cast_to': 'VARCHAR'},
    {'json_key': 'traffic_source_campaign', 'alias': 'traffic_campaign', 'cast_to': 'VARCHAR'},
    {'json_key': 'device_category', 'alias': 'device_category', 'cast_to': 'VARCHAR'},
    {'json_key': 'ga_session_id', 'alias': 'ga_session_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'ecommerce_transaction_id', 'alias': 'transaction_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'ecommerce_value', 'alias': 'purchase_revenue', 'cast_to': 'DOUBLE'},
    {'json_key': 'ecommerce_currency', 'alias': 'ecommerce_currency', 'cast_to': 'VARCHAR'},
]

_MIXPANEL_EVENTS_COLS = [
    {'json_key': 'event', 'alias': 'event_name', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_distinct_id', 'alias': 'user_pseudo_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_distinct_id', 'alias': 'user_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_time', 'alias': 'event_timestamp', 'cast_to': 'BIGINT',
     'expression': "CAST(raw_data_payload->>'$.prop_time' AS BIGINT) * 1000"},
    {'json_key': 'prop_city', 'alias': 'geo_city', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_country_code', 'alias': 'geo_country', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_device_type', 'alias': 'device_category', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_utm_source', 'alias': 'traffic_source', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_utm_medium', 'alias': 'traffic_medium', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_utm_campaign', 'alias': 'traffic_campaign', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_order_id', 'alias': 'prop_order_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'prop_revenue', 'alias': 'prop_revenue', 'cast_to': 'DOUBLE'},
]

_AMPLITUDE_EVENTS_COLS = [
    {'json_key': 'event_type', 'alias': 'event_name', 'cast_to': 'VARCHAR'},
    {'json_key': 'user_id', 'alias': 'user_pseudo_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'user_id', 'alias': 'user_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'event_time', 'alias': 'event_timestamp', 'cast_to': 'BIGINT'},
    {'json_key': 'session_id', 'alias': 'session_id', 'cast_to': 'VARCHAR'},
    {'json_key': 'city', 'alias': 'geo_city', 'cast_to': 'VARCHAR'},
    {'json_key': 'country', 'alias': 'geo_country', 'cast_to': 'VARCHAR'},
    {'json_key': 'device_type', 'alias': 'device_category', 'cast_to': 'VARCHAR'},
    {'json_key': 'utm_source', 'alias': 'traffic_source', 'cast_to': 'VARCHAR'},
    {'json_key': 'utm_medium', 'alias': 'traffic_medium', 'cast_to': 'VARCHAR'},
    {'json_key': 'utm_campaign', 'alias': 'traffic_campaign', 'cast_to': 'VARCHAR'},
]

# Raw SQL select bodies for models with computed fields or JSON arrays
_GOOGLE_ADS_PERF_SELECT = """\
    CAST(raw_data_payload->>'$.date_start' AS DATE) AS report_date,
    CAST(raw_data_payload->>'$.cost_micros' AS BIGINT) / 1000000.0 AS spend,
    CAST(raw_data_payload->>'$.impressions' AS BIGINT) AS impressions,
    CAST(raw_data_payload->>'$.clicks' AS BIGINT) AS clicks,
    CAST(raw_data_payload->>'$.conversions' AS DOUBLE) AS conversions,
    raw_data_payload->>'$.campaign_id' AS campaign_id,
    raw_data_payload->>'$.ad_group_id' AS ad_group_id,
    raw_data_payload->>'$.ad_id' AS ad_id,"""

_SHOPIFY_ORDERS_SELECT = """\
    CAST(raw_data_payload->>'$.id' AS BIGINT) AS order_id,
    raw_data_payload->>'$.name' AS order_name,
    raw_data_payload->>'$.email' AS email,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE) AS total_price,
    raw_data_payload->>'$.currency' AS currency,
    raw_data_payload->>'$.financial_status' AS financial_status,
    raw_data_payload->>'$.customer_email' AS customer_email,
    raw_data_payload->>'$.customer_id' AS customer_id,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS order_created_at,
    raw_data_payload->'$.line_items' AS line_items_json,"""

_BIGCOMMERCE_ORDERS_SELECT = """\
    CAST(raw_data_payload->>'$.id' AS BIGINT) AS order_id,
    raw_data_payload->>'$.status' AS order_status,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE) AS total_price,
    raw_data_payload->>'$.currency' AS currency,
    CAST(raw_data_payload->>'$.customer_id' AS BIGINT) AS customer_id,
    raw_data_payload->>'$.billing_email' AS billing_email,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS order_created_at,"""

_WOOCOMMERCE_ORDERS_SELECT = """\
    raw_data_payload->>'$.number' AS order_id,
    CAST(raw_data_payload->>'$.id' AS BIGINT) AS woocommerce_id,
    raw_data_payload->>'$.status' AS order_status,
    CAST(raw_data_payload->>'$.total_price' AS DOUBLE) AS total_price,
    raw_data_payload->>'$.currency' AS currency,
    CAST(raw_data_payload->>'$.customer_id' AS BIGINT) AS customer_id,
    raw_data_payload->>'$.billing_email' AS customer_email,
    CAST(raw_data_payload->>'$.created_at' AS TIMESTAMP) AS order_created_at,
    raw_data_payload->'$.line_items' AS line_items_json,"""

# Connector -> list of intermediate model specs
# Each spec: (model_suffix, generator_function_args)
INTERMEDIATE_SPECS = {
    'facebook_ads': [
        ('facebook_ads_facebook_insights', 'macro', 'facebook_ads', 'facebook_ads_api_v1_facebook_insights', _FB_INSIGHTS_COLS),
        ('facebook_ads_campaigns', 'macro', 'facebook_ads', 'facebook_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'instagram_ads': [
        ('instagram_ads_facebook_insights', 'macro', 'instagram_ads', 'facebook_ads_api_v1_facebook_insights', _FB_INSIGHTS_COLS),
        ('instagram_ads_campaigns', 'macro', 'instagram_ads', 'facebook_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'google_ads': [
        ('google_ads_ad_performance', 'raw_sql', 'google_ads', 'google_ads_api_v1_ad_performance', _GOOGLE_ADS_PERF_SELECT),
        ('google_ads_campaigns', 'macro', 'google_ads', 'google_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'bing_ads': [
        ('bing_ads_account_performance_report', 'macro', 'bing_ads', 'bing_ads_api_v1_account_performance_report', _BING_PERF_COLS),
        ('bing_ads_campaigns', 'macro', 'bing_ads', 'bing_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'linkedin_ads': [
        ('linkedin_ads_ad_analytics_by_campaign', 'macro', 'linkedin_ads', 'linkedin_ads_api_v1_ad_analytics_by_campaign', [
            {'json_key': 'date_start', 'alias': 'report_date', 'cast_to': 'DATE'},
            {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
            {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
            {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
            {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'},
        ]),
        ('linkedin_ads_campaigns', 'macro', 'linkedin_ads', 'linkedin_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'amazon_ads': [
        ('amazon_ads_sponsored_products_product_ads', 'macro', 'amazon_ads', 'amazon_ads_api_v1_sponsored_products_product_ads', [
            {'json_key': 'date', 'alias': 'report_date', 'cast_to': 'DATE'},
            {'json_key': 'campaign_id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'ad_group_id', 'alias': 'ad_group_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'ad_id', 'alias': 'ad_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
            {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
            {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
            {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'},
        ]),
        ('amazon_ads_campaigns', 'macro', 'amazon_ads', 'amazon_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'tiktok_ads': [
        ('tiktok_ads_ads_reports_daily', 'macro', 'tiktok_ads', 'tiktok_ads_api_v1_ads_reports_daily', [
            {'json_key': 'stat_time_day', 'alias': 'report_date', 'cast_to': 'DATE'},
            {'json_key': 'campaign_id', 'alias': 'campaign_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'adgroup_id', 'alias': 'ad_group_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'ad_id', 'alias': 'ad_id', 'cast_to': 'VARCHAR'},
            {'json_key': 'spend', 'alias': 'spend', 'cast_to': 'DOUBLE'},
            {'json_key': 'impressions', 'alias': 'impressions', 'cast_to': 'BIGINT'},
            {'json_key': 'clicks', 'alias': 'clicks', 'cast_to': 'BIGINT'},
            {'json_key': 'conversions', 'alias': 'conversions', 'cast_to': 'DOUBLE'},
        ]),
        ('tiktok_ads_campaigns', 'macro', 'tiktok_ads', 'tiktok_ads_api_v1_campaigns', _CAMPAIGNS_COLS),
    ],
    'shopify': [
        ('shopify_orders', 'raw_sql', 'shopify', 'shopify_api_v1_orders', _SHOPIFY_ORDERS_SELECT),
    ],
    'bigcommerce': [
        ('bigcommerce_orders', 'raw_sql', 'bigcommerce', 'bigcommerce_api_v1_orders', _BIGCOMMERCE_ORDERS_SELECT),
    ],
    'woocommerce': [
        ('woocommerce_orders', 'raw_sql', 'woocommerce', 'woocommerce_api_v1_orders', _WOOCOMMERCE_ORDERS_SELECT),
    ],
    'google_analytics': [
        ('google_analytics_events', 'macro', 'google_analytics', 'google_analytics_api_v1_events', _GA_EVENTS_COLS),
    ],
    'mixpanel': [
        ('mixpanel_events', 'macro', 'mixpanel', 'mixpanel_api_v1_events', _MIXPANEL_EVENTS_COLS),
    ],
    'amplitude': [
        ('amplitude_events', 'macro', 'amplitude', 'amplitude_api_v1_events', _AMPLITUDE_EVENTS_COLS),
    ],
}

# Source category sets for semantic config generation
AD_SOURCES = {'facebook_ads', 'instagram_ads', 'google_ads', 'bing_ads', 'linkedin_ads', 'amazon_ads', 'tiktok_ads'}
ECOMMERCE_SOURCES = {'shopify', 'bigcommerce', 'woocommerce'}
ANALYTICS_SOURCES = {'google_analytics', 'mixpanel', 'amplitude'}

SEMANTIC_CONFIG_DIR = PROJECT_ROOT / "services" / "platform-api" / "semantic_configs"

# Analytics shell models — always the same 6 per tenant
ANALYTICS_SHELLS = [
    ('fct', 'ad_performance', 'build_fct_ad_performance'),
    ('fct', 'orders', 'build_fct_orders'),
    ('fct', 'sessions', 'build_fct_sessions'),
    ('fct', 'events', 'build_fct_events'),
    ('dim', 'campaigns', 'build_dim_campaigns'),
    ('dim', 'users', 'build_dim_users'),
]


# ═══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ═══════════════════════════════════════════════════════════════

def load_env_file():
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())


def get_db_connection(target='dev'):
    if target in ('sandbox', 'local'):
        return duckdb.connect(str(PROJECT_ROOT / "warehouse" / "sandbox.duckdb"))
    token = os.environ.get("MOTHERDUCK_TOKEN")
    return duckdb.connect(f"md:my_db?motherduck_token={token}" if token else "md:my_db")


def calculate_dlt_schema_hash(dlt_schema: dict, table_name: str) -> str:
    table_meta = dlt_schema.get('tables', {}).get(table_name, {})
    columns = table_meta.get('columns', {})
    sorted_cols = sorted([
        (n, str(p.get('data_type')))
        for n, p in columns.items()
        if not n.startswith(("_dlt", "_airbyte"))
    ])
    signature = "|".join([f"{c}:{t}" for c, t in sorted_cols])
    return hashlib.md5(signature.encode('utf-8')).hexdigest()


def lookup_master_model(schema_hash: str, target: str = 'dev') -> str:
    con = get_db_connection(target)
    try:
        result = con.sql(
            f"SELECT master_model_id FROM main.connector_blueprints "
            f"WHERE source_schema_hash = '{schema_hash}' LIMIT 1"
        ).fetchone()
        return result[0] if result else 'unknown'
    finally:
        con.close()


def ensure_master_model_file(master_model_id: str):
    """Create the dbt master model .sql file if it doesn't already exist."""
    MASTER_MODELS_DIR.mkdir(parents=True, exist_ok=True)
    model_file = MASTER_MODELS_DIR / f"platform_mm__{master_model_id}.sql"
    if not model_file.exists():
        model_file.write_text(MASTER_MODEL_TEMPLATE)
        print(f"  [NEW] Created master model: platform_mm__{master_model_id}.sql")
    return model_file


def _write_if_new(filepath, content):
    """Write file only if it doesn't exist. Returns True if written."""
    if filepath.exists():
        return False
    filepath.parent.mkdir(parents=True, exist_ok=True)
    filepath.write_text(content)
    return True


# ═══════════════════════════════════════════════════════════════
# PHASE 1: Generate + land mock data
# ═══════════════════════════════════════════════════════════════

def generate_mock_data(tenant_config, target, days):
    """Run MockOrchestrator to generate and land data via dlt."""
    credentials = 'duckdb' if target in ('sandbox', 'local') else 'motherduck'
    orchestrator = MockOrchestrator(tenant_config, days=days, credentials=credentials)
    dlt_schema_dict, dlt_load_id = orchestrator.run()
    print(f"  [OK] Data landed (load_id: {dlt_load_id})")
    return dlt_schema_dict, dlt_load_id


# ═══════════════════════════════════════════════════════════════
# PHASE 2a: Create staging scaffolding
# ═══════════════════════════════════════════════════════════════

def create_sources_yml(tenant_slug, source_name, tables, target='dev'):
    src_dir = DBT_PROJECT_DIR / "models" / "sources" / tenant_slug / source_name
    src_dir.mkdir(parents=True, exist_ok=True)
    source_entry = {
        "name": f"{tenant_slug}_{source_name}", "schema": tenant_slug,
        "tables": [{"name": t} for t in tables]
    }
    if target not in ('sandbox', 'local'):
        source_entry["database"] = "my_db"
    source_cfg = {"version": 2, "sources": [source_entry]}
    with open(src_dir / "_sources.yml", "w") as f:
        yaml.dump(source_cfg, f, default_flow_style=False)


def create_staging_scaffolding(tenant_slug, target, dlt_schema_dict):
    """Create source YAMLs, staging pushers, and master model files."""
    tenant_prefix = f"raw_{tenant_slug}_"
    processed_sources = {}

    for table_name in dlt_schema_dict.get('tables', {}).keys():
        if not table_name.startswith(tenant_prefix) or "_dlt" in table_name:
            continue

        remainder = table_name[len(tenant_prefix):]
        matched_source = None
        for s in REGISTRY_KEYS:
            if remainder.startswith(s + "_"):
                matched_source = s
                break

        if not matched_source:
            continue

        object_name = remainder[len(matched_source) + 1:]

        if matched_source not in processed_sources:
            processed_sources[matched_source] = []
        processed_sources[matched_source].append(table_name)

        # Route via connector_blueprints
        schema_hash = calculate_dlt_schema_hash(dlt_schema_dict, table_name)
        master_model_id = lookup_master_model(schema_hash, target)

        if master_model_id == 'unknown':
            print(f"  [WARN] Hash {schema_hash[:8]} unknown for {table_name}. Skipping.")
            continue

        ensure_master_model_file(master_model_id)

        # Staging pusher
        stg_dir = DBT_PROJECT_DIR / "models" / "staging" / tenant_slug / matched_source
        stg_dir.mkdir(parents=True, exist_ok=True)
        stg_filename = f"stg_{tenant_slug}__{matched_source}_{object_name}.sql"
        stg_content = (
            f"{{{{ generate_staging_pusher("
            f"tenant_slug='{tenant_slug}', "
            f"source_name='{matched_source}', "
            f"schema_hash='{schema_hash}', "
            f"master_model_id='{master_model_id}', "
            f"source_table='{table_name}') }}}}"
        )
        with open(stg_dir / stg_filename, "w") as f:
            f.write(stg_content.strip())
        print(f"  [OK] Staging: {stg_filename}")

    # Source YAMLs
    for source_name, tables in processed_sources.items():
        create_sources_yml(tenant_slug, source_name, tables, target)

    print(f"  [OK] Staging scaffolding complete ({len(processed_sources)} sources)")
    return processed_sources


# ═══════════════════════════════════════════════════════════════
# PHASE 2b: Create intermediate models
# ═══════════════════════════════════════════════════════════════

def create_intermediate_models(tenant_slug, enabled_sources):
    """Auto-generate intermediate models for each enabled connector."""
    int_dir = DBT_PROJECT_DIR / "models" / "intermediate" / tenant_slug
    count = 0

    for source in enabled_sources:
        specs = INTERMEDIATE_SPECS.get(source, [])
        for spec in specs:
            suffix, model_type, src_platform, master_model_id, data = spec
            filename = f"int_{tenant_slug}__{suffix}.sql"
            filepath = int_dir / filename

            if model_type == 'macro':
                content = _macro_model(tenant_slug, src_platform, master_model_id, data)
            else:
                content = _raw_sql_model(tenant_slug, src_platform, master_model_id, data)

            if _write_if_new(filepath, content):
                print(f"  [OK] Intermediate: {filename}")
                count += 1

    print(f"  [OK] Intermediate models complete ({count} created)")


# ═══════════════════════════════════════════════════════════════
# PHASE 2c: Create analytics shell models
# ═══════════════════════════════════════════════════════════════

def create_analytics_shells(tenant_slug):
    """Auto-generate the 6 analytics shell models."""
    analytics_dir = DBT_PROJECT_DIR / "models" / "analytics" / tenant_slug
    count = 0

    for prefix, subject, factory in ANALYTICS_SHELLS:
        filename = f"{prefix}_{tenant_slug}__{subject}.sql"
        content = "{{ " + f"{factory}('{tenant_slug}')" + " }}"
        filepath = analytics_dir / filename

        if _write_if_new(filepath, content):
            print(f"  [OK] Analytics: {filename}")
            count += 1

    print(f"  [OK] Analytics shells complete ({count} created)")


# ═══════════════════════════════════════════════════════════════
# PHASE 2e: Generate BSL semantic config YAML
# ═══════════════════════════════════════════════════════════════

def _build_semantic_models(slug: str) -> list[dict]:
    """Build the 6 standard semantic model definitions for a tenant."""
    return [
        {
            'name': f'fct_{slug}__ad_performance',
            'label': 'Ad Performance',
            'description': 'Daily ad spend and engagement metrics across all ad platforms.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'report_date', 'type': 'date'},
                {'name': 'campaign_id', 'type': 'string'},
                {'name': 'ad_group_id', 'type': 'string'},
                {'name': 'ad_id', 'type': 'string'},
            ],
            'measures': [
                {'name': 'spend', 'type': 'number', 'agg': 'sum'},
                {'name': 'impressions', 'type': 'number', 'agg': 'sum'},
                {'name': 'clicks', 'type': 'number', 'agg': 'sum'},
                {'name': 'conversions', 'type': 'number', 'agg': 'sum'},
            ],
            'calculated_measures': [
                {'name': 'ctr', 'label': 'Click-Through Rate',
                 'sql': 'CASE WHEN SUM(impressions) > 0 THEN SUM(clicks) * 1.0 / SUM(impressions) ELSE 0 END',
                 'format': 'percent'},
                {'name': 'cpc', 'label': 'Cost Per Click',
                 'sql': 'CASE WHEN SUM(clicks) > 0 THEN SUM(spend) / SUM(clicks) ELSE 0 END',
                 'format': 'currency'},
                {'name': 'cpm', 'label': 'Cost Per Mille',
                 'sql': 'CASE WHEN SUM(impressions) > 0 THEN SUM(spend) * 1000.0 / SUM(impressions) ELSE 0 END',
                 'format': 'currency'},
            ],
            'joins': [
                {'to': f'dim_{slug}__campaigns', 'type': 'left',
                 'on': {'campaign_id': 'campaign_id', 'source_platform': 'source_platform'}},
            ],
        },
        {
            'name': f'fct_{slug}__orders',
            'label': 'Orders',
            'description': 'Ecommerce transactions with customer and financial details.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'order_date', 'type': 'timestamp'},
                {'name': 'currency', 'type': 'string'},
                {'name': 'financial_status', 'type': 'string'},
                {'name': 'customer_email', 'type': 'string'},
                {'name': 'customer_id', 'type': 'string'},
            ],
            'measures': [
                {'name': 'total_price', 'type': 'number', 'agg': 'sum'},
                {'name': 'order_id', 'type': 'number', 'agg': 'count_distinct'},
            ],
            'calculated_measures': [
                {'name': 'aov', 'label': 'Average Order Value',
                 'sql': 'CASE WHEN COUNT(DISTINCT order_id) > 0 THEN SUM(total_price) / COUNT(DISTINCT order_id) ELSE 0 END',
                 'format': 'currency'},
            ],
            'joins': [
                {'to': f'dim_{slug}__users', 'type': 'left',
                 'on': {'customer_email': 'customer_email'}},
            ],
        },
        {
            'name': f'fct_{slug}__sessions',
            'label': 'Sessions',
            'description': 'Sessionized web analytics with attribution, duration, and conversion flags.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'user_pseudo_id', 'type': 'string'},
                {'name': 'session_start_ts', 'type': 'timestamp_epoch'},
                {'name': 'session_end_ts', 'type': 'timestamp_epoch'},
                {'name': 'traffic_source', 'type': 'string'},
                {'name': 'traffic_medium', 'type': 'string'},
                {'name': 'traffic_campaign', 'type': 'string'},
                {'name': 'geo_country', 'type': 'string'},
                {'name': 'device_category', 'type': 'string'},
                {'name': 'is_conversion_session', 'type': 'boolean'},
            ],
            'measures': [
                {'name': 'session_duration_seconds', 'type': 'number', 'agg': 'avg'},
                {'name': 'events_in_session', 'type': 'number', 'agg': 'avg'},
                {'name': 'session_revenue', 'type': 'number', 'agg': 'sum'},
                {'name': 'session_id', 'type': 'string', 'agg': 'count_distinct'},
            ],
            'calculated_measures': [
                {'name': 'conversion_rate', 'label': 'Session Conversion Rate',
                 'sql': "CASE WHEN COUNT(DISTINCT session_id) > 0 THEN SUM(CASE WHEN is_conversion_session THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT session_id) ELSE 0 END",
                 'format': 'percent'},
            ],
            'joins': [
                {'to': f'dim_{slug}__users', 'type': 'left',
                 'on': {'user_pseudo_id': 'user_pseudo_id'}},
                {'to': f'dim_{slug}__campaigns', 'type': 'left',
                 'on': {'traffic_campaign': 'campaign_name'}},
            ],
        },
        {
            'name': f'fct_{slug}__events',
            'label': 'Events',
            'description': 'Raw analytics events with attribution and optional ecommerce data.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'event_name', 'type': 'string'},
                {'name': 'event_timestamp', 'type': 'timestamp_epoch'},
                {'name': 'user_pseudo_id', 'type': 'string'},
                {'name': 'session_id', 'type': 'string'},
                {'name': 'order_id', 'type': 'string'},
                {'name': 'traffic_source', 'type': 'string'},
                {'name': 'traffic_medium', 'type': 'string'},
                {'name': 'traffic_campaign', 'type': 'string'},
                {'name': 'geo_country', 'type': 'string'},
                {'name': 'device_category', 'type': 'string'},
            ],
            'measures': [
                {'name': 'event_timestamp', 'type': 'number', 'agg': 'count', 'label': 'event_count'},
                {'name': 'order_total', 'type': 'number', 'agg': 'sum'},
            ],
            'joins': [
                {'to': f'dim_{slug}__users', 'type': 'left',
                 'on': {'user_pseudo_id': 'user_pseudo_id'}},
            ],
        },
        {
            'name': f'dim_{slug}__campaigns',
            'label': 'Campaigns',
            'description': 'Campaign dimension with name and status across ad platforms.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'campaign_id', 'type': 'string'},
                {'name': 'campaign_name', 'type': 'string'},
                {'name': 'campaign_status', 'type': 'string'},
            ],
            'measures': [],
            'joins': [],
        },
        {
            'name': f'dim_{slug}__users',
            'label': 'Users',
            'description': 'Unified user dimension combining analytics and ecommerce identities.',
            'dimensions': [
                {'name': 'source_platform', 'type': 'string'},
                {'name': 'user_pseudo_id', 'type': 'string'},
                {'name': 'user_id', 'type': 'string'},
                {'name': 'customer_email', 'type': 'string'},
                {'name': 'customer_id', 'type': 'string'},
                {'name': 'is_customer', 'type': 'boolean'},
                {'name': 'first_seen_at', 'type': 'timestamp_epoch'},
                {'name': 'last_seen_at', 'type': 'timestamp_epoch'},
                {'name': 'first_geo_country', 'type': 'string'},
                {'name': 'first_device_category', 'type': 'string'},
            ],
            'measures': [
                {'name': 'total_events', 'type': 'number', 'agg': 'sum'},
                {'name': 'total_sessions', 'type': 'number', 'agg': 'sum'},
            ],
            'joins': [],
        },
    ]


def generate_semantic_config(tenant_slug: str, enabled_sources: list[str], business_name: str = None):
    """Auto-generate BSL semantic config YAML for the platform API."""
    config_path = SEMANTIC_CONFIG_DIR / f"{tenant_slug}.yaml"
    if config_path.exists():
        print(f"  [SKIP] Semantic config already exists: {config_path.name}")
        return

    # Resolve business_name: param > tenants.yaml > slugify fallback
    if not business_name:
        tenants_path = PROJECT_ROOT / "tenants.yaml"
        if tenants_path.exists():
            with open(tenants_path) as f:
                tenants_cfg = yaml.safe_load(f) or {}
            for t in tenants_cfg.get('tenants', []):
                if t.get('slug') == tenant_slug:
                    business_name = t.get('business_name')
                    break
    if not business_name:
        business_name = tenant_slug.replace('_', ' ').title()

    # Classify sources
    ads = sorted([s for s in enabled_sources if s in AD_SOURCES])
    ecommerce = sorted([s for s in enabled_sources if s in ECOMMERCE_SOURCES])
    analytics = sorted([s for s in enabled_sources if s in ANALYTICS_SOURCES])

    slug = tenant_slug

    config = {
        'tenant': {
            'slug': slug,
            'business_name': business_name,
            'source_platforms': {
                'ads': ads,
                'ecommerce': ecommerce,
                'analytics': analytics,
            }
        },
        'models': _build_semantic_models(slug)
    }

    SEMANTIC_CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    with open(config_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
    print(f"  [OK] Semantic config: {config_path.name}")


# ═══════════════════════════════════════════════════════════════
# PHASE 2f: Update selectors.yml with tenant selector
# ═══════════════════════════════════════════════════════════════

def update_selectors_yml(tenant_slug: str):
    """Add tenant selector to selectors.yml if not already present."""
    selectors_path = DBT_PROJECT_DIR / "selectors.yml"
    with open(selectors_path) as f:
        config = yaml.safe_load(f)

    # Check if selector already exists
    existing_names = {s['name'] for s in config.get('selectors', [])}
    if tenant_slug in existing_names:
        print(f"  [SKIP] Selector for {tenant_slug} already exists")
        return

    # Find the insertion point — before operational selectors
    selectors_list = config['selectors']
    OPERATIONAL_SELECTORS = {'reporting_refresh', 'safe_full_refresh', 'master_models_only'}
    insert_idx = len(selectors_list)
    for i, sel in enumerate(selectors_list):
        if sel['name'] in OPERATIONAL_SELECTORS:
            insert_idx = i
            break

    new_selector = {
        'name': tenant_slug,
        'definition': {
            'union': [
                {'tag': tenant_slug},
                {'method': 'fqn', 'value': tenant_slug},
            ]
        }
    }

    selectors_list.insert(insert_idx, new_selector)

    with open(selectors_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"  [OK] Added selector for {tenant_slug}")


# ═══════════════════════════════════════════════════════════════
# PHASE 2d: Update dbt_project.yml tenant_configs
# ═══════════════════════════════════════════════════════════════

def update_dbt_project_yml(tenant_slug, enabled_sources):
    """Add tenant to dbt_project.yml tenant_configs if not already present."""
    yml_path = DBT_PROJECT_DIR / "dbt_project.yml"
    with open(yml_path) as f:
        config = yaml.safe_load(f)

    tenants_list = config['vars']['tenant_configs']['tenants']

    # Check if already exists
    existing = next((t for t in tenants_list if t['slug'] == tenant_slug), None)
    if existing:
        print(f"  [SKIP] {tenant_slug} already in dbt_project.yml")
        return

    # Build sources config
    sources_cfg = {}
    for source in enabled_sources:
        entry = {'enabled': True}
        if source in ANALYTICS_CONNECTORS:
            entry['logic'] = {'conversion_events': ['purchase']}
        sources_cfg[source] = entry

    tenants_list.append({'slug': tenant_slug, 'sources': sources_cfg})

    with open(yml_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"  [OK] Added {tenant_slug} to dbt_project.yml")


# ═══════════════════════════════════════════════════════════════
# PHASE 3: Run dbt pipeline
# ═══════════════════════════════════════════════════════════════

def run_dbt_pipeline(target='dev', tenant_slug=None):
    """Run dbt pipeline, optionally scoped to a single tenant."""
    if os.environ.get("RENDER"):
        dbt_base = ["dbt"]
    else:
        dbt_base = ["uv", "run"]
        if target not in ('sandbox', 'local'):
            dbt_base += ["--env-file", "../../.env"]
        dbt_base.append("dbt")

    if tenant_slug:
        # Scoped run: only this tenant's models + shared infrastructure
        select = [
            "--select",
            "path:models/platform/master_models",
            f"path:models/staging/{tenant_slug}",
            f"path:models/intermediate/{tenant_slug}",
            f"path:models/analytics/{tenant_slug}",
            "path:models/platform/ops",
        ]
        label = f" (tenant: {tenant_slug})"
    else:
        select = []
        label = ""

    # Pipeline run
    print(f"  [RUN] dbt run --target {target}{label}")
    result = subprocess.run(
        [*dbt_base, "run", "--target", target, *select],
        cwd=str(DBT_PROJECT_DIR),
    )
    print(f"  [{'OK' if result.returncode == 0 else 'FAIL'}] Full run (exit {result.returncode})")
    if result.returncode != 0:
        return result.returncode

    # Reporting refresh (second pass — data flows through after staging MERGEs)
    if tenant_slug:
        refresh_select = [
            "--select",
            f"path:models/intermediate/{tenant_slug}",
            f"path:models/analytics/{tenant_slug}",
            "path:models/platform/ops",
        ]
    else:
        refresh_select = ["--selector", "reporting_refresh"]

    print(f"  [RUN] dbt run --target {target} (reporting refresh{label})")
    result = subprocess.run(
        [*dbt_base, "run", "--target", target, *refresh_select],
        cwd=str(DBT_PROJECT_DIR),
    )
    print(f"  [{'OK' if result.returncode == 0 else 'FAIL'}] Reporting refresh (exit {result.returncode})")
    return result.returncode


# ═══════════════════════════════════════════════════════════════
# PHASE 4: Activate tenant
# ═══════════════════════════════════════════════════════════════

def activate_tenant(tenant_slug: str):
    """Set tenant status to 'active' in tenants.yaml."""
    tenants_path = PROJECT_ROOT / "tenants.yaml"
    with open(tenants_path) as f:
        config = yaml.safe_load(f)

    for tenant in config.get('tenants', []):
        if tenant.get('slug') == tenant_slug:
            if tenant.get('status') == 'active':
                print(f"  [SKIP] {tenant_slug} already active")
                return
            tenant['status'] = 'active'
            break
    else:
        print(f"  [WARN] {tenant_slug} not found in tenants.yaml")
        return

    with open(tenants_path, 'w') as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    print(f"  [OK] Status: {tenant_slug} -> active")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

def onboard(tenant_slug, target='dev', days=30, skip_dbt=False):
    """Single entry point for tenant onboarding."""
    load_env_file()

    manifest = load_manifest(str(PROJECT_ROOT / "tenants.yaml"))
    tenant_config = next((t for t in manifest.tenants if t.slug == tenant_slug), None)
    if not tenant_config:
        print(f"[ERR] Tenant '{tenant_slug}' not found in tenants.yaml")
        return 1

    # Resolve enabled sources from tenant config (SourceRegistry is a Pydantic model)
    enabled_sources = [
        name for name, cfg in tenant_config.sources.model_dump().items()
        if cfg.get('enabled', False)
    ]

    # Phase 1: Mock data
    print(f"\n{'='*60}")
    print(f"  PHASE 1: Generate mock data for {tenant_slug}")
    print(f"{'='*60}")
    dlt_schema_dict, _load_id = generate_mock_data(tenant_config, target, days)

    # Phase 2: dbt scaffolding
    print(f"\n{'='*60}")
    print(f"  PHASE 2: Create dbt scaffolding")
    print(f"{'='*60}")

    # 2a: Sources, staging pushers, master models
    create_staging_scaffolding(tenant_slug, target, dlt_schema_dict)

    # 2b: Intermediate models (JSON extraction)
    create_intermediate_models(tenant_slug, enabled_sources)

    # 2c: Analytics shell models (factory one-liners)
    create_analytics_shells(tenant_slug)

    # 2d: Update dbt_project.yml
    update_dbt_project_yml(tenant_slug, enabled_sources)

    # 2e: BSL semantic config YAML
    generate_semantic_config(tenant_slug, enabled_sources,
                             business_name=tenant_config.business_name)

    # 2f: Update selectors.yml
    update_selectors_yml(tenant_slug)

    # Phase 3: dbt pipeline
    if skip_dbt:
        print("\n[SKIP] dbt runs (--skip-dbt flag)")
        return 0

    print(f"\n{'='*60}")
    print(f"  PHASE 3: Run dbt pipeline (target={target})")
    print(f"{'='*60}")
    exit_code = run_dbt_pipeline(target, tenant_slug=tenant_slug)

    # Phase 4: Activate tenant on success
    if exit_code == 0:
        print(f"\n{'='*60}")
        print(f"  PHASE 4: Activate tenant")
        print(f"{'='*60}")
        activate_tenant(tenant_slug)

    return exit_code


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Onboard a tenant: mock data + scaffolding + dbt")
    parser.add_argument("tenant_slug")
    parser.add_argument("--target", default="dev", choices=["dev", "sandbox", "local"])
    parser.add_argument("--days", type=int, default=180)
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt runs after scaffolding")
    args = parser.parse_args()
    sys.exit(onboard(args.tenant_slug, args.target, args.days, args.skip_dbt))
