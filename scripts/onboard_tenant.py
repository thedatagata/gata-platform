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
        Source YAMLs (_sources.yml), staging pushers (sync_to_master_hub),
        and master model files. Uses connector_blueprints for routing.

    Phase 3 — Run dbt pipeline
        Full run: materializes master models, staging post-hooks register
        source schema history + push data into master hubs. Governance
        models (tenant config history, hub key registry) derive from
        tenants.yaml which was already updated by the web app.
        Reporting refresh: second pass for intermediate + analytics layers
        to materialize star schema tables.
        BSL column catalog auto-populates from INFORMATION_SCHEMA,
        powering the platform API for the tenant.
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
# PHASE 2: Create dbt scaffolding
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


def create_scaffolding(tenant_slug, target, dlt_schema_dict):
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
        print(f"  [OK] Staging pusher: {stg_filename}")

    # Source YAMLs
    for source_name, tables in processed_sources.items():
        create_sources_yml(tenant_slug, source_name, tables, target)

    print(f"  [OK] Scaffolding complete ({len(processed_sources)} sources)")


# ═══════════════════════════════════════════════════════════════
# PHASE 3: Run dbt pipeline
# ═══════════════════════════════════════════════════════════════

def run_dbt_pipeline(target='dev'):
    """Run dbt full pipeline + reporting refresh.

    Full run materializes:
      - Master models (incremental sinks)
      - Staging post-hooks (sync_to_master_hub + sync_to_schema_history)
      - Governance models (tenant config history, hub key registry)
      - Platform ops (BSL column catalog)

    Reporting refresh re-runs:
      - Intermediate models (JSON extraction from master models)
      - Analytics models (star schema facts + dims)
    """
    env_file = str(PROJECT_ROOT / ".env")
    dbt_base = ["uv", "run", "--env-file", env_file, "dbt"]

    # Full pipeline
    print(f"  [RUN] dbt run --target {target}")
    result = subprocess.run(
        [*dbt_base, "run", "--target", target],
        cwd=str(DBT_PROJECT_DIR),
    )
    print(f"  [{'OK' if result.returncode == 0 else 'FAIL'}] Full run (exit {result.returncode})")
    if result.returncode != 0:
        return result.returncode

    # Reporting refresh (second pass)
    print(f"  [RUN] dbt run --target {target} --selector reporting_refresh")
    result = subprocess.run(
        [*dbt_base, "run", "--target", target, "--selector", "reporting_refresh"],
        cwd=str(DBT_PROJECT_DIR),
    )
    print(f"  [{'OK' if result.returncode == 0 else 'FAIL'}] Reporting refresh (exit {result.returncode})")
    return result.returncode


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

    # Phase 1: Mock data
    print(f"\n{'='*60}")
    print(f"  PHASE 1: Generate mock data for {tenant_slug}")
    print(f"{'='*60}")
    dlt_schema_dict, _load_id = generate_mock_data(tenant_config, target, days)

    # Phase 2: dbt scaffolding
    print(f"\n{'='*60}")
    print(f"  PHASE 2: Create dbt scaffolding")
    print(f"{'='*60}")
    create_scaffolding(tenant_slug, target, dlt_schema_dict)

    # Phase 3: dbt pipeline
    if skip_dbt:
        print("\n[SKIP] dbt runs (--skip-dbt flag)")
        return 0

    print(f"\n{'='*60}")
    print(f"  PHASE 3: Run dbt pipeline (target={target})")
    print(f"{'='*60}")
    return run_dbt_pipeline(target)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Onboard a tenant: mock data + scaffolding + dbt")
    parser.add_argument("tenant_slug")
    parser.add_argument("--target", default="dev", choices=["dev", "sandbox", "local"])
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--skip-dbt", action="store_true", help="Skip dbt runs after scaffolding")
    args = parser.parse_args()
    sys.exit(onboard(args.tenant_slug, args.target, args.days, args.skip_dbt))
