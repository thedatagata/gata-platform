import pathlib
import argparse
import sys
import duckdb
import os
import json
import hashlib
import yaml

# --- Path & Service Setup ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DBT_PROJECT_DIR = PROJECT_ROOT / "warehouse" / "gata_transformation" 
sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

from orchestrator import MockOrchestrator
from config import load_manifest

def load_env_file():
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

load_env_file()

def get_db_connection(target='dev'):
    if target in ('sandbox', 'local'):
        return duckdb.connect(str(PROJECT_ROOT / "warehouse" / "sandbox.duckdb"))
    token = os.environ.get("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f"md:my_db?motherduck_token={token}" if token else "md:my_db")
    return con

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
        query = f"SELECT master_model_id FROM main.connector_blueprints WHERE source_schema_hash = '{schema_hash}' LIMIT 1"
        result = con.sql(query).fetchone()
        return result[0] if result else 'unknown'
    finally:
        con.close()

def generate_sources_yml(tenant_slug, source_name, tables, target='dev'):
    src_dir = DBT_PROJECT_DIR / "models" / "sources" / tenant_slug / source_name
    src_dir.mkdir(parents=True, exist_ok=True)
    source_entry = {
        "name": f"{tenant_slug}_{source_name}", "schema": tenant_slug,
        "tables": [{"name": t} for t in tables]
    }
    if target not in ('sandbox', 'local'):
        source_entry["database"] = "my_db"
    source_cfg = {
        "version": 2,
        "sources": [source_entry]
    }
    with open(src_dir / "_sources.yml", "w") as f:
        yaml.dump(source_cfg, f, default_flow_style=False)

def generate_scaffolding(tenant_slug, target, days=30):
    manifest = load_manifest(str(PROJECT_ROOT / "tenants.yaml"))
    tenant_config = next((t for t in manifest.tenants if t.slug == tenant_slug), None)

    if not tenant_config:
        print(f"[ERR] Tenant {tenant_slug} not found.")
        return

    # 1. LAND DATA
    print(f"[LOAD] Loading mock data for {tenant_slug}...")
    orchestrator = MockOrchestrator(tenant_config, days=days, credentials='duckdb' if target in ('sandbox', 'local') else 'motherduck')
    dlt_schema_dict, _dlt_load_id = orchestrator.run()

    # 2. GENERATE DBT MODELS DYNAMICALLY
    tenant_prefix = f"raw_{tenant_slug}_"
    processed_sources = {}
    
    # Standard source list to prevent parsing errors
    registry_keys = [
        'facebook_ads', 'google_ads', 'linkedin_ads', 'bing_ads', 'amazon_ads', 
        'tiktok_ads', 'instagram_ads', 'shopify', 'woocommerce', 'bigcommerce', 
        'amplitude', 'mixpanel', 'google_analytics'
    ]

    for table_name in dlt_schema_dict.get('tables', {}).keys():
        if not table_name.startswith(tenant_prefix) or "_dlt" in table_name: continue
        
        # Robust Parsing: Remainder = shopify_orders
        remainder = table_name[len(tenant_prefix):]
        matched_source = None
        for s in registry_keys:
            if remainder.startswith(s + "_"):
                matched_source = s
                break
        
        if not matched_source: continue
        
        object_name = remainder[len(matched_source)+1:]
        
        if matched_source not in processed_sources: 
            processed_sources[matched_source] = []
        processed_sources[matched_source].append(table_name)

        # Unique Routing
        schema_hash = calculate_dlt_schema_hash(dlt_schema_dict, table_name)
        master_model_id = lookup_master_model(schema_hash, target)
        
        if master_model_id == 'unknown':
            print(f"[WARN] DNA {schema_hash[:8]} unknown for {table_name}. Skipping.")
            continue

        # 3. Create Staging Pusher
        stg_dir = DBT_PROJECT_DIR / "models" / "staging" / tenant_slug / matched_source
        stg_dir.mkdir(parents=True, exist_ok=True)
        stg_filename = f"stg_{tenant_slug}__{matched_source}_{object_name}.sql"
        
        # Use physical table_name in the macro call to avoid double-prefixing
        stg_content = f"{{{{ generate_staging_pusher(tenant_slug='{tenant_slug}', source_name='{matched_source}', schema_hash='{schema_hash}', master_model_id='{master_model_id}', source_table='{table_name}') }}}}"
        
        with open(stg_dir / stg_filename, "w") as f:
            f.write(stg_content.strip())
        print(f"[OK] Created staging pusher: {stg_filename}")

    # 4. Finalize Sources
    for source_name, tables in processed_sources.items():
        generate_sources_yml(tenant_slug, source_name, tables, target)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_slug")
    parser.add_argument("--target", default="dev")
    parser.add_argument("--days", type=int, default=30)
    args = parser.parse_args()
    generate_scaffolding(args.tenant_slug, args.target, args.days)