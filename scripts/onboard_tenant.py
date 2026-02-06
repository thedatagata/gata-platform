import yaml
import pathlib
import argparse
import sys
import duckdb
import os
import json
import hashlib
from datetime import datetime

# --- Configuration ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "sandbox.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "gata_transformation" # Fixed path to match repo structure
MASTER_MODEL_DIR = DBT_PROJECT_DIR / "models" / "platform" / "master_models"

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

def get_db_connection(target):
    if target in ['motherduck', 'dev', 'prod']:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        # Ensure we target the database where the registry lives
        conn_str = "md:connectors" 
        if token:
             conn_str += f"?motherduck_token={token}"
        
        try:
            return duckdb.connect(conn_str)
        except Exception as e:
            print(f"❌ Failed to connect to MotherDuck: {e}")
            sys.exit(1)
    else:
        # For local, assume connectors registry is in its own duckdb
        local_db = PROJECT_ROOT / "warehouse" / "connectors.duckdb"
        return duckdb.connect(str(local_db))

def lookup_master_model(schema_hash: str, target: str = 'local', registry_schema: str = 'main') -> str:
    """Queries the central registry to see if this schema hash is already mapped."""
    con = get_db_connection(target)
    try:
        # Pointing to the table created by the library initialization
        query = f"SELECT master_model_id FROM {registry_schema}.connector_blueprints WHERE source_schema_hash = '{schema_hash}' LIMIT 1"
        result = con.sql(query).fetchone()
        return result[0] if result else 'unknown'
    except Exception as e:
        print(f"⚠️  Registry lookup failed: {e}")
        return 'unknown'
    finally:
        con.close()

def ensure_master_model_exists(master_model_id, source_platform, object_name):
    """
    Checks if the dbt master model file exists. 
    If not, scaffolds a shell using the appropriate factory macro.
    """
    MASTER_MODEL_DIR.mkdir(parents=True, exist_ok=True)
    file_path = MASTER_MODEL_DIR / f"platform_mm__{master_model_id}.sql"
    
    if file_path.exists():
        return True

    print(f"✨ Scaffolding NEW Master Model file: {file_path.name}")
    
    # Determine which factory macro to use based on platform/object
    # This aligns with existing blended fact logic
    factory_call = ""
    if any(p in source_platform for p in ['ads', 'facebook', 'google', 'tiktok', 'linkedin', 'bing', 'amazon']):
        factory_call = f"{{{{ build_ads_blended_fact('{master_model_id}') }}}}"
    elif any(p in source_platform for p in ['shopify', 'woocommerce', 'bigcommerce']):
        factory_call = f"{{{{ build_ecommerce_fact('{master_model_id}') }}}}"
    elif 'analytics' in source_platform or source_platform in ['amplitude', 'mixpanel']:
        factory_call = f"{{{{ build_analytics_fact('{master_model_id}') }}}}"
    else:
        # Fallback to a basic union if category is unclear
        factory_call = f"-- Generic Master Model for {master_model_id}\nSELECT * FROM {{{{ ref('hub_tenant_sources') }}}} WHERE master_model_ref = '{master_model_id}'"

    sql_content = f"""
-- Master Model generated for {source_platform} {object_name}
{{{{ config(materialized='incremental', unique_key='hub_key') }}}}

{factory_call}
"""
    with open(file_path, "w") as f:
        f.write(sql_content.strip())
    
    return True

def generate_scaffolding(tenant_slug, target, registry_schema='main'):
    # ... (existing logic for reading tenants.yaml and setting up directories) ...
    # Assume 'sources' dict contains source_name and tables list
    
    for source_name, tables in sources.items():
        # (existing src/stg directory creation)
        
        for item in tables:
            phys_table = item["physical_table"]
            obj_name = item["object_name"]
            
            # (existing column and hash calculation logic)
            schema_hash = hashlib.md5(schema_json.encode('utf-8')).hexdigest()

            # 1. Lookup the assigned ID from the library registry
            master_model_id = lookup_master_model(schema_hash, target, registry_schema)
            
            # 2. PROACTIVE CHECK: Ensure the physical dbt model exists to target
            if master_model_id != 'unknown':
                ensure_master_model_exists(master_model_id, source_name, obj_name)

            # 3. Generate Staging Model linking to the master model
            # (existing staging model SQL generation using master_model_id)
            # ...