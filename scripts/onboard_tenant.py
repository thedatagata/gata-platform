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
    If not, scaffolds a thin sink shell.
    """
    MASTER_MODEL_DIR.mkdir(parents=True, exist_ok=True)
    file_path = MASTER_MODEL_DIR / f"platform_mm__{master_model_id}.sql"
    
    if file_path.exists():
        return True

    print(f"✨ Scaffolding NEW Master Model file: {file_path.name}")
    
    # Master Sink Template (Strict Empty Shell)
    sql_content = f"""
{{{{ config(materialized='table') }}}}

SELECT 
    CAST(NULL AS VARCHAR) as tenant_slug,
    CAST(NULL AS VARCHAR) as hub_key,
    CAST(NULL AS VARCHAR) as source_platform,
    CAST(NULL AS VARCHAR) as source_schema_hash,
    CAST(NULL AS JSON) as raw_data_payload,
    CAST(NULL AS TIMESTAMP) as loaded_at
WHERE 1=0
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
            stg_filename = f"stg_{tenant_slug}__{source_name}_{obj_name}.sql"
            stg_path = DBT_PROJECT_DIR / "models" / "staging" / tenant_slug / source_name / stg_filename
            stg_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Staging Model Template with Hardcoded Push
            stg_content = f"""
{{{{ config(materialized='view') }}}}

SELECT
    '{tenant_slug}'::VARCHAR as tenant_slug,
    {{{{ generate_tenant_key("'{tenant_slug}'") }}}} as hub_key,
    '{source_name}'::VARCHAR as source_platform,
    '{schema_hash}'::VARCHAR as source_schema_hash,
    raw_data_payload,
    current_timestamp as loaded_at
FROM {{{{ source('{tenant_slug}_{source_name}', '{phys_table}') }}}}

-- HARDCODED PUSH: Direct merge into designated immutable sink
{{% do sync_to_master_hub('{master_model_id}') %}}
"""
            with open(stg_path, "w") as f:
                f.write(stg_content.strip())
            
            print(f"   Created staging model: {stg_filename}")