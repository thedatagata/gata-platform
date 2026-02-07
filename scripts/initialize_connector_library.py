import polars as pl
import duckdb
import hashlib
import os
import sys
import yaml
import subprocess
import json
from pathlib import Path
from datetime import datetime

# Path setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

from orchestrator import MockOrchestrator
from config import TenantConfig, SourceRegistry, SourceConfig

def load_env_file():
    """Loads environment variables from the .env file at the project root."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

load_env_file()

def calculate_dlt_schema_hash(dlt_schema: dict, table_name: str) -> str:
    """
    Calculates structural DNA from physical relational columns.
    """
    table_meta = dlt_schema.get('tables', {}).get(table_name, {})
    columns = table_meta.get('columns', {})
    
    sorted_cols = sorted([
        (name, str(prop.get('data_type'))) 
        for name, prop in columns.items() 
        if not name.startswith(("_dlt", "_airbyte"))
    ])
    
    signature = "|".join([f"{c}:{t}" for c, t in sorted_cols])
    return hashlib.md5(signature.encode('utf-8')).hexdigest()

def get_db_connection(target='dev'):
    """Aligns connection with profiles.yml."""
    if target == 'local':
        return duckdb.connect(str(PROJECT_ROOT / "warehouse" / "sandbox.duckdb"))
    
    token = os.environ.get("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f"md:my_db?motherduck_token={token}" if token else "md:my_db")
    con.sql("CREATE SCHEMA IF NOT EXISTS main")
    return con

def load_connectors_catalog(target='dev'):
    """Orchestrates DNA discovery for the blueprint registry."""
    con = get_db_connection(target)
    existing_blueprints = []
    try:
        existing_blueprints = con.sql("SELECT * FROM main.connector_blueprints").to_df().to_dict('records')
        print(f"📂 Loaded {len(existing_blueprints)} existing blueprint mappings.")
    except Exception:
        print("🆕 Starting fresh blueprint registry.")

    with open(PROJECT_ROOT / "supported_connectors.yaml", "r") as f:
        manifest = yaml.safe_load(f)

    # 1. Physical DNA Discovery
    for connector_def in manifest['connectors']:
        source_name = connector_def['name']
        print(f"📦 Mapping UNIQUE DNA for: {source_name}...")
        
        dummy_tenant = TenantConfig(slug="library_sample", business_name="Library", sources=SourceRegistry())
        setattr(dummy_tenant.sources, source_name, SourceConfig(enabled=True))
        
        orch = MockOrchestrator(dummy_tenant, days=1, credentials='duckdb' if target == 'local' else 'motherduck')
        dlt_schema_dict = orch.run() 

        for table_name in dlt_schema_dict.get('tables', {}).keys():
            if not table_name.startswith(f"raw_library_sample_{source_name}_"):
                continue
            
            struct_hash = calculate_dlt_schema_hash(dlt_schema_dict, table_name)
            obj_identity = table_name.split(f"{source_name}_")[-1]
            master_id = f"{connector_def['master_model_id']}_{obj_identity}"
            
            if not any(b['source_schema_hash'] == struct_hash for b in existing_blueprints):
                print(f"✨ Registering: {struct_hash[:8]} -> platform_mm__{master_id}")
                existing_blueprints.append({
                    "source_name": source_name, "source_table_name": obj_identity,
                    "source_schema_hash": struct_hash, "master_model_id": master_id,
                    "version": connector_def['version'], "registered_at": datetime.now()
                })

    # 2. Save DNA Registry
    if existing_blueprints:
        df_blueprints = pl.DataFrame(existing_blueprints)
        con.sql("CREATE OR REPLACE TABLE main.connector_blueprints AS SELECT * FROM df_blueprints")
    
    con.close()
    
    # 3. Refresh DBT Registry
    dbt_dir = (PROJECT_ROOT / "warehouse" / "gata_transformation").absolute()
    print(f"🚀 Materializing Registry in dbt (target: {target})...")
    subprocess.run(
        f"dbt run --select platform_ops__master_model_registry --target {target}", 
        cwd=str(dbt_dir), check=True, shell=True
    )

if __name__ == "__main__":
    target_env = sys.argv[1] if len(sys.argv) > 1 else 'dev'
    load_connectors_catalog(target_env)
