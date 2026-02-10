import polars as pl
import duckdb
import hashlib
import os
import sys
import yaml
import subprocess
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

from orchestrator import MockOrchestrator
from config import TenantConfig, SourceRegistry, SourceConfig

def calculate_dlt_schema_hash(dlt_schema: dict, table_name: str) -> str:
    """Computes a structural hash based on physical columns and types."""
    table_meta = dlt_schema.get('tables', {}).get(table_name, {})
    columns = table_meta.get('columns', {})
    sorted_cols = sorted([(n, str(p.get('data_type'))) for n, p in columns.items() if not n.startswith(("_dlt", "_airbyte"))])
    signature = "|".join([f"{c}:{t}" for c, t in sorted_cols])
    return hashlib.md5(signature.encode('utf-8')).hexdigest()

def get_db_connection(target='dev'):
    if target in ('sandbox', 'local'):
        con = duckdb.connect(str(PROJECT_ROOT / "warehouse" / "sandbox.duckdb"))
        con.sql("CREATE SCHEMA IF NOT EXISTS main")
        return con
    token = os.environ.get("MOTHERDUCK_TOKEN")
    con = duckdb.connect(f"md:my_db?motherduck_token={token}" if token else "md:my_db")
    con.sql("CREATE SCHEMA IF NOT EXISTS main")
    return con

def load_connectors_catalog(target='dev'):
    con = get_db_connection(target)
    existing_blueprints = []
    try: 
        existing_blueprints = con.sql("SELECT * FROM main.connector_blueprints").to_df().to_dict('records')
    except: 
        print("[NEW] Starting fresh registry.")

    with open(PROJECT_ROOT / "supported_connectors.yaml", "r") as f:
        manifest = yaml.safe_load(f)

    for connector_def in manifest['connectors']:
        source_name = connector_def['name']
        print(f"[DNA] Mapping DNA for: {source_name}...")
        
        dummy_tenant = TenantConfig(slug="library_sample", business_name="Library", sources=SourceRegistry())
        setattr(dummy_tenant.sources, source_name, SourceConfig(enabled=True))
        
        # INCREASED TO 30 DAYS: Ensures high categorical density for DNA established
        orch = MockOrchestrator(dummy_tenant, days=30, credentials='duckdb' if target in ('sandbox', 'local') else 'motherduck')
        dlt_schema_dict = orch.run() 

        for table_name in dlt_schema_dict.get('tables', {}).keys():
            prefix = f"raw_library_sample_{source_name}_"
            if not table_name.startswith(prefix) or "_dlt" in table_name: continue
            
            struct_hash = calculate_dlt_schema_hash(dlt_schema_dict, table_name)
            obj_id = table_name[len(prefix):]
            master_id = f"{connector_def['master_model_id']}_{obj_id}"
            
            if not any(b['source_schema_hash'] == struct_hash for b in existing_blueprints):
                print(f"[REG] Registering: {struct_hash[:8]} -> platform_mm__{master_id}")
                existing_blueprints.append({
                    "source_name": source_name, "source_table_name": obj_id,
                    "source_schema_hash": struct_hash, "master_model_id": master_id,
                    "version": connector_def['version'], "registered_at": datetime.now()
                })

    if existing_blueprints:
        df_blueprints = pl.DataFrame(existing_blueprints)
        con.sql("CREATE OR REPLACE TABLE main.connector_blueprints AS SELECT * FROM df_blueprints")
    
    con.close()
    dbt_target = 'sandbox' if target in ('sandbox', 'local') else target
    subprocess.run(f"uv run --env-file ../../.env dbt run --select platform_ops__master_model_registry --target {dbt_target}", cwd=str(PROJECT_ROOT / "warehouse" / "gata_transformation"), check=True, shell=True)

if __name__ == "__main__":
    load_connectors_catalog(sys.argv[1] if len(sys.argv) > 1 else 'dev')
