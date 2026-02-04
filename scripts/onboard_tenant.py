import yaml
import pathlib
import argparse
import sys
import duckdb
import os
import json
import hashlib

# --- Configuration ---
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "warehouse" / "sandbox.duckdb"
DBT_PROJECT_DIR = PROJECT_ROOT / "warehouse" / "gata_transformation"

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
    if target == 'motherduck':
        token = os.environ.get("MOTHERDUCK_TOKEN")
        # Connect to my_db where both Registry (main) and Sources (facebook_ads) reside
        conn_str = "md:my_db"
        if token:
             conn_str += f"?motherduck_token={token}"
        else:
             print("⚠️  MOTHERDUCK_TOKEN not set in environment. Attempting to connect using local credentials...")
        
        try:
            return duckdb.connect(conn_str)
        except Exception as e:
            print(f"❌ Failed to connect to MotherDuck: {e}")
            sys.exit(1)
    else:
        if not DB_PATH.exists():
            print(f"❌ Database not found at {DB_PATH}. Run the mock data generator first.")
            sys.exit(1)
        return duckdb.connect(str(DB_PATH))

def load_tenants_manifest():
    manifest_path = PROJECT_ROOT / "tenants.yaml"
    if not manifest_path.exists():
        print(f"❌ manifests.yaml not found at {manifest_path}")
        sys.exit(1)
    with open(manifest_path, "r") as f:
        return yaml.safe_load(f)

def get_tenant_config(tenant_slug):
    data = load_tenants_manifest()
    for t in data.get("tenants", []):
        if t["slug"] == tenant_slug:
            return t
    return None

def get_tenant_tables(tenant_slug, target):
    """
    Reads tenants.yaml to find configured source tables for the tenant.
    """
    config = get_tenant_config(tenant_slug)
    if not config:
        print(f"❌ Tenant '{tenant_slug}' not found in tenants.yaml")
        return {}
    
    source_map = {}
    sources = config.get("sources", {})
    
    print(f"📖 Reading configuration for tenant: {tenant_slug}")
    
    for source_key, source_config in sources.items():
        if not source_config.get("enabled"):
            continue
            
        tables = source_config.get("tables", [])
        if not tables:
            continue
            
        print(f"   Source {source_key}: found {len(tables)} tables")
        
        source_map[source_key] = []
        for phys_table in tables:
            prefix = f"raw_{tenant_slug}_{source_key}_"
            if phys_table.startswith(prefix):
                 object_name = phys_table[len(prefix):]
            else:
                 object_name = phys_table
                 
            source_map[source_key].append({
                "physical_table": phys_table,
                "object_name": object_name
            })
            print(f"      - {phys_table} -> {object_name}")

    return source_map

def lookup_master_model(schema_hash: str, target: str = 'local', registry_schema: str = 'main') -> str:
    """
    Queries the central registry to see if this schema hash is already mapped.
    """
    con = get_db_connection(target)
    
    try:
        query = f"""
        SELECT master_model_id 
        FROM my_db.{registry_schema}.platform_ops__master_model_registry 
        WHERE source_schema_hash = '{schema_hash}'
        LIMIT 1
        """
        result = con.sql(query).fetchone()
        
        if result and result[0] != 'unmapped':
            return result[0] 
        else:
             print(f"   [DEBUG] Lookup failed for hash {schema_hash}. Result: {result}")
            
        return 'unknown'
        
    except Exception as e:
        # Don't fail hard, just return unknown so we generate a shell
        print(f"⚠️  Registry lookup failed: {e}")
        return 'unknown'
    finally:
        con.close()

def get_table_columns(schema, table, target):
    """Fetches column definitions for _sources.yml"""
    con = get_db_connection(target)
    try:
        # Schema here is the source_name (e.g. facebook_ads), which matches physical schema in my_db
        query = f"DESCRIBE {schema}.{table}"
        cols = con.sql(query).fetchall()
        return [{"name": c[0], "description": f"Type: {c[1]}"} for c in cols]
    finally:
        con.close()

def generate_scaffolding(tenant_slug, target, registry_schema='main'):
    sources = get_tenant_tables(tenant_slug, target)
    
    if not sources:
        print(f"⚠️  No data found for tenant '{tenant_slug}'. Generating nothing.")
        return

    # Status Check
    tenant_config = get_tenant_config(tenant_slug)
    if tenant_config.get('status') not in ['pending', 'onboarding']:
        print(f"⚠️  Skipping {tenant_slug}: Status is '{tenant_config.get('status')}' (expected 'pending' or 'onboarding').")
        return

    print(f"📂 Outputting models to: {DBT_PROJECT_DIR}")

    for source_name, tables in sources.items():
        print(f"🚀 Scaffolding {source_name}...")
        
        src_model_dir = DBT_PROJECT_DIR / "models" / "sources" / tenant_slug / source_name
        stg_model_dir = DBT_PROJECT_DIR / "models" / "staging" / tenant_slug / source_name
        
        src_model_dir.mkdir(parents=True, exist_ok=True)
        stg_model_dir.mkdir(parents=True, exist_ok=True)
        
        source_yaml_entries = []
        
        for item in tables:
            phys_table = item["physical_table"]
            obj_name = item["object_name"]
            
            try:
                columns = get_table_columns(source_name, phys_table, target)
            except Exception as e:
                print(f"❌ Error fetching columns for {source_name}.{phys_table}: {e}")
                continue
            
            source_yaml_entries.append({
                "name": phys_table,
                "description": f"Raw {obj_name} data for {tenant_slug}",
                "columns": columns
            })
            
            # --- Source Model ---
            src_filename = f"src_{tenant_slug}_{source_name}__{obj_name}.sql"
            src_path = src_model_dir / src_filename
            dbt_source_name = f"{tenant_slug}_{source_name}_raw"
            
            src_sql = f"""
WITH raw_source AS (
    SELECT *
    FROM {{{{ source('{dbt_source_name}', '{phys_table}') }}}}
)
SELECT * FROM raw_source
"""
            with open(src_path, "w") as f:
                f.write(src_sql.strip())
                
            # --- Staging Model ---
            stg_filename = f"stg_{tenant_slug}__{source_name}_{obj_name}.sql"
            stg_path = stg_model_dir / stg_filename
            src_ref = f"src_{tenant_slug}_{source_name}__{obj_name}"
            
            schema_dict = {c["name"]: c["description"] for c in columns}
            schema_json = json.dumps(schema_dict, sort_keys=True).replace("'", "''")
            schema_hash = hashlib.md5(schema_json.encode('utf-8')).hexdigest()

            master_model_id = lookup_master_model(schema_hash, target, registry_schema)
            
            # Build depends_on hints
            depends_on_lines = "-- depends_on: {{ ref('platform_sat__source_schema_history') }}"
            if master_model_id != 'unknown':
                depends_on_lines += f"\n-- depends_on: {{{{ ref('platform_mm__{master_model_id}') }}}}"

            # Always sync to schema history, optionally sync to master hub
            hooks = ["{{ sync_to_schema_history() }}"]
            if master_model_id != 'unknown':
                hooks.append(f"{{{{ sync_to_master_hub('{master_model_id}') }}}}")
            
            # Format as dbt list config: post_hook=["hook1", "hook2"]
            hooks_str = ", ".join([f'"{h}"' for h in hooks])
            master_model_config = f", post_hook=[{hooks_str}]"

            stg_sql = f"""
{depends_on_lines}
{{{{ config(materialized='view'{master_model_config}) }}}}

WITH latest_config AS (
    SELECT tenant_slug, tenant_skey 
    FROM {{{{ ref('platform_sat__tenant_config_history') }}}}
    WHERE tenant_slug = '{tenant_slug}'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tenant_slug ORDER BY updated_at DESC) = 1
),
source_meta AS (
    SELECT 
        '{schema_hash}'::VARCHAR as source_schema_hash,
        '{schema_json}'::JSON as source_schema,
        '{master_model_id}'::VARCHAR as master_model_ref
)

SELECT
    c.tenant_slug,
    c.tenant_skey,
    '{source_name}'::VARCHAR as source_platform,
    m.source_schema_hash,
    m.source_schema,
    m.master_model_ref,
    t._src_table,
    -- Data Vault Payload
    to_json(t) as raw_data_payload
FROM (
    SELECT *, '{source_name}_{obj_name}' as _src_table
    FROM {{{{ ref('{src_ref}') }}}}
) t
CROSS JOIN latest_config c
CROSS JOIN source_meta m
"""
            with open(stg_path, "w") as f:
                f.write(stg_sql.strip())

        if source_yaml_entries:
            dbt_source_def = {
                "version": 2,
                "sources": [{
                    "name": f"{tenant_slug}_{source_name}_raw",
                    "schema": source_name,
                    # No 'database' override needed since everything is in my_db
                    "tables": source_yaml_entries
                }]
            }
            yaml_path = src_model_dir / "_sources.yml"
            with open(yaml_path, "w") as f:
                yaml.dump(dbt_source_def, f, sort_keys=False)
            
    print(f"✅ Onboarding complete for {tenant_slug}")

def activate_tenant(tenant_slug):
    manifest_path = PROJECT_ROOT / "tenants.yaml"
    if not manifest_path.exists():
        print(f"❌ Manifest not found")
        return

    # Use string replacement to preserve comments
    with open(manifest_path, "r") as f:
        lines = f.readlines()
    
    new_lines = []
    in_tenant_block = False
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("- slug:"):
            current_slug = stripped.split("slug:")[1].strip()
            if current_slug == tenant_slug:
                in_tenant_block = True
            else:
                in_tenant_block = False
        
        if in_tenant_block and stripped.startswith("status:"):
             # Preserve indentation
             indent = line[:line.find("status:")]
             new_lines.append(f"{indent}status: active\n")
             in_tenant_block = False # Only update once per block
             print(f"🟢 Activated tenant: {tenant_slug}")
        else:
            new_lines.append(line)
            
    with open(manifest_path, "w") as f:
        f.writelines(new_lines)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scaffold dbt models for a tenant.")
    parser.add_argument("--tenant", required=True, help="Tenant slug")
    parser.add_argument("--target", default="local", help="Target database")
    parser.add_argument("--schema", default="main", help="Registry schema")
    parser.add_argument("--activate", action="store_true", help="Set status to active after success")
    args = parser.parse_args()
    
    generate_scaffolding(args.tenant, args.target, args.schema)
    
    if args.activate:
        activate_tenant(args.tenant)