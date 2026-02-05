import dlt
import argparse
import sys
import os
import duckdb
import json
from typing import Dict, Any

# 1. Add the current directory to path so we can import from local modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Force dlt to look for .dlt/ config in the script's directory
os.environ["DLT_PROJECT_DIR"] = current_dir

from config import load_manifest
from orchestrator import MockOrchestrator
from utils.bsl_mapper import generate_boring_manifest

def run_pipeline(config_path: str, target: str, days: int, specific_tenant: str = None):
    # --- Path Handling ---
    if not os.path.isabs(config_path):
        config_path = os.path.abspath(config_path)

    print(f"Loading Manifest from {config_path}...")
    try:
        manifest = load_manifest(config_path)
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    # Calculate project root
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))

    # --- Target Logic ---
    if target == "local":
        print("Target is LOCAL: Overriding generation window to 2 days for sampling.")
        effective_days = 2
        
        db_path = os.path.join(project_root, "warehouse", "sandbox.duckdb")
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        
        # dlt destination config
        credentials_str = f"duckdb:///{db_path}"
        os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = credentials_str 
        print(f"    -> Database: {db_path}")
        
        platform_db_conn_str = db_path 
        
        # Add duckdb connector for local Rill if needed (optional but good practice)
        # But instructions only mentioned motherduck.yaml

    else: # dev or prod
        print(f"Target is {target.upper()}: Generating full {days} days history.")
        effective_days = days
        destination = "motherduck"
        credentials_str = None 
        
        # For metadata insertion:
        md_token = os.environ.get('MOTHERDUCK_TOKEN')
        if not md_token:
            print("⚠️  MOTHERDUCK_TOKEN not found in env. Metadata insertion might fail.")
            
        platform_db_conn_str = f"md:my_db?motherduck_token={md_token}" if md_token else "md:my_db"

    # --- Execution Loop ---
    for tenant_config in manifest.tenants:
        tenant_slug = tenant_config.slug

        # Filter by specific tenant if provided
        if specific_tenant and tenant_slug != specific_tenant:
            continue

        # Status Check
        if tenant_config.status not in ['pending', 'onboarding']:
            if specific_tenant:
                 print(f"⚠️  Skipping {tenant_slug}: Status is '{tenant_config.status}' (expected 'pending' or 'onboarding').")
            continue

        print(f"Initializing Generator for {tenant_config.business_name} ({tenant_slug})...")

        # 1. Run Orchestrator (Data Gen + dlt Load)
        # Pass credentials string for local duckdb support in dlt
        orchestrator = MockOrchestrator(tenant_config, effective_days, credentials=credentials_str)
        try:
            dlt_schema = orchestrator.run() # Physical load happens here
            print(f"Data loaded via dlt for {tenant_slug}")
        except Exception as e:
            print(f"Error during data generation/loading for {tenant_slug}: {e}")
            continue

        # 2. Generate Semantic Layer
        print("  - Generating BSL Manifest...")
        try:
            # Debug: Print schema keys
            print(f"    DEBUG: dlt_schema type: {type(dlt_schema)}")
            if isinstance(dlt_schema, dict):
                print(f"    DEBUG: dlt_schema keys: {dlt_schema.keys()}")
                print(f"    DEBUG: dlt_schema tables count: {len(dlt_schema.get('tables', {}))}")
            
            # Generate the full manifest (containing "models" list)
            # This triggers Rill YAML generation as side effect
            bsl_manifest = generate_boring_manifest(dlt_schema, tenant_slug)
            print(f"    DEBUG: BSL Manifest models count: {len(bsl_manifest.get('models', []))}")
            
        except Exception as e:
             print(f"❌ Error generating BSL manifest: {e}")
             import traceback
             traceback.print_exc()
             bsl_manifest = {}
             
        # 3. Direct Push to Warehouse Satellite
        print("  - Registering Semantic Manifest in Warehouse...")
        try:
            con = duckdb.connect(platform_db_conn_str)
            
            # --- Per-Table Persistence Logic ---
            # 'dlt_schema' contains 'tables' dictionary. 
            # 'bsl_manifest' contains 'models' list, mapped from those tables.
            
            # Create a lookup for BSL models by name for easy access
            bsl_lookup = { m['name']: m for m in bsl_manifest.get('models', []) }
            
            dlt_tables = dlt_schema.get("tables", {})
            print(f"    DEBUG: Persisting metadata for {len(dlt_tables)} tables to {platform_db_conn_str}")
            
            # Iterate through each table in the physical schema
            for table_name, table_info in dlt_tables.items():
                if table_name.startswith("_dlt"): continue
                
                # Get the corresponding BSL model fragment
                model_manifest = bsl_lookup.get(table_name, {})
                
                # Prepare JSON payloads
                # source_schema: just the info for this specific table from dlt
                source_schema_json = json.dumps(table_info)
                
                # boring_semantic_manifest: just the BSL model for this specific table
                bsl_json = json.dumps(model_manifest)
                
                # Insert row for this specific table
                query = """
                    INSERT INTO main.platform_sat__source_schema_history 
                    (tenant_slug, source_table_name, source_schema, boring_semantic_manifest, updated_at)
                    VALUES (?, ?, ?, ?, current_timestamp)
                """
                con.execute(query, [tenant_slug, table_name, source_schema_json, bsl_json])
                
            con.close()
            print(f"Warehouse metadata registered for {tenant_slug} (Tables: {len(bsl_lookup)})")
            
        except Exception as e:
            print(f"Error writing metadata to warehouse: {e}")

if __name__ == "__main__":
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
    default_config = os.path.join(project_root, "tenants.yaml")

    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default=default_config, help="Path to tenant YAML config")
    parser.add_argument("--target", type=str, choices=["local", "dev", "prod"], default="local")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--tenant", type=str, help="Specific tenant slug to process")
    
    args = parser.parse_args()
    
    run_pipeline(args.config, args.target, args.days, args.tenant)