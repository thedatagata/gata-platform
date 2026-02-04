import dlt
import argparse
import sys
import os

# 1. Add the current directory to path so we can import from local modules
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

# Force dlt to look for .dlt/ config in the script's directory
os.environ["DLT_PROJECT_DIR"] = current_dir

from config import load_manifest
from orchestrator import MockOrchestrator

def run_pipeline(config_path: str, target: str, days: int, specific_tenant: str = None):
    # --- Path Handling ---
    if not os.path.isabs(config_path):
        config_path = os.path.abspath(config_path)

    print(f"📖 Loading Manifest from {config_path}...")
    try:
        manifest = load_manifest(config_path)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        return

    # Calculate project root
    project_root = os.path.abspath(os.path.join(current_dir, "../../"))

    # --- Target Logic ---
    if target == "local":
        print("🛠️  Target is LOCAL: Overriding generation window to 2 days for sampling.")
        effective_days = 2
        destination = "duckdb"
        
        db_path = os.path.join(project_root, "warehouse", "sandbox.duckdb")
        os.environ["DESTINATION__DUCKDB__CREDENTIALS"] = db_path
        print(f"    -> Database: {db_path}")
        
    else:
        print(f"☁️  Target is {target.upper()}: Generating full {days} days history.")
        effective_days = days
        destination = "motherduck"
        # Credentials loaded automatically from secrets.toml

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

        print(f"🚀 Initializing Generator for {tenant_config.business_name} ({tenant_slug})...")

        # 1. Run Orchestrator
        orchestrator = MockOrchestrator(tenant_config, effective_days)
        data_registry = orchestrator.run()

        # 2. Load Data via dlt
        for source_name, source_data in data_registry.items():
            
            # ------------------------------------------------------------------
            # ARCHITECTURE FIX 1: Source-Centric Schema
            # ------------------------------------------------------------------
            # Instead of "raw_tyrell_corp_facebook_ads", we just use "facebook_ads".
            # This keeps all tenants for a single source in one clean schema/dataset.
            dataset_name = source_name
            
            print(f"🚚 Loading {source_name} to {destination} ({dataset_name})...")
            
            pipeline = dlt.pipeline(
                pipeline_name=f"{tenant_slug}_{source_name}",
                destination=destination,
                dataset_name=dataset_name
            )
            
            # ------------------------------------------------------------------
            # ARCHITECTURE FIX 2: Tenant-Specific Table Names
            # ------------------------------------------------------------------
            # We explicitly rename the table to include the tenant slug AND source.
            # Example: 'campaigns' -> 'raw_tyrell_corp_facebook_ads_campaigns'
            resources = []
            for original_table_name, data in source_data.items():
                
                # Construct the new unique table name
                new_table_name = f"raw_{tenant_slug}_{source_name}_{original_table_name}"
                
                resources.append(
                    dlt.resource(data, name=new_table_name, write_disposition="replace")
                )
            
            info = pipeline.run(resources)
            print(info)

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