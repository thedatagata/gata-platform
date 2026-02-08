import yaml
import json
import subprocess
import os

def load_tenants_config():
    with open("tenants.yaml", "r") as f:
        return yaml.safe_load(f)

def run_dbt_factory(tenant_configs):
    print("🚀 Auto-Triggering Star Schema Factory via dlt runner...")
    
    # Inject tenant config as dbt variable
    vars_json = json.dumps({"tenant_configs": tenant_configs})
    
    project_dir = "warehouse/gata_transformation"
    # Assuming profiles.yml is in project_dir for now, or use default
    
    cmd = [
        "dbt", "build",
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
        "--vars", vars_json,
        "--select", "tag:marketing tag:ecommerce"  
    ]
    
    print(f"Running command: dbt build --vars '{{...}}' ...")
    
    # In a real scenario, we would execute this:
    # try:
    #     subprocess.run(cmd, check=True)
    #     print("✅ Factory build complete.")
    # except subprocess.CalledProcessError as e:
    #     print(f"❌ Factory build failed: {e}")
    #     raise

def main():
    print("Loading Tenant Configuration...")
    config = load_tenants_config()
    run_dbt_factory(config)

if __name__ == "__main__":
    main()
