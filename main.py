import yaml
import dlt
from dlt.common.pipeline import LoadInfo

def load_tenants_config():
    with open("tenants.yaml", "r") as f:
        return yaml.safe_load(f)

def run_dbt_factory(tenant_configs):
    print("🚀 Auto-Triggering Star Schema Factory via dlt runner...")
    
    # Initialize dlt pipeline (destination can be dummy or actual if configured)
    # Using 'duckdb' destination for local testing/dev as implied by previous context
    pipeline = dlt.pipeline(
        pipeline_name='gata_factory', 
        destination='duckdb', 
        dataset_name='gata_marts'
    )
    
    # Configure dbt runner
    # Pass tenant logic via vars for compile-time access
    # dlt.dbt.package handles the venv and execution
    dbt = dlt.dbt.package(
        pipeline, 
        "warehouse/gata_transformation",
        venv=dlt.dbt.get_runner_venv()
    )
    
    # Run all models with tag selection
    # Passing variables as a dict
    results = dbt.run(
        models=["tag:marketing", "tag:ecommerce", "tag:identity", "tag:behavioral"],
        vars={'tenant_configs': tenant_configs}
    )
    
    for r in results:
        print(f"✅ {r}")

def main():
    print("Loading Tenant Configuration...")
    config = load_tenants_config()
    run_dbt_factory(config)

if __name__ == "__main__":
    main()
