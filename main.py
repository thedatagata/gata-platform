import yaml
import dlt

def load_tenants_config():
    with open("tenants.yaml", "r") as f:
        return yaml.safe_load(f)

def run_dbt_factory(tenant_configs):
    print("🚀 Auto-Triggering Star Schema Factory via dlt runner...")
    
    # Initialize dlt pipeline
    pipeline = dlt.pipeline(
        pipeline_name='gata_factory', 
        destination='duckdb', 
        dataset_name='gata_marts'
    )
    
    # Configure dbt runner
    # dlt.dbt.package handles the venv and execution
    dbt = dlt.dbt.package(
        pipeline, 
        "warehouse/gata_transformation",
        venv=dlt.dbt.get_runner_venv()
    )
    
    # Run all models using dbt.run_all (or dbt.main.run equivalent inside package)
    # The return object of dlt.dbt.package acts like a runner.
    # The previous instruction used `dbt.run_all`. I will follow that.
    results = dbt.run_all(
        vars={'tenant_configs': tenant_configs}
    )
    
    print("✅ Factory build complete.")
    for r in results:
        print(r)

def main():
    print("Loading Tenant Configuration...")
    config = load_tenants_config()
    run_dbt_factory(config)

if __name__ == "__main__":
    main()
