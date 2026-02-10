import yaml
import os
from dlt.helpers.dbt import create_runner

def load_tenants_config():
    with open("tenants.yaml", "r") as f:
        return yaml.safe_load(f)

def run_dbt_factory(tenant_configs):
    print("🚀 Auto-Triggering Star Schema Factory via dlt runner (MotherDuck)...")
    
    # Use create_runner with YOUR profiles.yml — no dlt credential management needed
    runner = create_runner(
        venv=None,          # use current environment (dbt already installed)
        credentials=None,   # skip dlt credential injection — profiles.yml handles it
        working_dir=".",
        package_location="warehouse/gata_transformation",
        package_profiles_dir=os.path.abspath("warehouse/gata_transformation"),
        package_profile_name="swamp-duck",
    )
    
    # Execute transformations targeting MotherDuck
    results = runner.run(
        cmd_params=("--fail-fast", "--target", "dev"),
        additional_vars={"tenant_configs": tenant_configs},
    )
    
    print("✅ Factory build complete in MotherDuck.")
    for r in results:
        print(f"Model: {r.model_name} | Status: {r.status}")

def main():
    config = load_tenants_config()
    run_dbt_factory(config)

if __name__ == "__main__":
    main()
