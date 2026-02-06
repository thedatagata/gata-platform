import polars as pl
import duckdb
import hashlib
import os
import sys
import yaml
import subprocess
from pathlib import Path
from datetime import datetime

# Path setup
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

from config import GenConfig
# Imports for all generators (TikTok, Facebook, Shopify, etc.)
from sources.paid_ads.facebook_ads.fb_ads_data_generator import generate_facebook_data
from sources.paid_ads.google_ads.google_ads_data_generator import generate_google_ads
from sources.paid_ads.tiktok_ads.tiktok_ads_data_generator import generate_tiktok_data
from sources.paid_ads.shopify.shopify_data_generator import generate_shopify_data
# ... ensure other generators are imported here ...

def run_dbt_registry_update():
    dbt_dir = (PROJECT_ROOT / "gata_transformation").absolute()
    print(f"🚀 Materializing Master Model Registry in dbt...")
    try:
        subprocess.run(
            "dbt run --select platform_ops__master_model_registry",
            cwd=str(dbt_dir), check=True, shell=True 
        )
        print("✅ Registry Materialized.")
    except subprocess.CalledProcessError as e:
        print(f"❌ dbt Error: {e}")

def get_db_connection(target='dev'):
    if target in ['motherduck', 'dev', 'prod']:
        token = os.environ.get("MOTHERDUCK_TOKEN")
        conn_str = "md:connectors" 
        if token: conn_str += f"?motherduck_token={token}"
        con = duckdb.connect(conn_str)
        con.sql("CREATE SCHEMA IF NOT EXISTS main")
        return con
    else:
        db_path = PROJECT_ROOT / "warehouse" / "connectors.duckdb"
        os.makedirs(db_path.parent, exist_ok=True)
        return duckdb.connect(str(db_path))

def calculate_structural_hash(df: pl.DataFrame) -> str:
    schema = df.schema
    sorted_cols = sorted([(c, t) for c, t in schema.items() if not c.startswith(("_dlt", "_airbyte"))])
    signature = "|".join([f"{c}:{t}" for c, t in sorted_cols])
    return hashlib.md5(signature.encode('utf-8')).hexdigest()

def load_connectors_catalog(target='dev'):
    con = get_db_connection(target)
    
    # 1. Load the current Blueprint Registry from the database if it exists
    existing_blueprints = []
    try:
        existing_blueprints = con.sql("SELECT * FROM main.connector_blueprints").to_df().to_dict('records')
        print(f"📂 Loaded {len(existing_blueprints)} existing blueprint mappings.")
    except:
        print("🆕 No existing blueprint table found. Starting fresh.")

    with open(PROJECT_ROOT / "supported_connectors.yaml", "r") as f:
        manifest = yaml.safe_load(f)

    # Initialize Dummy Config
    dummy_gen = GenConfig(daily_spend_mean=100.0, campaign_count=2, cpc_mean=1.5, product_count=10)
    slug = "library_sample"
    
    # 2. Process each connector to define the library
    for connector_def in manifest['connectors']:
        source_name = connector_def['name']
        print(f"📦 Initializing Library Schema for: {source_name}...")
        
        # Call the relevant generator (logic simplified for brevity)
        # In practice, use a mapping dict or if/elif to call specific generators
        data = {} # Fetch from generator...
        
        for table_name, rows in data.items():
            df = pl.DataFrame(rows, strict=False)
            struct_hash = calculate_structural_hash(df)
            master_id = f"{connector_def['master_model_id']}_{table_name}"
            
            # LOOKUP: Check if this schema hash already has a target
            match = next((b for b in existing_blueprints if b['source_schema_hash'] == struct_hash), None)
            
            if not match:
                print(f"✨ Registering NEW Master Model target for hash {struct_hash[:8]} -> {master_id}")
                existing_blueprints.append({
                    "source_name": source_name,
                    "source_table_name": table_name,
                    "source_schema_hash": struct_hash,
                    "master_model_id": master_id,
                    "version": connector_def['version'],
                    "registered_at": datetime.now()
                })
            
            # Load dummy data to ensure schema existence in the database
            con.sql(f"CREATE SCHEMA IF NOT EXISTS {source_name}")
            con.sql(f"CREATE OR REPLACE TABLE {source_name}.{table_name} AS SELECT * FROM df")

    # 3. Save the definitive Library Registry
    blueprints_df = pl.DataFrame(existing_blueprints)
    con.sql("CREATE OR REPLACE TABLE main.connector_blueprints AS SELECT * FROM blueprints_df")
    
    # 4. Trigger dbt to finalize the transformation layer
    run_dbt_registry_update()
    con.close()
