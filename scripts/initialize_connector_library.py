
import polars as pl
import duckdb
import hashlib
import json
import os
import sys
import yaml
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT / "services" / "mock-data-engine"))

def load_env_file():
    """
    Manually load .env variables for direct DuckDB connections.
    This is necessary because 'duckdb.connect' does not automatically read 
    secrets from .dlt/secrets.toml or .env like dlt or dbt does.
    """
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    os.environ.setdefault(key.strip(), value.strip())

load_env_file()

from config import GenConfig, SourceRegistry, SourceConfig, TenantConfig
from orchestrator import MockOrchestrator

# Import all generators directly
from sources.paid_ads.linkedin_ads.linkedin_ads_data_generator import generate_linkedin_data
from sources.paid_ads.bing_ads.bing_ads_data_generator import generate_bing_data
from sources.paid_ads.amazon_ads.amazon_ads_data_generator import generate_amazon_data
from sources.product_analytics.amplitude.amplitude_data_generator import generate_amplitude_data
from sources.product_analytics.mixpanel.mixpanel_data_generator import generate_mixpanel_data
from sources.ecommerce_platforms.woocommerce.woocommerce_data_generator import generate_woocommerce_data
from sources.ecommerce_platforms.bigcommerce.bigcommerce_data_generator import generate_bigcommerce_data

def get_db_connection(target='local'):
    if target == 'motherduck':
        token = os.environ.get("MOTHERDUCK_TOKEN")
        conn_str = "md:" # Connect to default to allow DB creation
        if token:
             conn_str += f"?motherduck_token={token}"
        con = duckdb.connect(conn_str)
        con.sql("CREATE DATABASE IF NOT EXISTS connectors")
        con.sql("USE connectors")
        return con
    else:
        # Local test DB
        db_path = PROJECT_ROOT / "warehouse" / "connectors.duckdb"
        return duckdb.connect(str(db_path))

def calculate_structural_hash(df: pl.DataFrame) -> str:
    """
    Calculates MD5 hash of column names and types.
    Excludes ETL metadata columns if any (though generators produce clean api-like data).
    """
    schema = df.schema
    # Sort keys to ensure stability
    # Filter out ETL metadata columns
    sorted_cols = sorted([
        (col, dtype) for col, dtype in schema.items() 
        if not col.startswith("_dlt_") and not col.startswith("_airbyte_")
    ])
    # Create signature string: "col1:Int64|col2:Utf8|..."
    signature = "|".join([f"{col}:{dtype}" for col, dtype in sorted_cols])
    return hashlib.md5(signature.encode('utf-8')).hexdigest()

def generate_library_data():
    """
    Runs all generators with default/sample config for the 'library' tenant.
    Returns composite registry.
    """
    print("📚 Generatng Connector Library Data...")
    
    # Dummy Config
    dummy_gen = GenConfig(
        daily_spend_mean=100.0,
        campaign_count=2,
        daily_event_count=100,
        unique_user_base=50,
        daily_order_count=10,
        avg_order_value=50.0,
        product_catalog_size=20,
        avg_ctr=0.01,
        ad_group_count=3,
        sponsored_product_ratio=0.5
    )
    
    slug = "library_sample"
    registry = {}
    
    # Run each manually since Orchestrator is tied to tenants.yaml logic
    registry['linkedin_ads'] = generate_linkedin_data(slug, dummy_gen, 1)
    registry['bing_ads'] = generate_bing_data(slug, dummy_gen, 1)
    registry['amazon_ads'] = generate_amazon_data(slug, dummy_gen, 1)
    registry['amplitude'] = generate_amplitude_data(slug, dummy_gen, 1)
    registry['mixpanel'] = generate_mixpanel_data(slug, dummy_gen, 1)
    registry['woocommerce'] = generate_woocommerce_data(slug, dummy_gen, 1)
    registry['bigcommerce'] = generate_bigcommerce_data(slug, dummy_gen, 1)
    
    return registry

def load_connectors_catalog(target='local'):
    """
    Seeds the 'connectors' catalog with sample data and builds the blueprints table.
    """
    con = get_db_connection(target)
    
    # 1. Load Supported Connectors Manifest
    with open(PROJECT_ROOT / "supported_connectors.yaml", "r") as f:
        manifest = yaml.safe_load(f)
    
    blueprints = []
    data_registry = generate_library_data()
    
    # Create Schema
    con.sql("CREATE SCHEMA IF NOT EXISTS main")
    
    for source_name, tables in data_registry.items():
        print(f"📦 Processing {source_name}...")
        
        # Find master model ID mapping from manifest
        connector_def = next((c for c in manifest['connectors'] if c['name'] == source_name), None)
        if not connector_def:
            print(f"⚠️  Warning: {source_name} not found in supported_connectors.yaml")
            continue
            
        master_id_base = connector_def['master_model_id']
        
        for table_name, data_dicts in tables.items():
            if not data_dicts:
                continue
                
            df = pl.DataFrame(data_dicts)
            
            # Calculate Hash
            struct_hash = calculate_structural_hash(df)
            
            # Determine cleanup Master Model ID for this object
            # e.g., linkedin_ads_api_v1 + _campaigns ?? 
            # OR logic: manifest master_model_id is the BASE. 
            # We need to map table_name to a specific granular ID?
            # Or just store the base one?
            # User Prompt 4 says: "unique ID (e.g., linkedin_ads_api_v1)"
            # User Prompt 5 says: "union connector_blueprints... query registry first"
            # We likely want granular IDs in the blueprints table too.
            
            # Use simple convention: {base_id}_{object} ? 
            # But the Registry uses specific names like social_ads_campaigns_v1.
            # Let's construct a probable granular ID.
            # actually, for "API-native" sources, maybe we just use the base ID + object name?
            # e.g. linkedin_ads_api_v1 implies the WHOLE api.
            # But the user wants GRANULAR unioning.
            # Let's append the object name to the provided base. 
            
            # e.g. linkedin_ads_api_v1_campaigns
            granular_master_id = f"{master_id_base}_{table_name}"
            
            blueprints.append({
                "source_name": source_name,
                "source_table_name": table_name,
                "source_schema_hash": struct_hash,
                "master_model_id": granular_master_id,
                "version": connector_def['version']
            })
            
            # Create Table in Connectors Catalog (for reference/sampling)
            # Schema: source_name
            # Table: table_name
            con.sql(f"CREATE SCHEMA IF NOT EXISTS {source_name}")
            con.sql(f"CREATE OR REPLACE TABLE {source_name}.{table_name} AS SELECT * FROM df")
            
    # 2. Create Blueprints Table
    print("📝 writing connector_blueprints...")
    blueprints_df = pl.DataFrame(blueprints)
    con.sql("CREATE OR REPLACE TABLE main.connector_blueprints AS SELECT * FROM blueprints_df")
    
    print("✅ Connector Library Initialized!")
    con.close()

if __name__ == "__main__":
    target = "local"
    if len(sys.argv) > 1:
        target = sys.argv[1]
    
    load_connectors_catalog(target)
