import pathlib

PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
MASTER_MODEL_DIR = PROJECT_ROOT / "warehouse" / "gata_transformation" / "models" / "platform" / "master_models"

TEMPLATE = """{{ config(materialized='table') }}

SELECT 
    CAST(NULL AS VARCHAR) as tenant_slug,
    CAST(NULL AS VARCHAR) as tenant_skey,
    CAST(NULL AS VARCHAR) as source_platform,
    CAST(NULL AS VARCHAR) as source_schema_hash,
    CAST(NULL AS JSON) as source_schema,
    CAST(NULL AS JSON) as raw_data_payload,
    CAST(NULL AS TIMESTAMP) as loaded_at
WHERE 1=0
"""

def reset_master_models():
    if not MASTER_MODEL_DIR.exists():
        print(f"Directory not found: {MASTER_MODEL_DIR}")
        return

    for file_path in MASTER_MODEL_DIR.glob("platform_mm__*.sql"):
        print(f"Resetting {file_path.name}...")
        with open(file_path, "w") as f:
            f.write(TEMPLATE)
    print("Done.")

if __name__ == "__main__":
    reset_master_models()
