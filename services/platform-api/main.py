from fastapi import FastAPI, HTTPException
import duckdb
import os
import json
import yaml
import subprocess
from pathlib import Path

app = FastAPI()
TENANTS_YAML = Path(__file__).parent.parent.parent / "tenants.yaml"

@app.get("/semantic-layer/{tenant_slug}")
def get_semantic_layer(tenant_slug: str):
    # Determine connection string
    # Assuming prod/standard usage is MotherDuck
    md_token = os.environ.get('MOTHERDUCK_TOKEN')
    conn_str = f"md:my_db?motherduck_token={md_token}" if md_token else "md:my_db"
    
    # Fallback to local sandbox if env var implies local development (optional, but good for testing)
    if os.environ.get('GATA_ENV') == 'local':
         # Adjust path as needed relative to where this service runs
         conn_str = "../../warehouse/sandbox.duckdb"

    try:
        con = duckdb.connect(conn_str)
        # Query the new Boring Semantic Layer model
        # Using the aggregated view created in Phase 3
        result = con.execute("""
            SELECT semantic_manifest 
            FROM main.platform_ops__boring_semantic_layer 
            WHERE tenant_slug = ?
        """, [tenant_slug]).fetchall()
        
        con.close()
        
        if not result:
            return {"manifests": []}
            
        return {"manifests": [json.loads(r[0]) for r in result]}
        
    except Exception as e:
        return {"error": str(e)}

@app.post("/semantic-layer/update")
async def update_logic(tenant_slug: str, platform: str, logic_payload: dict):
    # 1. Update tenants.yaml 
    with open(TENANTS_YAML, 'r') as f:
        config = yaml.safe_load(f)
    
    for tenant in config.get('tenants', []):
        if tenant['slug'] == tenant_slug:
            # Check if source exists
            if platform in tenant.get('sources', {}):
                 # Ensure logic dict exists
                 if 'logic' not in tenant['sources'][platform]:
                     tenant['sources'][platform]['logic'] = {}
                 tenant['sources'][platform]['logic'] = logic_payload
            break
    
    with open(TENANTS_YAML, 'w') as f:
        yaml.safe_dump(config, f)

    # 2. Trigger dbt refresh 
    try:
        # Assuming dbt is available in the container/environment path
        # Running from project root assumption or adjust cwd
        project_root = Path(__file__).parent.parent.parent
        subprocess.run(["dbt", "run", "--select", "platform"], check=True, cwd=project_root)
        return {"status": "success", "message": f"Logic updated for {tenant_slug}"}
    except subprocess.CalledProcessError as e:
        raise HTTPException(status_code=500, detail="dbt execution failed")
