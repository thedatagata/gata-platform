import yaml
import os
from typing import Dict, Any

def generate_rill_yaml(tenant_slug: str, table_name: str, dimensions: list, measures: list):
    """Generates a Rill Metrics View configuration."""
    rill_config = {
        "type": "metrics_view",
        "title": f"{tenant_slug.title()} - {table_name.replace('raw_', '').replace('_', ' ').title()}",
        "model": table_name,
        "dimensions": [{"name": d["name"], "column": d["name"], "label": d["name"].title()} for d in dimensions],
        "measures": [{"name": m["name"], "expression": f"sum({m['name']})", "label": m["name"].title()} for m in measures]
    }
    
    # Save to the rill service directory
    # Infer project root from this file location: services/mock-data-engine/utils/bsl_mapper.py
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../"))
    path = os.path.join(project_root, f"services/rill-dashboard/dashboards/{tenant_slug}_{table_name}.yaml")
    
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        yaml.dump(rill_config, f)

def generate_boring_manifest(dlt_schema_dict: Dict[str, Any], tenant_slug: str) -> Dict[str, Any]:
    """Maps dlt type inference to Boring Semantic Layer roles and triggers Rill generation."""
    manifest = {"models": []}
    
    for table_name, table_info in dlt_schema_dict.get("tables", {}).items():
        # Skip dlt internal tables
        if table_name.startswith("_dlt"): continue
        
        model = {
            "name": table_name,
            "dimensions": [],
            "measures": [],
            "primary_key": None
        }
        
        for col_name, col_info in table_info.get("columns", {}).items():
            dtype = col_info.get("data_type")
            
            # Detect Primary Key
            if col_info.get("primary_key"):
                model["primary_key"] = col_name
            
            # Map technical types to semantic roles
            if dtype in ['text', 'timestamp', 'date', 'bool']:
                model["dimensions"].append({
                    "name": col_name, 
                    "type": "string" if dtype == 'text' else dtype
                })
            elif dtype in ['double', 'bigint', 'integer', 'decimal']:
                model["measures"].append({
                    "name": col_name, 
                    "type": "number", 
                    "agg": "sum"
                })
        
        # Trigger Rill YAML generation alongside BSL
        generate_rill_yaml(tenant_slug, table_name, model["dimensions"], model["measures"])
        
        manifest["models"].append(model)
        
    return manifest
