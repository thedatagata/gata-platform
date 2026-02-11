import yaml
from typing import Dict, Any

def generate_boring_manifest(dlt_schema_dict: Dict[str, Any], tenant_slug: str) -> Dict[str, Any]:
    """Maps dlt type inference to Boring Semantic Layer roles."""
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

        manifest["models"].append(model)

    return manifest
