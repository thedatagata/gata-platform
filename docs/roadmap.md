# Roadmap

## Phase 1: The Universal Intermediate Layer (Week 1)

**Goal**: Transform the raw_data_payload JSON from your Master Models into
typed, unified Intermediate models.

### 1.1. Create Universal Extraction Macros

Instead of writing `raw_data_payload->>'$.field'` in every model, create a macro
that handles the nuances of your JSON structure.

**File**: `macros/extract_json_field.sql`

```sql
{% macro extract_field(column_name, json_path, target_type='string') %}
    (raw_data_payload->>'$.{{ json_path }}')::{{ target_type }} as {{ column_name }}
{% endmacro %}
```

### 1.2. Build the Universal Fact Engines

These models union all Master Model sources for a specific entity (e.g., Orders)
using your Relational DNA registry.

**File**: `models/intermediate/ecommerce/int_unified_orders.sql`

```sql
-- This model dynamically unions all relations registered to the 'order' master model
{% set relations = dbt_utils.get_relations_by_prefix(
    schema=target.schema, 
    prefix='platform_mm__', 
    exclude='%products%'
) %}

WITH base_orders AS (
    {% for rel in relations %}
    SELECT 
        tenant_slug,
        source_name,
        {{ extract_field('order_id', 'id') }},
        {{ extract_field('total_amount', 'total_price', 'float') }},
        {{ extract_field('currency', 'currency') }},
        {{ extract_field('ordered_at', 'created_at', 'timestamp') }},
        raw_data_payload->'$.customer' as customer_raw -- Keep sub-JSON for Dim joining
    FROM {{ rel }}
    {% if not loop.last %} UNION ALL {% endif %}
    {% endfor %}
)
SELECT * FROM base_orders
```

**Architectural Decision**: Should we keep the raw JSON in the Intermediate
layer?

**Decision**: Yes. Store complex objects (like `customer_raw`) as JSON columns.
This allows downstream "Dimension Engines" to extract user data without needing
separate ingestion paths.

## Phase 2: The Star Schema & dlt Runner Integration (Week 2)

**Goal**: Materialize per-tenant Star Schemas and use the dlt dbt runner to
capture full lineage.

### 2.1. Define the Star Schema (Gold Layer)

Create the final Fact and Dimension tables that the app frontend will query.

**File**: `models/marts/fct_orders.sql`

```sql
SELECT
    {{ dbt_utils.generate_surrogate_key(['tenant_slug', 'order_id']) }} as order_key,
    tenant_slug,
    source_name,
    order_id,
    total_amount,
    ordered_at,
    -- Simple attribution logic from GA4 data if joined here
    source_name as channel
FROM {{ ref('int_unified_orders') }}
```

### 2.2. Implement the dlt dbt Runner

Update `mock-data-engine/main.py` to replace the subprocess call with the native
dlt dbt runner. This links the ingestion load_id to the transformation models,
providing end-to-end lineage.

**File**: `mock-data-engine/main.py` (Update Step 3)

```python
import dlt

def run_pipeline(config_path: str, target: str, days: int, specific_tenant: str = None):
    # ... (Existing Ingestion logic) ...

    # 3. RUN DBT VIA DLT RUNNER
    pipeline = dlt.pipeline(pipeline_name=f'transform_{tenant_slug}', destination='motherduck')
    
    dbt = dlt.dbt.package(
        pipeline,
        "warehouse/gata_transformation",
        target_name=target
    )
    
    # Run only models relevant to the current tenant for speed
    results = dbt.run_all(vars={'tenant_slug': tenant_slug})
    
    for m in results:
        print(f"Model {m.model_name} materialized in {m.time}s")
```

## Phase 3: AI-Ready Semantic Layer & API (Week 3)

**Goal**: Automatically populate the Boring Semantic Layer (BSL) from your Star
Schema and expose it to an LLM via API.

### 3.1. Create the "High-Fidelity" BSL Factory

Update `utils/bsl_mapper.py` to target your Gold layer (`fct_`, `dim_`) rather
than the raw `raw_` tables. This ensures the LLM queries clean, business-ready
data.

**File**: `mock-data-engine/utils/bsl_mapper.py`

```python
def generate_star_bsl(con, tenant_slug: str):
    """Generates BSL Manifest specifically for the Star Schema."""
    manifest = {"models": []}
    
    # Inspect physical DuckDB tables
    star_tables = con.sql("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'fct_%'").fetchall()
    
    for table_name in star_tables:
        model = {
            "name": table_name[0],
            "dimensions": ["ordered_at", "channel"],
            "measures": [
                {"name": "total_revenue", "expr": "sum(total_amount)", "label": "Total Revenue"}
            ]
        }
        manifest["models"].append(model)
        
    return manifest
```

### 3.2. Deploy the Natural Language API

Create a FastAPI service that uses the Model Context Protocol (MCP) to allow an
LLM to query your BSL.

**File**: `services/api/main.py` (Conceptual)

```python
from fastapi import FastAPI
from boring_semantic_layer import SemanticLayer

app = FastAPI()

@app.post("/ask")
async def ask_data(tenant_slug: str, question: str):
    # 1. Load the BSL Manifest from your warehouse satellite
    # 2. Inject Manifest into LLM Prompt
    # 3. LLM returns Ibis expression (not SQL) based on your metrics
    # 4. Execute and return results
    return {"answer": results}
```

## Critical Success Milestones

- **End of Week 1**: You can run `dbt run` and see a `unified_orders` table that
  combines Shopify and BigCommerce data into a single schema.
- **End of Week 2**: Your `main.py` run populates the `_dlt_loads` table with
  dbt model statuses, proving full lineage.
- **End of Week 3**: You can send a JSON payload to your API and receive a
  calculated "Total Revenue" numeric value derived from the Star Schema BSL.

### The "Job-Winning" Pivot

When you present this, emphasize that you didn't build a dashboard—you built an
**Autonomous Semantic Engine** where the LLM is constrained by the BSL to
prevent SQL hallucinations.
