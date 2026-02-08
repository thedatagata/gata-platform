Based on an audit of your current project state, the **Thin Master Model**
architecture is successfully established. Your staging pushers are already
packing physical source data into the `raw_data_payload` JSON column via the
`generate_staging_pusher` macro.

To move forward, the roadmap must focus on a **Dynamic Transformation Factory**.
This factory will "hydrate" the thin records by applying the current logic
(filters, renames, case statements) defined in your `TenantConfig` to the
historical data pooled in the hubs.

# Revised Roadmap: The Dynamic Transformation Factory

## Phase 1: The Standardized Intermediate Layer (Week 1)

**Goal**: Isolate tenant-platform data from the Master Model hubs and transform
JSON payloads into typed, domain-aligned records while applying the logic
config.

### 1.1. Universal Extraction & Logic Macros

Instead of manual JSON pathing, standardized macros will handle field extraction
and the injection of tenant-specific logic (e.g., "filter where campaign matches
X").

**File**: `macros/extract_json.sql`

```sql
{% macro extract_field(column_name, target_type='string') %}
    (raw_data_payload->>'$.{{ column_name }}')::{{ target_type }} as {{ column_name }}
{% endmacro %}
```

**File**: `macros/apply_tenant_logic.sql` This uses your existing
`get_client_logic` macro to dynamically apply filters from `tenants.yaml`.

### 1.2. The Unified Domain Hydrators

These models union all Master Model hubs for a specific entity (e.g.,
Performance) and prepare them for the Star Schema by applying the "Logic
Injection."

**File**: `models/intermediate/paid_ads/int_unified_ad_performance.sql`

```sql
{{ config(materialized='view') }}

WITH master_hub AS (
    -- Dynamically union all hubs matching the 'ad_performance' master_model_id
    {{ generate_master_union('paid_ads_api_v1_ad_performance') }}
),

hydrated AS (
    SELECT 
        tenant_slug,
        source_name,
        {{ extract_field('date_start', 'date') }},
        {{ extract_field('campaign_id') }},
        {{ extract_field('spend', 'double') }},
        {{ extract_field('impressions', 'bigint') }},
        -- Apply the logic configuration for filtering and custom case statements
        {{ apply_tenant_logic(tenant_slug, source_name, 'ad_performance') }}
    FROM master_hub
)
SELECT * FROM hydrated
```

## Phase 2: The Star Schema Factory (Week 2)

**Goal**: Materialize the final "Gold" layer facts and dimensions, orchestrated
by a factory that joins data based on the tenant's active source mix.

### 2.1. Dimension Engine

Build standardized dimensions (e.g., `dim_campaigns`, `dim_products`) by
extracting attributes from the hub records.

### 2.2. Mart Factory

A dbt model that acts as the "Star Schema Assembly Line," automatically joining
facts to dimensions using surrogate keys.

**File**: `models/marts/fct_ad_performance.sql`

```sql
SELECT 
    {{ dbt_utils.generate_surrogate_key(['tenant_slug', 'platform', 'campaign_id']) }} as campaign_key,
    f.*,
    d.campaign_name,
    d.objective
FROM {{ ref('int_unified_ad_performance') }} f
LEFT JOIN {{ ref('int_unified_campaigns') }} d 
    USING (tenant_slug, platform, campaign_id)
```

## Phase 3: DLT Workspace & Semantic Ingestion (Week 3)

**Goal**: Fully automate the execution loop using dlt's workspace features and
populate the Boring Semantic Layer (BSL) with Star Schema metadata.

### 3.1. DLT DBT Runner Integration

Update `mock-data-engine/main.py` to use `dlt.dbt.package`. This captures full
lineage from the ingestion load_id through to your final mart materialization.

### 3.2. Star-Layer BSL Generation

Update the `generate_boring_manifest` utility to target `fct_` and `dim_`
tables. Since the Star Schema is now standardized, the LLM will interact with a
consistent interface across all tenants.

## Critical Success Milestones

- **End of Week 1**: You can change a filter in `tenants.yaml`, run dbt, and see
  those records excluded from the hydrated intermediate models immediately.
- **End of Week 2**: A single surrogate key correctly links performance data
  from Google Ads and Facebook Ads for `tyrell_corp`.
- **End of Week 3**: The `onboard_tenant.py` script triggers a dlt workspace
  run, creating the Star Schema and updating the BSL manifest in one step.
