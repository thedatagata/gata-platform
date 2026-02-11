# Gata Platform: Transformation Warehouse

This project manages the data transformation pipeline for the **Gata Platform**,
a unified multi-tenant SaaS data operating system. It utilizes a modern **Shell
& Engine** architecture inspired by **Data Vault 2.0** principles to enable
"Zero-Touch" analytics and automated tenant onboarding.

---

##  Core Architecture

Our architecture utilizes a high-integrity "Master Hub" pattern. This separates
**Standardized Platform Data** (the "what") from **Volatile Client Logic** (the
"how"), allowing us to update a client's business definitions retrospectively by
re-parsing the historical payloads stored in the Hub.

### 1. The 4-Column Contract (Staging & Master Hubs)

Every Master Hub Model strictly follows the **4-Column Universal Contract**.
This standardization is critical for drift detection and programmatic validation
across all tenants.

- `tenant_skey`: Point-of-time lookup key linking data to the tenant's
  configuration at the time of ingestion.
- `source_schema_hash`: A deterministic MD5 hash of the source schema (keys +
  types) for that specific record. Used for automated drift detection.
- `source_schema`: A JSON object describing the column names and types found in
  the source.
- `raw_data_payload`: The sanitized, complete raw JSON object.

**Master Hubs** (e.g., `fct_master__google_ads_normalized`) are "Blind Unions".
They collect the 4-Column output from every tenant's staging model without
applying any business logic.

### 2. The Logic Injection Point (Intermediate Layer)

This is where the "Shell & Engine" architecture comes alive.

- **Shell Models**: Tenant-specific models (e.g.,
  `int_{tenant}__ads_combined_report`) that act as the container for that
  client's data.
- **Factories**: Macros called by the Shell that stitch together the necessary
  "Brains" for that tenant based on their configuration.
- **Engines**: "Brains" that parse the `raw_data_payload` from the Master Hub.
  They apply tenant-specific logic (like conversion mapping or funnel regex)
  defined in the manifest.

---

##  Connector Catalog Registry

The **Connector Catalog Registry** is the source of truth that maps granular
source tables to their standardized Master Models. It bridges the gap between:

1. **Legacy Tenants**: Mapped via `platform_sat__tenant_config_history`
   (historical schema hashes).
2. **New Connectors**: Mapped via `connectors.main.connector_blueprints`
   (generated from `supported_connectors.yaml`).

### The Registry Model (`platform_ops__master_model_registry`)

This model unions data from both sources to provide a unified lookup for the
onboarding script:

- **Input 1 (Legacy)**: Historical Tenant Configs.
- **Input 2 (Standard)**: The `connectors` database in MotherDuck, populated by
  the Connector Library.

### Standardized Naming Convention

All new connectors follow the **Granular Library Convention**:

`{source}_api_v1_{object}`

Examples:

- `linkedin_ads_api_v1_campaigns`
- `bing_ads_api_v1_ad_groups`

### Automation Script (`initialize_connector_library.py`)

This Python script is the heartbeat of the registry. It:

1. Reads `supported_connectors.yaml`.
2. Generates mock data for every supported object.
3. Calculates the "Canonical Schema Hash" (filtering out ETL metadata).
4. Upserts these blueprints into `connectors.main.connector_blueprints`.

Run this script whenever you add a new connector or update a schema version.

---

##  Key Macros & Patterns

### `build_granular_master_hub`

**Purpose**: Automatically unions all staging models for a specific
platform/table type into a single Master Hub. It enforces the 4-column contract
and performs graph checks.

```sql
{{ build_granular_master_hub('google_ads', 'campaign') }}
```

### `engine_google_ads` / `engine_facebook_ads` (The "Brains")

**Purpose**: Parses the `raw_data_payload` JSON from the Master Hub. It is
"logic-blind" until it receives the `logic_config` from the factory.

**Code Example** (Config Injection):

```sql
{% macro engine_google_analytics(tenant_slug, logic_config) %}
-- ... extracts standard fields ...
    -- Tenant-specific CD Special Labeling from Manifest
    FIRST_VALUE(
        CASE
            {% for pattern, label in logic_config.get('cd_specials', {}).items() %}
            WHEN REGEXP_CONTAINS(page_location, '{{ pattern }}') THEN '{{ label }}'
            {% endfor %}
        END IGNORE NULLS
    ) OVER (...) AS session_cd_landing_page
-- ...
{% endmacro %}
```

### `build_ads_blended_fact` (The Factory)

**Purpose**: The orchestrator. It reads the tenant's manifest, identifies which
platforms are active, extracts their specific logic configurations, and calls
the appropriate Engines to build the final report.

---

##  Unification & Roadmap

This warehouse is part of a larger **Monorepo Architecture** that bridges three
core components into a cohesive SaaS product:

1. **Orchestration (Dagster)**: The control plane orchestrating ingestion (dlt)
   and building (dbt).
2. **Transformation (dbt)**: This project - the engine for automated tenant
   onboarding and hub-and-spoke modeling.
3. **Semantic Layer (BSL)**: The foundation for AI-ready data and automated
   profiling.

### Key Objectives

- **Zero-Touch Onboarding**: Fully automated scaffolding of tenant models via
  `onboard_tenant.py`.
- **DuckDB / MotherDuck Integration**: Leveraging DuckDB for efficient local
  processing and MotherDuck for scalable cloud analytics.
- **AI-Driven Metadata**: Automated profiling and semantic layer generation to
  power the "Ask AI" features of the platform.
