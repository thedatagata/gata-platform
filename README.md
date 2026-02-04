# Gata Platform: The SaaS Data Operating System

**Status:** `PRE-ALPHA` **Vision:** A unified, multi-tenant data platform that
enables "Zero-Touch" analytics. **Frontend:**
[Gata Swamp (DasGata)](./apps/gata-swamp) **Backend:** Python + Dagster + dbt +
DuckDB + MotherDuck

---

## 1. The Core Vision

We are building the backend "Operating System" for the `dasgata` frontend.
Currently, `dasgata` is a demo app with hardcoded data. This repository
(`gata-platform`) transforms it into a real SaaS product where:

1. **Tenants are onboarded automatically** (Infrastructure-as-Code).
2. **Data pipelines are generated dynamically** (Software-Defined Assets).
3. **Semantic layers are inferred instantly** (AI Profiling + BSL).

### The "Zero-Touch" Workflow

1. **User** (in `gata-swamp` UI) clicks "Connect Shopify".
2. **Backend** updates `tenants.yaml` and triggers a Dagster job.
3. **Orchestrator** (Dagster) spins up a `dlt` pipeline to ingest data to
   MotherDuck/DuckDB.
4. **Transformation** (dbt) runs `onboard_tenant.py` to generate `src_` and
   `stg_` models.
5. **Semantic Layer** (BSL) profiles the new tables and generates a
   `shopify_semantics.json`.
6. **Frontend** detects the new JSON and enables "Ask AI" for Shopify data
   immediately.

---

## 2. Architecture & Tech Stack

We utilize a modern, high-performance, single-node scalable stack:

| Component          | Technology                      | Role                                       | Source Repo for Patterns |
| :----------------- | :------------------------------ | :----------------------------------------- | :----------------------- |
| **Control Plane**  | **Dagster**                     | Orchestration, Asset Management, Resources | `dagster-duck`           |
| **Ingestion**      | **dlt** (Data Load Tool)        | Extract & Load, Schema Mechanics           | `mock-data-engine`       |
| **Warehouse**      | **DuckDB** / **MotherDuck**     | Compute & Storage (Local Dev / Cloud Prod) | N/A                      |
| **Transformation** | **dbt Core**                    | Modeling, Testing, Documentation           | `dbt-meta-config`        |
| **Semantic Layer** | **BSL** (Boring Semantic Layer) | Metric Definition, Text-to-SQL             | `boring-ducklake`        |
| **API**            | **FastAPI**                     | REST Interface for Frontend                | New                      |

### Directory Structure (The Unification)

This monorepo unifies the lifecycle of all components:

```plaintext
gata-platform/
├── apps/
│   └── gata-swamp/            # [Frontend] The Deno/Fresh UI (DasGata)
├── orchestration/             # [Brain] Dagster Software-Defined Assets
│   ├── assets/                # Assets grouped by domain (ingestion, dbt, semantic)
│   ├── resources/             # Shared connections (MotherDuckResource)
│   └── jobs/                  # "Onboard Tenant", "Refresh Data"
├── services/
│   ├── mock-data-engine/      # [Ingestion] dlt pipelines (Testing grounds)
│   └── semantic-api/          # [Bridge] FastAPI wrapper around BSL
├── warehouse/                 # [Engine] The dbt Project
│   ├── models/                # Hub-and-Spoke Data Vault
│   ├── macros/                # "Code that writes code" (build_master_hub)
│   └── seeds/                 # Static reference data
├── scripts/                   # [Automation]
│   ├── onboard_tenant.py      # Scaffolds dbt models + runs BSL profiling
│   └── generate_semantics.py  # Standalone BSL generator
├── static/tenants/            # [Registry] JSON Semantic Definitions
├── tenants.yaml               # [Config] The Single Source of Truth
└── pyproject.toml             # Unified uv dependency management
```

---

## 3. Detailed Implementation Roadmap

This is the step-by-step guide to building the platform.

### Phase 1: The Orchestration Backbone (Dagster)

**Goal:** Replace ad-hoc scripts with a resilient control plane. **Reference:**
`dagster-duck`

- [ ] **Initialize Dagster Project:** Set up `orchestration/` with `dagster`,
      `dagster-webserver`, `dagster-duckdb`.
- [ ] **Unified MotherDuck Resource:**
  - Create a custom Dagster resource that holds the MotherDuck connection.
  - **Crucial:** accurate resource passing is required so `dlt` (ingestion) and
    `dbt` (transformation) share the _same_ transaction/session logic where
    possible, or at least share credentials securely.
- [ ] **Asset Factory (The "Meta-Asset"):**
  - usage: `assets/tenant_assets.py` reads `tenants.yaml`.
  - logic: For each tenant + source combo, dynamically yield a `dlt_asset`
    (Ingestion) and a `dbt_asset` (Transformation).
- [ ] **Job Definitions:**
  - `onboard_tenant_job`: Runs the scaffolding script -> full refresh dbt run.
  - `daily_refresh_job`: Runs incremental `dlt` -> incremental `dbt`.

### Phase 2: The Automated Warehouse (dbt)

**Goal:** Zero-touch tenant onboarding via "Hub-and-Spoke" architecture.
**Reference:** `dbt-meta-config`

- [ ] **dbt Project Initialization:** Set up `warehouse/` with `dbt-duckdb`
      adapter.
- [ ] **Port Key Macros:**
  - `generate_tenant_key(slug)`: Hashes tenant slug to a consistent surrogate
    key.
  - `build_master_hub(source)`: The "Magic Macro". It scans `information_schema`
    (or dbt graph) to find all `stg_{tenant}__*` models and automaticall
    `UNION ALL`s them into `fct_master_{source}`.
- [ ] **Refactor `onboard_tenant.py`:**
  - **Current State:** Hardcoded to BigQuery.
  - **New State:** Abstract `SchemaProvider`. Use DuckDB to inspect `dlt`
    landing tables (`raw_{tenant}_{source}`).
  - **Output:** Must generate:
    1. `src_{tenant}__{source}.sql`: Shim over raw data.
    2. `stg_{tenant}__{source}.sql`: Adds `tenant_skey`, `source_schema_hash`,
       and standardizes columns.
- [ ] **Implement "The Universal Contract":**
  - Define abstract intermediate models (`int_ads_performance`) that enforce a
    strict schema (e.g., `spend`, `clicks`, `impressions`, `date`).
  - Staging models must align to this contract to join the Master Hub.

### Phase 3: The Semantic Bridge (BSL)

**Goal:** Automated reporting and LLM-ready context. **Reference:**
`boring-ducklake` & `services/mock-data-engine`

- [ ] **Scaffold Semantic Service:** Create `services/semantic-api` (FastAPI).
- [ ] **Port BSL Core:** Move `boring_semantic_layer` logic into
      `services/semantic-api/core`.
- [ ] **Automated Profiling Hook:**
  - Update `scripts/onboard_tenant.py` (or a Dagster asset) to run `SUMMARIZE`
    on new tables.
  - **Why?** LLMs need cardinality (e.g., "Is `status` low-cardinality enum or
    free text?") to write good SQL.
- [ ] **Metadata Generation:**
  - Inputs: `dbt` manifest (columns) + Profiling stats.
  - Output: `static/tenants/{tenant}_semantics.json`.
  - **Format:** Must match `dasgata`'s expected JSON structure exactly.
- [ ] **Text-to-SQL Endpoint:**
  - endpoint: `POST /ask`
  - body: `{ "tenant_id": "...", "question": "Show me revenue by month" }`
  - logic: Loads `{tenant}_semantics.json` -> BSL -> LLM -> SQL -> DuckDB ->
    Result.

### Phase 4: Frontend Integration (Gata Swamp)

**Goal:** Self-Service Control Plane. **Reference:** `dasgata` (apps/gata-swamp)

- [ ] **Config-Driven UI:**
  - Update frontend to fetch capabilities from `GET /api/tenants/{id}/config`.
- [ ] **Dynamic "Ask AI":**
  - The "Smarter" dashboard in `dasgata` currently mocks the semantic layer.
  - **Update:** Point the chat interface to the Python `services/semantic-api`.
- [ ] **Onboarding Flow:**
  - Create a form: "Business Name", "Data Sources" (Select Multi).
  - Action: `POST /api/onboard` -> Triggers Dagster Job.

---

## 4. Development Workflow

### Prerequisites

- `uv` (for Python dependency management)
- `deno` (for Frontend)
- `duckdb` (CLI optional, but recommended)

### Getting Started

1. **Install Dependencies:**
   ```bash
   uv sync --all-groups
   ```
2. **Start the Platform (Dagster + API):**
   ```bash
   uv run dagster dev
   # OR
   uv run python services/semantic_api/main.py
   ```
3. **Start Frontend:**
   ```bash
   cd apps/gata-swamp && deno task start
   ```

### Adding a New Connector

To add a new data source (e.g., "TikTok Ads"):

### Adding a New Connector

To add a new data source (e.g., "TikTok Ads"):

1. **Mock Generator:** Add `tiktok_ads` generator to `services/mock-data-engine`
   conforming to the schema.
2. **Define Connector:** Add entry to `supported_connectors.yaml` with version
   and object list.
3. **Register Blueprint:** Run
   `uv run python scripts/initialize_connector_library.py motherduck` to hash
   and publish the blueprint to the registry.
4. **Master Model:** Scaffold `platform_mm__tiktok_ads_{object}.sql` in the
   warehouse using the standardized ID.
5. **Verify:** Run `onboard_tenant.py` to ensure the new source is automatically
   discovered and mapped.
