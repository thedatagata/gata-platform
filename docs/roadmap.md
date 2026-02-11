# GATA Platform Roadmap

This document tracks implementation phases, current status, and upcoming work. It serves as both a progress log and a coordination guide for agents working on the codebase.

---

## Completed Phases

### Phase 1: Foundation — Connector Library & Data Contracts 

**Goal:** Establish the structural fingerprinting system and Pydantic data contracts for all supported connectors.

**Delivered:**
- `supported_connectors.yaml` with 13 connectors across 3 domains (7 paid ads, 3 ecommerce, 3 analytics)
- `scripts/initialize_connector_library.py` — calculates MD5 schema hashes from physical column definitions
- Pydantic schemas in `services/mock-data-engine/schemas/` for all connector objects
- `connector_blueprints` registry mapping schema fingerprints → master model IDs
- Mock data generators producing realistic synthetic data for all connectors

**Key Files:**
- `supported_connectors.yaml`
- `scripts/initialize_connector_library.py`
- `services/mock-data-engine/schemas/*.py`
- `services/mock-data-engine/orchestrator.py` (Polars-based extraction with Pydantic enforcement)

---

### Phase 2: Tenant Onboarding & Source Layer 

**Goal:** Automate the creation of source shims and staging pushers for new tenants.

**Delivered:**
- `tenants.yaml` as the single source of truth for tenant configs (3 tenants defined: tyrell_corp, wayne_enterprises, stark_industries)
- `scripts/onboard_tenant.py` — reads tenants.yaml, scaffolds `_sources.yml` and staging models per tenant per platform
- Source shim generation: `models/sources/{tenant}/{platform}/_sources.yml`
- Staging pusher generation: `models/staging/{tenant}/{platform}/stg_{tenant}__{platform}_{object}.sql`
- Macros: `generate_staging_pusher`, `load_clients`, `gen_tenant_key`, `get_tenant_config`

**Current Model Count:**
- Sources: 9 `_sources.yml` files (tyrell_corp: 5 platforms, wayne_enterprises: 4 platforms)
- Staging: 28 pusher models

---

### Phase 3: Master Model Layer & Data Vault 

**Goal:** Build the multi-tenant sinks, hub key registry, and satellite tracking.

**Delivered:**
- 31 master models in `models/platform/master_models/` covering all 13 connectors and their objects
- Each master model has standard columns: `tenant_slug`, `source_platform`, `tenant_skey`, `source_schema_hash`, `loaded_at`, `raw_data_payload`
- Hub key registry: `models/platform/hubs/hub_key_registry.sql`
- Satellite tables for schema history, tenant config history, and tenant source configs
- Ops models: master model registry, schema hash registry, source table candidate registry
- Facebook/Instagram ads share `facebook_ads_api_v1` master models with `source_platform` differentiation

**Key Files:**
- `models/platform/master_models/platform_mm__*.sql` (31 models)
- `models/platform/satellites/platform_sat__*.sql` (3 satellites)
- `models/platform/ops/platform_ops__*.sql` (3 ops models)
- `macros/onboarding/sync_to_master_hub.sql`
- `macros/onboarding/sync_to_schema_history.sql`

---

### Phase 4: Push Circuit Closure & Intermediate Layer 

**Goal:** Wire staging pushers to master models via post-hooks and build tenant-isolated intermediate views.

**Delivered:**
- `sync_to_master_hub()` post-hook on all staging models — MERGE into target master model
- Intermediate models for tyrell_corp (16 models across 5 platforms) and wayne_enterprises (12 models across 4 platforms)
- Each intermediate model: filters by `tenant_slug` + `source_platform`, extracts typed fields from `raw_data_payload` JSON, applies tenant-specific logic from `tenants.yaml`
- All intermediate models materialized as `view` (lightweight projections, no data duplication)
- Naming: `int_{tenant}__{platform}_{object}.sql`
- `source_platform` filters correctly differentiate Facebook vs Instagram ads in shared master models

**Current Model Count:**
- Intermediate: 28 models (tyrell_corp: 16, wayne_enterprises: 12)

---

### Phase 5: Engines, Factories & Analytics Star Schema 

**Goal:** Build the shell-and-engine architecture that normalizes diverse sources into canonical star schemas.

**Delivered:**

**Engines (13 total):**
- Paid Ads (7): `engine_facebook_ads`, `engine_instagram_ads`, `engine_google_ads`, `engine_bing_ads`, `engine_linkedin_ads`, `engine_amazon_ads`, `engine_tiktok_ads`
  - Canonical: `tenant_slug, source_platform, report_date, campaign_id, ad_group_id, ad_id, spend, impressions, clicks, conversions`
- Ecommerce (3): `engine_shopify`, `engine_bigcommerce`, `engine_woocommerce`
  - Canonical: `tenant_slug, source_platform, order_id, order_date, total_price, currency, financial_status, customer_email, customer_id, line_items_json`
- Analytics (3): `engine_google_analytics`, `engine_amplitude`, `engine_mixpanel`
  - Canonical: sessionized output with 30-min windows, first-touch attribution, conversion flags, revenue tracking

**Factories (4 total):**
- `build_fct_ad_performance(tenant_slug, ad_sources)` — UNION ALL of paid ad engines via `engine_map`
- `build_fct_orders(tenant_slug, ecommerce_sources)` — UNION ALL of ecommerce engines
- `build_fct_sessions(tenant_slug, analytics_source, conversion_events)` — single analytics engine call
- `build_dim_campaigns(tenant_slug, ad_sources)` — UNION ALL of campaign intermediate models

**Analytics Shell Models (8 total):**
- Tyrell Corp (4): `fct_tyrell_corp__ad_performance`, `fct_tyrell_corp__orders`, `fct_tyrell_corp__sessions`, `dim_tyrell_corp__campaigns`
- Wayne Enterprises (4): `fct_wayne_enterprises__ad_performance`, `fct_wayne_enterprises__orders`, `fct_wayne_enterprises__sessions`, `dim_wayne_enterprises__campaigns`
- All materialized as `table`

**Key Files:**
- `macros/engines/paid_ads/engine_*.sql` (7 files)
- `macros/engines/ecommerce/engine_*.sql` (3 files)
- `macros/engines/analytics/engine_*.sql` (3 files)
- `macros/factories/build_*.sql` (4 files)
- `models/analytics/{tenant}/*.sql` (8 files)

---

### Phase 6: Observability & Metadata Capture 

**Goal:** Capture dbt execution artifacts for downstream automation and semantic layer population.

**Delivered:**
- `macros/dbt_metadata/metadata_ops.sql` — on-run-end hooks
  - `init_artifact_tables()` — creates artifact tables on first run
  - `upload_model_definitions()` — captures model DAG, dependencies, config, tags
  - `upload_run_results()` — captures execution status, timing, rows affected
  - `upload_test_definitions()` — captures test definitions and targets
- Observability models in `models/platform/observability/`:
  - Source, staging, and intermediate models for model definitions, run results, test results
  - `int_platform_observability__md_table_stats.sql` — MotherDuck table statistics
  - `int_platform_observability__source_candidate_map.sql` — source-to-master-model mapping

---

### Phase 7: dlt → dbt Integration 

**Goal:** Wire dlt ingestion to dbt transformation in a single pipeline.

**Delivered:**
- `main.py` at project root uses `dlt.helpers.dbt.create_runner()` with `credentials=None` and `package_profiles_dir` to bypass dlt credential injection and use existing `profiles.yml` directly
- `profiles.yml` configured with `sandbox` (local DuckDB file) and `dev` (MotherDuck) targets
- Pipeline: dlt lands data → triggers dbt run → on-run-end hooks capture metadata

---

### Phase 8: In-Browser Semantic Layer (Tier 1) 

**Goal:** Build a self-contained text-to-SQL engine that runs entirely client-side.

**Delivered:**
- `semantic-profiler.ts` — auto-profiles tables via DuckDB `SUMMARIZE`, infers field categories, detects castable types
- `semantic-config.ts` — metadata registry, SQL prompt generation, dimension/measure lookup functions
- `semantic-objects.ts` — `SemanticReportObj` class translates dimension/measure aliases to SQL, executes against DuckDB WASM
- `semantic-query-validator.ts` — validates LLM-generated SQL against metadata, generates correction prompts
- `webllm-handler.ts` — `WebLLMSemanticHandler` manages Qwen 2.5 Coder (3B/7B) in-browser, with retry loop and validation
- `semantic-catalog-generator.ts` — generates LLM prompts with grouped dimension/measure catalogs
- `slice-object-generator.ts` — generates WebDataRocks pivot table configurations from query results
- Pre-built metadata JSONs for sessions and users tables
- LaunchDarkly integration for model tier selection and validation strategy flags

**Key Files:**
- `app/utils/smarter/dashboard_utils/semantic-config.ts`
- `app/utils/smarter/dashboard_utils/semantic-objects.ts`
- `app/utils/smarter/dashboard_utils/semantic-query-validator.ts`
- `app/utils/smarter/autovisualization_dashboard/webllm-handler.ts`
- `app/utils/system/semantic-profiler.ts`

---

## In Progress

### Phase 9: Backend Semantic Layer (Tier 2) — BSL Integration 

**Goal:** Auto-generate Boring Semantic Layer definitions from dbt metadata and serve via FastAPI with MCP support.

**Status:** Architecture defined. Existing scaffolding in place. Implementation pending.

#### 9.1 BSL Population Script (Not Started)
Post-dbt-run Python script that:
1. Reads `model_artifacts__current` to discover `fct_*` and `dim_*` models per tenant
2. Runs `DESCRIBE` + `SUMMARIZE` against each analytics table
3. Reads `tenants.yaml` for tenant-specific context (conversion events, source mix)
4. Infers join paths from shared column names across fact and dimension tables
5. Generates BSL definition files per tenant (dimensions, measures, relationships, derived metrics)

**Expected Join Paths:**
- `fct_ad_performance` ↔ `dim_campaigns` on `campaign_id` + `source_platform` (many-to-one)
- `fct_sessions` ↔ `fct_orders` on `transaction_id` (one-to-one where exists)
- `fct_sessions` ↔ `dim_campaigns` on `traffic_campaign` ≈ `campaign_name` (attribution join)

**Expected Derived Metrics:**
- ROAS = revenue from sessions/orders ÷ spend from ad performance
- CAC = total spend ÷ converting sessions
- Conversion Rate = converting sessions ÷ total sessions

**Depends On:** `boring-semantic-layer` package (pip install), `ibis-framework`

#### 9.2 FastAPI Endpoint Upgrade (Partially Scaffolded)
Current state: `services/platform-api/main.py` has stub endpoints:
- `GET /semantic-layer/{tenant_slug}` — currently queries `platform_ops__boring_semantic_layer` (needs repurposing)
- `POST /semantic-layer/update` — triggers tenants.yaml update + dbt run

**Needed:**
- Swap raw schema query for BSL-powered endpoints
- `GET /semantic-layer/{tenant}/dimensions` — list available dimensions with descriptions
- `GET /semantic-layer/{tenant}/measures` — list available measures with descriptions
- `POST /semantic-layer/{tenant}/query` — execute semantic query, return data + generated SQL
- `GET /semantic-layer/{tenant}/relationships` — list join paths between tables

#### 9.3 MCP Endpoint (Not Started)
Expose BSL as MCP tools for AI agent consumption:
- `list_metrics()` — returns all available measures
- `get_dimension_values(dimension_name)` — returns distinct values for filtering
- `query(metrics, dimensions, filters)` — executes governed semantic query
- Standard MCP protocol so any AI agent (Claude, GPT, open-source) can consume

---

## Upcoming

### Phase 10: Full Pipeline Validation

**Goal:** End-to-end test: mock data generation → dlt ingestion → dbt run → analytics tables materialized → BSL populated → API serving.

**Tasks:**
- [ ] Run `dbt run --target sandbox` to validate full pipeline compiles for both active tenants
- [ ] Verify engine/factory output matches canonical schemas
- [ ] Verify intermediate models correctly filter by tenant_slug + source_platform
- [ ] Verify analytics tables contain only the expected tenant's data
- [ ] Verify observability hooks capture model definitions and run results
- [ ] Smoke test BSL population script against materialized analytics tables

---

### Phase 11: Stark Industries Onboarding (Third Tenant)

**Goal:** Prove the platform handles a third tenant with new source combinations (WooCommerce + Facebook/Instagram + Mixpanel).

**Tasks:**
- [ ] Generate mock data for WooCommerce orders/products and Mixpanel events/people
- [ ] Run `onboard_tenant.py` for stark_industries
- [ ] Verify scaffolded source/staging/intermediate models
- [ ] Create analytics shell models: `fct_stark_industries__ad_performance`, `fct_stark_industries__orders`, `fct_stark_industries__sessions`, `dim_stark_industries__campaigns`
- [ ] Run `dbt run` and verify all 3 tenants build correctly
- [ ] Verify BSL picks up the new tenant automatically

**New Engine Coverage:**
- WooCommerce engine (`engine_woocommerce`) already exists but untested with real tenant
- Mixpanel engine (`engine_mixpanel`) already exists but untested with real tenant
- Stripe charge ID extraction from WooCommerce `meta_data` for reliable transaction linking

---

### Phase 12: Frontend ↔ Backend Integration

**Goal:** Connect the Deno/Fresh app to the FastAPI backend for multi-table analytics.

**Tasks:**
- [ ] Frontend route for selecting query tier (single-table WASM vs multi-table backend)
- [ ] API client in `app/utils/services/` for FastAPI semantic endpoints
- [ ] Dashboard component that renders BSL query results using existing chart infrastructure
- [ ] Tenant context switching — use authenticated user's tenant_slug to scope API calls
- [ ] Fall-through: if WebLLM detects a multi-table query pattern (mentions of joins, cross-domain metrics), suggest switching to Tier 2

**Existing Infrastructure to Leverage:**
- `app/utils/services/motherduck-client.ts` — already handles server-side MotherDuck queries
- `app/routes/api/motherduck-token.ts` — token endpoint for MotherDuck access
- `app/islands/dashboard/smarter_dashboard/CustomDataDashboard.tsx` — main dashboard component
- `app/islands/onboarding/DashboardRouter.tsx` — routes between dashboard experiences

---

### Phase 13: MotherDuck AI Integration

**Goal:** Use MotherDuck's AI features (e.g., `prompt()`) with BSL context for complex queries.

**Tasks:**
- [ ] Feed BSL definitions as context to MotherDuck AI endpoints
- [ ] Compare MotherDuck AI-generated SQL vs BSL-generated SQL for accuracy
- [ ] Evaluate latency tradeoffs: BSL generates SQL locally vs MotherDuck generates remotely
- [ ] Hybrid approach: BSL handles standard metric queries, MotherDuck AI handles exploratory/ad-hoc

---

### Phase 14: Tenant Self-Service Onboarding

**Goal:** A new user signs up in the Deno/Fresh app, selects their data sources, and the platform provisions their pipeline without manual intervention.

**Tasks:**
- [ ] Onboarding UI: source selection screen backed by `supported_connectors.yaml`
- [ ] API endpoint: `POST /tenants/onboard` — writes to `tenants.yaml`, triggers `onboard_tenant.py`, runs dbt
- [ ] OAuth connector flows for real data sources (replacing mock data)
- [ ] Progress tracking: user sees pipeline status (ingesting → transforming → ready)
- [ ] BSL auto-generation triggers when analytics tables are first materialized

---

## Architecture Debt & Known Issues

### Identified Improvements
1. **Ecommerce-Stripe linking** — Current ecommerce factory uses fragile amount/date/currency matching between ecommerce and Stripe data. Should extract Stripe charge ID directly from order objects (available in Shopify `note_attributes` and WooCommerce `meta_data`).
2. **`platform_ops__boring_semantic_layer.sql`** — Currently a stub pulling from source schema history. Needs repurposing or replacing with BSL population output.
3. **Stark Industries not yet scaffolded** — Defined in `tenants.yaml` but source/staging/intermediate/analytics models not yet created.
4. **No automated testing** — dbt tests defined in observability but not yet wired into CI/CD.
5. **Analytics models hardcoded** — Each tenant's analytics shell models are manually created. Could be auto-generated from `tenants.yaml` + factory registry.

### Technical Debt
- `.agent/` directory from earlier AI-assisted development may contain outdated workflows — should be audited
- Some earlier unified multi-tenant intermediate models were deleted (broke tenant isolation) — git history contains remnants
- `services/rill-dashboard/` is optional/experimental and may not reflect current model structure

---

## Model Count Summary

| Layer | Tyrell Corp | Wayne Enterprises | Stark Industries | Platform | Total |
|:------|:------------|:------------------|:-----------------|:---------|:------|
| Sources | 5 _sources.yml | 4 _sources.yml | — | — | 9 |
| Staging | 16 | 12 | — | — | 28 |
| Master Models | — | — | — | 31 | 31 |
| Hubs | — | — | — | 1 | 1 |
| Satellites | — | — | — | 3 | 3 |
| Ops | — | — | — | 4 | 4 |
| Observability | — | — | — | 9 | 9 |
| Intermediate | 16 | 12 | — | — | 28 |
| Analytics | 4 | 4 | — | — | 8 |
| **Total** | **41** | **32** | **0** | **48** | **121** |
