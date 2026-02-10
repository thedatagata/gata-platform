# GATA Platform — Claude Code Context

## What This Is
Multi-tenant analytics platform. Customers (tenants) connect their data sources (ad platforms, ecommerce, analytics tools). We land their raw data, push it through a schema-enforced pipeline, and produce tenant-isolated star schemas for reporting.

## Architecture (5 Layers)

```
sources/{tenant}/{platform}/     → Raw data landing (auto-generated)
staging/{tenant}/{platform}/     → Schema hash routing + push to master models (auto-generated)
platform/master_models/          → Multi-tenant tables with raw_data_payload JSON column
intermediate/{tenant}/{platform}/ → Extract typed fields from JSON, tenant-filtered views
analytics/{tenant}/              → Star schema (facts + dims) via engine/factory macros
```

**Layers 1-3 are auto-generated** by `scripts/onboard_tenant.py`. Staging models use `sync_to_master_hub()` post-hook to MERGE data into master model tables.

**Layers 4-5 are hand-written.** Intermediate models extract fields from `raw_data_payload` JSON. Analytics models call factory macros that call engine macros.

## Key Patterns

### Engine/Factory Pattern (Shell Architecture)
- **Engines** (`macros/engines/{domain}/`): Source-specific SQL that reads from intermediate models and outputs a canonical column set. Example: `engine_facebook_ads_performance(tenant_slug)` reads `int_{tenant}__facebook_ads_facebook_insights` and outputs `tenant_slug, source_platform, report_date, campaign_id, ad_group_id, ad_id, spend, impressions, clicks, conversions`.
- **Factories** (`macros/factories/`): Union engines together. Example: `build_fct_ad_performance('tyrell_corp', ['facebook_ads', 'google_ads'])` calls both engines and UNION ALLs the results.
- **Shell Models** (`models/analytics/{tenant}/`): Thin SQL files that call a factory. Example: `{{ build_fct_ad_performance('tyrell_corp', ['facebook_ads', 'google_ads', 'instagram_ads']) }}`

### JSON Extraction (DuckDB Syntax)
All master models store raw data in a `raw_data_payload` JSON column. Intermediate models extract typed fields:
```sql
CAST(raw_data_payload->>'$.field_name' AS BIGINT)     -- scalar to type
raw_data_payload->>'$.field_name'                       -- scalar to VARCHAR
raw_data_payload->'$.nested_object'                     -- keep as JSON
```

### Push Circuit (sync_to_master_hub)
Staging models are views that SELECT from sources. A `post_hook` in `generate_staging_pusher` calls `sync_to_master_hub(master_model_id)` which does a MERGE INTO the master model table USING the staging view. The post-hook ensures the view exists before the MERGE fires. Master model tables must exist before staging models can push to them.

### Staging Pusher (generate_staging_pusher)
The macro in `macros/onboarding/generate_staging_pusher.sql` creates staging views that:
1. SELECT from tenant source tables
2. Pack all source columns into a single `raw_data_payload` JSON column via `row_to_json(base)`
3. Fire `sync_to_master_hub()` as a `post_hook` (runs after view creation)

## Active Tenants

| Tenant | Ad Sources | Ecommerce | Analytics | Status |
|--------|-----------|-----------|-----------|--------|
| tyrell_corp | facebook_ads, instagram_ads, google_ads | shopify | google_analytics | Onboarding |
| wayne_enterprises | bing_ads, google_ads | bigcommerce | google_analytics | Onboarding |
| stark_industries | facebook_ads, instagram_ads | woocommerce | mixpanel | Onboarding |

## Important File Locations

| What | Where |
|------|-------|
| dbt project root | `warehouse/gata_transformation/` |
| Tenant config | `tenants.yaml` (project root) |
| Connector catalog | `supported_connectors.yaml` (project root) |
| Engine macros | `warehouse/gata_transformation/macros/engines/{analytics,ecommerce,paid_ads}/` |
| Factory macros | `warehouse/gata_transformation/macros/factories/` |
| Push macro | `warehouse/gata_transformation/macros/onboarding/sync_to_master_hub.sql` |
| Staging pusher macro | `warehouse/gata_transformation/macros/onboarding/generate_staging_pusher.sql` |
| Intermediate models | `warehouse/gata_transformation/models/intermediate/{tenant}/{platform}/` |
| Analytics models | `warehouse/gata_transformation/models/analytics/{tenant}/` |
| Master models | `warehouse/gata_transformation/models/platform/master_models/` |
| Platform governance | `warehouse/gata_transformation/models/platform/{hubs,satellites,ops}/` |
| Mock data generators | `services/mock-data-engine/sources/{domain}/{platform}/` |
| Onboarding scripts | `scripts/onboard_tenant.py`, `scripts/initialize_connector_library.py` |

## dbt Commands

**Always run dbt using:** `uv run --env-file ../../.env dbt <command> --target <target>`
**Always run from:** `warehouse/gata_transformation/`

Example: `uv run --env-file ../../.env dbt run --target dev`

## dbt Targets

- **dev**: MotherDuck (`md:my_db`). Requires `MOTHERDUCK_TOKEN` env var in `.env` file at project root.
- **sandbox**: Local DuckDB file (`warehouse/sandbox.duckdb`). Uses `threads: 1` to avoid file locking. Fully functional — no MotherDuck dependency needed.

## Current Database State (Feb 2026)

**Pipeline is fully operational on both targets.** Last successful run: 145 PASS, 0 ERROR, 0 SKIP.

### MotherDuck (dev target)
- Raw data in tenant-specific schemas: `tyrell_corp.*`, `wayne_enterprises.*`, `stark_industries.*`
- Master model tables populated via staging MERGE (data flowing end-to-end)
- All intermediate views and analytics tables materialized with data
- `_sources.yml` files include `database: "my_db"` for MotherDuck resolution

### Sandbox (local target)
- Raw data in tenant-specific schemas within `warehouse/sandbox.duckdb`
- Full parity with dev — same 145 models pass
- `_sources.yml` files omit `database` key (local DuckDB resolves without it)
- Data landed via `dlt.destinations.duckdb(credentials=sandbox_path)` in orchestrator

## Onboarding Workflow (sandbox)

```bash
# 1. Initialize connector library (registers schema hashes → master model mappings)
python scripts/initialize_connector_library.py sandbox

# 2. Set tenant status to "onboarding" in tenants.yaml, then for each tenant:
python scripts/onboard_tenant.py <tenant_slug> --target sandbox --days 30

# 3. Run full dbt pipeline
cd warehouse/gata_transformation
uv run --env-file ../../.env dbt run --target sandbox
```

For dev (MotherDuck), replace `sandbox` with `dev` in all commands.

## Platform Governance Models

These track tenant config changes at table-level granularity (tenant + source + table):

- **`platform_sat__tenant_config_history`**: Unpacks `sources_config` JSON from tenant manifest using `json_keys()` + `from_json()`. Outputs: `tenant_slug, tenant_skey, source_name, table_name, table_logic, logic_hash, updated_at`
- **`hub_key_registry`**: Generates surrogate keys from config history for change tracking
- **`platform_sat__tenant_source_configs`**: Latest config per tenant/source/table (thin wrapper on config_history)
- **`platform_ops__source_table_candidate_registry`**: Joins config history with physical table inventory for onboarding recommendations

## Resolved Issues (Feb 2026)

1. **Staging MERGE bootstrap — FIXED:** Moved `sync_to_master_hub()` from inline `{% do %}` (ran before view creation) to `post_hook` in config (runs after view creation).
2. **`struct_pack(*)` error — FIXED:** DuckDB can't resolve `*` inside `struct_pack()`. Replaced with `row_to_json(base)`.
3. **`platform_sat__tenant_config_history` JSON unpacking — FIXED:** `json_transform` with `'["JSON"]'` lost object keys. Replaced with `json_keys()` + `from_json()`.
4. **Sandbox target not working — FIXED:** Scripts now route to `warehouse/sandbox.duckdb` when target is `sandbox`. Orchestrator uses `dlt.destinations.duckdb(credentials=path)` to land data in the correct file.
5. **`tojson` quoting bug — FIXED:** Analytics engines were using `tojson` which produces double quotes. Fixed to use single-quote wrapping via `{% for %}` loop.
6. **Windows cp1252 emoji crashes — FIXED:** Replaced emoji characters in Python scripts with ASCII-safe `[TAG]` prefixes.

## Conventions

- Intermediate models: `int_{tenant_slug}__{platform}_{object}.sql`, materialized as views
- Analytics models: `fct_{tenant_slug}__{metric}.sql` or `dim_{tenant_slug}__{dimension}.sql`, materialized as tables
- Staging models: `stg_{tenant_slug}__{platform}_{object}.sql`, materialized as views with `sync_to_master_hub()` post-hook
- Master models: `platform_mm__{connector_api_version}_{object}.sql`
- All intermediate models must include `tenant_slug`, `source_platform`, `tenant_skey`, `loaded_at` columns plus `raw_data_payload` as the last column
- Python scripts must use ASCII-safe print statements (no emojis) for Windows compatibility
