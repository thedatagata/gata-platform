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
Staging models are views that SELECT from sources. A post-hook calls `sync_to_master_hub(master_model_id)` which does a MERGE INTO the master model table USING the staging view. This means **master model tables must exist before staging models can push to them**.

## Active Tenants

| Tenant | Ad Sources | Ecommerce | Analytics | Status |
|--------|-----------|-----------|-----------|--------|
| tyrell_corp | facebook_ads, instagram_ads, google_ads | shopify | google_analytics | Active |
| wayne_enterprises | bing_ads, google_ads | bigcommerce | google_analytics | Active |
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
| Intermediate models | `warehouse/gata_transformation/models/intermediate/{tenant}/{platform}/` |
| Analytics models | `warehouse/gata_transformation/models/analytics/{tenant}/` |
| Master models | `warehouse/gata_transformation/models/platform/master_models/` |
| Mock data generators | `services/mock-data-engine/sources/{domain}/{platform}/` |
| Platform ops | `warehouse/gata_transformation/models/platform/ops/` |
| Platform satellites | `warehouse/gata_transformation/models/platform/satellites/` |

## dbt Targets

- **dev**: MotherDuck (`md:my_db`). Use `--target dev` for all runs. Requires `MOTHERDUCK_TOKEN` env var.
- **sandbox**: Local DuckDB file. Avoid — has file locking issues.

**Always run dbt from:** `warehouse/gata_transformation/`

## Current Database State (MotherDuck dev target, Feb 2026)

**CRITICAL: The entire pipeline has 0 data flowing through it.**
- Raw source data EXISTS in tenant-specific schemas (tyrell_corp.*, wayne_enterprises.*, stark_industries.*)
- Master model tables exist in main schema but contain 0 rows
- Staging push (sync_to_master_hub MERGE) has never successfully executed
- All intermediate views and analytics tables are empty as a result

**Data is in tenant schemas, not `main`.** The dbt sources point to:
- `tyrell_corp.raw_tyrell_corp_{platform}_{object}` (e.g., 1,550 rows in facebook_insights)
- `wayne_enterprises.raw_wayne_enterprises_{platform}_{object}`
- `stark_industries.raw_stark_industries_{platform}_{object}` (e.g., 60,000 mixpanel events)

## Current Known Issues (as of Feb 2026)

1. **Staging MERGE bootstrap — BLOCKING ALL DATA FLOW:** Staging views use `sync_to_master_hub()` post-hook which does MERGE INTO master_model USING the staging view. The staging views reference themselves before they exist, causing `Table stg_X does not exist`. This must be fixed first — nothing else matters until data reaches master models.
2. **`platform_sat__tenant_config_history` schema mismatch (1 error):** `Binder Error: Values list "src" does not have a column named "key"` — config unpacking doesn't match actual YAML structure.
3. **`connector_blueprints` — EXISTS on MotherDuck dev.** Only missing in sandbox. Not an issue for dev target.
4. **`tojson` quoting bug — FIXED:** Analytics engines were using `tojson` which produces double quotes. Fixed to use single-quote wrapping via `{% for %}` loop.
5. **Missing sessions models:** `fct_tyrell_corp__sessions` and `fct_stark_industries__sessions` don't exist yet — were blocked by tojson bug (now fixed). Will materialize on next successful dbt run.

## Conventions

- Intermediate models: `int_{tenant_slug}__{platform}_{object}.sql`, materialized as views
- Analytics models: `fct_{tenant_slug}__{metric}.sql` or `dim_{tenant_slug}__{dimension}.sql`, materialized as tables
- Staging models: `stg_{tenant_slug}__{platform}_{object}.sql`, materialized as views with `sync_to_master_hub()` post-hook
- Master models: `platform_mm__{connector_api_version}_{object}.sql`
- All intermediate models must include `tenant_slug`, `source_platform`, `tenant_skey`, `loaded_at` columns plus `raw_data_payload` as the last column
