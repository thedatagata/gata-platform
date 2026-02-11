# GATA Platform — dbt Project Audit Report

**Date:** February 10, 2026\
**Scope:** `warehouse/gata_transformation/` after Claude Code progress\
**Baseline:** 145 PASS → 125 PASS (post-cleanup) per CLAUDE.md

---

## Executive Summary

The foundation is in great shape. The 5-layer architecture (sources → staging →
master_models → intermediate → analytics) is clean, consistent, and correctly
implements the push-based data vault pattern with the shell/engine/factory
reporting layer. All 3 tenants (tyrell_corp, wayne_enterprises,
stark_industries) are fully onboarded with their respective source mixes, and
the pipeline runs end-to-end on both sandbox and MotherDuck targets.

This audit identified **3 critical fixes**, **7 moderate issues**, and **3
cleanup items**, plus several design considerations for the reporting layer
buildout.

---

## Current Inventory

| Layer               | Count | Notes                                                                               |
| ------------------- | ----- | ----------------------------------------------------------------------------------- |
| Source models       | ~40   | SELECT * passthrough from raw tables                                                |
| Staging models      | 40    | Views with sync_to_master_hub post-hook                                             |
| Master model sinks  | 30    | WHERE 1=0 shells hydrated by staging MERGE                                          |
| Intermediate models | 20    | JSON unpacking from master models, tenant-filtered                                  |
| Analytics models    | 12    | 4 per tenant (fct_ad_performance, fct_orders, fct_sessions, dim_campaigns)          |
| Platform governance | ~10   | Config history, key registry, BSL, observability                                    |
| Engines             | 13    | 7 paid ads, 3 ecommerce, 3 analytics                                                |
| Factories           | 4     | build_fct_ad_performance, build_fct_orders, build_fct_sessions, build_dim_campaigns |
| Utility macros      | 13    | Mix of active and orphaned                                                          |

---

## Findings

### 🔴 CRITICAL — Must Fix

#### C1. Emoji in sync_to_master_hub.sql will crash Windows

**File:** `macros/onboarding/sync_to_master_hub.sql`\
**Line:**
`{{ log("🚀 Hardcoded Push: " ~ source_relation ~ " -> " ~ target_relation, info=True) }}`\
**Issue:** CLAUDE.md documents that all emojis were replaced with ASCII-safe
`[TAG]` prefixes for Windows cp1252 compatibility. This one was missed. Every
staging model fires this macro via post_hook, so this affects all 40 staging
models on Windows terminals.\
**Fix:** Replace `🚀` with `[PUSH]` or similar ASCII prefix.

#### C2. Shopify engine discards extracted customer_id

**File:** `macros/engines/ecommerce/engine_shopify.sql`\
**Issue:** Returns `CAST(NULL AS VARCHAR) AS customer_id` despite
`int_{slug}__shopify_orders` already extracting `customer_id` from the JSON
payload. This means `fct_*__orders` for any Shopify tenant has NULL customer_id,
breaking downstream identity resolution.\
**Fix:** Change to `customer_id` (direct column reference from intermediate
model).

#### C3. Two tenants have zero conversion tracking

**Files:** `fct_wayne_enterprises__sessions.sql`,
`fct_stark_industries__sessions.sql`\
**Issue:** Both pass empty `[]` for conversion_events to their session
factories, making `is_conversion_session` always FALSE. This renders
session-level conversion analysis useless for these tenants. `tenants.yaml` only
has conversion_events for tyrell_corp's google_analytics.\
**Fix:** Add conversion_events to `tenants.yaml` for wayne_enterprises (GA) and
stark_industries (Mixpanel), then update the shell models to pass them.
Alternatively, make the factories read conversion_events from tenants.yaml
directly instead of requiring hardcoded lists in shell models.

---

### 🟡 MODERATE — Should Address

#### M1. 10+ orphaned macros from HiFi era

**Location:** `macros/utils/`\
**Unused macros that are never called by any model, engine, or factory:**

| Macro                           | File                              | Why Orphaned                                                              |
| ------------------------------- | --------------------------------- | ------------------------------------------------------------------------- |
| `standard_ad_columns`           | standard_ad_columns.sql           | Engines use direct column SELECT now                                      |
| `get_session_attribution_logic` | get_session_attribution_logic.sql | Pure BigQuery syntax (TIMESTAMP_DIFF, ARRAY_TO_STRING). Legacy MTA logic. |
| `get_date_dimension_spine`      | sql_utilities.sql                 | References old HiFi model names                                           |
| `generate_master_union`         | generate_master_union.sql         | Parallel factory pattern, never wired                                     |
| `apply_tenant_logic`            | apply_tenant_logic.sql            | Config injection macro, never called                                      |
| `extract_ga4_param`             | extract_ga4_param.sql             | Mock data flattens event_params; no array to parse                        |
| `extract_field`                 | extract_json.sql                  | Nice utility, but inline JSON syntax used everywhere                      |
| `get_client_logic`              | get_client_logic.sql              | Config-driven logic injection, never called by engines                    |
| `get_clients_to_process`        | get_clients_to_process.sql        | Wrapper on get_manifest, never called                                     |

**Impact:** Bloats the macro namespace, confuses contributors, and some contain
BigQuery syntax that would fail if accidentally invoked.\
**Recommendation:** Move to a `macros/_archive/` directory or delete.

#### M2. platform_ops__schema_hash_registry is stale

**File:** `models/platform/ops/platform_ops__schema_hash_registry.sql`\
**Issue:** Contains hardcoded CASE mapping for only facebook_ads, google_ads,
instagram_ads. The connector_blueprints system replaced this function via
`platform_ops__master_model_registry`. This model reads from
`platform_sat__source_schema_history` which is itself a sink that may have stale
data.\
**Recommendation:** Deprecate or delete. The master_model_registry is the
authoritative source.

#### M3. Bing Ads loses all dimensional granularity

**File:** `macros/engines/paid_ads/engine_bing_ads.sql` +
`int_*__bing_ads_account_performance_report.sql`\
**Issue:** The intermediate model only unpacks account-level metrics (date,
spend, impressions, clicks) from the `account_performance_report` table. The
engine then NULLs out campaign_id, ad_group_id, and ad_id. The bing_ads staging
layer includes a separate `campaigns` table with Id, Name, Status — but there's
no intermediate model for the bing_ads performance report that joins campaign
data.\
**Impact:** Wayne Enterprises' fct_ad_performance from bing_ads has no
campaign-level granularity — it's just daily totals.\
**Fix:** Either create a more granular intermediate model that joins the
account_performance_report with bing_ads_campaigns on a date/spend allocation
basis, or accept this as account-level data with a documented limitation. The
underlying mock data generator may need to emit campaign-level reports (e.g.,
`campaign_performance_report`) for this to be fully resolved.

#### M4. BigCommerce engine NULLs out customer_email and line_items

**File:** `macros/engines/ecommerce/engine_bigcommerce.sql` +
`int_wayne_enterprises__bigcommerce_orders.sql`\
**Issue:** The BigCommerce intermediate model only extracts 6 fields (order_id,
created_at, total_price, currency, status, customer_id) from the JSON payload.
The engine then outputs `CAST(NULL AS VARCHAR) AS customer_email` and
`CAST(NULL AS JSON) AS line_items_json`. If the raw_data_payload contains
`billing_email` or `line_items`, they're being silently discarded.\
**Impact:** Wayne Enterprises' fct_orders from BigCommerce has no customer email
for identity resolution and no line item detail for product-level analysis.\
**Fix:** Check the BigCommerce mock data schema for available fields. If
billing_email and line_items exist in the payload, add them to the intermediate
unpacker config and update the engine to pass them through.

#### M5. Inconsistent intermediate model patterns

**Location:** `models/intermediate/*/`\
**Issue:** Tyrell Corp's intermediate models use inline SQL with manual
`raw_data_payload->>'$.field'` extraction, while Wayne Enterprises and Stark
Industries use the `generate_intermediate_unpacker()` macro. Both patterns work,
but the inconsistency makes maintenance harder and means Tyrell Corp's models
don't benefit from future improvements to the macro.\
**Examples:**

- `int_tyrell_corp__shopify_orders.sql` — 25 lines of inline SQL
- `int_wayne_enterprises__bigcommerce_orders.sql` — 9-line macro call\
  **Recommendation:** Migrate Tyrell Corp intermediate models to use
  `generate_intermediate_unpacker()` for consistency. Low risk since the output
  schema stays identical.

#### M6. tenants.yaml is sparse for non-Tyrell tenants

**File:** `tenants.yaml`\
**Issue:** Tyrell Corp has complete table-level definitions with logic blocks
for all 5 sources. Wayne Enterprises has partial definitions (bing_ads has
tables, but google_ads and google_analytics have `enabled: true` with no tables
or logic blocks). Stark Industries is similarly incomplete (mixpanel events has
empty logic, facebook_ads insights has only `attribution_model`). The
`platform_sat__tenant_config_history` model tracks table-level logic, so missing
table definitions mean missing governance records.\
**Impact:** The config-driven rebuild story depends on complete manifests. If a
tenant's logic block is empty, there's nothing to version-control or diff when
they change their reporting requirements.\
**Fix:** Complete tenants.yaml with table-level definitions and logic blocks for
all sources on all tenants. At minimum: wayne_enterprises needs
`google_analytics.tables[events].logic.conversion_events`, and stark_industries
needs `mixpanel.tables[events].logic.conversion_events`.

#### M7. Ad source lists are hardcoded in analytics shell models

**Files:** All 12 analytics shell models in `models/analytics/*/`\
**Issue:** Each analytics model passes a hardcoded list of sources to its
factory call. For example, `fct_tyrell_corp__ad_performance.sql` passes
`['facebook_ads', 'google_ads', 'instagram_ads']`. If a tenant adds or removes a
source in tenants.yaml, the shell model must be manually edited to match.\
**Impact:** Defeats the config-driven onboarding goal. Adding a new source to a
tenant requires editing 1-4 SQL files in addition to tenants.yaml.\
**Fix:** Have factories read active sources from `get_tenant_config()` at
compile time instead of accepting explicit lists. The factory can introspect
tenants.yaml for enabled sources by platform category (paid_ads, ecommerce,
analytics) and auto-include the right engines. This is a medium-effort refactor
but high-value for the automation story.

---

### 🟢 CLEANUP — Nice to Have

#### L1. dbt_project.yml has no model-level configs

**File:** `dbt_project.yml`\
**Issue:** No `models:` block defining materialization defaults, schema
assignments, or tags per layer. Every model individually sets
`config(materialized='table')` or `config(materialized='view')`. This works but
means:

- No schema separation (all models land in the default schema)
- No way to run by layer (e.g., `dbt run --select tag:intermediate`)
- No centralized control over materialization strategy\
  **Recommendation:** Add a models block:

```yaml
models:
    gata_transformation:
        sources:
            +materialized: view
        staging:
            +materialized: view
        intermediate:
            +materialized: table
        analytics:
            +materialized: table
            +tags: ["reporting"]
        platform:
            +materialized: table
```

#### L2. Mixpanel intermediate model missing revenue/transaction fields

**File:** `int_stark_industries__mixpanel_events.sql`\
**Issue:** The model extracts 9 event fields but does not extract any revenue or
transaction_id fields from the Mixpanel payload. The Mixpanel engine handles
this gracefully (returns `CAST(0 AS DOUBLE) AS session_revenue` and
`CAST(NULL AS VARCHAR) AS transaction_id`), but if Mixpanel events carry
purchase revenue data, the intermediate model would need updating.\
**Impact:** Low — the engine has safe defaults. But it means Stark Industries
can never get session-level revenue from Mixpanel without an intermediate model
change.\
**Recommendation:** Add `purchase_revenue` and `transaction_id` extraction to
the Mixpanel intermediate if the mock data carries those fields, or document the
limitation.

#### L3. on-run-start/end artifact macros are untested

**File:** `dbt_project.yml` + `macros/dbt_metadata/`\
**Issue:** The project still fires `init_artifact_tables()`,
`upload_run_results()`, `upload_model_definitions()`, and
`upload_test_definitions()` on every run. These macros were inherited from the
HiFi era. They create and populate metadata tables for observability, but their
compatibility with the current DuckDB/MotherDuck target has not been verified in
this audit.\
**Recommendation:** Verify these macros run cleanly or disable them. If they
error silently, they may be wasting execution time or causing misleading
observability data.

---

## Design Considerations for Reporting Layer Buildout

### D1. Config-Driven Conversion Events (addresses C3 + M7)

The most impactful next step is making the reporting layer fully config-driven.
Currently, conversion_events and source lists are hardcoded in shell models. The
target architecture should be:

1. **tenants.yaml** defines conversion_events per analytics source and
   categorizes sources by type (paid_ads, ecommerce, analytics)
2. **Factories** read config at compile time via `get_tenant_config()` — no more
   parameter lists
3. **Shell models** become true shells — just
   `{{ build_fct_sessions(tenant_slug) }}` with zero parameters

This eliminates the manual editing required when a tenant changes their
conversion definition, and it means the reporting layer can be rebuilt
automatically when the manifest changes.

### D2. Identity Resolution Gap

No tenant currently has a `dim_users` table. The data lineage exists:

- GA4/Mixpanel events have `user_pseudo_id` + `transaction_id`
- Shopify/BigCommerce/WooCommerce orders have `customer_id` + `customer_email` +
  `order_id`
- `fct_sessions.transaction_id` can join to `fct_orders.order_id`

A `build_dim_users()` factory that joins sessions to orders via transaction_id
would produce the identity graph needed for customer-level analytics. This is a
prerequisite for any LTV, cohort, or attribution model.

### D3. Missing fct_events for Funnel Analysis

The intermediate models have event-level granularity, but the analytics layer
only has sessionized data. A `fct_events` table would enable:

- Funnel step analysis (view_item → add_to_cart → purchase drop-off rates)
- Event sequence analysis
- Real-time conversion monitoring

Tyrell Corp's tenants.yaml already defines `funnel_steps` — the plumbing is
there, just needs a factory and engine to produce the fct_events output.

### D4. Semantic Layer Readiness

The `platform_ops__boring_semantic_layer` model catalogs table/column metadata
from `information_schema` but produces no BSL-compatible YAML configs. The gap
between the current BSL model and the target BSL architecture is:

- No measure definitions (total_spend, total_revenue, CTR, CPC, etc.)
- No dimension definitions with transformations
- No join relationship definitions between fact and dimension tables
- No time_dimension designations

This should be addressed after the star schema is complete (including dim_users
and fct_events).

### D5. Two-Pass Execution Requirement

Per CLAUDE.md, master model sinks recreate as empty tables on a full `dbt run`,
which means the reporting layer has no data until staging views fire their
post_hooks. A single `dbt run` produces populated master models but empty
intermediate/analytics tables. A second `dbt run` (or selective
`dbt run --select intermediate+ analytics+`) is needed to hydrate the reporting
layer.

**Recommendation:** Document this in CLAUDE.md and add a `selectors.yml` entry:

```yaml
selectors:
    - name: reporting_refresh
      definition:
          method: path
          value: models/intermediate models/analytics
```

---

## Tenant Source Matrix (Current State)

| Source           | Tyrell Corp | Wayne Enterprises | Stark Industries |
| ---------------- | :---------: | :---------------: | :--------------: |
| Facebook Ads     |     ✅      |         —         |        ✅        |
| Instagram Ads    |     ✅      |         —         |        ✅        |
| Google Ads       |     ✅      |        ✅         |        —         |
| Bing Ads         |      —      |        ✅         |        —         |
| LinkedIn Ads     |      —      |         —         |        —         |
| Amazon Ads       |      —      |         —         |        —         |
| TikTok Ads       |      —      |         —         |        —         |
| Shopify          |     ✅      |         —         |        —         |
| BigCommerce      |      —      |        ✅         |        —         |
| WooCommerce      |      —      |         —         |        ✅        |
| Stripe           |      —      |         —         |        —         |
| Google Analytics |     ✅      |        ✅         |        —         |
| Mixpanel         |      —      |         —         |        ✅        |
| Amplitude        |      —      |         —         |        —         |

**Note:** Stripe source/staging was previously removed from Tyrell Corp.
LinkedIn Ads, Amazon Ads, TikTok Ads, and Amplitude have master model sinks and
engines but no tenant currently uses them.

---

## Summary of Actions

| ID | Severity | Effort | Description                                                                   |
| -- | -------- | ------ | ----------------------------------------------------------------------------- |
| C1 | Critical | 1 min  | Replace emoji in sync_to_master_hub.sql                                       |
| C2 | Critical | 1 min  | Fix Shopify engine customer_id NULL                                           |
| C3 | Critical | 15 min | Add conversion_events for wayne/stark to tenants.yaml + shell models          |
| M1 | Moderate | 10 min | Archive or delete 10 orphaned macros                                          |
| M2 | Moderate | 2 min  | Deprecate platform_ops__schema_hash_registry                                  |
| M3 | Moderate | 30 min | Document Bing Ads account-level limitation or fix mock data                   |
| M4 | Moderate | 15 min | Add customer_email + line_items to BigCommerce intermediate/engine            |
| M5 | Moderate | 30 min | Migrate Tyrell Corp intermediate models to use generate_intermediate_unpacker |
| M6 | Moderate | 20 min | Complete tenants.yaml table/logic definitions for all tenants                 |
| M7 | Moderate | 1-2 hr | Refactor factories to read source lists from tenants.yaml                     |
| L1 | Cleanup  | 15 min | Add models block to dbt_project.yml                                           |
| L2 | Cleanup  | 5 min  | Add revenue/transaction fields to Mixpanel intermediate                       |
| L3 | Cleanup  | 15 min | Verify or disable on-run-start/end artifact macros                            |

**Total estimated effort:** ~3-4 hours for all items, with C1-C3 being the most
urgent.
