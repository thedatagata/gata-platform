# Star Schema Expansion + BSL Tenant Configs

## Context

The current star schema has 4 tables per tenant (fct_ad_performance, fct_orders,
fct_sessions, dim_campaigns) but is missing critical models for full-funnel
analytics: users dimension, events fact (for funnel analysis), products
dimension, and identity resolution. The mock data engine already generates
realistic funnel data with anonymous users, cookie IDs, conversion
probabilities, and return visit patterns — but the dbt transformation layer only
consumes a fraction of it.

Additionally, `platform_ops__boring_semantic_layer.sql` is a misnomer — it's an
INFORMATION_SCHEMA catalog, not a BSL config. Real BSL YAML configs need to be
created per tenant so the API service can expose a semantic layer for natural
language queries.

**Goal:** Expand the star schema to:
`dim_users -> fct_sessions -> fct_events -> fct_orders` with supporting
`dim_products`, `dim_campaigns`, `fct_ad_performance`, and identity resolution.
Then create BSL YAML configs per tenant.

---

## Target Star Schema (per tenant)

```
dim_users ──(user_pseudo_id)──> fct_sessions ──(session_id)──> fct_events
    |                                |
    └──(customer_id)─────────────────┼──> fct_orders
                                     |
dim_products ──(product_id)──> fct_orders.line_items
dim_campaigns ──(campaign_id)──> fct_ad_performance
```

**New tables:** dim_users, fct_events, dim_products, int_identity_resolution
**Existing (unchanged):** fct_ad_performance, fct_orders, fct_sessions,
dim_campaigns

---

## Phase 1: New Intermediate Models (4 files)

### Products unpackers (use `generate_intermediate_unpacker` macro)

| File                                                                                    | Master Model                  | Fields                                                                                                                                          |
| --------------------------------------------------------------------------------------- | ----------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `models/intermediate/tyrell_corp/int_tyrell_corp__shopify_products.sql`                 | `shopify_api_v1_products`     | product_id (BIGINT), product_title, product_price (DOUBLE), created_at (TIMESTAMP)                                                              |
| `models/intermediate/wayne_enterprises/int_wayne_enterprises__bigcommerce_products.sql` | `bigcommerce_api_v1_products` | product_id (BIGINT), product_title (from `name`), product_price (DOUBLE), created_at (TIMESTAMP, will be NULL — BigCommerce generator omits it) |
| `models/intermediate/stark_industries/int_stark_industries__woocommerce_products.sql`   | `woocommerce_api_v1_products` | product_id (BIGINT), product_title (from `name`), product_price (DOUBLE), created_at (TIMESTAMP)                                                |

### Mixpanel People unpacker (needed for Stark Industries identity resolution)

| File                                                                             | Master Model             | Fields                   |
| -------------------------------------------------------------------------------- | ------------------------ | ------------------------ |
| `models/intermediate/stark_industries/int_stark_industries__mixpanel_people.sql` | `mixpanel_api_v1_people` | distinct_id, city, email |

---

## Phase 2: Identity Resolution Intermediate Models (3 files)

Cross-platform join models — new pattern (raw SQL, not unpacker macro).

### Resolution strategies by tenant

| Tenant                                    | Method                 | Join Path                                                                                                              |
| ----------------------------------------- | ---------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| tyrell_corp (GA4 + Shopify)               | `transaction_id` match | GA4 purchase events `transaction_id` = Shopify `CAST(order_id AS VARCHAR)`                                             |
| wayne_enterprises (GA4 + BigCommerce)     | `transaction_id` match | GA4 purchase events `transaction_id` = BigCommerce `CAST(order_id AS VARCHAR)` (no email — BigCommerce orders lack it) |
| stark_industries (Mixpanel + WooCommerce) | `email` match          | Mixpanel people `email` = WooCommerce orders `customer_email` (from `billing_email`)                                   |

### Output schema (all three)

```sql
tenant_slug, user_pseudo_id, customer_id, customer_email, resolution_method, first_resolved_at
```

### Files

- `models/intermediate/tyrell_corp/int_tyrell_corp__identity_resolution.sql`
- `models/intermediate/wayne_enterprises/int_wayne_enterprises__identity_resolution.sql`
- `models/intermediate/stark_industries/int_stark_industries__identity_resolution.sql`

### Key SQL pattern (GA4 tenants)

```sql
WITH ga4_purchases AS (
    SELECT user_pseudo_id, transaction_id, MIN(event_timestamp) AS first_resolved_at
    FROM {{ ref('int_{tenant}__google_analytics_events') }}
    WHERE event_name = 'purchase' AND transaction_id IS NOT NULL AND transaction_id != 'N/A'
    GROUP BY user_pseudo_id, transaction_id
),
ecom_orders AS (
    SELECT CAST(order_id AS VARCHAR) AS order_id_str, customer_id, email AS customer_email
    FROM {{ ref('int_{tenant}__{ecom}_orders') }}
)
SELECT DISTINCT ON (gp.user_pseudo_id)
    '{tenant}' AS tenant_slug, gp.user_pseudo_id, eo.customer_id, eo.customer_email,
    'ga4_transaction_id_match' AS resolution_method, gp.first_resolved_at
FROM ga4_purchases gp JOIN ecom_orders eo ON gp.transaction_id = eo.order_id_str
```

### Key SQL pattern (Mixpanel tenant - Stark Industries)

```sql
WITH mp_people AS (
    SELECT distinct_id AS user_pseudo_id, email
    FROM {{ ref('int_stark_industries__mixpanel_people') }}
    WHERE email IS NOT NULL AND email != ''
),
woo_customers AS (
    SELECT customer_email, customer_id, MIN(order_created_at) AS first_order_at
    FROM {{ ref('int_stark_industries__woocommerce_orders') }}
    WHERE customer_email IS NOT NULL GROUP BY customer_email, customer_id
)
SELECT 'stark_industries' AS tenant_slug, mp.user_pseudo_id, CAST(wc.customer_id AS VARCHAR),
    mp.email AS customer_email, 'mixpanel_email_match' AS resolution_method, ...
FROM mp_people mp JOIN woo_customers wc ON mp.email = wc.customer_email
```

---

## Phase 3: New Engine Macros (5 files)

### Event engines (reuse sessionization from existing session engines)

| File                                                          | Pattern                                                                                                                                                                      |
| ------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `macros/engines/analytics/engine_google_analytics_events.sql` | Same 30-min window (1,800,000,000 microseconds) as `engine_google_analytics.sql`. Output per-event row with `session_id` (= `user_pseudo_id                                  |
| `macros/engines/analytics/engine_mixpanel_events.sql`         | Same 30-min window (1,800,000 milliseconds) as `engine_mixpanel.sql`. Same output columns. `purchase_revenue` = 0, `transaction_id` = NULL (Mixpanel lacks ecommerce fields) |

### Product engines (simple pass-through from intermediate)

| File                                                       | Reads                                | Columns                                                                            |
| ---------------------------------------------------------- | ------------------------------------ | ---------------------------------------------------------------------------------- |
| `macros/engines/ecommerce/engine_shopify_products.sql`     | `int_{tenant}__shopify_products`     | tenant_slug, source_platform, product_id, product_title, product_price, created_at |
| `macros/engines/ecommerce/engine_bigcommerce_products.sql` | `int_{tenant}__bigcommerce_products` | same                                                                               |
| `macros/engines/ecommerce/engine_woocommerce_products.sql` | `int_{tenant}__woocommerce_products` | same                                                                               |

---

## Phase 4: New Factory Macros (3 files)

| File                                      | Pattern                                                                                                                        | Params                                               |
| ----------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------- |
| `macros/factories/build_fct_events.sql`   | Routes to event engine by analytics_source (like `build_fct_sessions`)                                                         | `(tenant_slug, analytics_source)`                    |
| `macros/factories/build_dim_products.sql` | UNION ALL product engines (like `build_fct_orders`)                                                                            | `(tenant_slug, ecommerce_sources)`                   |
| `macros/factories/build_dim_users.sql`    | Reads `fct_{tenant}__events` for user_base, LEFT JOINs `int_{tenant}__identity_resolution` + `fct_{tenant}__orders` aggregates | `(tenant_slug, analytics_source, ecommerce_sources)` |

### `build_dim_users` output columns

```
tenant_slug, source_platform, user_pseudo_id (PK), customer_id (nullable),
customer_email (nullable), is_identified (boolean), first_seen_at, last_seen_at,
device_category, geo_country, total_sessions, total_events, total_orders, lifetime_value
```

**Note:** `build_dim_users` refs `fct_{tenant}__events` and
`fct_{tenant}__orders` — dbt DAG resolves ordering automatically.

---

## Phase 5: Analytics Shell Models (9 files)

All follow the existing 2-line pattern: `{{ config(materialized='table') }}` +
factory call.

| Model                                                                    | Factory Call                                                                |
| ------------------------------------------------------------------------ | --------------------------------------------------------------------------- |
| `models/analytics/tyrell_corp/fct_tyrell_corp__events.sql`               | `build_fct_events('tyrell_corp', 'google_analytics')`                       |
| `models/analytics/wayne_enterprises/fct_wayne_enterprises__events.sql`   | `build_fct_events('wayne_enterprises', 'google_analytics')`                 |
| `models/analytics/stark_industries/fct_stark_industries__events.sql`     | `build_fct_events('stark_industries', 'mixpanel')`                          |
| `models/analytics/tyrell_corp/dim_tyrell_corp__products.sql`             | `build_dim_products('tyrell_corp', ['shopify'])`                            |
| `models/analytics/wayne_enterprises/dim_wayne_enterprises__products.sql` | `build_dim_products('wayne_enterprises', ['bigcommerce'])`                  |
| `models/analytics/stark_industries/dim_stark_industries__products.sql`   | `build_dim_products('stark_industries', ['woocommerce'])`                   |
| `models/analytics/tyrell_corp/dim_tyrell_corp__users.sql`                | `build_dim_users('tyrell_corp', 'google_analytics', ['shopify'])`           |
| `models/analytics/wayne_enterprises/dim_wayne_enterprises__users.sql`    | `build_dim_users('wayne_enterprises', 'google_analytics', ['bigcommerce'])` |
| `models/analytics/stark_industries/dim_stark_industries__users.sql`      | `build_dim_users('stark_industries', 'mixpanel', ['woocommerce'])`          |

---

## Phase 6: Rename Boring Semantic Layer (1 rename + CLAUDE.md update)

- Rename `platform_ops__boring_semantic_layer.sql` ->
  `platform_ops__star_schema_catalog.sql`
- SQL content unchanged (INFORMATION_SCHEMA catalog auto-discovers new fct/dim
  tables)
- No downstream refs exist (leaf node) — safe rename
- Update CLAUDE.md references

---

## Phase 7: BSL YAML Configs (4 files)

### Directory: `semantic_layer/` (project root)

```
semantic_layer/
    profiles.yml
    tyrell_corp/semantic_model.yaml
    wayne_enterprises/semantic_model.yaml
    stark_industries/semantic_model.yaml
```

### `profiles.yml` — dual target support

```yaml
sandbox:
    type: duckdb
    path: "warehouse/sandbox.duckdb"
dev:
    type: duckdb
    path: "md:my_db"
```

### Per-tenant `semantic_model.yaml` structure

Each defines all 7 star schema tables with BSL dimensions (groupable attributes)
and measures (aggregations):

- **fct_ad_performance**: dims = report_date, source_platform, campaign_id,
  ad_group_id, ad_id; measures = total_spend, total_impressions, total_clicks,
  total_conversions, cpc, ctr
- **fct_orders**: dims = order_date, currency, financial_status, customer_email,
  customer_id; measures = total_revenue, order_count, avg_order_value
- **fct_sessions**: dims = user_pseudo_id, traffic_source/medium/campaign,
  geo_country, device_category, is_conversion_session; measures = session_count,
  avg_duration, total_revenue, conversion_rate
- **fct_events**: dims = event_name, funnel_step_index, session_id,
  user_pseudo_id, traffic_source/medium/campaign; measures = event_count,
  unique_sessions, unique_users, total_revenue
- **dim_campaigns**: dims = campaign_id, campaign_name, campaign_status,
  source_platform; measures = campaign_count
- **dim_products**: dims = product_id, product_title, product_price; measures =
  product_count, avg_price
- **dim_users**: dims = user_pseudo_id, customer_id, is_identified,
  device_category, geo_country; measures = user_count, identified_users,
  avg_sessions, avg_orders, total_ltv

---

## File Inventory

### New Files (28)

| #     | Path (relative to `warehouse/gata_transformation/`)                                     | Type         |
| ----- | --------------------------------------------------------------------------------------- | ------------ |
| 1     | `models/intermediate/tyrell_corp/int_tyrell_corp__shopify_products.sql`                 | Intermediate |
| 2     | `models/intermediate/wayne_enterprises/int_wayne_enterprises__bigcommerce_products.sql` | Intermediate |
| 3     | `models/intermediate/stark_industries/int_stark_industries__woocommerce_products.sql`   | Intermediate |
| 4     | `models/intermediate/stark_industries/int_stark_industries__mixpanel_people.sql`        | Intermediate |
| 5     | `models/intermediate/tyrell_corp/int_tyrell_corp__identity_resolution.sql`              | Intermediate |
| 6     | `models/intermediate/wayne_enterprises/int_wayne_enterprises__identity_resolution.sql`  | Intermediate |
| 7     | `models/intermediate/stark_industries/int_stark_industries__identity_resolution.sql`    | Intermediate |
| 8     | `macros/engines/analytics/engine_google_analytics_events.sql`                           | Engine       |
| 9     | `macros/engines/analytics/engine_mixpanel_events.sql`                                   | Engine       |
| 10    | `macros/engines/ecommerce/engine_shopify_products.sql`                                  | Engine       |
| 11    | `macros/engines/ecommerce/engine_bigcommerce_products.sql`                              | Engine       |
| 12    | `macros/engines/ecommerce/engine_woocommerce_products.sql`                              | Engine       |
| 13    | `macros/factories/build_fct_events.sql`                                                 | Factory      |
| 14    | `macros/factories/build_dim_products.sql`                                               | Factory      |
| 15    | `macros/factories/build_dim_users.sql`                                                  | Factory      |
| 16    | `models/analytics/tyrell_corp/fct_tyrell_corp__events.sql`                              | Shell        |
| 17    | `models/analytics/wayne_enterprises/fct_wayne_enterprises__events.sql`                  | Shell        |
| 18    | `models/analytics/stark_industries/fct_stark_industries__events.sql`                    | Shell        |
| 19    | `models/analytics/tyrell_corp/dim_tyrell_corp__products.sql`                            | Shell        |
| 20    | `models/analytics/wayne_enterprises/dim_wayne_enterprises__products.sql`                | Shell        |
| 21    | `models/analytics/stark_industries/dim_stark_industries__products.sql`                  | Shell        |
| 22    | `models/analytics/tyrell_corp/dim_tyrell_corp__users.sql`                               | Shell        |
| 23    | `models/analytics/wayne_enterprises/dim_wayne_enterprises__users.sql`                   | Shell        |
| 24    | `models/analytics/stark_industries/dim_stark_industries__users.sql`                     | Shell        |
| 25-28 | `semantic_layer/profiles.yml` + 3 tenant `semantic_model.yaml` files                    | BSL Config   |

### Modified Files (2)

- `platform_ops__boring_semantic_layer.sql` -> renamed to
  `platform_ops__star_schema_catalog.sql`
- `CLAUDE.md` — update model documentation, add new tables

---

## Verification

### After Phases 1-5 (full dbt build)

```bash
cd warehouse/gata_transformation
uv run --env-file ../../.env dbt run --target sandbox
uv run --env-file ../../.env dbt run --target sandbox --select "models/intermediate models/analytics models/platform/ops"
```

**Expected:** ~140 models pass (was 124). New: 4 product/people intermediates +
3 identity resolution + 9 analytics shells = +16.

### Spot checks

```sql
-- Events have funnel data
SELECT event_name, funnel_step_index, COUNT(*) FROM fct_tyrell_corp__events GROUP BY 1,2 ORDER BY 2;
-- Products exist
SELECT COUNT(*) FROM dim_tyrell_corp__products;  -- should be 50-100
-- Users with resolution
SELECT is_identified, COUNT(*) FROM dim_tyrell_corp__users GROUP BY 1;  -- should see TRUE and FALSE
-- Identity resolution rows
SELECT COUNT(*) FROM int_tyrell_corp__identity_resolution;  -- should match order customer count
-- Session IDs match between events and sessions
SELECT COUNT(DISTINCT session_id) FROM fct_tyrell_corp__events;  -- should ~ match session count
```

### After Phase 7 (BSL)

```python
uv run python -c "
from boring_semantic_layer import from_yaml
models = from_yaml('semantic_layer/tyrell_corp/semantic_model.yaml')
print(list(models.keys()))
users = models['dim_users']
print(users.group_by('is_identified').aggregate('user_count').to_pandas())
"
```
