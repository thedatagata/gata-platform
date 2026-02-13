# Mock Data Engine

Pydantic-validated synthetic data generators for 13 connectors across 3 domains. Produces realistic records that dlt lands into DuckDB/MotherDuck.

## How It Works

`MockOrchestrator` (in `orchestrator.py`) reads a tenant's config, discovers enabled sources, runs the appropriate generators, validates output against Pydantic schemas, and lands data via dlt pipelines.

```bash
# Called by scripts/onboard_tenant.py — not typically run directly
uv run python scripts/onboard_tenant.py <tenant_slug> --target sandbox --days 30
```

## Supported Connectors (13)

### Paid Advertising (7)

| Connector | API Version | Objects |
|-----------|-------------|---------|
| Facebook Ads | v19 | ads, ad_sets, campaigns, facebook_insights |
| Instagram Ads | v19 | ads, ad_sets, campaigns, facebook_insights |
| Google Ads | v16 | ads, ad_groups, ad_performance, campaigns, customers |
| Bing Ads | v13 | campaigns, ad_groups, ads, account_performance_report |
| LinkedIn Ads | v202401 | campaigns, ad_analytics, ad_analytics_by_campaign |
| Amazon Ads | v3 | sponsored_products campaigns/ad_groups/product_ads |
| TikTok Ads | v1.3 | campaigns, ad_groups, ads, ads_reports_daily |

### Ecommerce (3)

| Connector | API Version | Objects |
|-----------|-------------|---------|
| Shopify | v1 | orders, products |
| BigCommerce | v3 | orders, products |
| WooCommerce | v3 | orders, products |

### Analytics (3)

| Connector | API Version | Objects |
|-----------|-------------|---------|
| Google Analytics | GA4 v1 | events |
| Amplitude | v2 | events, users |
| Mixpanel | v2 | events, people |

Facebook Ads and Instagram Ads share the same Meta API and `master_model_id` (`facebook_ads_api_v1`). They are differentiated downstream by `source_platform`.

## Key Files

| File | Purpose |
|------|---------|
| `orchestrator.py` | MockOrchestrator — routes config to generators, lands via dlt |
| `sources/{domain}/{platform}/` | Per-connector mock generators (13 total) |
