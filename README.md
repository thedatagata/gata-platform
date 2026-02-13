# GATA Platform

Multi-tenant analytics platform with automated onboarding, source-agnostic star schemas, and a config-free semantic layer.

**Stack:** dlt / dbt / DuckDB / MotherDuck / Deno Fresh / FastAPI / Boring Semantic Layer / WebLLM / Ollama

## What It Does

1. **Automated onboarding** — a new tenant selects data sources and the platform scaffolds ingestion, staging, and reporting models without manual SQL
2. **Source-agnostic star schemas** — 13 source-specific engines normalize to 6 canonical star schema tables per tenant, regardless of source mix
3. **Raw data preservation** — every record carries its original JSON payload, so reporting logic can be rebuilt from raw data without re-ingesting
4. **Config-free semantic layer** — the BSL reads the star schema catalog at runtime, classifies columns, infers aggregations and calculated measures (CTR, CPC, AOV), and discovers joins — all automatically
5. **Natural language analytics** — hybrid two-tier query engine using in-browser AI (WebLLM) for single-table EDA and a backend LLM agent (Ollama) for multi-table OLAP

## Architecture

```
Ingestion (dlt) → Transformation (dbt, 5 layers) → Semantic Layer (BSL) → API + Frontend
```

Data flows through source shims, staging pushers (MERGE into master models), tenant-isolated intermediate extraction, and engine/factory-assembled star schemas. The BSL auto-populates from the star schema with zero config. See the [dbt project README](warehouse/gata_transformation/README.md) for pipeline details.

## Services

| Service | Description | README |
|---------|-------------|--------|
| **dbt Pipeline** | 136-model transformation layer (5 layers, engine/factory pattern) | [warehouse/gata_transformation/](warehouse/gata_transformation/README.md) |
| **Platform API** | FastAPI backend — BSL metadata, query execution, NL analytics, observability | [services/platform-api/](services/platform-api/README.md) |
| **Mock Data Engine** | Pydantic-validated synthetic data generators for 13 connectors | [services/mock-data-engine/](services/mock-data-engine/README.md) |
| **Frontend** | Deno Fresh app — onboarding, dashboards, in-browser semantic layer | [app/](app/README.md) |
| **Scripts** | Tenant onboarding and connector library initialization | [scripts/](scripts/README.md) |

## Quick Start (Sandbox — No External Dependencies)

```bash
# Install Python dependencies
uv sync --all-groups

# Initialize connector library (registers schema fingerprints)
uv run python scripts/initialize_connector_library.py sandbox

# Onboard tenants
uv run python scripts/onboard_tenant.py tyrell_corp --target sandbox --days 30
uv run python scripts/onboard_tenant.py wayne_enterprises --target sandbox --days 30
uv run python scripts/onboard_tenant.py stark_industries --target sandbox --days 30

# Run dbt pipeline
cd warehouse/gata_transformation
uv run --env-file ../../.env dbt run --target sandbox
uv run --env-file ../../.env dbt run --target sandbox --selector reporting_refresh
```

## Running Services

```bash
# API (from services/platform-api/)
GATA_ENV=local uv run uvicorn main:app --port 8001

# Frontend (from app/)
deno task start
```

## Demo Tenants

| Tenant | Paid Ads | Ecommerce | Analytics |
|--------|----------|-----------|-----------|
| Tyrell Corporation | Facebook Ads, Google Ads, Instagram Ads | Shopify | Google Analytics |
| Wayne Enterprises | Bing Ads, Google Ads | BigCommerce | Google Analytics |
| Stark Industries | Facebook Ads, Instagram Ads | WooCommerce | Mixpanel |

## Key Config Files

| File | Purpose |
|------|---------|
| `tenants.yaml` | Demo tenant configs (source mix, generation params, business logic) |
| `supported_connectors.yaml` | Connector catalog (13 sources across 3 domains) |
| `pyproject.toml` | Python dependencies (managed by uv) |
| `CLAUDE.md` | Detailed developer context for AI-assisted development |

## Prerequisites

- Python 3.11+ with [uv](https://github.com/astral-sh/uv)
- Deno 1.40+ (frontend)
- MotherDuck account (optional — only for `dev` target)
- Ollama (optional — for natural language queries)
