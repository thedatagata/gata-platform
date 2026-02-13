# Platform API

FastAPI backend serving the Boring Semantic Layer (BSL), structured query execution, natural language analytics, and pipeline observability.

## Running

```bash
cd services/platform-api

# Sandbox mode (local DuckDB)
GATA_ENV=local uv run uvicorn main:app --port 8001

# MotherDuck mode (requires MOTHERDUCK_TOKEN in .env)
uv run --env-file ../../.env uvicorn main:app --port 8001
```

## BSL Auto-Inference

The BSL requires **zero manual configuration**. On first request for a tenant:

1. `platform_ops__bsl_column_catalog` (dbt model) classifies every star schema column as dimension or measure using deterministic SQL rules
2. `bsl_model_builder.py` reads the catalog and auto-infers:
   - Dimensions with types (string, date, timestamp_epoch, boolean)
   - Measures with aggregations (sum, avg, count_distinct)
   - Calculated measures: CTR, CPC, CPM, AOV, conversion_rate
   - Joins from matching column names across fact/dim tables
3. Tenant is immediately queryable — no YAML config needed

Optional YAML overrides can be placed in `semantic_configs/{tenant_slug}.yaml` for custom descriptions, labels, or non-standard aggregations.

## Endpoints

### Semantic Layer

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/semantic-layer/{tenant}/models` | List models with dimension/measure counts |
| `GET` | `/semantic-layer/{tenant}/models/{name}` | Full model detail (dims, measures, joins) |
| `GET` | `/semantic-layer/{tenant}/catalog` | Complete catalog for frontend consumption |
| `POST` | `/semantic-layer/{tenant}/query` | Structured query -> SQL -> results |
| `POST` | `/semantic-layer/{tenant}/ask` | Natural language query via LLM agent |
| `GET` | `/semantic-layer/{tenant}/config` | YAML enrichment config (if exists) |

### Observability

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/observability/{tenant}/summary` | Pipeline health: pass/fail/error counts |
| `GET` | `/observability/{tenant}/runs` | Per-model run results with timing |
| `GET` | `/observability/{tenant}/tests` | Test results with messages |
| `GET` | `/observability/{tenant}/identity-resolution` | Identity resolution stats |

### LLM

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/semantic-layer/llm-status` | Ollama availability check |
| `POST` | `/semantic-layer/llm-refresh` | Force re-check Ollama |

## Natural Language Queries

`POST /ask` routes questions through an LLM agent loop:

- **With Ollama** (Qwen 2.5 Coder 7B): Agent receives model catalog, calls BSL Tools (`list_models`, `get_model`, `query_model`) to explore schema and execute Ibis queries. Returns records + ECharts chart specs.
- **Without Ollama**: Keyword fallback matches question to the most relevant model and executes a basic group-by query.

## Query Builder

`query_builder.py` compiles `SemanticQueryRequest` (dimensions + measures + filters + joins) into parameterized SQL. For tenants without YAML configs, the query builder config is auto-generated from BSL metadata at request time.

## Tests

```bash
uv run python -m pytest test_bsl_query.py test_bsl_agent.py -v
# 67 tests (38 auto-inference + 29 agent/provider)
```

## Key Files

| File | Purpose |
|------|---------|
| `main.py` | API endpoints and request handling |
| `bsl_model_builder.py` | Auto-inference engine (catalog -> BSL models) |
| `bsl_agent.py` | LLM agent loop (Ollama + BSL Tools) |
| `query_builder.py` | Structured query -> SQL compiler |
| `llm_provider.py` | Ollama/Anthropic provider with fallback |
| `models.py` | Pydantic request/response models |
| `semantic_configs/` | Per-tenant YAML overrides (optional) |
