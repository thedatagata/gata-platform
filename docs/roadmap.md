# Backend API Service — Claude Code Execution Plan

## Objective

Get `services/platform-api/` production-ready with a governed query builder,
model discovery endpoints, and tenant-scoped observability before shifting to
Deno Fresh frontend integration.

## Current State Assessment

### What Exists

| Asset                | Location                                               | Status                                                                                                            |
| -------------------- | ------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------- |
| FastAPI app          | `services/platform-api/main.py`                        | 3 endpoints: GET `/semantic-layer/{tenant}`, GET `/semantic-layer/{tenant}/config`, POST `/semantic-layer/update` |
| Semantic configs     | `services/platform-api/semantic_configs/{tenant}.yaml` | Complete for all 3 tenants — 6 models each with dimensions, measures, calculated_measures, joins                  |
| pyproject.toml       | `services/platform-api/pyproject.toml`                 | fastapi, uvicorn, duckdb, pyyaml                                                                                  |
| Tests                | `services/platform-api/test_main.py`                   | 1 test (update_logic_integrity)                                                                                   |
| Rill dashboards      | `services/rill-dashboard/`                             | Stale — raw source tables, not star schema. Cannot run on Windows                                                 |
| BSL mapper           | `services/mock-data-engine/utils/bsl_mapper.py`        | Contains `generate_rill_yaml()` coupled to `generate_boring_manifest()`                                           |
| Observability models | `warehouse/.../platform/observability/`                | 8 intermediate models — all global except `identity_resolution_stats`                                             |
| DuckDB connection    | main.py                                                | MotherDuck via token or local sandbox — already working                                                           |

### What's Missing

- Query builder that compiles structured semantic queries → SQL
- Model discovery endpoints (GET /models, GET /models/{name})
- Query execution endpoint (POST /query)
- Tenant-scoped observability in run_results and test_results
- Observability API endpoints
- Rill removal + bsl_mapper cleanup
- Pydantic models for request/response validation
- CORS config for Deno Fresh frontend

---

## Phase 1: Remove Rill + Clean BSL Mapper

### Prompt 1.1 — Delete Rill & Clean bsl_mapper

```
Delete the entire `services/rill-dashboard/` directory.

In `services/mock-data-engine/utils/bsl_mapper.py`:
1. Remove the `generate_rill_yaml()` function entirely
2. In `generate_boring_manifest()`, remove the line that calls `generate_rill_yaml()`
3. Remove the `import os` if it's only used by generate_rill_yaml (check first)
4. Keep everything else in generate_boring_manifest unchanged

Verify: Run `python -c "from utils.bsl_mapper import generate_boring_manifest; print('OK')"` from `services/mock-data-engine/` to confirm the import still works.
```

**Expected changes:**

- DELETE: `services/rill-dashboard/` (entire directory)
- MODIFY: `services/mock-data-engine/utils/bsl_mapper.py` — remove
  `generate_rill_yaml()` + its call

---

## Phase 2: Query Builder + API Endpoints

### Prompt 2.1 — Pydantic Models

```
Create `services/platform-api/models.py` with Pydantic models for the semantic query API.

Use these exact schemas:

1. `SemanticQueryRequest` — the POST body for /query:
   - model: str (required — e.g. "fct_tyrell_corp__ad_performance")
   - dimensions: list[str] (default [])
   - measures: list[str] (default [])
   - calculated_measures: list[str] (default [])
   - filters: list[QueryFilter] (default [])
   - joins: list[str] (default [] — model names to join)
   - order_by: list[OrderByClause] (default [])
   - limit: int | None (default 1000, max 10000)

2. `QueryFilter`:
   - field: str
   - op: str — must be one of: =, !=, >, <, >=, <=, IN, LIKE, BETWEEN, IS NULL, IS NOT NULL
   - value: str | int | float | list | None (None for IS NULL/IS NOT NULL)

3. `OrderByClause`:
   - field: str
   - dir: Literal["asc", "desc"] (default "asc")

4. `SemanticQueryResponse`:
   - sql: str
   - data: list[dict]
   - columns: list[ColumnInfo]
   - row_count: int

5. `ColumnInfo`:
   - name: str
   - type: str

6. `ModelSummary` — for GET /models listing:
   - name: str
   - label: str
   - description: str
   - dimension_count: int
   - measure_count: int
   - has_joins: bool

7. `ModelDetail` — for GET /models/{name}:
   - name: str
   - label: str
   - description: str
   - dimensions: list[dict]
   - measures: list[dict]
   - calculated_measures: list[dict]
   - joins: list[dict]

Add `from pydantic import BaseModel, field_validator` at the top. Add a validator on QueryFilter.op that rejects operators not in the allowlist.
```

### Prompt 2.2 — Query Builder

```
Create `services/platform-api/query_builder.py`. This module compiles a SemanticQueryRequest into parameterized SQL using a tenant's BSL YAML config.

Read the BSL config format from `services/platform-api/semantic_configs/tyrell_corp.yaml` to understand the structure. Each model has: dimensions (list of {name, type}), measures (list of {name, type, agg}), calculated_measures (list of {name, label, sql, format}), joins (list of {to, type, on}).

Implement class `QueryBuilder`:

__init__(self, config: dict):
  - Takes the parsed YAML config dict (the full tenant config with 'models' list)
  - Builds a lookup: model_name → model_config

build_query(self, tenant_slug: str, request: SemanticQueryRequest) -> tuple[str, list]:
  Returns (sql_string, parameters_list).

  Logic:
  1. Find the model in config — raise ValueError if not found
  2. Validate all requested dimensions exist in model config — raise ValueError for unknowns
  3. Validate all requested measures exist in model config — raise ValueError for unknowns
  4. Validate all requested calculated_measures exist — raise ValueError for unknowns
  5. Validate all requested joins exist in model's joins list (match by 'to' field) — raise ValueError for unknowns

  6. Build SELECT clause:
     - Dimensions: just the column name (e.g., "source_platform")
     - Measures: wrap in agg function from config (e.g., SUM(spend), COUNT(DISTINCT order_id) for agg="count_distinct", AVG(session_duration_seconds) for agg="avg")
     - Calculated measures: use the raw SQL expression from config (e.g., the ctr CASE expression)

  7. Build FROM clause: just the model name as a table reference (e.g., "fct_tyrell_corp__ad_performance")

  8. Build JOIN clauses: for each requested join, look up the join config on the model:
     - Get join type ("left" → "LEFT JOIN")
     - Get the target table name from "to"
     - Build ON clause from the "on" dict: {campaign_id: campaign_id, source_platform: source_platform} → "base.campaign_id = dim_tyrell_corp__campaigns.campaign_id AND base.source_platform = dim_tyrell_corp__campaigns.source_platform"
     - Use "base" as alias for the FROM table

  9. Build WHERE clause:
     - ALWAYS include: tenant_slug = ? (parameterized) — this is the tenant isolation guard
     - For each filter: build "field op ?" with the value as a parameter
     - Special cases: IS NULL / IS NOT NULL have no value parameter; IN uses "field IN (?, ?, ...)" with each list item as a parameter; BETWEEN uses "field BETWEEN ? AND ?" expecting value to be a 2-element list

  10. Build GROUP BY: positional references (1, 2, 3...) for each dimension if any measures are present

  11. Build ORDER BY from request

  12. Build LIMIT from request

  Return the full SQL string and the flat parameters list.

  IMPORTANT: Use "base" alias on the FROM table when joins are present. When no joins, no alias needed.
  IMPORTANT: Prefix dimension/measure column references with "base." when joins are present to avoid ambiguity.

Also implement:
  get_model_summary(self, model_name: str) -> dict
  list_models(self) -> list[dict]

These return the model metadata from config for the API discovery endpoints.
```

### Prompt 2.3 — Wire Up API Endpoints

````
Modify `services/platform-api/main.py` to add the query builder endpoints and CORS support.

Changes:

1. Add imports at top:
   - `from fastapi.middleware.cors import CORSMiddleware`
   - `from models import SemanticQueryRequest, SemanticQueryResponse, ColumnInfo, ModelSummary, ModelDetail`
   - `from query_builder import QueryBuilder`
   - `from typing import Optional`

2. Add CORS middleware after `app = FastAPI()`:
   ```python
   app.add_middleware(
       CORSMiddleware,
       allow_origins=["http://localhost:8000", "http://localhost:3000", "https://*.deno.dev"],
       allow_credentials=True,
       allow_methods=["*"],
       allow_headers=["*"],
   )
````

3. Add helper function `_get_db_connection()` that centralizes the DuckDB
   connection logic already in get_semantic_layer (MotherDuck token check,
   GATA_ENV local fallback). Return a duckdb.Connection.

4. Add helper function `_get_query_builder(tenant_slug: str) -> QueryBuilder`
   that loads the semantic config YAML and returns a QueryBuilder instance.
   Raise HTTPException 404 if config doesn't exist.

5. Add endpoints:

   GET /semantic-layer/{tenant_slug}/models
   - Load config, return list of ModelSummary objects

   GET /semantic-layer/{tenant_slug}/models/{model_name}
   - Load config, return ModelDetail for the specific model
   - 404 if model not found

   POST /semantic-layer/{tenant_slug}/query
   - Body: SemanticQueryRequest
   - Build SQL via QueryBuilder
   - Execute against DuckDB with parameters
   - Return SemanticQueryResponse with sql, data, columns, row_count
   - Wrap in try/except: ValueError → 400, duckdb errors → 500

6. Refactor existing get_semantic_layer() to use _get_db_connection()

7. Add `pydantic` to dependencies in `pyproject.toml` (it comes with fastapi but
   be explicit)

Keep existing endpoints unchanged. The POST /semantic-layer/update endpoint
stays as-is.

```
### Prompt 2.4 — Query Builder Tests
```

Create `services/platform-api/test_query_builder.py` with unit tests for the
query builder.

Load the actual tyrell_corp.yaml config for realistic testing.

Tests:

1. test_simple_dimensions_only — request dimensions=[source_platform,
   report_date] from fct_tyrell_corp__ad_performance with no measures. SQL
   should SELECT those columns with no GROUP BY.

2. test_dimensions_with_measures — request dimensions=[source_platform],
   measures=[spend, clicks] from ad_performance. SQL should have SUM(spend),
   SUM(clicks) and GROUP BY 1.

3. test_calculated_measures — request dimensions=[source_platform],
   measures=[spend, impressions, clicks], calculated_measures=[ctr]. SQL should
   include the CASE WHEN expression from config.

4. test_count_distinct_agg — request measures=[order_id] from
   fct_tyrell_corp__orders. Should generate COUNT(DISTINCT order_id).

5. test_join — request from ad_performance with
   joins=[dim_tyrell_corp__campaigns]. Should generate LEFT JOIN with ON clause
   matching campaign_id AND source_platform.

6. test_filters — request with filters=[{field: report_date, op: >=, value:
   "2025-01-01"}]. SQL should include WHERE clause with parameterized value.

7. test_tenant_isolation — every generated SQL must include "tenant_slug = ?" as
   the first WHERE condition. Verify the tenant_slug is in the parameters list.

8. test_invalid_dimension — request a dimension that doesn't exist in config.
   Should raise ValueError.

9. test_invalid_join — request a join to a model not in the config's joins list.
   Should raise ValueError.

10. test_invalid_filter_operator — create a QueryFilter with op="DROP TABLE" —
    should be rejected by Pydantic validator.

11. test_list_models — should return 6 models for tyrell_corp.

12. test_model_summary_fields — verify name, label, description,
    dimension_count, measure_count, has_joins are populated.

Use pytest. Run with:
`cd services/platform-api && python -m pytest test_query_builder.py -v`

```
### Prompt 2.5 — Integration Tests
```

Update `services/platform-api/test_main.py` to add integration tests for the new
endpoints using FastAPI's TestClient.

Add tests:

1. test_get_models_list — GET /semantic-layer/tyrell_corp/models returns 200
   with 6 models.

2. test_get_model_detail — GET
   /semantic-layer/tyrell_corp/models/fct_tyrell_corp__ad_performance returns
   200 with dimensions, measures, calculated_measures, joins.

3. test_get_model_not_found — GET /semantic-layer/tyrell_corp/models/nonexistent
   returns 404.

4. test_get_config_not_found — GET /semantic-layer/nonexistent_tenant/config
   returns 404.

5. test_query_endpoint_validation_error — POST /semantic-layer/tyrell_corp/query
   with invalid dimension returns 400.

6. test_query_endpoint_invalid_operator — POST with filter op="DROP" returns 422
   (Pydantic validation).

Note: The POST /query test that actually executes SQL requires a database
connection. Skip it if MOTHERDUCK_TOKEN is not set and sandbox.duckdb doesn't
exist:

7. test_query_endpoint_executes (mark with @pytest.mark.skipif) — POST a simple
   query, verify response has sql, data, columns, row_count fields.

Keep the existing test_update_logic_integrity test unchanged.

```
---

## Phase 3: Tenant-Scoped Observability

### Prompt 3.1 — Tenant-Scoped dbt Models
```

Create two new dbt models in
`warehouse/gata_transformation/models/platform/observability/`:

1. `int_platform_observability__tenant_run_results.sql`:

{{ config(materialized='table') }}

WITH tenant_models AS ( SELECT invocation_id, dlt_load_id, node_id, node_name,
run_result_status, rows_affected, execution_time_seconds, run_started_at, CASE
WHEN node_name LIKE '%tyrell_corp%' THEN 'tyrell_corp' WHEN node_name LIKE
'%wayne_enterprises%' THEN 'wayne_enterprises' WHEN node_name LIKE
'%stark_industries%' THEN 'stark_industries' ELSE NULL END AS tenant_slug FROM
{{ ref('int_platform_observability__run_results') }} )

SELECT tenant_slug, node_name AS model_name, run_result_status AS status,
rows_affected, execution_time_seconds, run_started_at, dlt_load_id FROM
tenant_models WHERE tenant_slug IS NOT NULL

2. `int_platform_observability__tenant_test_results.sql`:

{{ config(materialized='table') }}

WITH tenant_tests AS ( SELECT invocation_id, node_id, node_name, test_status,
test_message, execution_time_seconds, run_started_at, CASE WHEN node_name LIKE
'%tyrell_corp%' THEN 'tyrell_corp' WHEN node_name LIKE '%wayne_enterprises%'
THEN 'wayne_enterprises' WHEN node_name LIKE '%stark_industries%' THEN
'stark_industries' ELSE NULL END AS tenant_slug FROM {{
ref('int_platform_observability__test_results') }} )

SELECT tenant_slug, node_name AS test_name, test_status AS status, test_message
AS message, execution_time_seconds, run_started_at FROM tenant_tests WHERE
tenant_slug IS NOT NULL

After creating both files, run:
`cd warehouse/gata_transformation && uv run --env-file ../../.env dbt run --target sandbox --select int_platform_observability__tenant_run_results int_platform_observability__tenant_test_results`

Verify both models compile and run successfully.

```
### Prompt 3.2 — Observability API Endpoints
```

Add observability endpoints to `services/platform-api/main.py`.

Add these Pydantic models to `services/platform-api/models.py`:

1. `ObservabilitySummary`:
   - tenant_slug: str
   - models_count: int
   - last_run_at: str | None
   - pass_count: int
   - fail_count: int
   - error_count: int
   - skip_count: int
   - avg_execution_time: float

2. `RunResult`:
   - model_name: str
   - status: str
   - rows_affected: int | None
   - execution_time_seconds: float
   - run_started_at: str

3. `TestResult`:
   - test_name: str
   - status: str
   - message: str | None
   - execution_time_seconds: float
   - run_started_at: str

4. `IdentityResolutionStats`:
   - tenant_slug: str
   - total_users: int
   - resolved_customers: int
   - anonymous_users: int
   - resolution_rate: float
   - total_events: int
   - total_sessions: int

Add endpoints to main.py:

GET /observability/{tenant_slug}/summary

- Query int_platform_observability__tenant_run_results for the tenant
- Aggregate: count distinct models, last run_started_at, count by status, avg
  execution_time
- Return ObservabilitySummary

GET /observability/{tenant_slug}/runs

- Query int_platform_observability__tenant_run_results WHERE tenant_slug = ?
- Return list[RunResult] ordered by run_started_at DESC
- Optional query param: limit (default 50)

GET /observability/{tenant_slug}/tests

- Query int_platform_observability__tenant_test_results WHERE tenant_slug = ?
- Return list[TestResult] ordered by run_started_at DESC
- Optional query param: limit (default 50)

GET /observability/{tenant_slug}/identity-resolution

- Query int_platform_observability__identity_resolution_stats WHERE tenant_slug
  = ?
- Return IdentityResolutionStats (latest row by dlt_load_id)

All endpoints use _get_db_connection() for database access. Return 404 if no
data found for tenant.

````
---

## Verification Checklist

After all phases complete, run these checks:

```bash
# Phase 1 — Rill removed
dir services\rill-dashboard  # Should not exist

# Phase 2 — API starts and responds
cd services\platform-api
uvicorn main:app --reload
# In another terminal:
curl http://localhost:8000/semantic-layer/tyrell_corp/models
curl http://localhost:8000/semantic-layer/tyrell_corp/models/fct_tyrell_corp__ad_performance
curl -X POST http://localhost:8000/semantic-layer/tyrell_corp/query -H "Content-Type: application/json" -d "{\"model\": \"fct_tyrell_corp__ad_performance\", \"dimensions\": [\"source_platform\"], \"measures\": [\"spend\", \"clicks\"]}"

# Phase 2 — Tests pass
python -m pytest test_query_builder.py test_main.py -v

# Phase 3 — dbt models compile
cd warehouse\gata_transformation
uv run --env-file ..\..\env dbt compile --target sandbox --select int_platform_observability__tenant_run_results int_platform_observability__tenant_test_results

# Phase 3 — Observability endpoints
curl http://localhost:8000/observability/tyrell_corp/summary
curl http://localhost:8000/observability/tyrell_corp/identity-resolution
````

---

## Files Summary

| File                                                                              | Action | Phase |
| --------------------------------------------------------------------------------- | ------ | ----- |
| `services/rill-dashboard/`                                                        | DELETE | 1     |
| `services/mock-data-engine/utils/bsl_mapper.py`                                   | MODIFY | 1     |
| `services/platform-api/models.py`                                                 | CREATE | 2     |
| `services/platform-api/query_builder.py`                                          | CREATE | 2     |
| `services/platform-api/main.py`                                                   | MODIFY | 2 + 3 |
| `services/platform-api/pyproject.toml`                                            | MODIFY | 2     |
| `services/platform-api/test_query_builder.py`                                     | CREATE | 2     |
| `services/platform-api/test_main.py`                                              | MODIFY | 2     |
| `warehouse/.../observability/int_platform_observability__tenant_run_results.sql`  | CREATE | 3     |
| `warehouse/.../observability/int_platform_observability__tenant_test_results.sql` | CREATE | 3     |

---

## Dependency Order

```
Phase 1 (Rill cleanup) ──→ Phase 2 (Query builder + endpoints) ──→ Frontend ready
                       \
                        ──→ Phase 3 (Observability) ──→ Frontend ready
```

Phases 2 and 3 can run in parallel after Phase 1. The frontend integration
(Phase 4 from the original plan) depends on both being complete.
