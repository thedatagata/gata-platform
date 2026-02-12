# BSL Implementation Plan v2 — Metadata-Driven + ECharts

## What Changed From v1

The original plan (5 prompts) built static YAML configs per tenant and wired
them through a custom tool-calling loop. That code is already deployed to
`services/platform-api/`. This v2 plan fixes the gaps found during audit:

| Gap                    | v1 (Current)                                                                           | v2 (Updated)                                                                                                         |
| ---------------------- | -------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| BSL config source      | Static YAML files at `semantic_configs/*.yaml`                                         | Auto-generated from `platform_ops__boring_semantic_layer` dbt catalog                                                |
| `from_config()` format | `_generate_bsl_config()` outputs dicts with `{"expr": "...", "description": "..."}`    | Plain strings `"_.col"` / `"_.col.sum()"` matching BSL's expected format                                             |
| Table key mapping      | Passes tables keyed by `table_name` but config uses `subject` as model key             | Correct: config key = subject, `"table"` field = physical table name, tables dict keyed by physical name             |
| Chart generation       | `chart_spec` always `None` — agent loop doesn't extract ECharts from BSLTools response | BSLTools' `_query_model` → `generate_chart_with_data(chart_backend="echarts")` → ECharts JSON extracted and returned |
| Agent loop             | Custom tool execution duplicates BSLTools logic                                        | Uses BSLTools' `execute()` which returns JSON with `records`, `chart.data`, `sql`                                    |
| New tenant onboarding  | Must create YAML manually                                                              | Auto-discovers from dbt catalog; YAML is optional enrichment overlay                                                 |
| LLM model              | Plan says 14B                                                                          | Changed to 7B (better speed/quality tradeoff for tool calling)                                                       |
| Calculated measures    | Stored as SQL string metadata only                                                     | Common patterns (CTR, CPC, AOV) converted to Ibis expressions via `calc_measure()`                                   |

## Architecture

```
          ┌──────────────────────────────────┐
          │   dbt run (on-run-end hooks)      │
          │   → platform_ops__boring_          │
          │     semantic_layer (dbt catalog)   │
          └──────────────┬───────────────────┘
                         │ columns, types per fct_*/dim_*
                         ▼
    ┌─────────────────────────────────────────────┐
    │  bsl_model_builder.py                        │
    │  ┌───────────────────────────────────┐      │
    │  │ _read_catalog(con, tenant_slug)    │      │
    │  │ → [{table_name, columns, types}]   │      │
    │  └───────────────┬───────────────────┘      │
    │                  │                           │
    │  ┌───────────────▼───────────────────┐      │
    │  │ _classify_column(name, type)       │      │
    │  │ → "dimension" | "measure" | "skip" │      │
    │  └───────────────┬───────────────────┘      │
    │                  │                           │
    │  ┌───────────────▼───────────────────┐      │
    │  │ _load_yaml_enrichments() (optional)│      │
    │  │ → descriptions, joins, calc_measures│      │
    │  └───────────────┬───────────────────┘      │
    │                  │                           │
    │  ┌───────────────▼───────────────────┐      │
    │  │ _generate_bsl_config()             │      │
    │  │ → BSL from_config() format dict     │      │
    │  └───────────────┬───────────────────┘      │
    │                  │                           │
    │  ┌───────────────▼───────────────────┐      │
    │  │ from_config(config, tables=ibis)   │      │
    │  │ → dict[str, SemanticModel]          │      │
    │  └──────────────────────────────────┘      │
    └─────────────────────────────────────────────┘
                         │
          ┌──────────────┴──────────────┐
          │                             │
┌────────▼────────┐          ┌─────────▼─────────┐
│ FastAPI Endpoints │          │ BSL Agent Loop     │
│ GET /dimensions   │          │ Ollama Qwen2.5-7B  │
│ GET /measures     │          │ BSLTools execute()  │
│ POST /query       │          │ → ECharts JSON      │
└──────────────────┘          └────────────────────┘
```

## Current File State (Already Deployed)

```
services/platform-api/
├── llm_provider.py          ✅ Working (Ollama → Anthropic → none fallback)
├── bsl_model_builder.py     🔧 Needs fixes (from_config format, table key mapping)
├── bsl_agent.py             🔧 Needs fixes (ECharts extraction, agent loop cleanup)
├── main.py                  ✅ 14 endpoints working
├── models.py                ✅ Pydantic models complete
├── query_builder.py         ✅ Structured query compiler
├── semantic_configs/        ⚡ Becomes optional enrichment overlay
│   ├── tyrell_corp.yaml
│   ├── wayne_enterprises.yaml
│   └── stark_industries.yaml
└── test_bsl_agent.py        🔧 Needs updates for new flow
```

## Verified Data Sources

The dbt catalog table is populated and returns correct metadata:

```sql
-- 18 star schema tables across 3 tenants (6 per tenant)
-- Each with column_name, data_type, ordinal_position
SELECT * FROM platform_ops__boring_semantic_layer
WHERE tenant_slug = 'tyrell_corp'
-- Returns: fct_ad_performance, fct_events, fct_orders, fct_sessions,
--          dim_campaigns, dim_users
```

---

## Prompt 1 — Fix `bsl_model_builder.py` (from_config Format + Auto-Generation)

**Problem**: `_generate_bsl_config()` outputs nested dicts with description
metadata that BSL's `from_config()` doesn't understand. Also, the table-to-model
mapping is inconsistent.

**BSL `from_config()` expects this exact format:**

```python
{
    "ad_performance": {                    # model name (our "subject")
        "table": "fct_tyrell_corp__ad_performance",  # physical table name
        "description": "Daily ad spend...",
        "dimensions": {
            "source_platform": "_.source_platform",        # plain string
            "report_date": {                                # or dict with expr
                "expr": "_.report_date",
                "description": "Report date",
                "is_time_dimension": True
            }
        },
        "measures": {
            "spend": "_.spend.sum()",                       # plain string
            "impressions": "_.impressions.sum()",
            "ctr": {                                        # or dict with expr
                "expr": "_.clicks.sum() / _.impressions.sum()",
                "description": "Click-Through Rate"
            }
        },
        "joins": {
            "campaigns": {                 # join alias
                "model": "campaigns",      # references another model key
                "type": "one",
                "left_on": "campaign_id",
                "right_on": "campaign_id",
                "how": "left"
            }
        }
    }
}
```

**The `tables` parameter must be keyed by physical table name:**

```python
tables = {
    "fct_tyrell_corp__ad_performance": con.table("fct_tyrell_corp__ad_performance"),
    "dim_tyrell_corp__campaigns": con.table("dim_tyrell_corp__campaigns"),
    ...
}
```

### Changes to `bsl_model_builder.py`:

**1. Replace `_generate_bsl_config()` — fix output format:**

The current version wraps everything in `{"expr": ..., "description": ...}`
dicts. For dimensions without special metadata, use plain strings. For measures,
use plain aggregation expression strings. Only use the dict format when we have
metadata to add (descriptions, is_time_dimension, etc.).

```python
def _generate_bsl_config(
    catalog: list[dict],
    enrichments: dict,
    con: ibis.BaseBackend,
) -> dict:
    """Generate BSL from_config() compatible config dict."""
    bsl_config = {}

    for entry in catalog:
        table_name = entry["table_name"]
        subject = entry["subject"]
        columns = entry["columns"]

        enrich = enrichments.get(table_name, {})
        dim_overrides = enrich.get("dimension_overrides", {})
        measure_overrides = enrich.get("measure_overrides", {})
        description = enrich.get("description", f"{entry['table_type'].title()}: {subject}")

        model_config = {
            "table": table_name,  # physical table name — matches tables dict key
            "description": description,
            "dimensions": {},
            "measures": {},
        }

        for col in columns:
            col_name = col["column_name"]
            data_type = col["data_type"]
            role = _classify_column(col_name, data_type)

            if role == "skip":
                continue

            # --- YAML override check ---
            if col_name in measure_overrides:
                override = measure_overrides[col_name]
                agg = override.get("agg", _infer_aggregation(col_name, data_type))
                label = override.get("label", col_name)
                desc = override.get("description", f"{label} ({agg})")
                model_config["measures"][col_name] = {
                    "expr": _ibis_agg_expr(col_name, agg),
                    "description": desc,
                }
                continue

            if col_name in dim_overrides:
                override = dim_overrides[col_name]
                dim_config = {"expr": f"_.{col_name}"}
                if override.get("description"):
                    dim_config["description"] = override["description"]
                dtype = override.get("type", "")
                if dtype in ("date", "timestamp", "timestamp_epoch"):
                    dim_config["is_time_dimension"] = True
                model_config["dimensions"][col_name] = dim_config
                continue

            # --- Auto-classified ---
            if role == "dimension":
                if data_type in ("DATE", "TIMESTAMP"):
                    model_config["dimensions"][col_name] = {
                        "expr": f"_.{col_name}",
                        "is_time_dimension": True,
                    }
                else:
                    # Plain string format — no metadata needed
                    model_config["dimensions"][col_name] = f"_.{col_name}"
            elif role == "measure":
                agg = _infer_aggregation(col_name, data_type)
                # Plain string format
                model_config["measures"][col_name] = _ibis_agg_expr(col_name, agg)

        # --- Calculated measures from YAML ---
        for calc in enrich.get("calculated_measures", []):
            calc_name = calc.get("name", "")
            ibis_expr = _convert_calculated_measure(calc, model_config)
            if ibis_expr:
                model_config["measures"][calc_name] = {
                    "expr": ibis_expr,
                    "description": calc.get("label", calc_name),
                }

        bsl_config[subject] = model_config

    return bsl_config
```

**2. Add `_convert_calculated_measure()` — convert common SQL patterns to
Ibis:**

```python
# Common calculated measure patterns → Ibis expressions
CALC_MEASURE_PATTERNS = {
    "ctr": "ibis.ifelse(_.impressions.sum() > 0, _.clicks.sum().cast('float64') / _.impressions.sum(), 0)",
    "cpc": "ibis.ifelse(_.clicks.sum() > 0, _.spend.sum() / _.clicks.sum(), 0)",
    "cpm": "ibis.ifelse(_.impressions.sum() > 0, _.spend.sum() * 1000.0 / _.impressions.sum(), 0)",
    "aov": "ibis.ifelse(_.order_id.nunique() > 0, _.total_price.sum() / _.order_id.nunique(), 0)",
    "conversion_rate": "ibis.ifelse(_.session_id.nunique() > 0, _.is_conversion_session.cast('int64').sum().cast('float64') / _.session_id.nunique(), 0)",
}

def _convert_calculated_measure(calc_config: dict, model_config: dict) -> str | None:
    """Convert a YAML calculated measure to an Ibis expression string.

    Uses pattern matching for common formulas (CTR, CPC, AOV, etc.).
    Falls back to None for complex custom SQL that can't be auto-converted.
    """
    name = calc_config.get("name", "").lower()

    # Check known patterns
    if name in CALC_MEASURE_PATTERNS:
        return CALC_MEASURE_PATTERNS[name]

    # For unknown patterns, log and skip
    logger.info(
        f"[BSL] Calculated measure '{name}' has custom SQL that "
        f"cannot be auto-converted to Ibis. Skipping."
    )
    return None
```

**3. Fix `_wire_joins()` — use correct model key references:**

```python
def _wire_joins(bsl_config: dict, enrichments: dict, catalog: list[dict]) -> dict:
    """Wire joins using subject-based model references."""
    # Build lookup: physical table_name → subject (BSL model key)
    table_to_subject = {e["table_name"]: e["subject"] for e in catalog}

    for entry in catalog:
        table_name = entry["table_name"]
        subject = entry["subject"]

        enrich = enrichments.get(table_name, {})
        yaml_joins = enrich.get("joins", [])

        if not yaml_joins or subject not in bsl_config:
            continue

        bsl_joins = {}
        for jdef in yaml_joins:
            target_table = jdef.get("to", "")
            target_subject = table_to_subject.get(target_table)

            if not target_subject or target_subject not in bsl_config:
                logger.warning(f"[BSL] Join target '{target_table}' not in catalog")
                continue

            on_clause = jdef.get("on", {})
            if not on_clause:
                continue

            # Take first key pair (BSL from_config supports single-key joins)
            left_on, right_on = next(iter(on_clause.items()))

            join_type = "one"  # dim lookups are one-to-one
            if target_table.startswith("fct_"):
                join_type = "many"

            bsl_joins[target_subject] = {
                "model": target_subject,   # references another model KEY in bsl_config
                "type": join_type,
                "left_on": left_on,
                "right_on": right_on,
                "how": jdef.get("type", "left"),
            }

        if bsl_joins:
            bsl_config[subject]["joins"] = bsl_joins

    return bsl_config
```

**4. Fix `create_tenant_semantic_models()` — correct table dict key mapping:**

```python
def create_tenant_semantic_models(
    tenant_slug: str,
    con: Optional[ibis.BaseBackend] = None,
) -> dict[str, SemanticModel]:
    if not BSL_AVAILABLE:
        raise RuntimeError("boring-semantic-layer not installed")

    if con is None:
        con = _get_ibis_connection()

    catalog = _read_catalog(con, tenant_slug)
    if not catalog:
        raise ValueError(f"No star schema tables for '{tenant_slug}'")

    enrichments = _load_yaml_enrichments(tenant_slug)
    bsl_config = _generate_bsl_config(catalog, enrichments, con)
    bsl_config = _wire_joins(bsl_config, enrichments, catalog)

    # Build tables dict keyed by PHYSICAL table name
    # (matches the "table" field in each model config)
    tables = {}
    for entry in catalog:
        table_name = entry["table_name"]
        try:
            tables[table_name] = con.table(table_name)
        except Exception as e:
            logger.warning(f"[BSL] Could not load '{table_name}': {e}")

    # from_config() reads config[model_name]["table"] to find the
    # matching key in the tables dict
    models = from_config(bsl_config, tables=tables)

    logger.info(f"[BSL] Built {len(models)} models for '{tenant_slug}'")
    return models
```

### Verification:

```bash
cd services/platform-api
python -c "
from bsl_model_builder import create_tenant_semantic_models
import ibis
con = ibis.duckdb.connect('md:my_db')
models = create_tenant_semantic_models('tyrell_corp', con)
for name, m in models.items():
    dims = list(m.get_dimensions().keys())
    measures = list(m.get_measures().keys())
    print(f'{name}: {len(dims)} dims, {len(measures)} measures')
    print(f'  dims: {dims[:5]}...')
    print(f'  measures: {measures[:5]}...')
"
```

Expected output:

```
ad_performance: 5 dims, 7 measures
  dims: ['source_platform', 'report_date', 'campaign_id', 'ad_group_id', 'ad_id']...
  measures: ['spend', 'impressions', 'clicks', 'conversions', 'ctr', 'cpc', 'cpm']...
orders: 6 dims, 3 measures
sessions: 10 dims, 5 measures
events: 11 dims, 2 measures
campaigns: 4 dims, 0 measures
users: 10 dims, 2 measures
```

---

## Prompt 2 — Fix `bsl_agent.py` (ECharts Extraction + Agent Loop)

**Problem**: The agent loop manually parses BSLTools responses and never
extracts chart data. BSLTools' built-in `_query_model()` already calls
`generate_chart_with_data()` which returns JSON with `chart.data` containing
ECharts specs when `chart_backend="echarts"`.

### Key Insight

When `GATABSLTools.execute("query_model", {"query": "..."})` is called:

1. It calls `self._query_model(query=...)`
2. Which calls `safe_eval(query, context={**self.models})` to execute the BSL
   query
3. Which calls
   `generate_chart_with_data(result, chart_backend="echarts", return_json=False)`
4. Returns a JSON string like:

```json
{
  "total_rows": 25,
  "columns": ["source_platform", "spend"],
  "records": [...],
  "chart": {
    "backend": "echarts",
    "format": "json",
    "data": {
      "title": {"text": "..."},
      "xAxis": {...},
      "yAxis": {...},
      "series": [{...}],
      "tooltip": {...}
    }
  }
}
```

The `chart.data` field IS the ECharts option object that the frontend can render
directly.

### Changes to `bsl_agent.py`:

**1. Fix `GATABSLTools.__init__` — set chart_backend correctly:**

```python
class GATABSLTools(BSLTools):
    """BSLTools variant that accepts pre-built SemanticModel dicts."""

    def __init__(
        self,
        models: dict[str, "SemanticModel"],
        chart_backend: str = "echarts",
    ):
        # Don't call super().__init__() which tries from_yaml()
        # Set all attributes that the parent's methods need
        self.model_path = None
        self.profile = None
        self.profile_file = None
        self.chart_backend = chart_backend
        self._error_callback = None
        self.models = models
```

**2. Rewrite `_run_agent_loop` — properly extract ECharts from tool responses:**

```python
def _run_agent_loop(
    question: str,
    bsl_tools: GATABSLTools,
    llm: Any,
    tenant_slug: str,
) -> AgentResponse:
    """Run the LLM agent loop with BSLTools.

    BSLTools' execute() returns JSON strings. For query_model, the JSON
    contains {records, chart: {data: <echarts_option>}, total_rows}.
    We parse this to extract records and chart_spec for the response.
    """
    start = time.time()
    response = AgentResponse(provider="llm")

    system_prompt = _build_system_prompt(tenant_slug, bsl_tools.models)
    lc_tools = bsl_tools.get_callable_tools()
    llm_with_tools = llm.bind_tools(lc_tools)

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=question),
    ]

    max_iterations = 8
    for i in range(max_iterations):
        ai_message = llm_with_tools.invoke(messages)
        messages.append(ai_message)

        if not ai_message.tool_calls:
            response.answer = ai_message.content
            break

        for tool_call in ai_message.tool_calls:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            response.tool_calls.append(f"{tool_name}({json.dumps(tool_args)})")

            try:
                # BSLTools.execute() returns a string (JSON for query_model)
                result_str = bsl_tools.execute(tool_name, tool_args)

                # Extract structured data from query_model responses
                if tool_name == "query_model" and result_str:
                    _extract_query_results(result_str, response, tool_args)

                messages.append(ToolMessage(
                    content=str(result_str),
                    tool_call_id=tool_call["id"],
                ))

            except Exception as e:
                logger.warning(f"[BSL Agent] Tool {tool_name} failed: {e}")
                messages.append(ToolMessage(
                    content=f"Error: {e}",
                    tool_call_id=tool_call["id"],
                ))

    response.execution_time_ms = int((time.time() - start) * 1000)
    return response


def _extract_query_results(result_str: str, response: AgentResponse, tool_args: dict):
    """Parse BSLTools query_model JSON response to extract records + ECharts."""
    try:
        parsed = json.loads(result_str) if isinstance(result_str, str) else result_str

        if isinstance(parsed, dict):
            # Records
            if "records" in parsed:
                response.records = parsed["records"]

            # ECharts spec — this is the key fix
            chart_block = parsed.get("chart", {})
            if isinstance(chart_block, dict) and "data" in chart_block:
                response.chart_spec = chart_block["data"]

            # SQL (if BSL exposes it)
            if "sql" in parsed:
                response.sql = parsed["sql"]

            # Model name from query string
            query_str = tool_args.get("query", "")
            if "." in query_str:
                response.model_used = query_str.split(".")[0]

    except (json.JSONDecodeError, AttributeError, TypeError) as e:
        logger.debug(f"[BSL Agent] Could not parse query result: {e}")
```

**3. Update keyword fallback to also return chart suggestions:**

```python
def _fallback_keyword_search(
    question: str,
    models: dict[str, "SemanticModel"],
) -> AgentResponse:
    """Keyword-based model suggestion + basic query execution when no LLM."""
    q_lower = question.lower()
    matched_model = None
    best_score = 0

    for model_name, keywords in KEYWORD_MAP.items():
        score = sum(1 for kw in keywords if kw in q_lower)
        if score > best_score and model_name in models:
            best_score = score
            matched_model = model_name

    if not matched_model:
        matched_model = next(iter(models), None)

    if not matched_model:
        return AgentResponse(answer="No semantic models available.", error="No models")

    model = models[matched_model]
    dims = list(model.get_dimensions().keys())
    measures = list(model.get_measures().keys())

    # Try a basic query: first dimension + first measure
    records = []
    chart_spec = None
    if dims and measures:
        try:
            query = model.group_by(dims[0]).aggregate(measures[0])
            df = query.execute()
            records = df.head(50).to_dict(orient="records")

            # Generate ECharts
            try:
                chart_result = query.chart(backend="echarts", format="json")
                if isinstance(chart_result, str):
                    chart_spec = json.loads(chart_result)
                elif isinstance(chart_result, dict):
                    chart_spec = chart_result
            except Exception:
                pass
        except Exception as e:
            logger.debug(f"[BSL Fallback] Basic query failed: {e}")

    answer = (
        f"Based on your question, here are results from **{matched_model}** "
        f"grouped by `{dims[0]}` with `{measures[0]}` aggregated.\n\n"
        f"_No LLM available — showing default grouping. "
        f"Start Ollama for natural language queries._"
    ) if records else (
        f"Most relevant model: **{matched_model}**\n"
        f"Dimensions: {', '.join(dims[:5])}\n"
        f"Measures: {', '.join(measures[:5])}\n\n"
        f"_No LLM available. Use structured query endpoint._"
    )

    return AgentResponse(
        answer=answer,
        records=records,
        chart_spec=chart_spec,
        model_used=matched_model,
        provider="keyword_fallback",
    )
```

### Verification:

```bash
# Test with keyword fallback (no Ollama needed)
curl -X POST http://localhost:8001/semantic-layer/tyrell_corp/ask \
  -H "Content-Type: application/json" \
  -d '{"question": "Show me ad spend by platform"}'

# Response should include chart_spec with ECharts JSON:
# {
#   "answer": "Based on your question...",
#   "records": [...],
#   "chart_spec": {
#     "xAxis": {"type": "category", "data": ["facebook_ads", "google_ads"]},
#     "yAxis": {"type": "value"},
#     "series": [{"type": "bar", "data": [1234.56, 789.01]}]
#   },
#   "model_used": "ad_performance",
#   "provider": "keyword_fallback"
# }
```

---

## Prompt 3 — Update `main.py` Endpoints (ECharts + Auto-Discovery)

**Problem**: Endpoints fall back to YAML-only `QueryBuilder` when BSL fails.
Need to make BSL the primary path and remove YAML dependency from model
discovery.

### Changes:

**1. Update `/dimensions` and `/measures` to include descriptions:**

```python
@app.get("/semantic-layer/{tenant_slug}/dimensions")
def get_dimensions(tenant_slug: str):
    """Live dimension catalog from BSL (auto-generated from dbt metadata)."""
    models = _get_bsl_models(tenant_slug)
    result = {}
    for model_name, model in models.items():
        dims = {}
        for dim_name, dim in model.get_dimensions().items():
            dims[dim_name] = {
                "description": dim.description or dim_name,
                "is_time_dimension": getattr(dim, "is_time_dimension", False),
            }
        result[model_name] = {
            "description": model.description or model_name,
            "dimensions": dims,
        }
    return result


@app.get("/semantic-layer/{tenant_slug}/measures")
def get_measures(tenant_slug: str):
    """Live measure catalog from BSL (auto-generated from dbt metadata)."""
    models = _get_bsl_models(tenant_slug)
    result = {}
    for model_name, model in models.items():
        measures = {}
        for measure_name, measure in model.get_measures().items():
            measures[measure_name] = {
                "description": measure.description or measure_name,
            }
        result[model_name] = {
            "description": model.description or model_name,
            "measures": measures,
        }
    return result
```

**2. Add a new `/catalog` endpoint (combined dimensions + measures for
frontend):**

```python
@app.get("/semantic-layer/{tenant_slug}/catalog")
def get_catalog(tenant_slug: str):
    """Full semantic catalog for frontend consumption.

    Returns all models with their dimensions, measures, and metadata.
    This replaces the static JSON files the frontend previously loaded.
    """
    models = _get_bsl_models(tenant_slug)
    catalog = {}
    for model_name, model in models.items():
        dims = {
            name: {
                "description": dim.description or name,
                "is_time_dimension": getattr(dim, "is_time_dimension", False),
                "type": "time" if getattr(dim, "is_time_dimension", False) else "categorical",
            }
            for name, dim in model.get_dimensions().items()
        }
        measures = {
            name: {
                "description": m.description or name,
            }
            for name, m in model.get_measures().items()
        }
        catalog[model_name] = {
            "description": model.description or model_name,
            "dimensions": dims,
            "measures": measures,
            "dimension_count": len(dims),
            "measure_count": len(measures),
        }
    return catalog
```

**3. Fix `/models` to not require YAML:**

```python
@app.get("/semantic-layer/{tenant_slug}/models", response_model=list[ModelSummary])
def list_models(tenant_slug: str):
    """List available semantic models (auto-discovered from dbt catalog)."""
    models = _get_bsl_models(tenant_slug)
    return [
        ModelSummary(
            name=name,
            label=model.description or name,
            description=model.description or f"Semantic model: {name}",
            dimension_count=len(model.get_dimensions()),
            measure_count=len(model.get_measures()),
            has_joins=False,  # TODO: detect from model
        )
        for name, model in models.items()
    ]
```

**4. Add `/ask` response to always include ECharts when records > 1:**

The `/ask` endpoint already returns `AskResponse` with `chart_spec`. The fix is
in Prompt 2 (agent loop). No changes needed here — just verify the chart_spec
field flows through.

### Verification:

```bash
# Catalog endpoint (replaces static JSON for frontend)
curl http://localhost:8001/semantic-layer/tyrell_corp/catalog | python -m json.tool

# Should return all 6 models with dims/measures auto-classified from dbt
```

---

## Prompt 4 — Wire ECharts Through BSLTools `_query_model`

**Problem**: BSLTools' `_query_model()` calls `generate_chart_with_data()` which
supports ECharts, but the default `return_json=False` means it tries to render
in terminal mode. For API usage, we need `return_json=True`.

### Key Change

The `GATABSLTools` needs to override `_query_model` to force `return_json=True`
since we're in API mode, not CLI mode:

```python
class GATABSLTools(BSLTools):
    """BSLTools variant for API mode with pre-built models."""

    def __init__(self, models, chart_backend="echarts"):
        self.model_path = None
        self.profile = None
        self.profile_file = None
        self.chart_backend = chart_backend
        self._error_callback = None
        self.models = models

    def _query_model(
        self,
        query: str,
        get_records: bool = True,
        records_limit: int | None = None,
        records_displayed_limit: int | None = 10,
        get_chart: bool = True,
        chart_backend: str | None = None,
        chart_format: str | None = None,
        chart_spec: dict | None = None,
    ) -> str:
        """Override to force return_json=True and echarts backend for API mode."""
        from boring_semantic_layer.agents.utils.chart_handler import generate_chart_with_data
        from boring_semantic_layer.utils import safe_eval
        from returns.result import Failure, Success
        import ibis
        from ibis import _

        try:
            result = safe_eval(query, context={**self.models, "ibis": ibis, "_": _})
            if isinstance(result, Failure):
                raise result.failure()
            query_result = result.unwrap() if isinstance(result, Success) else result

            return generate_chart_with_data(
                query_result,
                get_records=get_records,
                records_limit=records_limit,
                records_displayed_limit=records_displayed_limit,
                get_chart=get_chart,
                chart_backend=chart_backend or self.chart_backend,
                chart_format=chart_format or "json",
                chart_spec=chart_spec,
                default_backend=self.chart_backend,
                return_json=True,  # KEY: API mode returns JSON string
                error_callback=self._error_callback,
            )

        except Exception as e:
            # Reuse parent's error handling logic
            error_str = str(e)
            if len(error_str) > 300:
                error_str = error_str[:300] + "..."

            error_msg = f"Query Error: {error_str}"

            model_name = self._extract_model_name(query)
            if "has no attribute" in error_str and model_name:
                schema = self._get_model(model_name)
                error_msg += f"\n\nAvailable fields for '{model_name}':\n{schema}"

            from langchain_core.tools import ToolException
            raise ToolException(error_msg) from e
```

### Data Flow Verification:

```
User: "Show me ad spend by platform"
  → Ollama decides: query_model(query="ad_performance.group_by('source_platform').aggregate('spend')")
  → GATABSLTools._query_model() calls safe_eval → BSL query → Ibis → DuckDB
  → generate_chart_with_data(return_json=True, chart_backend="echarts")
  → Returns JSON string:
    {
      "total_rows": 3,
      "columns": ["source_platform", "spend"],
      "records": [
        {"source_platform": "facebook_ads", "spend": 12345.67},
        {"source_platform": "google_ads", "spend": 8901.23},
        {"source_platform": "instagram_ads", "spend": 4567.89}
      ],
      "chart": {
        "backend": "echarts",
        "format": "json",
        "data": {                          ← This is the ECharts option object
          "xAxis": {"type": "category", "data": ["facebook_ads", "google_ads", "instagram_ads"]},
          "yAxis": {"type": "value"},
          "series": [{"type": "bar", "data": [12345.67, 8901.23, 4567.89], "name": "spend"}],
          "tooltip": {"trigger": "axis"},
          "title": {"text": "spend by source_platform"}
        }
      }
    }
  → _extract_query_results() parses this:
    response.records = [...]
    response.chart_spec = chart.data   ← ECharts option for frontend
  → LLM sees the JSON, writes a natural language answer
  → AskResponse returned with answer + records + chart_spec
```

---

## Prompt 5 — Tests + Ollama Setup + Memory Update

### Step 1: Update `test_bsl_agent.py`

```python
"""Tests for BSL v2 — metadata-driven with ECharts."""

import pytest
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


class TestModelBuilder:
    """Test auto-generation from dbt catalog."""

    def test_classify_column_varchar_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("source_platform", "VARCHAR") == "dimension"

    def test_classify_column_double_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("spend", "DOUBLE") == "measure"

    def test_classify_column_bigint_id_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("campaign_id", "BIGINT") == "dimension"

    def test_classify_column_bigint_count_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("total_clicks", "BIGINT") == "measure"

    def test_classify_column_json_is_skipped(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("line_items_json", "JSON") == "skip"

    def test_classify_column_tenant_slug_is_skipped(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("tenant_slug", "VARCHAR") == "skip"

    def test_infer_aggregation_spend_is_sum(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("spend", "DOUBLE") == "sum"

    def test_infer_aggregation_duration_is_avg(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("session_duration_seconds", "DOUBLE") == "avg"

    def test_convert_ctr_calc_measure(self):
        from bsl_model_builder import _convert_calculated_measure
        calc = {"name": "ctr", "label": "CTR", "sql": "..."}
        result = _convert_calculated_measure(calc, {})
        assert result is not None
        assert "impressions" in result and "clicks" in result


class TestAgentResponse:
    """Test ECharts extraction from BSLTools responses."""

    def test_extract_query_results_with_chart(self):
        from bsl_agent import _extract_query_results, AgentResponse

        result_json = json.dumps({
            "total_rows": 3,
            "columns": ["source_platform", "spend"],
            "records": [{"source_platform": "facebook", "spend": 100}],
            "chart": {
                "backend": "echarts",
                "format": "json",
                "data": {"xAxis": {}, "yAxis": {}, "series": []}
            }
        })

        response = AgentResponse()
        _extract_query_results(result_json, response, {"query": "ad_performance.group_by(...)"})

        assert len(response.records) == 1
        assert response.chart_spec is not None
        assert "xAxis" in response.chart_spec
        assert response.model_used == "ad_performance"

    def test_extract_query_results_no_chart(self):
        from bsl_agent import _extract_query_results, AgentResponse

        result_json = json.dumps({
            "total_rows": 1,
            "columns": ["total_spend"],
            "records": [{"total_spend": 500}],
        })

        response = AgentResponse()
        _extract_query_results(result_json, response, {"query": "ad_performance.aggregate(...)"})

        assert len(response.records) == 1
        assert response.chart_spec is None


class TestKeywordFallback:
    """Test keyword-based model selection."""

    def test_ad_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "ad_performance" in KEYWORD_MAP
        assert "spend" in KEYWORD_MAP["ad_performance"]

    def test_order_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "orders" in KEYWORD_MAP
        assert "revenue" in KEYWORD_MAP["orders"]


class TestLLMProvider:
    """Test LLM provider resolution."""

    def test_ollama_unavailable(self):
        from llm_provider import _try_ollama, LLMProviderConfig
        config = LLMProviderConfig(ollama_base_url="http://localhost:99999")
        result = _try_ollama(config)
        assert not result.is_available

    def test_provider_none(self):
        from llm_provider import get_llm_provider
        with patch.dict("os.environ", {"BSL_LLM_PROVIDER": "none"}):
            provider = get_llm_provider(force_refresh=True)
            assert provider.llm is None
            assert provider.provider_name == "none"
```

### Step 2: Run tests

```bash
cd services/platform-api
python -m pytest test_bsl_agent.py -v
```

### Step 3: Integration test script

```bash
# Full flow test (works without Ollama — uses keyword fallback)
cd services/platform-api
python -c "
from bsl_model_builder import create_tenant_semantic_models
import ibis, json

# Test all 3 tenants auto-generate from catalog
for tenant in ['tyrell_corp', 'wayne_enterprises', 'stark_industries']:
    con = ibis.duckdb.connect('md:my_db')
    models = create_tenant_semantic_models(tenant, con)
    print(f'{tenant}: {len(models)} models')
    for name, m in models.items():
        d = len(m.get_dimensions())
        me = len(m.get_measures())
        print(f'  {name}: {d} dims, {me} measures')
    con.disconnect()
    print()
"
```

---

## File Change Summary

| File                      | Action      | Key Changes                                                                              |
| ------------------------- | ----------- | ---------------------------------------------------------------------------------------- |
| `bsl_model_builder.py`    | **Rewrite** | Fix `from_config()` format, add `_convert_calculated_measure()`, fix table key mapping   |
| `bsl_agent.py`            | **Rewrite** | Add `_extract_query_results()`, override `_query_model` for API mode, ECharts extraction |
| `main.py`                 | **Update**  | Add `/catalog` endpoint, fix `/models` to not require YAML, update dim/measure endpoints |
| `models.py`               | No changes  | Already has AskResponse with chart_spec                                                  |
| `llm_provider.py`         | **Minor**   | Change default model to `qwen2.5-coder:7b`                                               |
| `test_bsl_agent.py`       | **Rewrite** | Add model builder tests, ECharts extraction tests                                        |
| `semantic_configs/*.yaml` | **Keep**    | Now optional enrichment overlay (descriptions, joins, calc measures)                     |

## Post-Implementation Checklist

1. `dbt run` populates `platform_ops__boring_semantic_layer` ✅ (already
   working)
2. `create_tenant_semantic_models('tyrell_corp')` builds 6 SemanticModel objects
   from catalog
3. `GET /semantic-layer/tyrell_corp/catalog` returns dims/measures without YAML
4. `POST /semantic-layer/tyrell_corp/ask` returns ECharts JSON in `chart_spec`
5. New tenant added to dbt → auto-discovered by BSL (no YAML needed)
6. Existing tenants with YAML get enriched configs (descriptions, joins, calc
   measures)
7. Ollama running → full NL agent loop with tool calling
8. Ollama not running → keyword fallback with basic query + ECharts
