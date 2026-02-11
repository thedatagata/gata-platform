# Semantic Layer Strategy: Two-Tier Architecture

This document describes GATA Platform's approach to bridging the gap between raw star schema tables and user-facing analytics. The platform uses two semantic layers — one in-browser for single-table exploration, one server-side for governed multi-table OLAP — each optimized for different use cases.

---

## 1. Why Two Tiers

The platform serves two fundamentally different query patterns:

**Single-Table EDA** — A user uploads a CSV, or pulls a single fact table into DuckDB WASM, and wants to ask natural language questions about it. This requires zero backend infrastructure, instant feedback, and tolerance for approximate results. The LLM only needs to understand one table's columns.

**Multi-Table OLAP** — A user asks "what's my ROAS by campaign?" which requires joining `fct_ad_performance` → `dim_campaigns` for campaign names, or "revenue by traffic source" which joins `fct_sessions` → `fct_orders` on `transaction_id`. These queries require governed join paths, deterministic SQL generation, and access to the full star schema. An in-browser SLM cannot reliably manage multi-table joins.

Rather than forcing one tool to handle both patterns, the platform uses the right tool for each job.

---

## 2. Tier 1: In-Browser Semantic Layer (Existing)

### Location
```
app/utils/smarter/dashboard_utils/    # Core semantic runtime
app/utils/system/semantic-profiler.ts  # Auto-profiling engine
app/static/smarter/                    # Pre-built metadata JSONs
```

### How It Works

The in-browser semantic layer is a self-contained system with four components:

#### 2.1 Semantic Profiler (`semantic-profiler.ts`)
When a user loads a table (from file upload, MotherDuck, or pre-built dataset), the profiler runs DuckDB's `SUMMARIZE` command against it and infers:
- **Field metadata**: column type, data type category (categorical / numerical / temporal / identifier / continuous), cardinality
- **Members**: for low-cardinality categoricals, the distinct values (e.g., traffic_source members: ['direct', 'email', 'google_paid', ...])
- **Default dimensions**: categorical and temporal fields become dimensions with auto-generated `alias_name` and optional `transformation` (e.g., `CASE WHEN is_anonymous = 1 THEN 'Anonymous' ELSE 'Identified' END`)
- **Default measures**: numerical fields get standard aggregations (sum, avg, min, max, count) and formulas (count_distinct)

The profiler also performs smart type detection — it checks if VARCHAR columns are actually castable to DATE or DOUBLE via `TRY_CAST`, enabling correct categorization of string-encoded numbers and dates.

#### 2.2 Semantic Config (`semantic-config.ts`)
The metadata registry. Stores the profiled metadata in an in-memory cache (or reads from pre-built static JSON files for known datasets). Provides lookup functions:
- `getSemanticMetadata(table)` — returns the full metadata object
- `findDimensionByAlias(alias)` — resolves an alias to its source column and transformation
- `findMeasureByAlias(alias)` — resolves an alias to its aggregation type or formula SQL
- `generateSQLPrompt(table)` — produces a complete LLM system prompt with dimension/measure mappings, SQL generation rules, and examples

#### 2.3 Semantic Report Object (`semantic-objects.ts`)
The query engine. `SemanticReportObj` accepts a DuckDB WASM instance and a table name, then executes semantic queries:

```typescript
const report = new SemanticReportObj(db, "session_facts");
const data = await report.query({
  dimensions: ["traffic_source", "session_date"],
  measures: ["session_count", "total_revenue"],
  filters: ["session_date >= '2024-01-01'"],
  orderBy: ["session_date ASC"],
  limit: 100
});
```

Internally, it translates aliases to SQL: `session_count` becomes `COUNT(DISTINCT session_id)`, `traffic_source` stays as-is (no transformation), `total_revenue` becomes `SUM(session_revenue)`. The generated SQL is logged for debugging.

#### 2.4 Semantic Query Validator (`semantic-query-validator.ts`)
Catches LLM hallucinations before execution. When the WebLLM generates SQL, the validator:
- Extracts column references from SELECT and GROUP BY clauses
- Checks each against the metadata registry
- Detects invalid DuckDB functions (e.g., `CURDATE()` → should be `CURRENT_DATE`)
- Generates a correction prompt with the specific errors and relevant field mappings
- Returns the correction prompt to the WebLLM for retry (up to 3 attempts)

### WebLLM Integration (`webllm-handler.ts`)
The `WebLLMSemanticHandler` class manages the full text-to-SQL pipeline:
1. Loads Qwen 2.5 Coder (3B or 7B, controlled by LaunchDarkly flag) via `@mlc-ai/web-llm`
2. Generates a SQL prompt from the semantic config (dimension/measure mappings, rules, examples)
3. Sends to the in-browser LLM with `temperature: 0.0` for deterministic output
4. Validates the generated SQL against the semantic metadata
5. If invalid, retries with the correction prompt (up to 3 attempts)
6. Executes against DuckDB WASM
7. Returns data + query metadata + performance metrics

### The Metadata Format
```json
{
  "table": "session_facts",
  "description": "Session-level analytics...",
  "fields": {
    "session_revenue": {
      "description": "Revenue generated in session",
      "md_data_type": "DOUBLE",
      "ingest_data_type": "number",
      "data_type_category": "continuous",
      "members": null
    }
  },
  "dimensions": {
    "is_anonymous": {
      "alias_name": "user_status",
      "transformation": "CASE WHEN is_anonymous = 1 THEN 'Anonymous' ELSE 'Identified' END"
    }
  },
  "measures": {
    "session_revenue": {
      "aggregations": [
        { "sum": { "alias": "total_revenue", "format": "currency", "currency": "USD" } },
        { "avg": { "alias": "avg_revenue_per_session", "format": "currency" } }
      ],
      "formula": null
    },
    "session_id": {
      "aggregations": [],
      "formula": {
        "session_count": {
          "sql": "COUNT(DISTINCT session_id)",
          "format": "number",
          "description": "Total unique sessions"
        }
      }
    }
  }
}
```

This format is the contract between the profiler, the LLM prompt generator, the query engine, and the validator. Any table that provides metadata in this shape can be queried semantically.

---

## 3. Tier 2: Backend BSL — Boring Semantic Layer (In Progress)

### Purpose
Define the **cube geometry** — how the star schema's tables relate to each other — so that multi-table queries are governed and deterministic rather than LLM-hallucinated.

When a user asks "show me ROAS by campaign name", the system needs to know:
- ROAS = `spend` from `fct_ad_performance` ÷ `session_revenue` from `fct_sessions`
- "campaign name" lives in `dim_campaigns`
- `fct_ad_performance` joins `dim_campaigns` on `campaign_id` + `source_platform`
- `fct_sessions` joins `fct_orders` on `transaction_id` (for revenue attribution)

No single-table profiler can express these relationships. BSL defines them explicitly.

### Why BSL (Boring Semantic Layer)
- **Library, not platform** — `pip install boring-semantic-layer`. No servers, no SaaS subscription.
- **Built on Ibis** — Metrics defined as `sales.sum()` compile to DuckDB SQL locally, Snowflake in production. Same model, any backend.
- **Python-native** — Full IDE support, type checking, debugging. No YAML-only configuration.
- **MCP-native** — Exposes `list_metrics()`, `get_dimension_values()`, `query()` as MCP tools for AI agents. The LLM doesn't write SQL — it queries the semantic interface.
- **Deterministic** — No dynamic join path inference. You define the joins explicitly. The resulting SQL is predictable and debuggable.

### Auto-Population from dbt Metadata

The platform already captures everything needed to generate BSL definitions automatically:

#### Data Source 1: `model_artifacts__current`
Captured by `upload_model_definitions()` on-run-end hook. Contains:
- `node_id` — dbt model unique identifier
- `model_name` — e.g., `fct_tyrell_corp__ad_performance`
- `depends_on_nodes` — JSON array of upstream model references (encodes the DAG)
- `materialization` — table vs view
- `tags`, `meta`, `config` — model metadata

#### Data Source 2: DuckDB `DESCRIBE` / `SUMMARIZE`
Run against each `fct_*` and `dim_*` table in `analytics/{tenant}/` to get:
- Column names and types
- Cardinality (approx_unique)
- Min/max/avg for numerics
- NULL percentages

#### Data Source 3: `tenants.yaml`
Provides tenant-specific context:
- Which sources are enabled (determines which engines contributed to the facts)
- Conversion events (for session-level flags)
- Custom logic overrides

#### Data Source 4: Engine Canonical Schemas
The engines enforce column contracts. Shared column names across tables reveal join paths:
- `campaign_id` appears in `fct_ad_performance` AND `dim_campaigns` → join path
- `transaction_id` appears in `fct_sessions` AND `fct_orders` → join path
- `source_platform` appears everywhere → partition/filter key

### BSL Generation Flow

```
dbt run (on-run-end hooks capture metadata)
    │
    ▼
Post-run Python script
    │
    ├── Read model_artifacts__current → discover fct_* and dim_* per tenant
    ├── DESCRIBE each analytics table → columns, types
    ├── SUMMARIZE each table → cardinality, ranges
    ├── Read tenants.yaml → conversion events, source mix
    ├── Infer join paths from shared column names
    │
    ▼
Generate BSL definitions per tenant
    │
    ├── Dimensions: categorical/temporal columns from facts + all dim columns
    ├── Measures: numerical columns with appropriate aggregations
    ├── Relationships: explicit join paths with cardinality (many-to-one, etc.)
    ├── Derived metrics: ROAS, CAC, conversion rate (cross-table formulas)
    │
    ▼
Write to BSL config (YAML/JSON per tenant)
    │
    ▼
FastAPI serves BSL endpoints per tenant
    ├── GET  /semantic-layer/{tenant}/dimensions
    ├── GET  /semantic-layer/{tenant}/measures
    ├── POST /semantic-layer/{tenant}/query
    └── MCP  /semantic-layer/{tenant}/mcp  (for AI agents)
```

### Expected Join Paths Per Tenant

For a tenant like Tyrell Corp with Facebook Ads + Google Ads + Instagram Ads + Shopify + Google Analytics:

```
dim_campaigns ──── campaign_id + source_platform ───── fct_ad_performance
                                                            │
                                                       source_platform
                                                            │
fct_sessions ──── transaction_id ──────────────────── fct_orders
     │
     └── traffic_campaign (matches campaign_name in dim_campaigns for attribution)
```

### Integration with MotherDuck AI
MotherDuck provides AI features like `prompt()` that can generate SQL from natural language. The BSL definitions serve as context for these features — rather than stuffing raw table schemas into a prompt, we provide the semantic model with pre-defined metrics, join paths, and business terminology. This dramatically reduces hallucination risk.

---

## 4. How the Two Tiers Interact

### User Flow: Single-Table Exploration (Tier 1)
1. User navigates to dashboard, selects a single table (e.g., `fct_sessions`)
2. Frontend fetches the table via DuckDB WASM (from MotherDuck or file upload)
3. Semantic profiler auto-generates metadata via `SUMMARIZE`
4. User types natural language query
5. WebLLM (Qwen 2.5 Coder) generates SQL using semantic prompt
6. Validator checks SQL against metadata, retries if needed
7. DuckDB WASM executes query client-side
8. AutoChart renders visualization

No backend involved. Instant feedback. Works offline.

### User Flow: Cross-Table Analytics (Tier 2)
1. User asks a question that requires joins (e.g., "ROAS by campaign name this month")
2. Frontend sends request to FastAPI backend
3. FastAPI loads BSL definition for the tenant
4. BSL resolves the query: identifies required tables (fct_ad_performance + dim_campaigns), join paths, measures (spend / revenue), and filters (date range)
5. BSL generates deterministic SQL via Ibis
6. SQL executes against MotherDuck
7. Results returned to frontend for visualization

Backend-mediated. Governed joins. Deterministic SQL.

### Shared Metadata Format
Both tiers use compatible metadata structures. The Tier 1 `SemanticMetadata` interface (fields, dimensions, measures) maps directly to BSL's dimension/measure definitions. This means:
- The in-browser profiler can bootstrap Tier 1 metadata from BSL definitions (pulling single-table metadata from the backend instead of re-profiling)
- BSL definitions can incorporate Tier 1 profiling results (cardinality, members) for richer metadata
- The frontend doesn't need to know which tier is answering — same dimension/measure contract

---

## 5. The Semantic Layer as OLAP Cube Definition

The purpose of the semantic layer, in both tiers, is to define the OLAP cube. This means:

1. **Dimensions** — The axes you can slice data by. Traffic source, campaign name, order date, device category. Each dimension maps to a physical column (possibly with a transformation) and has known members.

2. **Measures** — The values you aggregate. Spend, impressions, revenue, session count. Each measure maps to an aggregation function over a physical column, or a formula combining multiple aggregations.

3. **Relationships** — How the tables in the star schema connect. This is what separates a collection of flat tables from a cube. The `campaign_id` field in `fct_ad_performance` connects to the same field in `dim_campaigns`, creating a many-to-one relationship that lets you slice ad performance by campaign name.

4. **Derived Metrics** — Cross-table calculations. ROAS requires `revenue` from sessions/orders and `spend` from ad performance. CAC requires `spend` and `converting_sessions`. These only work when join paths are explicitly defined.

Without these four elements, an LLM trying to answer multi-table questions will hallucinate join conditions, invent column names, or produce syntactically valid but semantically wrong SQL. The semantic layer constrains the LLM to a governed "menu" of valid queries.

---

## 6. Future: MCP as the Universal Interface

The Model Context Protocol (MCP) transforms the semantic layer into an API for AI agents. With BSL + MCP:

- The LLM doesn't write SQL. It calls `list_metrics()` to see what's available, `get_dimension_values("campaign_name")` to see valid filter values, and `query(metrics=["roas"], dimensions=["campaign_name"], filters=["date >= '2024-01-01'"])` to get results.
- The semantic layer generates the SQL deterministically. The LLM is a "Semantic Query Writer", not a "SQL Writer".
- This works for any AI agent — Claude, GPT, open-source models — because MCP is a standard protocol.
- The same BSL definitions that power the FastAPI endpoints also power the MCP server. One source of truth, multiple consumption modes.

This is the "Chat-BI" vision: natural language becomes the interface for data, backed by deterministic, reliable semantic definitions.
