# Frontend Application

Deno Fresh web application providing tenant onboarding, analytics dashboards, and an in-browser semantic layer.

## Running

```bash
cd app
deno task start
```

## Architecture

### Onboarding Flow
- **ExperienceGateway**: Source selection + account creation for new tenants
- **DashboardRouter**: Routes between demo mode and connected (live data) mode

### Analytics Dashboard
- **CustomDataDashboard**: Connected-mode dashboard that loads live catalog from the Platform API, displays period-over-period KPI cards, and enables structured queries against the BSL semantic layer

### Two-Tier Semantic Layer

**Tier 1 — In-Browser (Single-Table EDA)**
- DuckDB WASM loads a single table client-side
- Semantic Profiler auto-detects column types, cardinality, and categories
- WebLLM (Qwen 2.5 Coder 3B) generates SQL from natural language
- Query Validator checks SQL against metadata registry with retry
- Fully offline, no backend dependency

**Tier 2 — Backend BSL (Multi-Table OLAP)**
- `platform-api-client.ts` provides typed access to all Platform API endpoints
- `bsl-config-adapter.ts` adapts BSL API metadata to frontend format
- Supports multi-table joins, calculated measures, and natural language queries via Ollama

## Key Directories

| Path | Purpose |
|------|---------|
| `islands/dashboard/` | Interactive dashboard components |
| `islands/onboarding/` | Tenant onboarding flow |
| `islands/charts/` | AutoChart, FunnelChart visualizations |
| `utils/api/` | Platform API client + BSL config adapter |
| `utils/smarter/` | Tier 1 semantic layer (profiler, config, WebLLM) |
| `routes/` | Fresh file-based routing |
