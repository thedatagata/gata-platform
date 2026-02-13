# Scripts

Automation scripts for tenant onboarding and platform initialization.

## onboard_tenant.py

Full tenant scaffolding — generates mock data, calculates schema fingerprints, and creates dbt source/staging models.

```bash
uv run python scripts/onboard_tenant.py <tenant_slug> --target sandbox --days 30
```

What it does:
1. Reads tenant config from `tenants.yaml` (demo tenants) or database (app tenants)
2. Generates mock data via `MockOrchestrator` and lands it in DuckDB/MotherDuck
3. Calculates schema fingerprints and looks up `master_model_id` from `connector_blueprints`
4. Generates `_sources.yml` shims and staging pusher `.sql` files
5. After `dbt run`, the BSL semantic layer auto-populates with zero config

## initialize_connector_library.py

Builds the `connector_blueprints` registry mapping physical schema fingerprints to master models. Run once (or when connector schemas change).

```bash
uv run python scripts/initialize_connector_library.py sandbox
```

For each of the 13 supported connectors, it creates a dummy tenant, generates a sample dlt schema, computes an MD5 fingerprint of each table's columns/types, and registers the mapping.

## setup_ollama.py

Pulls and verifies the Ollama model used for natural language queries.

```bash
uv run python scripts/setup_ollama.py
```
