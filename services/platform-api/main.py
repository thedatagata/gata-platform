"""
GATA Platform API — Semantic Layer + Observability

Endpoints:
  Semantic Layer (BSL-powered):
    GET  /semantic-layer/{tenant}/models         — List BSL models (from dbt catalog)
    GET  /semantic-layer/{tenant}/models/{name}   — Model detail (dims, measures)
    GET  /semantic-layer/{tenant}/dimensions       — All dimensions (BSL live catalog)
    GET  /semantic-layer/{tenant}/measures         — All measures (BSL live catalog)
    POST /semantic-layer/{tenant}/query            — Structured query execution
    POST /semantic-layer/{tenant}/ask              — Natural language query (LLM agent)
    GET  /semantic-layer/{tenant}                  — Raw dbt catalog manifest
    GET  /semantic-layer/{tenant}/config           — YAML config (enrichments)
    POST /semantic-layer/update                    — Update tenant logic + trigger dbt

  LLM Provider:
    GET  /semantic-layer/llm-status                — Provider status
    POST /semantic-layer/llm-refresh               — Force refresh

  Observability:
    GET  /observability/{tenant}/summary
    GET  /observability/{tenant}/runs
    GET  /observability/{tenant}/tests
    GET  /observability/{tenant}/identity-resolution
"""

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
import json
import yaml
import subprocess
import logging
from pathlib import Path

from models import (
    SemanticQueryRequest, SemanticQueryResponse, ColumnInfo,
    ModelSummary, ModelDetail,
    ObservabilitySummary, RunResult, TestResult, IdentityResolutionStats,
    AskRequest, AskResponse, LLMProviderStatus, ReadinessStatus,
    OnboardRequest,
)
from query_builder import QueryBuilder
from bsl_agent import ask as bsl_ask
from bsl_model_builder import get_tenant_semantic_models, get_tenant_metadata
from llm_provider import get_llm_provider

logger = logging.getLogger(__name__)

app = FastAPI(title="GATA Platform API", version="0.2.0")
TENANTS_YAML = Path(__file__).parent.parent.parent / "tenants.yaml"

CORS_ORIGINS = os.environ.get(
    "CORS_ORIGINS",
    "http://localhost:8000,http://localhost:8002,http://localhost:3000"
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[o.strip() for o in CORS_ORIGINS.split(",")],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- Helpers ---

def _get_db_connection() -> duckdb.DuckDBPyConnection:
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    if md_token:
        conn_str = f"md:my_db?motherduck_token={md_token}"
    elif os.environ.get("GATA_ENV") == "local":
        conn_str = str(Path(__file__).parent.parent.parent / "warehouse" / "sandbox.duckdb")
    else:
        conn_str = "md:my_db"
    return duckdb.connect(conn_str)


def _get_query_builder(tenant_slug: str) -> QueryBuilder:
    """Get a QueryBuilder for the tenant.

    Prefers hand-written YAML config if it exists, otherwise auto-generates
    a QueryBuilder-compatible config from the BSL metadata catalog.
    """
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if config_path.exists():
        with open(config_path) as f:
            config = yaml.safe_load(f)
        return QueryBuilder(config)

    # Auto-generate from BSL metadata (no YAML needed)
    _get_bsl_models(tenant_slug)  # ensure models + metadata are built
    metadata = get_tenant_metadata(tenant_slug)
    if not metadata:
        raise HTTPException(
            status_code=404,
            detail=f"No semantic models found for tenant: {tenant_slug}. Run dbt first.",
        )

    models = []
    for subject, meta in metadata.items():
        table_name = meta["table"]
        columns = meta.get("columns", {})

        dimensions = [
            {"name": col_name, "type": info.get("bsl_type", "string")}
            for col_name, info in columns.items()
            if info.get("role") == "dimension"
        ]
        measures = [
            {"name": col_name, "type": info.get("bsl_type", "number"), "agg": info.get("agg", "sum")}
            for col_name, info in columns.items()
            if info.get("role") == "measure"
        ]

        # Map join targets from subject name → physical table name
        joins = []
        for j in meta.get("joins", []):
            target_subject = j["to"]
            target_table = (
                metadata[target_subject]["table"]
                if target_subject in metadata
                else target_subject
            )
            joins.append({
                "to": target_table,
                "type": j.get("type", "left"),
                "on": j.get("on", {}),
            })

        models.append({
            "name": table_name,
            "label": meta.get("label", subject),
            "description": meta.get("description", ""),
            "dimensions": dimensions,
            "measures": measures,
            "calculated_measures": meta.get("calculated_measures", []),
            "joins": joins,
        })

    logger.info(f"Auto-generated QueryBuilder config for '{tenant_slug}': {len(models)} models")
    return QueryBuilder({"models": models})


def _get_bsl_models(tenant_slug: str) -> dict:
    """Get BSL SemanticModel objects for a tenant (cached)."""
    try:
        return get_tenant_semantic_models(tenant_slug)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail=f"No config for tenant: {tenant_slug}")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to build BSL models for {tenant_slug}: {e}")
        raise HTTPException(status_code=500, detail=f"BSL model build failed: {e}")


# ═══════════════════════════════════════════════════════════
# Health Check (Render)
# ═══════════════════════════════════════════════════════════

@app.get("/health")
def health_check():
    return {"status": "healthy", "service": "gata-platform-api"}


# ═══════════════════════════════════════════════════════════
# Readiness Endpoint (tenant pipeline status)
# ═══════════════════════════════════════════════════════════

@app.get("/readiness/{tenant_slug}", response_model=ReadinessStatus)
def check_readiness(tenant_slug: str):
    """Check if a tenant's data pipeline is ready for querying.

    Infers pipeline state from warehouse data:
      - ready:      BSL column catalog has rows for tenant
      - cataloging: boring_semantic_layer has rows but catalog not yet
      - modeling:   tenant exists in tenants.yaml, dbt still running
      - ingesting:  tenant exists, data landing in progress
      - starting:   tenant just registered
      - error:      something went wrong
    """
    try:
        con = _get_db_connection()

        # Check BSL column catalog (last to materialize = fully ready)
        try:
            bsl_count = con.execute(
                "SELECT COUNT(*) FROM main.platform_ops__bsl_column_catalog WHERE tenant_slug = ?",
                [tenant_slug],
            ).fetchone()[0]
        except duckdb.Error:
            bsl_count = 0

        if bsl_count > 0:
            # Pipeline complete — get a load_id from semantic layer
            load_id = None
            try:
                row = con.execute(
                    "SELECT table_name FROM main.platform_ops__boring_semantic_layer WHERE tenant_slug = ? LIMIT 1",
                    [tenant_slug],
                ).fetchone()
                if row:
                    load_id = row[0]  # use table_name as reference marker
            except duckdb.Error:
                pass
            con.close()
            return ReadinessStatus(
                is_ready=True,
                last_load_id=load_id,
                status="ready",
                message=f"Pipeline complete — {bsl_count} columns cataloged",
            )

        # Check boring semantic layer (analytics tables exist but catalog not yet populated)
        try:
            bsl_rows = con.execute(
                "SELECT COUNT(*) FROM main.platform_ops__boring_semantic_layer WHERE tenant_slug = ?",
                [tenant_slug],
            ).fetchone()[0]
        except duckdb.Error:
            bsl_rows = 0

        if bsl_rows > 0:
            con.close()
            return ReadinessStatus(
                is_ready=False,
                status="cataloging",
                message="Indexing semantic layer...",
            )

        con.close()

        # Check tenants.yaml to determine if tenant is registered
        if TENANTS_YAML.exists():
            with open(TENANTS_YAML) as f:
                tenants_cfg = yaml.safe_load(f) or {}
            for t in tenants_cfg.get("tenants", []):
                if t.get("slug") == tenant_slug:
                    tenant_status = t.get("status", "unknown")
                    if tenant_status == "onboarding":
                        return ReadinessStatus(
                            is_ready=False,
                            status="modeling",
                            message="Building star schema...",
                        )
                    elif tenant_status == "active":
                        # Active but no catalog data — might need a dbt run
                        return ReadinessStatus(
                            is_ready=False,
                            status="cataloging",
                            message="Refreshing catalog...",
                        )
                    else:
                        return ReadinessStatus(
                            is_ready=False,
                            status="starting",
                            message="Initializing pipeline...",
                        )

        # Tenant not found at all
        return ReadinessStatus(
            is_ready=False,
            status="error",
            message=f"Tenant '{tenant_slug}' not found",
        )
    except Exception as e:
        logger.error(f"Readiness check failed for {tenant_slug}: {e}")
        return ReadinessStatus(
            is_ready=False,
            status="error",
            message=str(e),
        )


# ═══════════════════════════════════════════════════════════
# Tenant Onboarding Endpoint
# ═══════════════════════════════════════════════════════════

@app.post("/onboard")
async def onboard_tenant(request: OnboardRequest, background_tasks: BackgroundTasks):
    """Register a new tenant and kick off the onboarding pipeline.

    1. Writes tenant config to tenants.yaml
    2. Launches mock data generation + dbt pipeline in background
    3. Returns immediately — frontend polls /readiness/{slug} for status
    """
    import sys
    from pathlib import Path as _Path

    tenant_slug = request.tenant_slug
    business_name = request.business_name
    sources = request.sources

    # 1. Update tenants.yaml with the new tenant
    tenants_path = _Path(__file__).parent.parent.parent / "tenants.yaml"
    with open(tenants_path) as f:
        config = yaml.safe_load(f) or {"tenants": []}

    existing = next((t for t in config["tenants"] if t["slug"] == tenant_slug), None)
    if existing:
        existing["sources"] = sources
        existing["business_name"] = business_name
        logger.info(f"[ONBOARD] Updated existing tenant: {tenant_slug}")
    else:
        config["tenants"].append({
            "slug": tenant_slug,
            "business_name": business_name,
            "status": "onboarding",
            "sources": sources,
        })
        logger.info(f"[ONBOARD] Registered new tenant: {tenant_slug}")

    with open(tenants_path, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)

    # 2. Launch onboarding pipeline in background
    project_root = _Path(__file__).parent.parent.parent
    sys.path.insert(0, str(project_root / "scripts"))
    sys.path.insert(0, str(project_root / "services" / "mock-data-engine"))

    from onboard_tenant import onboard as run_onboard

    def _run_pipeline():
        try:
            exit_code = run_onboard(tenant_slug, target="dev", days=180)
            if exit_code == 0:
                logger.info(f"[ONBOARD] Pipeline completed for {tenant_slug}")
            else:
                logger.error(f"[ONBOARD] Pipeline failed for {tenant_slug} (exit {exit_code})")
        except Exception as e:
            logger.error(f"[ONBOARD] Pipeline error for {tenant_slug}: {e}")

    background_tasks.add_task(_run_pipeline)

    return {
        "success": True,
        "tenant_slug": tenant_slug,
        "message": f"Onboarding pipeline started for {tenant_slug}",
    }


# ═══════════════════════════════════════════════════════════
# BSL LLM Status Endpoints (before {tenant_slug} routes)
# ═══════════════════════════════════════════════════════════

@app.get("/health/llm", response_model=LLMProviderStatus)
def get_llm_health():
    """Lightweight health check for backend LLM availability.

    Used by the dashboard to determine if conversational analytics
    can be enabled (requires both WebLLM and backend LLM).
    """
    provider = get_llm_provider()
    return LLMProviderStatus(
        provider=provider.provider_name,
        model=provider.model_name,
        is_available=provider.is_available,
        error=provider.error_message,
    )


@app.get("/semantic-layer/llm-status", response_model=LLMProviderStatus)
def get_llm_status():
    """Check the current LLM provider status."""
    provider = get_llm_provider()
    return LLMProviderStatus(
        provider=provider.provider_name,
        model=provider.model_name,
        is_available=provider.is_available,
        error=provider.error_message,
    )


@app.post("/semantic-layer/llm-refresh")
def refresh_llm_provider():
    """Force refresh the LLM provider (re-check Ollama availability)."""
    provider = get_llm_provider(force_refresh=True)
    return {
        "status": "refreshed",
        "provider": provider.provider_name,
        "is_available": provider.is_available,
        "model": provider.model_name,
        "error": provider.error_message,
    }


# ═══════════════════════════════════════════════════════════
# BSL-Powered Semantic Layer Endpoints
# ═══════════════════════════════════════════════════════════

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


@app.get("/semantic-layer/{tenant_slug}/catalog")
def get_catalog(tenant_slug: str):
    """Full semantic catalog for frontend consumption.

    Returns all models with their dimensions, measures, calculated measures,
    joins, and metadata. This replaces the static JSON files the frontend
    previously loaded.
    """
    _get_bsl_models(tenant_slug)  # ensure models + metadata are built
    metadata = get_tenant_metadata(tenant_slug)

    catalog = {}
    for model_name, model_meta in metadata.items():
        columns = model_meta.get("columns", {})
        dims = {
            col_name: {
                "type": info.get("bsl_type", "string"),
                "is_time_dimension": info.get("is_time_dimension", False),
            }
            for col_name, info in columns.items()
            if info.get("role") == "dimension"
        }
        measures = {
            col_name: {
                "type": info.get("bsl_type", "number"),
                "agg": info.get("agg", "sum"),
            }
            for col_name, info in columns.items()
            if info.get("role") == "measure"
        }
        catalog[model_name] = {
            "label": model_meta.get("label", model_name),
            "description": model_meta.get("description", ""),
            "table": model_meta.get("table", ""),
            "dimensions": dims,
            "measures": measures,
            "calculated_measures": model_meta.get("calculated_measures", []),
            "joins": model_meta.get("joins", []),
            "dimension_count": len(dims),
            "measure_count": len(measures),
            "has_joins": model_meta.get("has_joins", False),
        }
    return catalog


# ═══════════════════════════════════════════════════════════
# Raw Catalog + Config Endpoints (preserved)
# ═══════════════════════════════════════════════════════════

@app.get("/semantic-layer/{tenant_slug}")
def get_semantic_layer(tenant_slug: str):
    """Return raw dbt catalog manifests from platform_ops__boring_semantic_layer."""
    try:
        con = _get_db_connection()
        result = con.execute("""
            SELECT semantic_manifest
            FROM main.platform_ops__boring_semantic_layer
            WHERE tenant_slug = ?
        """, [tenant_slug]).fetchall()
        con.close()

        if not result:
            return {"manifests": []}
        return {"manifests": [json.loads(r[0]) for r in result]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/semantic-layer/{tenant_slug}/config")
def get_semantic_config(tenant_slug: str):
    """Returns the hand-written YAML semantic config for a tenant (optional override).

    YAML configs are optional enrichments — tenants get full BSL functionality
    from the auto-classified catalog even without a YAML config file.
    """
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        return {"models": [], "_note": "No YAML override config — using auto-generated catalog"}
    with open(config_path) as f:
        return yaml.safe_load(f)


@app.post("/semantic-layer/update")
async def update_logic(tenant_slug: str, platform: str, logic_payload: dict):
    """Update tenant logic in tenants.yaml and trigger dbt refresh."""
    if os.environ.get("RENDER"):
        raise HTTPException(501, "Use the dbt pipeline for production updates")
    with open(TENANTS_YAML, "r") as f:
        config = yaml.safe_load(f)

    for tenant in config.get("tenants", []):
        if tenant["slug"] == tenant_slug:
            if platform in tenant.get("sources", {}):
                if "logic" not in tenant["sources"][platform]:
                    tenant["sources"][platform]["logic"] = {}
                tenant["sources"][platform]["logic"] = logic_payload
            break

    with open(TENANTS_YAML, "w") as f:
        yaml.safe_dump(config, f)

    try:
        project_root = Path(__file__).parent.parent.parent
        dbt_cwd = project_root / "warehouse" / "gata_transformation"
        subprocess.run(["dbt", "run", "--select", "platform"], check=True, cwd=dbt_cwd)

        # Invalidate BSL model + metadata caches after dbt run
        from bsl_model_builder import _tenant_cache, _tenant_metadata_cache
        _tenant_cache.pop(tenant_slug, None)
        _tenant_metadata_cache.pop(tenant_slug, None)

        return {"status": "success", "message": f"Logic updated for {tenant_slug}"}
    except subprocess.CalledProcessError:
        raise HTTPException(status_code=500, detail="dbt execution failed")


# ═══════════════════════════════════════════════════════════
# Model Discovery Endpoints (metadata-driven from dbt catalog)
# ═══════════════════════════════════════════════════════════

@app.get("/semantic-layer/{tenant_slug}/models", response_model=list[ModelSummary])
def list_models(tenant_slug: str):
    """List available semantic models (auto-discovered from dbt catalog).

    Uses metadata (from platform_ops__bsl_column_catalog) for accurate counts.
    Calculated measures are not included in measure_count.
    """
    models = _get_bsl_models(tenant_slug)
    metadata = get_tenant_metadata(tenant_slug)
    result = []
    for name, model in models.items():
        meta = metadata.get(name, {})
        columns = meta.get("columns", {})
        dim_count = sum(1 for c in columns.values() if c.get("role") == "dimension")
        measure_count = sum(1 for c in columns.values() if c.get("role") == "measure")
        result.append(ModelSummary(
            name=name,
            label=meta.get("label", model.description or name),
            description=meta.get("description", f"Semantic model: {name}"),
            dimension_count=dim_count,
            measure_count=measure_count,
            has_joins=meta.get("has_joins", False),
        ))
    return result


@app.get("/semantic-layer/{tenant_slug}/models/{model_name}", response_model=ModelDetail)
def get_model_detail(tenant_slug: str, model_name: str):
    """Get detailed schema for a specific model.

    Returns enriched metadata: dimensions with types, measures with aggs,
    auto-inferred calculated measures, and auto-inferred joins.
    """
    models = _get_bsl_models(tenant_slug)
    if model_name not in models:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")

    metadata = get_tenant_metadata(tenant_slug)
    model_meta = metadata.get(model_name, {})
    columns = model_meta.get("columns", {})

    # Build dimensions from metadata (accurate types)
    dims = [
        {
            "name": col_name,
            "type": info.get("bsl_type", "string"),
            "is_time_dimension": info.get("is_time_dimension", False),
        }
        for col_name, info in columns.items()
        if info.get("role") == "dimension"
    ]

    # Build measures from metadata (accurate aggs)
    measures = [
        {
            "name": col_name,
            "type": info.get("bsl_type", "number"),
            "agg": info.get("agg", "sum"),
        }
        for col_name, info in columns.items()
        if info.get("role") == "measure"
    ]

    return ModelDetail(
        name=model_name,
        label=model_meta.get("label", model_name),
        description=model_meta.get("description", ""),
        dimensions=dims,
        measures=measures,
        calculated_measures=model_meta.get("calculated_measures", []),
        joins=model_meta.get("joins", []),
    )


# ═══════════════════════════════════════════════════════════
# Query Execution (structured — preserved)
# ═══════════════════════════════════════════════════════════

@app.post("/semantic-layer/{tenant_slug}/query", response_model=SemanticQueryResponse)
def execute_query(tenant_slug: str, request: SemanticQueryRequest):
    """Execute a structured semantic query via the QueryBuilder.

    The frontend sends BSL subject names (e.g., 'ad_performance') but the
    QueryBuilder uses physical table names (e.g., 'fct_tenant__ad_performance').
    We translate here so both YAML-based and auto-generated configs work.
    """
    # Resolve subject name → physical table name
    metadata = get_tenant_metadata(tenant_slug)
    if request.model in metadata:
        request.model = metadata[request.model]["table"]
    for i, j in enumerate(request.joins):
        if j in metadata:
            request.joins[i] = metadata[j]["table"]

    qb = _get_query_builder(tenant_slug)
    try:
        sql, params = qb.build_query(tenant_slug, request)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    try:
        con = _get_db_connection()
        result = con.execute(sql, params)
        columns = [ColumnInfo(name=desc[0], type=str(desc[1])) for desc in result.description]
        rows = result.fetchall()
        data = [dict(zip([c.name for c in columns], row)) for row in rows]
        con.close()
        return SemanticQueryResponse(sql=sql, data=data, columns=columns, row_count=len(data))
    except duckdb.Error as e:
        raise HTTPException(status_code=500, detail=f"Query execution error: {e}")


# ═══════════════════════════════════════════════════════════
# BSL Natural Language Agent Endpoint
# ═══════════════════════════════════════════════════════════

@app.post("/semantic-layer/{tenant_slug}/ask", response_model=AskResponse)
def ask_question(tenant_slug: str, request: AskRequest):
    """Ask a natural language analytics question.

    Routes through BSL agent with Ollama LLM if available,
    falls back to keyword-based model suggestion otherwise.
    """
    # Validate tenant exists in BSL catalog
    _get_bsl_models(tenant_slug)

    result = bsl_ask(request.question, tenant_slug, request.semantic_context)

    # Trim records to max_records
    if len(result.records) > request.max_records:
        result.records = result.records[:request.max_records]

    return AskResponse(**result.to_dict())


# ═══════════════════════════════════════════════════════════
# Observability Endpoints (unchanged)
# ═══════════════════════════════════════════════════════════

@app.get("/observability/{tenant_slug}/summary", response_model=ObservabilitySummary)
def get_observability_summary(tenant_slug: str):
    try:
        con = _get_db_connection()
        row = con.execute("""
            SELECT
                COUNT(DISTINCT model_name) AS models_count,
                MAX(run_started_at) AS last_run_at,
                COUNT(CASE WHEN status = 'success' THEN 1 END) AS pass_count,
                COUNT(CASE WHEN status = 'fail' THEN 1 END) AS fail_count,
                COUNT(CASE WHEN status = 'error' THEN 1 END) AS error_count,
                COUNT(CASE WHEN status = 'skipped' THEN 1 END) AS skip_count,
                AVG(execution_time_seconds) AS avg_execution_time
            FROM main.int_platform_observability__tenant_run_results
            WHERE tenant_slug = ?
        """, [tenant_slug]).fetchone()
        con.close()

        if not row or row[0] == 0:
            raise HTTPException(status_code=404, detail=f"No run data for tenant: {tenant_slug}")

        return ObservabilitySummary(
            tenant_slug=tenant_slug,
            models_count=row[0],
            last_run_at=str(row[1]) if row[1] else None,
            pass_count=row[2],
            fail_count=row[3],
            error_count=row[4],
            skip_count=row[5],
            avg_execution_time=round(row[6], 4) if row[6] else 0.0,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/observability/{tenant_slug}/runs", response_model=list[RunResult])
def get_run_results(tenant_slug: str, limit: int = 50):
    try:
        con = _get_db_connection()
        rows = con.execute("""
            SELECT model_name, status, rows_affected, execution_time_seconds, run_started_at
            FROM main.int_platform_observability__tenant_run_results
            WHERE tenant_slug = ?
            ORDER BY run_started_at DESC
            LIMIT ?
        """, [tenant_slug, limit]).fetchall()
        con.close()

        if not rows:
            raise HTTPException(status_code=404, detail=f"No run data for tenant: {tenant_slug}")

        return [
            RunResult(
                model_name=r[0],
                status=r[1],
                rows_affected=r[2],
                execution_time_seconds=r[3],
                run_started_at=str(r[4]),
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/observability/{tenant_slug}/tests", response_model=list[TestResult])
def get_test_results(tenant_slug: str, limit: int = 50):
    try:
        con = _get_db_connection()
        rows = con.execute("""
            SELECT test_name, status, message, execution_time_seconds, run_started_at
            FROM main.int_platform_observability__tenant_test_results
            WHERE tenant_slug = ?
            ORDER BY run_started_at DESC
            LIMIT ?
        """, [tenant_slug, limit]).fetchall()
        con.close()

        if not rows:
            raise HTTPException(status_code=404, detail=f"No test data for tenant: {tenant_slug}")

        return [
            TestResult(
                test_name=r[0],
                status=r[1],
                message=r[2],
                execution_time_seconds=r[3],
                run_started_at=str(r[4]),
            )
            for r in rows
        ]
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/observability/{tenant_slug}/identity-resolution", response_model=IdentityResolutionStats)
def get_identity_resolution(tenant_slug: str):
    try:
        con = _get_db_connection()
        row = con.execute("""
            SELECT tenant_slug, total_users, resolved_customers, anonymous_users,
                   identity_resolution_rate, total_events, total_sessions
            FROM main.int_platform_observability__identity_resolution_stats
            WHERE tenant_slug = ?
            ORDER BY dlt_load_id DESC
            LIMIT 1
        """, [tenant_slug]).fetchone()
        con.close()

        if not row:
            raise HTTPException(status_code=404, detail=f"No identity resolution data for tenant: {tenant_slug}")

        return IdentityResolutionStats(
            tenant_slug=row[0],
            total_users=row[1],
            resolved_customers=row[2],
            anonymous_users=row[3],
            resolution_rate=float(row[4]),
            total_events=row[5],
            total_sessions=row[6],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    port = int(os.environ.get("PLATFORM_API_PORT", "8001"))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
