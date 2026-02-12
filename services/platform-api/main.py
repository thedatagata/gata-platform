from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import duckdb
import os
import json
import yaml
import subprocess
from pathlib import Path

from models import (
    SemanticQueryRequest, SemanticQueryResponse, ColumnInfo,
    ModelSummary, ModelDetail,
    ObservabilitySummary, RunResult, TestResult, IdentityResolutionStats,
    AskRequest, AskResponse, LLMProviderStatus,
)
from query_builder import QueryBuilder
from bsl_agent import ask as bsl_ask
from llm_provider import get_llm_provider

app = FastAPI()
TENANTS_YAML = Path(__file__).parent.parent.parent / "tenants.yaml"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8000", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# --- BSL LLM Status Endpoints (before {tenant_slug} routes to avoid path conflicts) ---

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
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        raise HTTPException(status_code=404, detail=f"No config for tenant: {tenant_slug}")
    with open(config_path) as f:
        config = yaml.safe_load(f)
    return QueryBuilder(config)


# --- Semantic Layer Endpoints (existing) ---

@app.get("/semantic-layer/{tenant_slug}")
def get_semantic_layer(tenant_slug: str):
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
    """Returns the BSL semantic config for a tenant."""
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        raise HTTPException(status_code=404, detail=f"No config for tenant: {tenant_slug}")
    with open(config_path) as f:
        return yaml.safe_load(f)


@app.post("/semantic-layer/update")
async def update_logic(tenant_slug: str, platform: str, logic_payload: dict):
    # 1. Update tenants.yaml
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

    # 2. Trigger dbt refresh
    try:
        project_root = Path(__file__).parent.parent.parent
        dbt_cwd = project_root / "warehouse" / "gata_transformation"
        subprocess.run(["dbt", "run", "--select", "platform"], check=True, cwd=dbt_cwd)
        return {"status": "success", "message": f"Logic updated for {tenant_slug}"}
    except subprocess.CalledProcessError:
        raise HTTPException(status_code=500, detail="dbt execution failed")


# --- Model Discovery Endpoints ---

@app.get("/semantic-layer/{tenant_slug}/models", response_model=list[ModelSummary])
def list_models(tenant_slug: str):
    qb = _get_query_builder(tenant_slug)
    return qb.list_models()


@app.get("/semantic-layer/{tenant_slug}/models/{model_name}", response_model=ModelDetail)
def get_model_detail(tenant_slug: str, model_name: str):
    qb = _get_query_builder(tenant_slug)
    try:
        return qb.get_model_detail(model_name)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"Model '{model_name}' not found")


# --- Query Execution Endpoint ---

@app.post("/semantic-layer/{tenant_slug}/query", response_model=SemanticQueryResponse)
def execute_query(tenant_slug: str, request: SemanticQueryRequest):
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


# --- Observability Endpoints ---

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


# --- BSL Natural Language Agent Endpoints ---

@app.post("/semantic-layer/{tenant_slug}/ask", response_model=AskResponse)
def ask_question(tenant_slug: str, request: AskRequest):
    """
    Ask a natural language analytics question.

    Routes through BSL agent with Ollama LLM if available,
    falls back to keyword-based model suggestion otherwise.
    """
    config_path = Path(__file__).parent / "semantic_configs" / f"{tenant_slug}.yaml"
    if not config_path.exists():
        raise HTTPException(status_code=404, detail=f"No config for tenant: {tenant_slug}")

    result = bsl_ask(request.question, tenant_slug)

    # Trim records to max_records
    if len(result.records) > request.max_records:
        result.records = result.records[:request.max_records]

    return AskResponse(**result.to_dict())
