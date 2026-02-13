from pydantic import BaseModel, field_validator
from typing import Literal


# --- Query API Models ---

VALID_OPS = {"=", "!=", ">", "<", ">=", "<=", "IN", "LIKE", "BETWEEN", "IS NULL", "IS NOT NULL"}


class QueryFilter(BaseModel):
    field: str
    op: str
    value: str | int | float | list | None = None

    @field_validator("op")
    @classmethod
    def validate_operator(cls, v: str) -> str:
        if v.upper() not in VALID_OPS:
            raise ValueError(f"Invalid operator '{v}'. Must be one of: {', '.join(sorted(VALID_OPS))}")
        return v.upper()


class OrderByClause(BaseModel):
    field: str
    dir: Literal["asc", "desc"] = "asc"


class SemanticQueryRequest(BaseModel):
    model: str
    dimensions: list[str] = []
    measures: list[str] = []
    calculated_measures: list[str] = []
    filters: list[QueryFilter] = []
    joins: list[str] = []
    order_by: list[OrderByClause] = []
    limit: int | None = 1000

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v: int | None) -> int | None:
        if v is not None and v > 10000:
            raise ValueError("Limit cannot exceed 10000")
        return v


class ColumnInfo(BaseModel):
    name: str
    type: str


class SemanticQueryResponse(BaseModel):
    sql: str
    data: list[dict]
    columns: list[ColumnInfo]
    row_count: int


# --- Model Discovery Models ---

class ModelSummary(BaseModel):
    name: str
    label: str
    description: str
    dimension_count: int = 0
    measure_count: int = 0
    has_joins: bool = False


class ModelDetail(BaseModel):
    name: str
    label: str
    description: str
    dimensions: list[dict] = []
    measures: list[dict] = []
    calculated_measures: list[dict] = []
    joins: list[dict] = []


# --- Observability Models ---

class ObservabilitySummary(BaseModel):
    tenant_slug: str
    models_count: int
    last_run_at: str | None
    pass_count: int
    fail_count: int
    error_count: int
    skip_count: int
    avg_execution_time: float


class RunResult(BaseModel):
    model_name: str
    status: str
    rows_affected: int | None
    execution_time_seconds: float
    run_started_at: str


class TestResult(BaseModel):
    test_name: str
    status: str
    message: str | None
    execution_time_seconds: float
    run_started_at: str


class IdentityResolutionStats(BaseModel):
    tenant_slug: str
    total_users: int
    resolved_customers: int
    anonymous_users: int
    resolution_rate: float
    total_events: int
    total_sessions: int


# --- BSL Agent Models ---

class AskRequest(BaseModel):
    """Natural language analytics question."""
    question: str
    max_records: int = 100

    @field_validator("max_records")
    @classmethod
    def validate_max_records(cls, v: int) -> int:
        if v > 1000:
            raise ValueError("max_records cannot exceed 1000")
        return v


class AskResponse(BaseModel):
    """Response from BSL agent."""
    answer: str
    records: list[dict] = []
    sql: str = ""
    chart_spec: dict | None = None
    model_used: str = ""
    provider: str = ""
    execution_time_ms: int = 0
    tool_calls: list[str] = []
    error: str | None = None


class LLMProviderStatus(BaseModel):
    """Status of the LLM provider."""
    provider: str
    model: str = ""
    is_available: bool = False
    error: str = ""


class ReadinessStatus(BaseModel):
    """Tenant pipeline readiness status."""
    is_ready: bool
    last_load_id: str | None = None
    status: Literal['starting', 'ingesting', 'modeling', 'cataloging', 'ready', 'error']
    message: str | None = None
    last_dbt_status: str | None = None
