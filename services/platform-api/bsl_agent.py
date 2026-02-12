"""
BSL Agent Service — Natural Language Analytics via BSL Tools

Wraps per-tenant BSL SemanticModel objects (built from dbt metadata)
into an agent loop that the LLM can call via tool functions.

Architecture follows the dlthub demo pattern:
  - BSL SemanticModel.get_dimensions() / .get_measures() → catalog
  - BSL query API: model.group_by("dim").aggregate("measure")
  - LLM chooses which tools to call based on the user's question
  - Keyword fallback when no LLM is available

The key difference from vanilla BSLTools: instead of loading models from
a YAML file, we inject pre-built models from create_tenant_semantic_models()
which reads the dbt catalog.
"""

import json
import time
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, Any

logger = logging.getLogger(__name__)

# BSL imports
try:
    from boring_semantic_layer.agents.tools import BSLTools
    from boring_semantic_layer import SemanticModel
    BSL_AVAILABLE = True
except ImportError:
    BSL_AVAILABLE = False
    logger.warning("[BSL Agent] boring-semantic-layer not installed")

# LLM imports
try:
    from langchain_core.messages import HumanMessage, AIMessage, ToolMessage, SystemMessage
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False


# ───────────────────────────────────────────────────────────
# Response model
# ───────────────────────────────────────────────────────────

@dataclass
class AgentResponse:
    answer: str = ""
    records: list[dict] = field(default_factory=list)
    sql: str = ""
    chart_spec: Optional[dict] = None
    model_used: str = ""
    provider: str = "none"
    execution_time_ms: int = 0
    tool_calls: list[str] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return asdict(self)


# ───────────────────────────────────────────────────────────
# BSLTools subclass that accepts pre-built models
# ───────────────────────────────────────────────────────────

class GATABSLTools(BSLTools):
    """BSLTools variant for API mode with pre-built models.

    Overrides _query_model to force return_json=True and echarts backend
    since we're serving an API, not a CLI.
    """

    def __init__(
        self,
        models: dict[str, SemanticModel],
        chart_backend: str = "echarts",
    ):
        # Skip BSLTools.__init__ which calls from_yaml()
        # Instead, set the attributes directly
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
            error_str = str(e)
            if len(error_str) > 300:
                error_str = error_str[:300] + "..."

            error_msg = f"Query Error: {error_str}"

            # Try to provide schema hints on attribute errors
            model_name = query.split(".")[0] if "." in query else None
            if "has no attribute" in error_str and model_name:
                try:
                    schema = self._get_model(model_name)
                    error_msg += f"\n\nAvailable fields for '{model_name}':\n{schema}"
                except Exception:
                    pass

            from langchain_core.tools import ToolException
            raise ToolException(error_msg) from e


# ───────────────────────────────────────────────────────────
# Agent loop
# ───────────────────────────────────────────────────────────

def _build_system_prompt(tenant_slug: str, models: dict[str, SemanticModel]) -> str:
    """Build a system prompt with tenant context and model catalog."""
    model_descriptions = []
    for name, model in models.items():
        dims = list(model.get_dimensions().keys())
        measures = list(model.get_measures().keys())
        desc = model.description or name
        model_descriptions.append(
            f"- **{name}**: {desc}\n"
            f"  Dimensions: {', '.join(dims)}\n"
            f"  Measures: {', '.join(measures)}"
        )

    models_text = "\n".join(model_descriptions)

    return f"""You are a data analyst assistant for tenant '{tenant_slug}'.
You have access to semantic models that let you query analytics data.

Available models:
{models_text}

To answer questions:
1. Call list_models to see what's available
2. Call get_model(model_name) to see dimensions and measures
3. Call query_model with an Ibis-style query string like:
   model_name.group_by("dimension").aggregate("measure1", "measure2")

Query syntax examples:
- ad_performance.group_by("source_platform").aggregate("spend", "clicks")
- orders.group_by("financial_status").aggregate("total_price")
- sessions.group_by("traffic_source", "traffic_medium").aggregate("session_id", "session_revenue")

Always call get_model first to see exact field names before querying.
"""


def _extract_query_results(result_str: str, response: AgentResponse, tool_args: dict):
    """Parse BSLTools query_model JSON response to extract records + ECharts."""
    try:
        parsed = json.loads(result_str) if isinstance(result_str, str) else result_str

        if isinstance(parsed, dict):
            # Records
            if "records" in parsed:
                response.records = parsed["records"]

            # ECharts spec — extract from chart.data, not chart directly
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


# ───────────────────────────────────────────────────────────
# Keyword fallback (no LLM needed)
# ───────────────────────────────────────────────────────────

KEYWORD_MAP = {
    "ad_performance": ["ad", "ads", "spend", "impression", "click", "ctr", "cpc", "cpm", "campaign spend"],
    "orders": ["order", "revenue", "aov", "purchase", "transaction", "total_price", "sales"],
    "sessions": ["session", "conversion", "bounce", "traffic", "attribution", "utm"],
    "events": ["event", "pageview", "funnel", "page_view", "add_to_cart", "checkout"],
    "users": ["user", "customer", "identity", "anonymous", "visitor", "resolved"],
    "campaigns": ["campaign", "campaign_name", "campaign_status"],
}


def _fallback_keyword_search(
    question: str,
    models: dict[str, SemanticModel],
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


# ───────────────────────────────────────────────────────────
# Public API — main entry point
# ───────────────────────────────────────────────────────────

def ask(question: str, tenant_slug: str) -> AgentResponse:
    """Ask a natural language analytics question against a tenant's semantic models.

    Routes through:
    1. LLM agent loop (Ollama → Anthropic fallback) if available
    2. Keyword fallback if no LLM available

    This is the function wired to POST /semantic-layer/{tenant}/ask
    """
    from bsl_model_builder import get_tenant_semantic_models
    from llm_provider import get_llm_provider

    start = time.time()

    # Build BSL models from dbt metadata
    try:
        models = get_tenant_semantic_models(tenant_slug)
    except Exception as e:
        return AgentResponse(
            answer=f"Failed to load semantic models for '{tenant_slug}': {e}",
            error=str(e),
            execution_time_ms=int((time.time() - start) * 1000),
        )

    if not models:
        return AgentResponse(
            answer=f"No semantic models found for tenant '{tenant_slug}'.",
            error="No models",
        )

    # Create BSLTools wrapper with pre-built models
    bsl_tools = GATABSLTools(models=models, chart_backend="echarts")

    # Try LLM agent loop
    provider = get_llm_provider()
    if provider.is_available and provider.llm and LANGCHAIN_AVAILABLE:
        try:
            response = _run_agent_loop(question, bsl_tools, provider.llm, tenant_slug)
            response.provider = provider.provider_name
            return response
        except Exception as e:
            logger.warning(f"[BSL Agent] LLM agent failed, falling back: {e}")

    # Keyword fallback
    return _fallback_keyword_search(question, models)
