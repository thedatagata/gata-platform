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
import re
import time
import logging
import uuid
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

    @staticmethod
    def _sanitize_query(query: str) -> str:
        """Fix common LLM query syntax mistakes before evaluation.

        - Strips keyword argument syntax in aggregate: aggregate(x='y') → aggregate('y')
        - Strips aggregate wrappers: aggregate('sum(spend)') → aggregate('spend')
        """
        # Fix kwarg syntax: aggregate(total_sessions='total_sessions') → aggregate('total_sessions')
        def _fix_kwargs(match: re.Match) -> str:
            args_str = match.group(1)
            # Extract values from key='value' patterns
            cleaned = re.sub(r"\w+\s*=\s*(['\"])", r"\1", args_str)
            return f".aggregate({cleaned})"

        query = re.sub(r"\.aggregate\(([^)]+)\)", _fix_kwargs, query)

        # Strip agg wrappers: 'sum(spend)' → 'spend', 'count_distinct(session_id)' → 'session_id'
        query = re.sub(
            r"['\"](?:sum|avg|count|min|max|count_distinct)\((\w+)\)['\"]",
            r"'\1'",
            query,
            flags=re.IGNORECASE,
        )

        return query

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

        # Sanitize common LLM syntax mistakes
        query = self._sanitize_query(query)
        logger.debug(f"[BSL Agent] Executing query: {repr(query)}")

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

            # Always try to provide available fields — the BSL internal dumps
            # are huge and useless; the LLM needs to know what DOES work.
            model_name = query.split(".")[0] if "." in query else None
            schema_hint = ""
            if model_name and model_name in self.models:
                try:
                    dims = list(self.models[model_name].get_dimensions().keys())
                    measures = list(self.models[model_name].get_measures().keys())
                    schema_hint = (
                        f"\n\nAvailable fields for '{model_name}':\n"
                        f"  Dimensions: {', '.join(dims)}\n"
                        f"  Measures: {', '.join(measures)}"
                    )
                except Exception:
                    pass

            # Keep error message concise — truncate BSL internal dumps
            if len(error_str) > 150:
                error_str = error_str[:150] + "..."

            error_msg = f"Query failed: {error_str}{schema_hint}"

            from langchain_core.tools import ToolException
            raise ToolException(error_msg) from e


# ───────────────────────────────────────────────────────────
# Agent loop
# ───────────────────────────────────────────────────────────

def _build_system_prompt(
    tenant_slug: str,
    models: dict[str, SemanticModel],
    semantic_context: str = "",
) -> str:
    """Build a system prompt with tenant context and model catalog.

    If *semantic_context* is provided (from the frontend WebLLM enricher),
    it is injected as a hint section so the backend LLM can make better
    model/field choices without full exploration.
    """
    model_descriptions = []
    for name, model in models.items():
        dims = list(model.get_dimensions().keys())
        measures = list(model.get_measures().keys())

        # Add derived _date dimensions for epoch timestamps.
        # BSL's get_dimensions() doesn't include them, but the Ibis table
        # has them via mutate() and queries resolve correctly.
        extra_date_dims = []
        for d in dims:
            # Strip model prefix: "sessions.session_start_ts" → "session_start_ts"
            short = d.split(".")[-1] if "." in d else d
            if short.endswith("_ts") or short.endswith("_timestamp"):
                date_name = short.replace("_ts", "_date").replace("_timestamp", "_date")
                if date_name not in [x.split(".")[-1] for x in dims]:
                    extra_date_dims.append(f"{name}.{date_name}")
        all_dims = dims + extra_date_dims

        desc = model.description or name
        model_descriptions.append(
            f"- **{name}**: {desc}\n"
            f"  Dimensions: {', '.join(all_dims)}\n"
            f"  Measures: {', '.join(measures)}"
        )

    models_text = "\n".join(model_descriptions)

    context_section = ""
    if semantic_context:
        context_section = f"""

### Frontend Context Hints
The user's browser-side AI has analyzed the question and suggests:

{semantic_context}
"""

    return f"""You are a data analyst for tenant '{tenant_slug}'.

Available models (with EXACT field names you MUST use):
{models_text}
{context_section}
## Query syntax (CRITICAL — follow exactly)

query_model takes a SINGLE string argument called "query". The query string uses
this Ibis-style chain syntax with POSITIONAL string arguments:

  model_name.group_by('dim1', 'dim2').aggregate('measure1', 'measure2')

### Rules
- All dimension and measure names are positional STRING arguments in single quotes
- NEVER use keyword arguments like aggregate(x='y') — ONLY positional: aggregate('y')
- NEVER use aggregate wrappers like sum(spend) — just use the measure name: 'spend'
- Use EXACT field names from the model — do not invent names like 'session_start_date'
- For date-level time series, use _date dimensions (e.g. 'session_start_date', 'session_end_date')
  NOT the raw epoch _ts dimensions

### Examples
- ad_performance.group_by('source_platform').aggregate('spend', 'clicks')
- sessions.group_by('session_start_date', 'device_category').aggregate('total_sessions')
- sessions.group_by('session_start_date').aggregate('total_sessions', 'session_revenue')
- orders.group_by('financial_status').aggregate('total_price')
- ad_performance.group_by('report_date', 'source_platform').aggregate('spend', 'impressions')

Call get_model(model_name) first to see exact field names.
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


def _try_extract_text_tool_calls(
    content: str,
    model_names: list[str],
) -> list[tuple[str, dict]]:
    """Extract tool calls from text when the LLM outputs them as prose.

    Smaller Ollama models (qwen2.5-coder:7b etc.) sometimes output tool call
    descriptions as text instead of using LangChain's structured tool calling.
    This function recovers those by pattern-matching BSL expressions and
    JSON-like tool call blocks from the AI's text content.

    Returns a list of (tool_name, tool_args) tuples.
    """
    if not content:
        return []

    calls: list[tuple[str, dict]] = []

    # Strip code fences for uniform parsing
    stripped = re.sub(r"```(?:json|python)?\s*", "", content)
    stripped = stripped.replace("```", "")

    # --- Pattern 1: JSON tool call with "query" field ---
    # Matches: {"name": "query_model", "arguments": {"query": "sessions.group_by(...)..."}}
    query_match = re.search(
        r'"name"\s*:\s*"query_model"[\s\S]*?"query"\s*:\s*"((?:[^"\\]|\\.)*)"',
        stripped,
    )
    if query_match:
        query_expr = query_match.group(1).replace('\\"', '"')
        calls.append(("query_model", {"query": query_expr}))
        return calls

    # --- Pattern 2: JSON tool call with structured args (no "query" field) ---
    # Models often output: {"name": "query_model", "arguments": {"model_name": "sessions",
    #   "group_by": ["traffic_source"], "aggregate": ["session_revenue"]}}
    # Reconstruct a BSL expression from these structured args.
    if '"query_model"' in stripped and '"model_name"' in stripped:
        mn_match = re.search(r'"model_name"\s*:\s*"(\w+)"', stripped)
        if mn_match and mn_match.group(1) in model_names:
            model = mn_match.group(1)
            expr = _reconstruct_bsl_expression(model, stripped)
            if expr:
                calls.append(("query_model", {"query": expr}))
                return calls

    # --- Pattern 3: JSON tool call for get_model / list_models ---
    name_match = re.search(r'"name"\s*:\s*"(get_model|list_models)"', stripped)
    if name_match:
        tool_name = name_match.group(1)
        if tool_name == "get_model":
            mn_match = re.search(r'"model_name"\s*:\s*"(\w+)"', stripped)
            if mn_match:
                calls.append(("get_model", {"model_name": mn_match.group(1)}))
                return calls
        else:
            calls.append(("list_models", {}))
            return calls

    # --- Pattern 4: BSL expression in code blocks or inline ---
    # e.g., sessions.group_by("traffic_source").aggregate("session_revenue")
    for name in model_names:
        pattern = rf'({re.escape(name)}\.\w+\([\s\S]*?\)(?:\.\w+\([\s\S]*?\))*)'
        for match in re.finditer(pattern, stripped):
            expr = match.group(1).strip()
            if any(kw in expr for kw in ("group_by", "aggregate", "filter", "with_dimensions", "with_measures")):
                calls.append(("query_model", {"query": expr}))
                return calls

    # --- Pattern 5: Simple single-line fallback ---
    for line in stripped.split("\n"):
        line = line.strip().strip("`")
        for name in model_names:
            if line.startswith(name + ".") and (
                "group_by" in line or "aggregate" in line or "filter" in line
            ):
                calls.append(("query_model", {"query": line}))
                return calls

    return calls


def _reconstruct_bsl_expression(model_name: str, text: str) -> str:
    """Reconstruct a BSL query expression from structured JSON args.

    When the LLM outputs a tool call with model_name/group_by/aggregate fields
    instead of a query string, this builds the equivalent BSL expression.
    """
    def _extract_string_list(key: str) -> list[str]:
        """Extract a JSON array of strings for the given key."""
        pattern = rf'"{key}"\s*:\s*\[(.*?)\]'
        match = re.search(pattern, text, re.DOTALL)
        if not match:
            return []
        items = re.findall(r'"([^"]+)"', match.group(1))
        return items

    def _clean_measure(m: str) -> str:
        """Strip aggregate wrappers: sum(revenue) → revenue."""
        agg_match = re.match(r'(?:sum|avg|count|min|max|count_distinct)\((\w+)\)', m, re.IGNORECASE)
        return agg_match.group(1) if agg_match else m

    group_by = _extract_string_list("group_by")
    aggregate = [_clean_measure(m) for m in _extract_string_list("aggregate")]
    measures = [_clean_measure(m) for m in _extract_string_list("with_measures")]

    # Use aggregate if present, fall back to with_measures
    agg_fields = aggregate or measures
    if not group_by or not agg_fields:
        return ""

    dims_str = ", ".join(f"'{d}'" for d in group_by)
    agg_str = ", ".join(f"'{m}'" for m in agg_fields)
    return f"{model_name}.group_by({dims_str}).aggregate({agg_str})"


def _run_agent_loop(
    question: str,
    bsl_tools: GATABSLTools,
    llm: Any,
    tenant_slug: str,
    semantic_context: str = "",
) -> AgentResponse:
    """Run the LLM agent loop with BSLTools.

    BSLTools' execute() returns JSON strings. For query_model, the JSON
    contains {records, chart: {data: <echarts_option>}, total_rows}.
    We parse this to extract records and chart_spec for the response.

    Includes a text-based fallback: when the LLM outputs tool calls as
    prose instead of structured calls (common with smaller Ollama models),
    we parse the text for BSL expressions and execute them manually.
    """
    start = time.time()
    response = AgentResponse(provider="llm")

    system_prompt = _build_system_prompt(tenant_slug, bsl_tools.models, semantic_context)
    lc_tools = bsl_tools.get_callable_tools()
    llm_with_tools = llm.bind_tools(lc_tools)
    known_model_names = list(bsl_tools.models.keys())

    messages = [
        SystemMessage(content=system_prompt),
        HumanMessage(content=question),
    ]

    max_iterations = 8
    for i in range(max_iterations):
        ai_message = llm_with_tools.invoke(messages)
        messages.append(ai_message)

        tool_calls = ai_message.tool_calls or []

        # --- Fallback: parse text for tool calls when structured calling fails ---
        if not tool_calls and ai_message.content:
            text_calls = _try_extract_text_tool_calls(ai_message.content, known_model_names)
            if text_calls:
                logger.info(
                    f"[BSL Agent] Recovered {len(text_calls)} tool call(s) from text "
                    f"(model didn't use structured calling)"
                )
                # Convert to the same format as structured tool_calls
                tool_calls = [
                    {
                        "name": name,
                        "args": args,
                        "id": f"text_{uuid.uuid4().hex[:8]}",
                    }
                    for name, args in text_calls
                ]

        if not tool_calls:
            response.answer = ai_message.content
            break

        for tool_call in tool_calls:
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

def ask(question: str, tenant_slug: str, semantic_context: str = "") -> AgentResponse:
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
            response = _run_agent_loop(question, bsl_tools, provider.llm, tenant_slug, semantic_context)
            response.provider = provider.provider_name
            return response
        except Exception as e:
            logger.warning(f"[BSL Agent] LLM agent failed, falling back: {e}")

    # Keyword fallback
    return _fallback_keyword_search(question, models)
