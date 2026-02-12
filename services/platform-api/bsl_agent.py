"""
BSL Agent Service.

Orchestrates the LLM + BSLTools agent loop for natural language analytics.
Handles the full flow: question -> BSL tools -> MotherDuck query -> response.

When an LLM is available (Ollama running), uses BSLTools' agent pattern
with tool calling. When no LLM is available, falls back to keyword-based
model selection + the structured QueryBuilder.
"""

import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

from llm_provider import get_llm_provider, LLMProvider
from bsl_model_builder import (
    get_tenant_semantic_tables,
    _load_tenant_config,
)


@dataclass
class AgentResponse:
    """Response from the BSL agent."""
    answer: str = ""
    records: list[dict] = field(default_factory=list)
    sql: str = ""
    chart_spec: Optional[dict] = None
    model_used: str = ""
    provider: str = "none"
    execution_time_ms: int = 0
    tool_calls: list[dict] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "answer": self.answer,
            "records": self.records,
            "sql": self.sql,
            "chart_spec": self.chart_spec,
            "model_used": self.model_used,
            "provider": self.provider,
            "execution_time_ms": self.execution_time_ms,
            "tool_calls": self.tool_calls,
            "error": self.error,
        }


def _run_bsl_agent_loop(
    question: str,
    tenant_slug: str,
    provider: LLMProvider,
) -> AgentResponse:
    """
    Run the full BSL agent loop with an LLM.

    Uses LangChain tool-calling with BSL semantic tables. The LLM decides
    which tools to call (list_models, query_model, get_schema) and we
    execute them against the tenant's semantic tables.
    """
    start = time.time()
    response = AgentResponse(provider=provider.provider_name)

    try:
        # Get the tenant's semantic tables
        semantic_tables = get_tenant_semantic_tables(tenant_slug)
        if not semantic_tables:
            response.error = f"No semantic tables found for tenant: {tenant_slug}"
            return response

        # Load tenant config for context
        config = _load_tenant_config(tenant_slug)
        tenant_name = config.get("tenant", {}).get("business_name", tenant_slug)

        # Build the system prompt with tenant context
        model_descriptions = []
        for model_config in config.get("models", []):
            name = model_config["name"]
            label = model_config.get("label", name)
            desc = model_config.get("description", "")
            dims = [d["name"] for d in model_config.get("dimensions", [])]
            measures = [m["name"] for m in model_config.get("measures", [])]
            calc = [c["name"] for c in model_config.get("calculated_measures", [])]

            model_descriptions.append(
                f"- **{label}** (`{name}`): {desc}\n"
                f"  Dimensions: {', '.join(dims)}\n"
                f"  Measures: {', '.join(measures)}\n"
                f"  Calculated: {', '.join(calc) if calc else 'none'}"
            )

        system_prompt = (
            f"You are a data analyst for {tenant_name}. "
            "You answer questions about their analytics data by querying "
            "their semantic models.\n\n"
            f"Available models:\n{chr(10).join(model_descriptions)}\n\n"
            "When answering:\n"
            "1. Pick the most relevant model for the question\n"
            "2. Use query_semantic_model to fetch data\n"
            "3. Keep answers concise and data-driven\n"
            "4. If the question is ambiguous, pick the most likely interpretation\n\n"
            f"The data is for tenant '{tenant_slug}' and all queries are "
            "automatically scoped to their data."
        )

        # Build tools and bind to LLM
        from langchain_core.messages import SystemMessage, HumanMessage

        lc_messages = [
            SystemMessage(content=system_prompt),
            HumanMessage(content=question),
        ]

        tools = _build_bsl_tools(semantic_tables, tenant_slug)
        llm_with_tools = provider.llm.bind_tools(tools)
        ai_response = llm_with_tools.invoke(lc_messages)

        # Extract tool calls — either from structured tool_calls or
        # parsed from text content (smaller models like 7b often emit
        # tool call JSON as plain text instead of using the protocol)
        tool_calls_to_run = []

        if hasattr(ai_response, 'tool_calls') and ai_response.tool_calls:
            for tc in ai_response.tool_calls:
                tool_calls_to_run.append({
                    "name": tc.get("name", ""),
                    "args": tc.get("args", {}),
                })

        if not tool_calls_to_run:
            # Try parsing tool call JSON from the text content
            parsed = _parse_tool_call_from_text(
                getattr(ai_response, 'content', '') or ''
            )
            if parsed:
                tool_calls_to_run.append(parsed)

        # Execute tool calls
        tool_error = ""
        for tool_call in tool_calls_to_run:
            tool_name = tool_call["name"]
            tool_args = tool_call["args"]
            response.tool_calls.append(tool_call)

            result = _execute_bsl_tool(
                tool_name, tool_args, semantic_tables, tenant_slug
            )

            if result.get("records"):
                response.records = result["records"]
            if result.get("sql"):
                response.sql = result["sql"]
            if result.get("model"):
                response.model_used = result["model"]
            if result.get("error"):
                tool_error = result["error"]

        # Build the answer — prefer data summary over raw LLM text
        if response.records:
            response.answer = (
                f"Found {len(response.records)} records from "
                f"**{response.model_used}**."
            )
        elif tool_error:
            response.answer = f"Query failed: {tool_error}"
            response.error = tool_error
        elif tool_calls_to_run:
            # Tool calls were made but returned no data
            response.answer = (
                f"Query executed against {response.model_used or 'unknown model'} "
                f"but returned no records."
            )
        elif hasattr(ai_response, 'content') and ai_response.content:
            # No tool calls parsed — return the LLM's text as-is
            response.answer = ai_response.content
        else:
            response.answer = "I processed your question but didn't find matching data."

    except Exception as e:
        logger.error(f"[BSL Agent] Error: {e}", exc_info=True)
        response.error = str(e)
        response.answer = f"Error processing question: {e}"

    response.execution_time_ms = int((time.time() - start) * 1000)
    return response


def _parse_tool_call_from_text(content: str) -> Optional[dict]:
    """
    Parse a tool call from LLM text content.

    Smaller models (e.g., qwen2.5-coder:7b) often emit tool call JSON
    as plain text rather than using the structured tool-calling protocol.
    This function extracts the tool name and arguments from that text.
    """
    if not content:
        return None

    try:
        # Try direct JSON parse first
        data = json.loads(content.strip())
        if isinstance(data, dict) and "name" in data and "arguments" in data:
            return {"name": data["name"], "args": data["arguments"]}
    except (json.JSONDecodeError, KeyError):
        pass

    # Try extracting JSON from markdown code blocks or surrounding text
    import re
    json_patterns = [
        r'```(?:json)?\s*(\{.*?\})\s*```',  # ```json { ... } ```
        r'(\{[^{}]*"name"\s*:.*?"arguments"\s*:\s*\{.*?\}\s*\})',  # inline
    ]
    for pattern in json_patterns:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                if "name" in data and "arguments" in data:
                    return {"name": data["name"], "args": data["arguments"]}
            except (json.JSONDecodeError, KeyError):
                continue

    return None


def _build_bsl_tools(
    semantic_tables: dict,
    tenant_slug: str,
) -> list:
    """
    Build LangChain-compatible tool definitions from BSL semantic tables.

    These are tool schemas that the LLM uses to decide which queries to run.
    """
    from langchain_core.tools import tool

    table_names = list(semantic_tables.keys())

    @tool
    def list_available_models() -> str:
        """List all available semantic models with their dimensions and measures."""
        descriptions = []
        for name in semantic_tables:
            descriptions.append(f"Model: {name}")
        return "\n".join(descriptions)

    @tool
    def query_semantic_model(
        model_name: str,
        dimensions: list[str],
        measures: list[str],
        limit: int = 100,
    ) -> str:
        """
        Query a semantic model by grouping dimensions and aggregating measures.

        Args:
            model_name: Name of the model to query (e.g., 'fct_tyrell_corp__ad_performance')
            dimensions: List of dimension names to group by
            measures: List of measure names to aggregate
            limit: Maximum rows to return (default 100)
        """
        st = semantic_tables.get(model_name)
        if st is None:
            return f"Model '{model_name}' not found. Available: {table_names}"

        try:
            query = st
            if dimensions:
                query = query.group_by(*dimensions)
            if measures:
                query = query.aggregate(*measures)

            result = query.execute()

            # Convert to records
            if hasattr(result, 'to_dict'):
                records = result.to_dict(orient='records')
            else:
                records = result.to_pandas().head(limit).to_dict(orient='records')

            return json.dumps(records[:limit], default=str)

        except Exception as e:
            return f"Query error: {e}"

    @tool
    def get_model_schema(model_name: str) -> str:
        """
        Get the schema (dimensions and measures) for a specific model.

        Args:
            model_name: Name of the model to describe
        """
        st = semantic_tables.get(model_name)
        if st is None:
            return f"Model '{model_name}' not found. Available: {table_names}"

        try:
            info = {
                "name": model_name,
                "type": type(st).__name__,
                "repr": str(st),
            }
            return json.dumps(info, default=str)
        except Exception as e:
            return f"Error describing model: {e}"

    return [list_available_models, query_semantic_model, get_model_schema]


def _execute_bsl_tool(
    tool_name: str,
    tool_args: dict,
    semantic_tables: dict,
    tenant_slug: str,
) -> dict:
    """Execute a BSL tool call and return structured results."""
    result = {"records": [], "sql": "", "model": "", "error": ""}

    if tool_name == "query_semantic_model":
        model_name = tool_args.get("model_name", "")
        dimensions = tool_args.get("dimensions", [])
        measures = tool_args.get("measures", [])
        limit = tool_args.get("limit", 100)

        st = semantic_tables.get(model_name)
        if st is None:
            result["error"] = f"Model '{model_name}' not found"
            return result

        result["model"] = model_name

        # Filter dimensions/measures to only those that exist as
        # registered BSL fields — prevents errors from LLM hallucination
        valid_dims = []
        valid_measures = []

        config = _load_tenant_config(tenant_slug)
        for mc in config.get("models", []):
            if mc["name"] == model_name:
                known_dims = {d["name"] for d in mc.get("dimensions", [])}
                known_measures = {m["name"] for m in mc.get("measures", [])}
                valid_dims = [d for d in dimensions if d in known_dims]
                valid_measures = [m for m in measures if m in known_measures]

                skipped = (
                    set(dimensions) - known_dims
                ) | (set(measures) - known_measures)
                if skipped:
                    logger.warning(
                        f"[BSL Tool] Skipped unknown fields: {skipped}"
                    )
                break

        if not valid_dims and not valid_measures:
            result["error"] = (
                f"No valid dimensions or measures for {model_name}. "
                f"Requested dims={dimensions}, measures={measures}"
            )
            return result

        try:
            query = st
            if valid_dims:
                query = query.group_by(*valid_dims)
            if valid_measures:
                query = query.aggregate(*valid_measures)

            # Get the compiled SQL
            try:
                compiled = query.compile()
                result["sql"] = str(compiled)
            except Exception:
                result["sql"] = "(SQL compilation not available)"

            # Execute
            df = query.execute()
            if hasattr(df, 'to_dict'):
                result["records"] = df.head(limit).to_dict(orient='records')
            else:
                result["records"] = (
                    df.to_pandas().head(limit).to_dict(orient='records')
                )

        except Exception as e:
            logger.error(f"[BSL Tool] Query error: {e}")
            result["error"] = str(e)

    return result


def _fallback_keyword_search(
    question: str,
    tenant_slug: str,
) -> AgentResponse:
    """
    Fallback when no LLM is available.
    Uses keyword matching to select the most relevant model,
    then returns its data through the existing QueryBuilder.
    """
    start = time.time()
    response = AgentResponse(provider="keyword_fallback")

    config = _load_tenant_config(tenant_slug)
    question_lower = question.lower()

    # Keyword -> model mapping
    model_keywords = {
        "ad": "ad_performance",
        "spend": "ad_performance",
        "impression": "ad_performance",
        "click": "ad_performance",
        "ctr": "ad_performance",
        "campaign": "campaigns",
        "order": "orders",
        "revenue": "orders",
        "aov": "orders",
        "purchase": "orders",
        "session": "sessions",
        "conversion": "sessions",
        "bounce": "sessions",
        "event": "events",
        "pageview": "events",
        "user": "users",
        "customer": "users",
        "identity": "users",
    }

    # Find best matching model
    best_model = None
    best_score = 0
    for keyword, model_suffix in model_keywords.items():
        if keyword in question_lower:
            # Find the full model name for this tenant
            for m in config.get("models", []):
                if model_suffix in m["name"]:
                    score = len(keyword)
                    if score > best_score:
                        best_score = score
                        best_model = m
                        break

    if best_model:
        response.model_used = best_model["name"]
        dims = [d["name"] for d in best_model.get("dimensions", [])]
        measures = [m["name"] for m in best_model.get("measures", [])]
        response.answer = (
            f"Based on your question, the most relevant model is "
            f"**{best_model.get('label', best_model['name'])}** "
            f"(`{best_model['name']}`). "
            f"Dimensions: {', '.join(dims[:5])}. "
            f"Measures: {', '.join(measures[:5])}. "
            f"Use the structured query endpoint to query this model, "
            f"or install Ollama for natural language queries."
        )
    else:
        available = [m["name"] for m in config.get("models", [])]
        response.answer = (
            f"I couldn't determine the right model for your question. "
            f"Available models: {', '.join(available)}. "
            f"Try being more specific, or use the structured query endpoint."
        )

    response.execution_time_ms = int((time.time() - start) * 1000)
    return response


def ask(question: str, tenant_slug: str) -> AgentResponse:
    """
    Main entry point for natural language analytics questions.

    Routes to LLM agent loop if available, keyword fallback otherwise.
    """
    provider = get_llm_provider()

    if provider.is_available and provider.llm is not None:
        return _run_bsl_agent_loop(question, tenant_slug, provider)
    else:
        logger.info(
            f"[BSL Agent] No LLM available ({provider.error_message}). "
            f"Using keyword fallback."
        )
        return _fallback_keyword_search(question, tenant_slug)
