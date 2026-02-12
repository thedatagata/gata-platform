"""
MCP Server for GATA Semantic Layer

Follows the dlthub demo's mcp_server.py pattern exactly:
  MCPSemanticModel wraps per-tenant BSL SemanticModel objects
  and exposes them as MCP tools (list_models, get_model, query_model).

Usage:
    python mcp_server.py
    python mcp_server.py --host 0.0.0.0 --port 9000 --tenant tyrell_corp

Or import and call run_mcp_server() programmatically.
"""

import argparse
import logging
import os
from typing import Optional

from boring_semantic_layer import MCPSemanticModel

from bsl_model_builder import create_tenant_semantic_models, _get_ibis_connection

logger = logging.getLogger(__name__)

MCP_SERVER_HOST = "0.0.0.0"
MCP_SERVER_PORT = 9000


def run_mcp_server(
    host: str = MCP_SERVER_HOST,
    port: int = MCP_SERVER_PORT,
    tenant_slug: Optional[str] = None,
):
    """Start the MCP server with BSL semantic models.

    If tenant_slug is provided, serves only that tenant's models.
    Otherwise, serves all tenants' models keyed by subject name
    prefixed with tenant slug (e.g., "tyrell_corp__ad_performance").
    """
    con = _get_ibis_connection()

    if tenant_slug:
        # Single-tenant mode (matches demo pattern exactly)
        models = create_tenant_semantic_models(tenant_slug, con=con)
        server_name = f"GATA Semantic Layer — {tenant_slug}"
    else:
        # Multi-tenant mode: load all tenants from the catalog
        from bsl_model_builder import _read_catalog
        # Discover all tenants
        result = con.raw_sql("""
            SELECT DISTINCT tenant_slug
            FROM main.platform_ops__boring_semantic_layer
        """).fetchall()
        tenant_slugs = [row[0] for row in result if row[0]]

        models = {}
        for slug in tenant_slugs:
            try:
                tenant_models = create_tenant_semantic_models(slug, con=con)
                for subject, model in tenant_models.items():
                    # Namespace models by tenant
                    models[f"{slug}__{subject}"] = model
                logger.info(f"[MCP] Loaded {len(tenant_models)} models for {slug}")
            except Exception as e:
                logger.warning(f"[MCP] Failed to load models for {slug}: {e}")

        server_name = "GATA Semantic Layer"

    logger.info(
        f"[MCP] Starting server '{server_name}' with {len(models)} models "
        f"on {host}:{port}"
    )

    mcp_server = MCPSemanticModel(
        models=models,
        name=server_name,
    )

    mcp_server.run(
        transport="streamable-http",
        **{
            "host": host,
            "port": port,
        }
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="GATA Semantic Layer MCP Server")

    parser.add_argument("--host", default=MCP_SERVER_HOST, type=str, help="Server host")
    parser.add_argument("--port", default=MCP_SERVER_PORT, type=int, help="Server port")
    parser.add_argument("--tenant", default=None, type=str, help="Single tenant slug (optional)")

    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    run_mcp_server(args.host, args.port, args.tenant)
