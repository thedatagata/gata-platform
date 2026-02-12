"""
BSL Query Materializer — Persist semantic query results via dlt

Follows the dlthub demo's materializer.py pattern exactly:
  Takes a BSL SemanticModel (query result) and pipes it into a dlt pipeline
  as a new table using dlt.resource with write_disposition="replace".

Usage as module:
    from materializer import materialize
    materialize(pipeline, query_result, "my_table_name")

Usage standalone:
    python materializer.py --pipeline gata_pipeline --table-name test_output
"""

import argparse
import logging

import dlt
from boring_semantic_layer import SemanticModel

logger = logging.getLogger(__name__)


def materialize(
    pipeline: dlt.Pipeline,
    semantic_model: SemanticModel,
    table_name: str,
) -> int:
    """Materialize a BSL query result into a dlt pipeline destination.

    This is the exact pattern from the dlthub demo — create a dlt.resource
    that yields the SemanticModel execution result, then run the pipeline.

    Args:
        pipeline: An attached dlt pipeline (e.g., dlt.attach(pipeline_name="gata"))
        semantic_model: A BSL SemanticModel with .execute() (after group_by/aggregate)
        table_name: Name for the materialized table in the destination

    Returns:
        0 on success
    """
    @dlt.resource(name=table_name, write_disposition="replace")
    def create():
        yield semantic_model.execute()

    logger.info(f"[Materializer] Materializing query to table: {table_name}")
    pipeline.run(create())
    logger.info(f"[Materializer] Successfully materialized: {table_name}")

    return 0


if __name__ == "__main__":
    from bsl_model_builder import create_tenant_semantic_models

    parser = argparse.ArgumentParser(description="Materialize a BSL query to dlt")
    parser.add_argument("-p", "--pipeline", required=False, type=str, default="gata_pipeline")
    parser.add_argument("-t", "--table-name", required=False, type=str, default="test_materialized")
    parser.add_argument("--tenant", required=False, type=str, default="tyrell_corp")

    args = parser.parse_args()

    pipeline = dlt.attach(pipeline_name=args.pipeline)
    models = create_tenant_semantic_models(args.tenant)

    # Example: top campaigns by ad spend
    ad_model = models.get("ad_performance")
    if ad_model:
        query = (
            ad_model
            .group_by("source_platform", "campaign_id")
            .aggregate("spend", "impressions", "clicks")
        )
        materialize(pipeline, query, args.table_name)
    else:
        print(f"No ad_performance model found for {args.tenant}")
