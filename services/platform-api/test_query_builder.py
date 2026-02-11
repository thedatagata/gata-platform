import pytest
import yaml
from pathlib import Path
from pydantic import ValidationError

from query_builder import QueryBuilder
from models import SemanticQueryRequest, QueryFilter


@pytest.fixture
def tyrell_config():
    config_path = Path(__file__).parent / "semantic_configs" / "tyrell_corp.yaml"
    with open(config_path) as f:
        return yaml.safe_load(f)


@pytest.fixture
def qb(tyrell_config):
    return QueryBuilder(tyrell_config)


def test_simple_dimensions_only(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform", "report_date"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "source_platform" in sql
    assert "report_date" in sql
    assert "GROUP BY" not in sql
    assert params == ["tyrell_corp"]


def test_dimensions_with_measures(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform"],
        measures=["spend", "clicks"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "SUM(spend)" in sql
    assert "SUM(clicks)" in sql
    assert "GROUP BY 1" in sql
    assert params == ["tyrell_corp"]


def test_calculated_measures(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform"],
        measures=["spend", "impressions", "clicks"],
        calculated_measures=["ctr"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "AS ctr" in sql
    assert "SUM(impressions)" in sql


def test_count_distinct_agg(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__orders",
        measures=["order_id"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "COUNT(DISTINCT order_id)" in sql


def test_join(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform"],
        measures=["spend"],
        joins=["dim_tyrell_corp__campaigns"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "LEFT JOIN dim_tyrell_corp__campaigns" in sql
    assert "base.campaign_id = dim_tyrell_corp__campaigns.campaign_id" in sql
    assert "base.source_platform = dim_tyrell_corp__campaigns.source_platform" in sql
    assert "FROM fct_tyrell_corp__ad_performance AS base" in sql


def test_filters(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform"],
        filters=[QueryFilter(field="report_date", op=">=", value="2025-01-01")],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "report_date >= ?" in sql
    assert "tyrell_corp" in params
    assert "2025-01-01" in params


def test_tenant_isolation(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["source_platform"],
    )
    sql, params = qb.build_query("tyrell_corp", req)
    assert "tenant_slug = ?" in sql
    assert params[0] == "tyrell_corp"


def test_invalid_dimension(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        dimensions=["nonexistent_field"],
    )
    with pytest.raises(ValueError, match="Unknown dimension"):
        qb.build_query("tyrell_corp", req)


def test_invalid_join(qb):
    req = SemanticQueryRequest(
        model="fct_tyrell_corp__ad_performance",
        joins=["dim_nonexistent"],
    )
    with pytest.raises(ValueError, match="Unknown join target"):
        qb.build_query("tyrell_corp", req)


def test_invalid_filter_operator():
    with pytest.raises(ValidationError):
        QueryFilter(field="x", op="DROP TABLE", value="y")


def test_list_models(qb):
    models = qb.list_models()
    assert len(models) == 6


def test_model_summary_fields(qb):
    summary = qb.get_model_summary("fct_tyrell_corp__ad_performance")
    assert summary["name"] == "fct_tyrell_corp__ad_performance"
    assert summary["label"] == "Ad Performance"
    assert summary["description"] != ""
    assert summary["dimension_count"] == 5
    assert summary["measure_count"] == 4
    assert summary["has_joins"] is True
