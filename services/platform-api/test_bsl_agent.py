"""Tests for BSL v2 — metadata-driven with ECharts."""

import pytest
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


class TestModelBuilder:
    """Test auto-generation from dbt catalog."""

    def test_classify_column_varchar_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("source_platform", "VARCHAR") == "dimension"

    def test_classify_column_double_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("spend", "DOUBLE") == "measure"

    def test_classify_column_bigint_id_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("campaign_id", "BIGINT") == "dimension"

    def test_classify_column_bigint_count_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("total_clicks", "BIGINT") == "measure"

    def test_classify_column_json_is_skipped(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("line_items_json", "JSON") == "skip"

    def test_classify_column_tenant_slug_is_skipped(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("tenant_slug", "VARCHAR") == "skip"

    def test_infer_aggregation_spend_is_sum(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("spend", "DOUBLE") == "sum"

    def test_infer_aggregation_duration_is_avg(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("session_duration_seconds", "DOUBLE") == "avg"

    def test_convert_ctr_calc_measure(self):
        from bsl_model_builder import _convert_calculated_measure
        calc = {"name": "ctr", "label": "CTR", "sql": "..."}
        result = _convert_calculated_measure(calc, {})
        assert result is not None
        assert "impressions" in result and "clicks" in result

    def test_convert_cpc_calc_measure(self):
        from bsl_model_builder import _convert_calculated_measure
        calc = {"name": "cpc", "label": "CPC"}
        result = _convert_calculated_measure(calc, {})
        assert result is not None
        assert "spend" in result and "clicks" in result

    def test_convert_unknown_calc_measure_returns_none(self):
        from bsl_model_builder import _convert_calculated_measure
        calc = {"name": "custom_metric", "sql": "CUSTOM SQL"}
        result = _convert_calculated_measure(calc, {})
        assert result is None

    def test_ibis_agg_expr_sum(self):
        from bsl_model_builder import _ibis_agg_expr
        assert _ibis_agg_expr("spend", "sum") == "_.spend.sum()"

    def test_ibis_agg_expr_count_distinct(self):
        from bsl_model_builder import _ibis_agg_expr
        assert _ibis_agg_expr("order_id", "count_distinct") == "_.order_id.nunique()"


class TestAgentResponse:
    """Test ECharts extraction from BSLTools responses."""

    def test_extract_query_results_with_chart(self):
        from bsl_agent import _extract_query_results, AgentResponse

        result_json = json.dumps({
            "total_rows": 3,
            "columns": ["source_platform", "spend"],
            "records": [{"source_platform": "facebook", "spend": 100}],
            "chart": {
                "backend": "echarts",
                "format": "json",
                "data": {"xAxis": {}, "yAxis": {}, "series": []}
            }
        })

        response = AgentResponse()
        _extract_query_results(result_json, response, {"query": "ad_performance.group_by(...)"})

        assert len(response.records) == 1
        assert response.chart_spec is not None
        assert "xAxis" in response.chart_spec
        assert response.model_used == "ad_performance"

    def test_extract_query_results_no_chart(self):
        from bsl_agent import _extract_query_results, AgentResponse

        result_json = json.dumps({
            "total_rows": 1,
            "columns": ["total_spend"],
            "records": [{"total_spend": 500}],
        })

        response = AgentResponse()
        _extract_query_results(result_json, response, {"query": "ad_performance.aggregate(...)"})

        assert len(response.records) == 1
        assert response.chart_spec is None

    def test_extract_query_results_invalid_json(self):
        from bsl_agent import _extract_query_results, AgentResponse

        response = AgentResponse()
        _extract_query_results("not valid json", response, {"query": "test"})

        # Should not crash, records should stay empty
        assert response.records == []
        assert response.chart_spec is None

    def test_extract_query_results_chart_without_data_key(self):
        from bsl_agent import _extract_query_results, AgentResponse

        result_json = json.dumps({
            "records": [{"x": 1}],
            "chart": {"backend": "echarts"}  # no "data" key
        })

        response = AgentResponse()
        _extract_query_results(result_json, response, {"query": "test.group_by(...)"})

        assert len(response.records) == 1
        assert response.chart_spec is None  # no data key → no chart


class TestKeywordFallback:
    """Test keyword-based model selection."""

    def test_ad_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "ad_performance" in KEYWORD_MAP
        assert "spend" in KEYWORD_MAP["ad_performance"]

    def test_order_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "orders" in KEYWORD_MAP
        assert "revenue" in KEYWORD_MAP["orders"]

    def test_session_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "sessions" in KEYWORD_MAP
        assert "conversion" in KEYWORD_MAP["sessions"]

    def test_event_keywords(self):
        from bsl_agent import KEYWORD_MAP
        assert "events" in KEYWORD_MAP
        assert "pageview" in KEYWORD_MAP["events"]


class TestLLMProvider:
    """Test LLM provider resolution."""

    def test_ollama_unavailable(self):
        from llm_provider import _try_ollama, LLMProviderConfig
        config = LLMProviderConfig(ollama_base_url="http://localhost:99999")
        result = _try_ollama(config)
        assert not result.is_available

    def test_provider_none(self):
        from llm_provider import get_llm_provider
        with patch.dict("os.environ", {"BSL_LLM_PROVIDER": "none"}):
            provider = get_llm_provider(force_refresh=True)
            assert provider.llm is None
            assert provider.provider_name == "none"

    def test_anthropic_without_key_unavailable(self):
        from llm_provider import _try_anthropic, LLMProviderConfig
        with patch.dict("os.environ", {}, clear=True):
            config = LLMProviderConfig()
            result = _try_anthropic(config)
            assert not result.is_available
            assert "ANTHROPIC_API_KEY not set" in result.error_message


class TestGenerateBSLConfig:
    """Test BSL config generation from catalog data."""

    def test_plain_string_dimension(self):
        from bsl_model_builder import _generate_bsl_config

        catalog = [{
            "table_name": "fct_test__ad_performance",
            "table_type": "fact",
            "subject": "ad_performance",
            "columns": [
                {"column_name": "source_platform", "data_type": "VARCHAR"},
            ],
        }]

        config = _generate_bsl_config(catalog, {}, None)
        assert config["ad_performance"]["dimensions"]["source_platform"] == "_.source_platform"

    def test_time_dimension_gets_dict(self):
        from bsl_model_builder import _generate_bsl_config

        catalog = [{
            "table_name": "fct_test__orders",
            "table_type": "fact",
            "subject": "orders",
            "columns": [
                {"column_name": "order_date", "data_type": "DATE"},
            ],
        }]

        config = _generate_bsl_config(catalog, {}, None)
        dim = config["orders"]["dimensions"]["order_date"]
        assert isinstance(dim, dict)
        assert dim["is_time_dimension"] is True

    def test_plain_string_measure(self):
        from bsl_model_builder import _generate_bsl_config

        catalog = [{
            "table_name": "fct_test__ad_performance",
            "table_type": "fact",
            "subject": "ad_performance",
            "columns": [
                {"column_name": "spend", "data_type": "DOUBLE"},
            ],
        }]

        config = _generate_bsl_config(catalog, {}, None)
        assert config["ad_performance"]["measures"]["spend"] == "_.spend.sum()"

    def test_calculated_measures_not_in_bsl_config(self):
        """Calc measures (ibis.ifelse) are kept in metadata only, not BSL config.

        BSL's from_config() eval context doesn't include `ibis`, so calculated
        measures like CTR/CPC that use ibis.ifelse() cannot be parsed at config
        build time. They live in the metadata dict for API consumption only.
        """
        from bsl_model_builder import _generate_bsl_config

        catalog = [{
            "table_name": "fct_test__ad_performance",
            "table_type": "fact",
            "subject": "ad_performance",
            "columns": [
                {"column_name": "spend", "data_type": "DOUBLE"},
                {"column_name": "impressions", "data_type": "DOUBLE"},
                {"column_name": "clicks", "data_type": "DOUBLE"},
            ],
        }]
        enrichments = {
            "fct_test__ad_performance": {
                "calculated_measures": [
                    {"name": "ctr", "label": "Click-Through Rate"},
                    {"name": "cpc", "label": "Cost Per Click"},
                ],
                "dimension_overrides": {},
                "measure_overrides": {},
            }
        }

        config = _generate_bsl_config(catalog, enrichments, None)
        # Only base measures (spend, impressions, clicks) in BSL config
        assert "ctr" not in config["ad_performance"]["measures"]
        assert "cpc" not in config["ad_performance"]["measures"]
        assert "spend" in config["ad_performance"]["measures"]
        assert "impressions" in config["ad_performance"]["measures"]
        assert "clicks" in config["ad_performance"]["measures"]

    def test_table_field_matches_physical_name(self):
        from bsl_model_builder import _generate_bsl_config

        catalog = [{
            "table_name": "fct_tyrell_corp__orders",
            "table_type": "fact",
            "subject": "orders",
            "columns": [],
        }]

        config = _generate_bsl_config(catalog, {}, None)
        assert config["orders"]["table"] == "fct_tyrell_corp__orders"
