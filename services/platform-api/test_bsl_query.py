"""Tests for BSL auto-inference engine and metadata-driven API endpoints."""

import pytest
import json
from unittest.mock import patch, MagicMock
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))


# ── Auto-inference: column classification ──────────────────

class TestColumnClassification:
    """Test _classify_column() for enriched catalog classification."""

    def test_varchar_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("source_platform", "VARCHAR") == "dimension"

    def test_date_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("order_date", "DATE") == "dimension"

    def test_timestamp_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("event_timestamp", "TIMESTAMP") == "dimension"

    def test_boolean_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("is_conversion_session", "BOOLEAN") == "dimension"

    def test_double_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("spend", "DOUBLE") == "measure"

    def test_float_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("total_price", "FLOAT") == "measure"

    def test_bigint_id_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("campaign_id", "BIGINT") == "dimension"

    def test_bigint_key_is_dimension(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("campaign_key", "BIGINT") == "dimension"

    def test_bigint_total_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("total_clicks", "BIGINT") == "measure"

    def test_bigint_count_is_measure(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("count_sessions", "BIGINT") == "measure"

    def test_json_is_skip(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("line_items_json", "JSON") == "skip"

    def test_tenant_slug_is_skip(self):
        from bsl_model_builder import _classify_column
        assert _classify_column("tenant_slug", "VARCHAR") == "skip"


# ── Auto-inference: calculated measures ────────────────────

class TestAutoInferCalcMeasures:
    """Test _auto_infer_calculated_measures() column pattern detection."""

    def test_ctr_from_clicks_and_impressions(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "ad_performance": [
                {"column_name": "clicks", "data_type": "BIGINT"},
                {"column_name": "impressions", "data_type": "BIGINT"},
                {"column_name": "spend", "data_type": "DOUBLE"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = [c["name"] for c in result.get("ad_performance", [])]
        assert "ctr" in calc_names

    def test_cpc_from_spend_and_clicks(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "ad_performance": [
                {"column_name": "clicks", "data_type": "BIGINT"},
                {"column_name": "spend", "data_type": "DOUBLE"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = [c["name"] for c in result.get("ad_performance", [])]
        assert "cpc" in calc_names

    def test_cpm_from_spend_and_impressions(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "ad_performance": [
                {"column_name": "impressions", "data_type": "BIGINT"},
                {"column_name": "spend", "data_type": "DOUBLE"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = [c["name"] for c in result.get("ad_performance", [])]
        assert "cpm" in calc_names

    def test_aov_from_price_and_order_id(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "orders": [
                {"column_name": "total_price", "data_type": "DOUBLE"},
                {"column_name": "order_id", "data_type": "BIGINT"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = [c["name"] for c in result.get("orders", [])]
        assert "aov" in calc_names

    def test_conversion_rate_from_session_columns(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "sessions": [
                {"column_name": "is_conversion_session", "data_type": "BOOLEAN"},
                {"column_name": "session_id", "data_type": "VARCHAR"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = [c["name"] for c in result.get("sessions", [])]
        assert "conversion_rate" in calc_names

    def test_no_calc_measures_when_columns_missing(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "ad_performance": [
                {"column_name": "spend", "data_type": "DOUBLE"},
                # No clicks or impressions
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        assert "ad_performance" not in result

    def test_all_ad_calc_measures_inferred(self):
        from bsl_model_builder import _auto_infer_calculated_measures
        columns_by_subject = {
            "ad_performance": [
                {"column_name": "spend", "data_type": "DOUBLE"},
                {"column_name": "clicks", "data_type": "BIGINT"},
                {"column_name": "impressions", "data_type": "BIGINT"},
            ]
        }
        result = _auto_infer_calculated_measures(columns_by_subject)
        calc_names = {c["name"] for c in result.get("ad_performance", [])}
        assert calc_names == {"ctr", "cpc", "cpm"}


# ── Auto-inference: joins ──────────────────────────────────

class TestAutoInferJoins:
    """Test _auto_infer_joins() column matching across fact/dim tables."""

    def test_join_on_matching_id_column(self):
        from bsl_model_builder import _auto_infer_joins
        catalog = [
            {
                "table_name": "fct_test__ad_performance",
                "table_type": "fact",
                "subject": "ad_performance",
                "columns": [
                    {"column_name": "campaign_id", "data_type": "BIGINT"},
                    {"column_name": "spend", "data_type": "DOUBLE"},
                ],
            },
            {
                "table_name": "dim_test__campaigns",
                "table_type": "dimension",
                "subject": "campaigns",
                "columns": [
                    {"column_name": "campaign_id", "data_type": "BIGINT"},
                    {"column_name": "campaign_name", "data_type": "VARCHAR"},
                ],
            },
        ]
        result = _auto_infer_joins(catalog)
        assert "ad_performance" in result
        assert result["ad_performance"][0]["to"] == "campaigns"
        assert result["ad_performance"][0]["on"] == {"campaign_id": "campaign_id"}

    def test_no_join_on_excluded_columns(self):
        from bsl_model_builder import _auto_infer_joins
        catalog = [
            {
                "table_name": "fct_test__ad_performance",
                "table_type": "fact",
                "subject": "ad_performance",
                "columns": [
                    {"column_name": "tenant_slug", "data_type": "VARCHAR"},
                    {"column_name": "source_platform", "data_type": "VARCHAR"},
                    {"column_name": "spend", "data_type": "DOUBLE"},
                ],
            },
            {
                "table_name": "dim_test__campaigns",
                "table_type": "dimension",
                "subject": "campaigns",
                "columns": [
                    {"column_name": "tenant_slug", "data_type": "VARCHAR"},
                    {"column_name": "source_platform", "data_type": "VARCHAR"},
                    {"column_name": "campaign_name", "data_type": "VARCHAR"},
                ],
            },
        ]
        result = _auto_infer_joins(catalog)
        assert "ad_performance" not in result

    def test_prefers_id_column_as_join_key(self):
        from bsl_model_builder import _auto_infer_joins
        catalog = [
            {
                "table_name": "fct_test__sessions",
                "table_type": "fact",
                "subject": "sessions",
                "columns": [
                    {"column_name": "user_pseudo_id", "data_type": "VARCHAR"},
                    {"column_name": "device_category", "data_type": "VARCHAR"},
                ],
            },
            {
                "table_name": "dim_test__users",
                "table_type": "dimension",
                "subject": "users",
                "columns": [
                    {"column_name": "user_pseudo_id", "data_type": "VARCHAR"},
                    {"column_name": "device_category", "data_type": "VARCHAR"},
                ],
            },
        ]
        result = _auto_infer_joins(catalog)
        assert "sessions" in result
        # Should prefer user_pseudo_id (ends with _id) over device_category
        assert result["sessions"][0]["on"] == {"user_pseudo_id": "user_pseudo_id"}

    def test_no_joins_without_dim_tables(self):
        from bsl_model_builder import _auto_infer_joins
        catalog = [
            {
                "table_name": "fct_test__ad_performance",
                "table_type": "fact",
                "subject": "ad_performance",
                "columns": [{"column_name": "spend", "data_type": "DOUBLE"}],
            },
        ]
        result = _auto_infer_joins(catalog)
        assert result == {}


# ── Column metadata builder ───────────────────────────────

class TestBuildColumnMetadata:
    """Test _build_column_metadata() API metadata construction."""

    def _make_catalog(self):
        return [{
            "table_name": "fct_test__ad_performance",
            "table_type": "fact",
            "subject": "ad_performance",
            "columns": [
                {"column_name": "source_platform", "data_type": "VARCHAR",
                 "semantic_role": "dimension", "bsl_type": "string",
                 "is_time_dimension": False, "inferred_agg": None},
                {"column_name": "report_date", "data_type": "DATE",
                 "semantic_role": "dimension", "bsl_type": "date",
                 "is_time_dimension": True, "inferred_agg": None},
                {"column_name": "spend", "data_type": "DOUBLE",
                 "semantic_role": "measure", "bsl_type": "number",
                 "is_time_dimension": False, "inferred_agg": "sum"},
                {"column_name": "impressions", "data_type": "BIGINT",
                 "semantic_role": "measure", "bsl_type": "number",
                 "is_time_dimension": False, "inferred_agg": "sum"},
                {"column_name": "clicks", "data_type": "BIGINT",
                 "semantic_role": "measure", "bsl_type": "number",
                 "is_time_dimension": False, "inferred_agg": "sum"},
            ],
        }]

    def test_dimension_has_correct_type(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["columns"]["source_platform"]["bsl_type"] == "string"
        assert meta["ad_performance"]["columns"]["source_platform"]["role"] == "dimension"

    def test_date_dimension_flagged(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["columns"]["report_date"]["is_time_dimension"] is True
        assert meta["ad_performance"]["columns"]["report_date"]["bsl_type"] == "date"

    def test_measure_has_agg(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["columns"]["spend"]["agg"] == "sum"
        assert meta["ad_performance"]["columns"]["spend"]["role"] == "measure"

    def test_auto_label_derived_from_subject(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["label"] == "Ad Performance"

    def test_auto_description_from_table_type(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["description"] == "Fact: ad_performance"

    def test_calc_measures_included(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        auto_calcs = {
            "ad_performance": [
                {"name": "ctr", "label": "CTR", "sql": "...", "format": "percent"},
            ]
        }
        meta = _build_column_metadata(catalog, auto_calcs, {}, {})
        calc_names = [c["name"] for c in meta["ad_performance"]["calculated_measures"]]
        assert "ctr" in calc_names

    def test_joins_included(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        auto_joins = {
            "ad_performance": [
                {"to": "campaigns", "type": "left", "on": {"campaign_id": "campaign_id"}},
            ]
        }
        meta = _build_column_metadata(catalog, {}, auto_joins, {})
        assert meta["ad_performance"]["has_joins"] is True
        assert meta["ad_performance"]["joins"][0]["to"] == "campaigns"

    def test_yaml_description_overrides_auto(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        enrichments = {
            "fct_test__ad_performance": {
                "description": "Paid advertising performance metrics",
                "label": "Ads",
                "dimension_overrides": {},
                "measure_overrides": {},
            }
        }
        meta = _build_column_metadata(catalog, {}, {}, enrichments)
        assert meta["ad_performance"]["description"] == "Paid advertising performance metrics"
        assert meta["ad_performance"]["label"] == "Ads"

    def test_table_field_matches_physical_name(self):
        from bsl_model_builder import _build_column_metadata
        catalog = self._make_catalog()
        meta = _build_column_metadata(catalog, {}, {}, {})
        assert meta["ad_performance"]["table"] == "fct_test__ad_performance"


# ── BSL config generation ─────────────────────────────────

class TestGenerateBSLConfigEnriched:
    """Test that enriched catalog produces correct BSL config."""

    def test_dimension_with_date_gets_is_time_dimension(self):
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

    def test_measure_gets_ibis_expr(self):
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


# ── Infer aggregation ─────────────────────────────────────

class TestInferAggregation:
    """Test aggregation inference from column names."""

    def test_duration_is_avg(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("session_duration_seconds", "DOUBLE") == "avg"

    def test_id_is_count_distinct(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("order_id", "BIGINT") == "count_distinct"

    def test_spend_is_sum(self):
        from bsl_model_builder import _infer_aggregation
        assert _infer_aggregation("spend", "DOUBLE") == "sum"

    def test_count_column_is_sum(self):
        from bsl_model_builder import _infer_aggregation
        # count_sessions is pre-aggregated, so sum is correct
        assert _infer_aggregation("count_sessions", "BIGINT") == "sum"
