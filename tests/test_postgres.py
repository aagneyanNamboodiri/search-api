from unittest.mock import patch

from src.db.postgres import (
    fetch_detected_schema_ids,
    fetch_session_metric_ids,
    fetch_session_uuids,
)


class TestFetchDetectedSchemaIds:
    @patch("src.db.postgres._execute_query")
    def test_single_entity(self, mock_exec):
        mock_exec.return_value = [(100, "common_brand"), (101, "common_brand")]
        result = fetch_detected_schema_ids(
            "tenant_a", [("common_brand", "brand_id", [36, 37])]
        )
        assert result == [(100, "common_brand"), (101, "common_brand")]
        sql = mock_exec.call_args[0][0]
        assert "brand_id" in sql
        assert "36" in sql
        assert "37" in sql

    @patch("src.db.postgres._execute_query")
    def test_multiple_entities_uses_union_all(self, mock_exec):
        mock_exec.return_value = []
        fetch_detected_schema_ids("tenant_a", [
            ("common_brand", "brand_id", [1]),
            ("common_category", "category_id", [2]),
        ])
        sql = mock_exec.call_args[0][0]
        assert "UNION ALL" in sql

    def test_empty_queries_returns_empty(self):
        assert fetch_detected_schema_ids("tenant_a", []) == []

    @patch("src.db.postgres._execute_query")
    def test_skips_entity_with_empty_ids(self, mock_exec):
        mock_exec.return_value = [(100, "common_brand")]
        fetch_detected_schema_ids("tenant_a", [
            ("common_brand", "brand_id", [1]),
            ("common_category", "category_id", []),
        ])
        sql = mock_exec.call_args[0][0]
        assert "UNION ALL" not in sql

    @patch("src.db.postgres._execute_query")
    def test_sql_is_schema_qualified(self, mock_exec):
        mock_exec.return_value = []
        fetch_detected_schema_ids(
            "scm_acme", [("common_brand", "brand_id", [1])]
        )
        sql = mock_exec.call_args[0][0]
        assert "scm_acme" in sql
        assert "analytics_detected_schema" in sql


class TestFetchSessionMetricIds:
    @patch("src.db.postgres._execute_query")
    def test_returns_pairs(self, mock_exec):
        mock_exec.return_value = [(500, 100), (501, 101)]
        result = fetch_session_metric_ids("tenant_a", [100, 101])
        assert result == [(500, 100), (501, 101)]

    def test_empty_ids_returns_empty(self):
        assert fetch_session_metric_ids("tenant_a", []) == []

    @patch("src.db.postgres._execute_query")
    def test_queries_shelf_facings(self, mock_exec):
        mock_exec.return_value = []
        fetch_session_metric_ids("scm_acme", [100])
        sql = mock_exec.call_args[0][0]
        assert "analytics_shelf_facings_detail" in sql
        assert "detected_schema_id" in sql

    @patch("src.db.postgres._execute_query")
    def test_sql_is_schema_qualified(self, mock_exec):
        mock_exec.return_value = []
        fetch_session_metric_ids("scm_acme", [100])
        sql = mock_exec.call_args[0][0]
        assert "scm_acme" in sql


class TestFetchSessionUuids:
    @patch("src.db.postgres._execute_query")
    def test_returns_id_uuid_pairs(self, mock_exec):
        mock_exec.return_value = [(500, "uuid-abc"), (501, "uuid-def")]
        result = fetch_session_uuids("tenant_a", [500, 501])
        assert result == [(500, "uuid-abc"), (501, "uuid-def")]

    def test_empty_ids_returns_empty(self):
        assert fetch_session_uuids("tenant_a", []) == []

    @patch("src.db.postgres._execute_query")
    def test_filters_soft_deleted(self, mock_exec):
        mock_exec.return_value = []
        fetch_session_uuids("scm_acme", [500])
        sql = mock_exec.call_args[0][0]
        assert "is_deleted" in sql

    @patch("src.db.postgres._execute_query")
    def test_sql_is_schema_qualified(self, mock_exec):
        mock_exec.return_value = []
        fetch_session_uuids("scm_acme", [500])
        sql = mock_exec.call_args[0][0]
        assert "scm_acme" in sql
        assert "analytics_session_facings_detail" in sql
