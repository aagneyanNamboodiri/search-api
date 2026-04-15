from unittest.mock import patch

from src.search.service import SearchProcessor


MOCK_ES_RESULTS = {
    "common_brand": {
        "docs": [
            {"id": 36, "name": "brand-a", "_score": 5.0},
            {"id": 37, "name": "brand-b", "_score": 4.0},
        ],
        "total": 41,
    },
    "common_category": {
        "docs": [
            {"id": 5, "name": "cat-a", "_score": 3.0},
        ],
        "total": 7,
    },
}


class TestExtractEntityQueries:
    def test_extracts_ids_grouped_by_fk(self):
        proc = SearchProcessor("tenant_a", "beer")
        queries = proc._extract_entity_queries(MOCK_ES_RESULTS)

        by_entity = {q[0]: q for q in queries}
        assert "common_brand" in by_entity
        assert by_entity["common_brand"] == ("common_brand", "brand_id", [36, 37])
        assert "common_category" in by_entity
        assert by_entity["common_category"] == ("common_category", "category_id", [5])

    def test_skips_docs_without_id(self):
        proc = SearchProcessor("tenant_a", "beer")
        es_results = {
            "common_brand": {
                "docs": [{"name": "no-id-doc", "_score": 1.0}],
                "total": 1,
            },
        }
        queries = proc._extract_entity_queries(es_results)
        assert queries == []

    def test_skips_unknown_entity(self):
        proc = SearchProcessor("tenant_a", "beer")
        es_results = {
            "common_unknown": {
                "docs": [{"id": 1, "_score": 1.0}],
                "total": 1,
            },
        }
        queries = proc._extract_entity_queries(es_results)
        assert queries == []


class TestGroupSessionsByEntity:
    def test_groups_correctly(self):
        proc = SearchProcessor("tenant_a", "beer")

        ds_rows = [(100, "common_brand"), (200, "common_category")]
        sm_rows = [(500, 100), (600, 200)]
        uuid_rows = [(500, "uuid-1"), (600, "uuid-2")]

        grouped = proc._group_sessions_by_entity(ds_rows, sm_rows, uuid_rows)

        assert grouped["common_brand"] == ["uuid-1"]
        assert grouped["common_category"] == ["uuid-2"]

    def test_session_in_multiple_entities(self):
        proc = SearchProcessor("tenant_a", "beer")

        ds_rows = [(100, "common_brand"), (100, "common_category")]
        sm_rows = [(500, 100)]
        uuid_rows = [(500, "uuid-1")]

        grouped = proc._group_sessions_by_entity(ds_rows, sm_rows, uuid_rows)

        assert "uuid-1" in grouped["common_brand"]
        assert "uuid-1" in grouped["common_category"]

    def test_empty_inputs(self):
        proc = SearchProcessor("tenant_a", "beer")
        grouped = proc._group_sessions_by_entity([], [], [])
        assert grouped == {}

    def test_deduplicates_uuids(self):
        proc = SearchProcessor("tenant_a", "beer")

        ds_rows = [(100, "common_brand"), (101, "common_brand")]
        sm_rows = [(500, 100), (501, 101)]
        uuid_rows = [(500, "uuid-same"), (501, "uuid-same")]

        grouped = proc._group_sessions_by_entity(ds_rows, sm_rows, uuid_rows)
        assert grouped["common_brand"] == ["uuid-same"]


class TestResolveEntities:
    def test_none_returns_all(self):
        proc = SearchProcessor("tenant_a", "beer", entity=None)
        assert len(proc.entities) == 2

    def test_filter_to_brand(self):
        proc = SearchProcessor("tenant_a", "beer", entity="common_brand")
        assert len(proc.entities) == 1
        assert proc.entities[0].table_name == "common_brand"

    def test_unknown_entity_returns_empty(self):
        proc = SearchProcessor("tenant_a", "beer", entity="common_nonexistent")
        assert proc.entities == []


class TestProcess:
    @patch("src.search.service.fetch_session_uuids")
    @patch("src.search.service.fetch_session_metric_ids")
    @patch("src.search.service.fetch_detected_schema_ids")
    @patch("src.search.service.msearch_es")
    def test_full_pipeline(self, mock_es, mock_ds, mock_sm, mock_uuid):
        mock_es.return_value = MOCK_ES_RESULTS
        mock_ds.return_value = [(100, "common_brand"), (200, "common_category")]
        mock_sm.return_value = [(500, 100), (600, 200)]
        mock_uuid.return_value = [(500, "uuid-1"), (600, "uuid-2")]

        proc = SearchProcessor("tenant_a", "beer")
        result = proc.process()

        assert "uuid-1" in result["results"]["common_brand"]["session_uuids"]
        assert "uuid-2" in result["results"]["common_category"]["session_uuids"]
        assert result["results"]["common_brand"]["has_sessions"] is True
        assert result["results"]["common_brand"]["total"] == 41
        assert "es_results" not in result

    @patch("src.search.service.fetch_session_uuids")
    @patch("src.search.service.fetch_session_metric_ids")
    @patch("src.search.service.fetch_detected_schema_ids")
    @patch("src.search.service.msearch_es")
    def test_debug_includes_es_results(self, mock_es, mock_ds, mock_sm, mock_uuid):
        mock_es.return_value = MOCK_ES_RESULTS
        mock_ds.return_value = []
        mock_sm.return_value = []
        mock_uuid.return_value = []

        proc = SearchProcessor("tenant_a", "beer", debug=True)
        result = proc.process()

        assert result["es_results"] is not None
        assert "common_brand" in result["es_results"]

    @patch("src.search.service.fetch_session_uuids")
    @patch("src.search.service.fetch_session_metric_ids")
    @patch("src.search.service.fetch_detected_schema_ids")
    @patch("src.search.service.msearch_es")
    def test_no_sessions_has_sessions_false(self, mock_es, mock_ds, mock_sm, mock_uuid):
        mock_es.return_value = {
            "common_brand": {"docs": [{"id": 1, "_score": 1.0}], "total": 1},
        }
        mock_ds.return_value = []
        mock_sm.return_value = []
        mock_uuid.return_value = []

        proc = SearchProcessor("tenant_a", "beer")
        result = proc.process()

        assert result["results"]["common_brand"]["has_sessions"] is False
        assert result["results"]["common_brand"]["session_uuids"] == []
