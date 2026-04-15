from unittest.mock import patch, MagicMock

from src.constants import SearchField, SearchableEntity
from src.db.elasticsearch import build_msearch_payload, msearch_es


BRAND_ENTITY = SearchableEntity(
    table_name="common_brand",
    search_fields=[
        SearchField(name="internal_name", boost=1.0),
        SearchField(name="external_name", boost=0.2),
    ],
)
CATEGORY_ENTITY = SearchableEntity(
    table_name="common_category",
    search_fields=[
        SearchField(name="internal_name", boost=1.0),
    ],
    fuzziness="1",
)


class TestBuildMsearchPayload:
    def test_returns_header_body_pairs(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY], page=1, page_size=10
        )
        assert len(result) == 2
        assert result[0] == {"index": "tenant_a"}
        assert isinstance(result[1], dict)

    def test_body_contains_from_and_size(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY], page=2, page_size=5
        )
        body = result[1]
        assert body["from"] == 5
        assert body["size"] == 5

    def test_page_1_has_from_zero(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY], page=1, page_size=15
        )
        assert result[1]["from"] == 0

    def test_must_clause_has_table_name_term(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY]
        )
        must = result[1]["query"]["bool"]["must"]
        assert must == [{"term": {"table_name.keyword": "common_brand"}}]

    def test_should_clause_has_search_fields(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY]
        )
        should = result[1]["query"]["bool"]["should"]
        assert len(should) == 2

        field_names = [list(s["match"].keys())[0] for s in should]
        assert "internal_name" in field_names
        assert "external_name" in field_names

    def test_fuzziness_and_boost_passed_through(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [CATEGORY_ENTITY]
        )
        should = result[1]["query"]["bool"]["should"]
        match_config = should[0]["match"]["internal_name"]
        assert match_config["fuzziness"] == "1"
        assert match_config["boost"] == 1.0

    def test_multiple_entities_produce_multiple_pairs(self):
        result = build_msearch_payload(
            "tenant_a", "beer", [BRAND_ENTITY, CATEGORY_ENTITY]
        )
        assert len(result) == 4
        assert result[0] == {"index": "tenant_a"}
        assert result[2] == {"index": "tenant_a"}

    def test_query_string_passed_to_match(self):
        result = build_msearch_payload(
            "tenant_a", "sooka", [BRAND_ENTITY]
        )
        should = result[1]["query"]["bool"]["should"]
        for match_clause in should:
            field_name = list(match_clause["match"].keys())[0]
            assert match_clause["match"][field_name]["query"] == "sooka"


class TestMsearchEs:
    def _mock_es_response(self, responses):
        return {"responses": responses}

    def _make_hit(self, source, score=1.0):
        return {"_source": source, "_score": score}

    @patch("src.db.elasticsearch.get_es_client")
    def test_returns_docs_and_total(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.msearch.return_value = self._mock_es_response([
            {
                "hits": {
                    "total": {"value": 50},
                    "hits": [
                        self._make_hit({"id": 1, "name": "Brand A"}, 5.0),
                    ],
                }
            }
        ])

        result = msearch_es("tenant_a", "beer", [BRAND_ENTITY])

        assert "common_brand" in result
        assert result["common_brand"]["total"] == 50
        assert len(result["common_brand"]["docs"]) == 1
        assert result["common_brand"]["docs"][0]["id"] == 1
        assert result["common_brand"]["docs"][0]["_score"] == 5.0

    @patch("src.db.elasticsearch.get_es_client")
    def test_empty_hits(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.msearch.return_value = self._mock_es_response([
            {"hits": {"total": {"value": 0}, "hits": []}}
        ])

        result = msearch_es("tenant_a", "zzz", [BRAND_ENTITY])

        assert result["common_brand"]["total"] == 0
        assert result["common_brand"]["docs"] == []

    @patch("src.db.elasticsearch.get_es_client")
    def test_pagination_params_forwarded(self, mock_get_client):
        mock_client = MagicMock()
        mock_get_client.return_value = mock_client
        mock_client.msearch.return_value = self._mock_es_response([
            {"hits": {"total": {"value": 0}, "hits": []}}
        ])

        msearch_es("tenant_a", "beer", [BRAND_ENTITY], page=3, page_size=10)

        call_args = mock_client.msearch.call_args
        searches = call_args.kwargs.get("searches", call_args.args[0] if call_args.args else None)
        body = searches[1]
        assert body["from"] == 20
        assert body["size"] == 10
