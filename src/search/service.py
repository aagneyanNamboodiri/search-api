from src.constants import SearchableEntity, SEARCHABLE_ENTITIES, ENTITY_TO_DETECTED_SCHEMA_FK
from src.db.elasticsearch import msearch_es
from src.db.postgres import (
    fetch_detected_schema_ids,
    fetch_session_metric_ids,
    fetch_session_uuids,
)


class SearchProcessor:
    def __init__(
        self,
        tenant_name: str,
        search_query: str,
        entities: list[SearchableEntity] | None = None,
    ):
        self.tenant_name = tenant_name
        self.search_query = search_query
        self.entities = entities or SEARCHABLE_ENTITIES

    def _extract_entity_ids(
        self,
        es_results: dict[str, list[dict]],
    ) -> dict[str, list[int]]:
        """
        Convert ES results keyed by table_name into a dict keyed by the
        corresponding FK column in analytics_detected_schema.

        e.g. {"common_brand": [{id: 12, ...}]} -> {"brand_id": [12]}
        """
        entity_id_map: dict[str, list[int]] = {}
        for table_name, docs in es_results.items():
            fk_col = ENTITY_TO_DETECTED_SCHEMA_FK.get(table_name)
            if fk_col is None:
                continue
            ids = [doc["id"] for doc in docs if "id" in doc]
            if ids:
                entity_id_map[fk_col] = ids
        return entity_id_map

    def process(self) -> dict:
        es_results = msearch_es(
            self.tenant_name,
            self.search_query,
            self.entities,
        )

        entity_id_map = self._extract_entity_ids(es_results)

        detected_schema_ids = fetch_detected_schema_ids(
            self.tenant_name, entity_id_map
        )

        session_metric_ids = fetch_session_metric_ids(
            self.tenant_name, detected_schema_ids
        )

        session_uuids = fetch_session_uuids(
            self.tenant_name, session_metric_ids
        )

        return {
            "es_results": es_results,
            "session_uuids": session_uuids,
        }
