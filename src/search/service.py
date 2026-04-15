from collections import defaultdict

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
        page: int = 1,
        page_size: int = 15,
        entity: str | None = None,
        debug: bool = False,
    ):
        self.tenant_name = tenant_name
        self.search_query = search_query
        self.page = page
        self.page_size = page_size
        self.debug = debug
        self.entities = self._resolve_entities(entity)

    def _resolve_entities(
        self,
        entity: str | None,
    ) -> list[SearchableEntity]:
        """Filter to a single entity if requested, otherwise use all."""
        if entity is None:
            return SEARCHABLE_ENTITIES
        return [e for e in SEARCHABLE_ENTITIES if e.table_name == entity]

    def _extract_entity_queries(
        self,
        es_results: dict[str, dict],
    ) -> list[tuple[str, str, list[int]]]:
        """
        Convert ES results into (entity_name, fk_col, ids) tuples
        for the PG pipeline.
        """
        entity_queries = []
        for table_name, data in es_results.items():
            fk_col = ENTITY_TO_DETECTED_SCHEMA_FK.get(table_name)
            if fk_col is None:
                continue
            ids = [doc["id"] for doc in data["docs"] if "id" in doc]
            if ids:
                entity_queries.append((table_name, fk_col, ids))
        return entity_queries

    def _group_sessions_by_entity(
        self,
        ds_rows: list[tuple[int, str]],
        sm_rows: list[tuple[int, int]],
        uuid_rows: list[tuple[int, str]],
    ) -> dict[str, list[str]]:
        """
        Chain the mappings from all 3 PG steps to group session_uuids
        by entity type.
        """
        # detected_schema_id -> set of entity names
        ds_to_entities: dict[int, set[str]] = defaultdict(set)
        for ds_id, entity_name in ds_rows:
            ds_to_entities[ds_id].add(entity_name)

        # session_metric_id -> set of entity names (via detected_schema)
        sm_to_entities: dict[int, set[str]] = defaultdict(set)
        for sm_id, ds_id in sm_rows:
            sm_to_entities[sm_id].update(ds_to_entities.get(ds_id, set()))

        # entity -> deduplicated session_uuids
        entity_sessions: dict[str, set[str]] = defaultdict(set)
        for sm_id, uuid in uuid_rows:
            for entity_name in sm_to_entities.get(sm_id, set()):
                entity_sessions[entity_name].add(str(uuid))

        return {k: sorted(v) for k, v in entity_sessions.items()}

    def process(self) -> dict:
        es_results = msearch_es(
            self.tenant_name,
            self.search_query,
            self.entities,
            page=self.page,
            page_size=self.page_size,
        )

        entity_queries = self._extract_entity_queries(es_results)

        ds_rows = fetch_detected_schema_ids(self.tenant_name, entity_queries)
        ds_ids = list({row[0] for row in ds_rows})

        sm_rows = fetch_session_metric_ids(self.tenant_name, ds_ids)
        sm_ids = list({row[0] for row in sm_rows})

        uuid_rows = fetch_session_uuids(self.tenant_name, sm_ids)

        grouped = self._group_sessions_by_entity(ds_rows, sm_rows, uuid_rows)

        results = {}
        for table_name, data in es_results.items():
            session_uuids = grouped.get(table_name, [])
            results[table_name] = {
                "session_uuids": session_uuids,
                "has_sessions": len(session_uuids) > 0,
                "total": data["total"],
                "page": self.page,
                "page_size": self.page_size,
            }

        response: dict = {"results": results}
        if self.debug:
            response["es_results"] = {
                k: v["docs"] for k, v in es_results.items()
            }

        return response
