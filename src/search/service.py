from src.constants import SearchableEntity, SEARCHABLE_ENTITIES
from src.db.elasticsearch import msearch_es


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

    def process(self) -> dict:
        es_results = msearch_es(
            self.tenant_name,
            self.search_query,
            self.entities,
        )

        # TODO Phase 2: Extract `id` from each _source doc (not _id),
        # group by entity, and query PostgreSQL via fetch_records_by_ids.

        return {"es_results": es_results}
