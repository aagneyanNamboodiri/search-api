from elasticsearch import Elasticsearch

from src.config.settings import settings
from src.constants import SearchableEntity, SEARCHABLE_ENTITIES

_es_client: Elasticsearch | None = None


def get_es_client() -> Elasticsearch:
    global _es_client
    if _es_client is None:
        kwargs: dict = {"hosts": [settings.es_url]}
        if settings.es_username and settings.es_password:
            kwargs["basic_auth"] = (settings.es_username, settings.es_password)
        if not settings.es_verify_certs:
            kwargs["verify_certs"] = False
        _es_client = Elasticsearch(**kwargs)
    return _es_client


def build_msearch_payload(
    tenant_name: str,
    search_query: str,
    entities: list[SearchableEntity],
    page: int = 1,
    page_size: int = 15,
) -> list[dict]:
    """Build the flat header/body list that ES msearch expects."""
    es_from = (page - 1) * page_size
    searches = []
    for entity in entities:
        header = {"index": tenant_name}
        body = {
            "from": es_from,
            "size": page_size,
            "query": {
                "bool": {
                    "must": [
                        {"term": {"table_name.keyword": entity.table_name}}
                    ],
                    "should": [
                        {
                            "match": {
                                f.name: {
                                    "query": search_query,
                                    "fuzziness": entity.fuzziness,
                                    "boost": f.boost,
                                }
                            }
                        }
                        for f in entity.search_fields
                    ],
                }
            },
        }
        searches.extend([header, body])
    return searches


def msearch_es(
    tenant_name: str,
    search_query: str,
    entities: list[SearchableEntity] | None = None,
    page: int = 1,
    page_size: int = 15,
) -> dict[str, dict]:
    """
    Run an _msearch across all searchable entities for a tenant.

    Returns a dict keyed by table_name, each value containing:
      - "docs": list of _source dicts (with _score)
      - "total": total ES hits for this entity
    """
    if entities is None:
        entities = SEARCHABLE_ENTITIES

    client = get_es_client()
    searches = build_msearch_payload(
        tenant_name, search_query, entities, page, page_size
    )
    response = client.msearch(searches=searches)

    results: dict[str, dict] = {}
    for i, entity in enumerate(entities):
        resp = response["responses"][i]
        hits_obj = resp.get("hits", {})
        hits = hits_obj.get("hits", [])
        total = hits_obj.get("total", {}).get("value", 0)
        results[entity.table_name] = {
            "docs": [
                {**hit["_source"], "_score": hit["_score"]}
                for hit in hits
            ],
            "total": total,
        }

    return results
