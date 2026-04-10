from elasticsearch import Elasticsearch

from src.config.settings import settings

_es_client: Elasticsearch | None = None


def get_es_client() -> Elasticsearch:
    global _es_client
    if _es_client is None:
        _es_client = Elasticsearch(
            hosts=[{"host": settings.es_host, "port": settings.es_port}]
        )
    return _es_client


def search_es(tenant: str, query: str, size: int = 10) -> list[dict]:
    """
    Query Elasticsearch with a text query against the tenant's index.

    The tenant name is used as the ES index name.
    Returns a list of dicts, each containing at least an '_id' field.
    """
    client = get_es_client()

    # TODO: Build the actual ES query body for your index/mapping.
    body = {
        "size": size,
        "query": {
            "multi_match": {
                "query": query,
                "fields": ["*"],  # TODO: specify actual fields
            }
        },
    }

    response = client.search(index=tenant, body=body)

    hits = response.get("hits", {}).get("hits", [])
    results = []
    for hit in hits:
        doc = hit["_source"]
        doc["_id"] = hit["_id"]
        doc["_score"] = hit["_score"]
        results.append(doc)

    return results
