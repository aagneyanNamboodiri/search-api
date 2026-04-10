from src.db.elasticsearch import search_es
from src.db.postgres import fetch_records_by_ids


def search(tenant: str, query: str, size: int = 10) -> dict:
    """
    1. Query Elasticsearch with the text query against the tenant's index
    2. Extract IDs from ES results
    3. Fetch matching records from PostgreSQL for the same tenant
    """
    es_results = search_es(tenant, query, size=size)

    # TODO: Adjust the key used to extract IDs from ES documents
    ids = [doc["_id"] for doc in es_results]

    pg_results = fetch_records_by_ids(tenant, ids)

    return {
        "es_results": es_results,
        "pg_results": pg_results,
    }
