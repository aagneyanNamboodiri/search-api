from pydantic import BaseModel


class SearchRequest(BaseModel):
    tenant: str
    query: str
    size: int = 10


class SearchResponse(BaseModel):
    es_results: list[dict]
    pg_results: list[dict]
