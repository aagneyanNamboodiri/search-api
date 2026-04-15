from pydantic import BaseModel


class SearchRequest(BaseModel):
    tenant_name: str
    search_query: str


class SearchResponse(BaseModel):
    es_results: dict[str, list[dict]]
