from pydantic import BaseModel


class SearchRequest(BaseModel):
    tenant_name: str
    search_query: str
    page: int = 1
    page_size: int = 15
    entity: str | None = None
    debug: bool = False


class EntityResult(BaseModel):
    session_uuids: list[str]
    has_sessions: bool
    total: int
    page: int
    page_size: int


class SearchResponse(BaseModel):
    results: dict[str, EntityResult]
    es_results: dict[str, list[dict]] | None = None
