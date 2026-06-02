from typing import Any, Literal

from pydantic import BaseModel, Field

SortOption = Literal["relevance", "recent"]


class SearchParams(BaseModel):
    """Incoming search parameters.

    Intentionally minimal for now. We'll expand this with filters, sorting,
    and faceting once we design the query for our use case.
    """

    q: str = Field(..., min_length=1, description="Free-text query string.")
    page: int = Field(1, ge=1, description="1-based page number.")
    size: int = Field(10, ge=1, le=100, description="Results per page.")

    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size


class SearchHit(BaseModel):
    id: str
    score: float | None = None
    source: dict[str, Any]


class SearchResponse(BaseModel):
    total: int
    page: int
    size: int
    took_ms: int
    hits: list[SearchHit]


class StoreClause(BaseModel):
    """A single store filter, e.g. {field: "store.city", value: "Miami"}."""

    field: str = Field(..., min_length=1)
    value: str = Field(..., min_length=1)


class SearchRequest(BaseModel):
    """A batched session search request.

    A query is at most one store clause (hard filter) plus a schema part that is
    either one level clause (`level` + `value`, e.g. brand:XYZ) or one wildcard
    `term`. The store clause alone (no schema part) returns a single sessions
    batch.
    """

    store: StoreClause | None = None
    level: str | None = Field(None, description="Specific level key, e.g. 'brand'.")
    value: str | None = Field(None, description="Value for the level clause.")
    term: str | None = Field(None, description="Wildcard free-text term.")
    sort: SortOption = "relevance"
    dedup: bool = False
    size: int = Field(5, ge=1, le=50)


class MoreRequest(BaseModel):
    """Fetch the next page of a single batch (shared by both strategies)."""

    store: StoreClause | None = None
    level: str = Field(..., description="ES array name, 'store', or 'sessions'.")
    query: str | None = Field(None, description="Text to match; None = match_all.")
    sort: SortOption = "relevance"
    offset: int = Field(0, ge=0)
    size: int = Field(5, ge=1, le=50)


class BatchHit(BaseModel):
    session_uuid: str
    score: float | None = None
    matched: list[str] = Field(default_factory=list)
    category_title: str | None = None
    store: dict[str, Any] | None = None


class Batch(BaseModel):
    level: str
    label: str
    offset: int
    has_more: bool
    hits: list[BatchHit]


class BatchedSearchResponse(BaseModel):
    took_ms: int
    strategy: Literal["multi", "single"]
    deduped: bool
    batches: list[Batch]
