from typing import Any

from pydantic import BaseModel, Field


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
