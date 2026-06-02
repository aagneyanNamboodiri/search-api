from typing import Annotated

from fastapi import APIRouter, HTTPException, Query

from app.api.dependencies import ElasticsearchDep
from app.schemas.search import Batch, BatchedSearchResponse, MoreRequest, SearchRequest
from app.services.search_runner import (
    SearchRequestError,
    run_more,
    run_multi_search,
    run_single_search,
)

router = APIRouter(prefix="/v1", tags=["search-v1"])

IndexQuery = Annotated[str, Query(min_length=1, description="Target Elasticsearch index.")]


@router.post("/multi-search", response_model=BatchedSearchResponse)
async def multi_search(
    es: ElasticsearchDep, request: SearchRequest, index: IndexQuery
) -> BatchedSearchResponse:
    """Batched search via _msearch: one independent query per level."""
    try:
        return await run_multi_search(es, index, request)
    except SearchRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/single-search", response_model=BatchedSearchResponse)
async def single_search(
    es: ElasticsearchDep, request: SearchRequest, index: IndexQuery
) -> BatchedSearchResponse:
    """Batched search via a single query with a filters + top_hits aggregation."""
    try:
        return await run_single_search(es, index, request)
    except SearchRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@router.post("/search/more", response_model=Batch)
async def search_more(
    es: ElasticsearchDep, request: MoreRequest, index: IndexQuery
) -> Batch:
    """Fetch the next page of a single batch (shared by both strategies)."""
    try:
        return await run_more(es, index, request)
    except SearchRequestError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
