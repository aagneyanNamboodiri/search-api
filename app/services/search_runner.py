"""Execute batched searches and assemble the strategy-agnostic response.

This module resolves a `SearchRequest` into level specs, runs either the
multi-search (`_msearch`) or single-search (`filters` agg) strategy, and
normalizes both into the same `BatchedSearchResponse`.
"""

from typing import Any

from elasticsearch import AsyncElasticsearch

from app.core.search_fields import (
    ALL_LEVELS,
    build_store_filter,
    normalize_level_key,
    schema_level_keys,
)
from app.schemas.search import (
    Batch,
    BatchedSearchResponse,
    BatchHit,
    MoreRequest,
    SearchRequest,
)
from app.services.query_builder import (
    LevelSpec,
    build_level_search,
    build_msearch_body,
    build_single_search_body,
)

SESSIONS_LEVEL = "sessions"


class SearchRequestError(ValueError):
    """Raised when a request cannot be turned into a valid search plan."""


def _label_for(level_key: str) -> str:
    level = ALL_LEVELS.get(level_key)
    return level.label if level else "Sessions"


def _resolve_store_filter(request: SearchRequest | MoreRequest) -> dict[str, Any] | None:
    if request.store is None:
        return None
    store_filter = build_store_filter(request.store.field, request.store.value)
    if store_filter is None:
        raise SearchRequestError(f"Unknown store field: {request.store.field!r}")
    return store_filter


def plan_search(request: SearchRequest) -> tuple[list[LevelSpec], dict[str, Any] | None]:
    """Resolve a request into (level specs, store filter).

    - level + value -> a single level batch.
    - term -> every schema level (plus store when no store filter).
    - store only -> a single sessions batch.
    """
    store_filter = _resolve_store_filter(request)

    if request.level and request.value:
        level_key = normalize_level_key(request.level)
        if level_key is None:
            raise SearchRequestError(f"Unknown level: {request.level!r}")
        return [(level_key, request.value)], store_filter

    if request.term:
        specs: list[LevelSpec] = [(key, request.term) for key in schema_level_keys()]
        if store_filter is None:
            specs.append(("store", request.term))
        return specs, store_filter

    if store_filter is not None:
        return [(SESSIONS_LEVEL, None)], store_filter

    raise SearchRequestError("Empty query: provide a store clause, level, or term.")


def _normalize_hit(raw: dict[str, Any]) -> BatchHit:
    source = raw.get("_source", {}) or {}

    category_title: str | None = None
    categories = source.get("categories")
    if isinstance(categories, list) and categories:
        first = categories[0]
        if isinstance(first, dict):
            category_title = first.get("title")

    matched: list[str] = []
    for fragments in (raw.get("highlight") or {}).values():
        if isinstance(fragments, list):
            matched.extend(fragments)

    return BatchHit(
        session_uuid=source.get("session_uuid", ""),
        score=raw.get("_score"),
        matched=matched,
        category_title=category_title,
        store=source.get("store"),
    )


def _assemble_batch(
    level_key: str, raw_hits: list[dict[str, Any]], *, offset: int, size: int
) -> Batch:
    """Trim the size+1 sentinel into has_more and normalize the page of hits."""
    has_more = len(raw_hits) > size
    page = raw_hits[:size]
    return Batch(
        level=level_key,
        label=_label_for(level_key),
        offset=offset,
        has_more=has_more,
        hits=[_normalize_hit(hit) for hit in page],
    )


def _dedup_batches(batches: list[Batch]) -> list[Batch]:
    """Keep each session only in its highest-scoring batch (first wins on ties)."""
    best: dict[str, tuple[float, str]] = {}
    for batch in batches:
        for hit in batch.hits:
            score = hit.score if hit.score is not None else float("-inf")
            current = best.get(hit.session_uuid)
            if current is None or score > current[0]:
                best[hit.session_uuid] = (score, batch.level)

    deduped: list[Batch] = []
    for batch in batches:
        kept = [h for h in batch.hits if best[h.session_uuid][1] == batch.level]
        deduped.append(batch.model_copy(update={"hits": kept}))
    return deduped


def _finalize(
    batches: list[Batch], *, strategy: str, deduped: bool, took_ms: int
) -> BatchedSearchResponse:
    if deduped:
        batches = _dedup_batches(batches)
    return BatchedSearchResponse(
        took_ms=took_ms, strategy=strategy, deduped=deduped, batches=batches
    )


async def run_multi_search(
    es: AsyncElasticsearch, index: str, request: SearchRequest
) -> BatchedSearchResponse:
    specs, store_filter = plan_search(request)
    searches = build_msearch_body(
        specs=specs, store_filter=store_filter, sort=request.sort, size=request.size
    )
    response = await es.msearch(searches=searches, index=index)

    responses = response.get("responses", [])
    batches = [
        _assemble_batch(
            level_key, sub.get("hits", {}).get("hits", []), offset=0, size=request.size
        )
        for (level_key, _), sub in zip(specs, responses)
    ]

    took_ms = response.get("took") or max(
        (sub.get("took", 0) for sub in responses), default=0
    )
    return _finalize(
        batches, strategy="multi", deduped=request.dedup, took_ms=took_ms
    )


async def run_single_search(
    es: AsyncElasticsearch, index: str, request: SearchRequest
) -> BatchedSearchResponse:
    specs, store_filter = plan_search(request)
    body = build_single_search_body(
        specs=specs, store_filter=store_filter, sort=request.sort, size=request.size
    )
    response = await es.search(index=index, body=body)

    buckets = response["aggregations"]["by_level"]["buckets"]
    batches = [
        _assemble_batch(
            level_key,
            buckets[level_key]["top"]["hits"]["hits"],
            offset=0,
            size=request.size,
        )
        for level_key, _ in specs
    ]

    return _finalize(
        batches,
        strategy="single",
        deduped=request.dedup,
        took_ms=response.get("took", 0),
    )


async def run_more(
    es: AsyncElasticsearch, index: str, more: MoreRequest
) -> Batch:
    store_filter = _resolve_store_filter(more)

    level_key = more.level.strip().lower()
    if level_key != SESSIONS_LEVEL and level_key not in ALL_LEVELS:
        normalized = normalize_level_key(level_key)
        if normalized is None:
            raise SearchRequestError(f"Unknown level: {more.level!r}")
        level_key = normalized

    body = build_level_search(
        level_key=level_key,
        query_text=more.query,
        store_filter=store_filter,
        sort=more.sort,
        offset=more.offset,
        size=more.size,
    )
    response = await es.search(index=index, body=body)
    return _assemble_batch(
        level_key,
        response.get("hits", {}).get("hits", []),
        offset=more.offset,
        size=more.size,
    )
