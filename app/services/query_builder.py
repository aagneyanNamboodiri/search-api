"""Pure functions that turn a resolved search into Elasticsearch query bodies.

A "level spec" is a `(level_key, query_text)` pair where `level_key` is an ES
array name, "store", or the pseudo-level "sessions", and `query_text` is the
text to match within that level (None means match_all).

Nothing here talks to Elasticsearch; the runner owns execution. Keeping these
builders pure makes the two strategies easy to compare and test.
"""

from typing import Any

from app.core.search_fields import ALL_LEVELS, HIT_SOURCE_FIELDS

LevelSpec = tuple[str, str | None]


def _matcher(level_key: str, query_text: str | None) -> dict[str, Any]:
    """Build the query clause that matches documents for a level."""
    if query_text is None or level_key not in ALL_LEVELS:
        return {"match_all": {}}

    level = ALL_LEVELS[level_key]
    return {
        "multi_match": {
            "query": query_text,
            "fields": list(level.boosted_fields),
            "type": "best_fields",
            "lenient": True,
        }
    }


def _named_matcher(level_key: str, query_text: str | None) -> dict[str, Any]:
    """A matcher tagged with `_name` so single-search can attribute hits."""
    matcher = _matcher(level_key, query_text)
    inner = next(iter(matcher.values()))
    inner["_name"] = level_key
    return matcher


def _sort(sort: str) -> list[Any]:
    if sort == "recent":
        return [{"visit_date": {"order": "desc"}}, "_score"]
    return ["_score"]


def _highlight_fields(level_keys: list[str]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for key in level_keys:
        level = ALL_LEVELS.get(key)
        if not level:
            continue
        for field in level.highlight_fields:
            fields[field] = {}
    return fields


def build_level_search(
    *,
    level_key: str,
    query_text: str | None,
    store_filter: dict[str, Any] | None,
    sort: str,
    offset: int,
    size: int,
) -> dict[str, Any]:
    """Build a single-level `_search` body (shared by multi-search and show-more).

    Requests `size + 1` so the runner can derive `has_more` without paying for
    an exact `track_total_hits` count.
    """
    bool_query: dict[str, Any] = {"must": [_matcher(level_key, query_text)]}
    if store_filter:
        bool_query["filter"] = [store_filter]

    body: dict[str, Any] = {
        "from": offset,
        "size": size + 1,
        "track_total_hits": False,
        "_source": list(HIT_SOURCE_FIELDS),
        "query": {"bool": bool_query},
        "sort": _sort(sort),
    }

    highlight = _highlight_fields([level_key])
    if highlight:
        body["highlight"] = {"fields": highlight}

    return body


def build_msearch_body(
    *,
    specs: list[LevelSpec],
    store_filter: dict[str, Any] | None,
    sort: str,
    size: int,
) -> list[dict[str, Any]]:
    """Build the alternating header/body sequence for `_msearch`.

    The index is supplied at call time, so every header is an empty object.
    """
    searches: list[dict[str, Any]] = []
    for level_key, query_text in specs:
        searches.append({})
        searches.append(
            build_level_search(
                level_key=level_key,
                query_text=query_text,
                store_filter=store_filter,
                sort=sort,
                offset=0,
                size=size,
            )
        )
    return searches


def build_single_search_body(
    *,
    specs: list[LevelSpec],
    store_filter: dict[str, Any] | None,
    sort: str,
    size: int,
) -> dict[str, Any]:
    """Build one `_search` that batches every level via a `filters` aggregation.

    The top-level `should` query produces relevance scores; the `filters` agg
    buckets documents per level, and each bucket's `top_hits` returns that
    batch's page. Note: `top_hits` scores reflect the global should-query, not a
    per-level score (this is the key semantic difference from multi-search).
    """
    level_keys = [level_key for level_key, _ in specs]

    should = [_named_matcher(level_key, query_text) for level_key, query_text in specs]
    bool_query: dict[str, Any] = {"should": should, "minimum_should_match": 1}
    if store_filter:
        bool_query["filter"] = [store_filter]

    filters_map = {
        level_key: _matcher(level_key, query_text) for level_key, query_text in specs
    }

    top_hits: dict[str, Any] = {
        "size": size + 1,
        "from": 0,
        "_source": list(HIT_SOURCE_FIELDS),
        "sort": _sort(sort),
    }
    highlight = _highlight_fields(level_keys)
    if highlight:
        top_hits["highlight"] = {"fields": highlight}

    return {
        "size": 0,
        "track_total_hits": False,
        "query": {"bool": bool_query},
        "aggs": {
            "by_level": {
                "filters": {"filters": filters_map},
                "aggs": {"top": {"top_hits": top_hits}},
            }
        },
    }
