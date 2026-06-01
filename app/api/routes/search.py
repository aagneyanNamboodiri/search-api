from typing import Annotated

from fastapi import APIRouter, Query

from app.api.dependencies import ElasticsearchDep, SettingsDep
from app.schemas.search import SearchHit, SearchParams, SearchResponse

router = APIRouter(tags=["search"])


@router.get("/search", response_model=SearchResponse)
async def search(
    es: ElasticsearchDep,
    settings: SettingsDep,
    params: Annotated[SearchParams, Query()],
) -> SearchResponse:
    """Run a search against Elasticsearch.

    NOTE: This is a baseline implementation using a simple multi_match query.
    The query body and parameters will be refined in subsequent iterations.
    """
    body = {
        "from": params.offset,
        "size": params.size,
        "query": {
            "multi_match": {
                "query": params.q,
                "type": "best_fields",
            }
        },
    }

    result = await es.search(index=settings.elasticsearch_index, body=body)

    hits = [
        SearchHit(id=hit["_id"], score=hit.get("_score"), source=hit["_source"])
        for hit in result["hits"]["hits"]
    ]

    return SearchResponse(
        total=result["hits"]["total"]["value"],
        page=params.page,
        size=params.size,
        took_ms=result["took"],
        hits=hits,
    )
