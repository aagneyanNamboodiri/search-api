from fastapi import APIRouter

from app.api.dependencies import ElasticsearchDep, SettingsDep

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe: the app is up and serving requests."""
    return {"status": "ok"}


@router.get("/health/elasticsearch")
async def elasticsearch_health(
    es: ElasticsearchDep, settings: SettingsDep
) -> dict[str, object]:
    """Readiness probe: verify connectivity to Elasticsearch."""
    reachable = await es.ping()
    return {
        "elasticsearch": "up" if reachable else "down",
        "index": settings.elasticsearch_index,
    }
