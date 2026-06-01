import logging
from typing import Any

from elasticsearch import AsyncElasticsearch

from app.core.config import Settings

logger = logging.getLogger(__name__)


def _build_client_kwargs(settings: Settings) -> dict[str, Any]:
    """Translate settings into AsyncElasticsearch constructor kwargs."""
    kwargs: dict[str, Any] = {
        "request_timeout": settings.elasticsearch_request_timeout,
        "verify_certs": settings.elasticsearch_verify_certs,
    }

    if settings.elasticsearch_cloud_id:
        kwargs["cloud_id"] = settings.elasticsearch_cloud_id
    else:
        kwargs["hosts"] = [settings.elasticsearch_url]

    if settings.elasticsearch_api_key:
        kwargs["api_key"] = settings.elasticsearch_api_key
    elif settings.elasticsearch_username and settings.elasticsearch_password:
        kwargs["basic_auth"] = (
            settings.elasticsearch_username,
            settings.elasticsearch_password,
        )

    return kwargs


def create_client(settings: Settings) -> AsyncElasticsearch:
    """Create a singleton-style AsyncElasticsearch client.

    The client maintains an internal connection pool, so a single instance
    should be shared across the app lifecycle rather than created per request.
    """
    return AsyncElasticsearch(**_build_client_kwargs(settings))
