from typing import Annotated

from elasticsearch import AsyncElasticsearch
from fastapi import Depends, Request

from app.core.config import Settings, get_settings


def get_elasticsearch(request: Request) -> AsyncElasticsearch:
    """Return the shared AsyncElasticsearch client stored on app state."""
    return request.app.state.elasticsearch


SettingsDep = Annotated[Settings, Depends(get_settings)]
ElasticsearchDep = Annotated[AsyncElasticsearch, Depends(get_elasticsearch)]
