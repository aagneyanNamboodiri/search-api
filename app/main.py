import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import health, search
from app.core.config import get_settings
from app.core.elastic import create_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage the Elasticsearch client lifecycle alongside the app."""
    settings = get_settings()
    app.state.elasticsearch = create_client(settings)
    logger.info("Elasticsearch client initialized.")
    try:
        yield
    finally:
        await app.state.elasticsearch.close()
        logger.info("Elasticsearch client closed.")


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title=settings.app_name,
        debug=settings.debug,
        lifespan=lifespan,
    )
    app.include_router(health.router)
    app.include_router(search.router)
    return app


app = create_app()
