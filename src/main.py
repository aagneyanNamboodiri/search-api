from fastapi import FastAPI

from src.models.search import SearchRequest, SearchResponse
from src.search.service import SearchProcessor

app = FastAPI(title="Search API")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/search", response_model=SearchResponse)
async def search_endpoint(request: SearchRequest):
    processor = SearchProcessor(
        tenant_name=request.tenant_name,
        search_query=request.search_query,
    )
    return processor.process()
