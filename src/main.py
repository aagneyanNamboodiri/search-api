from fastapi import FastAPI

from src.models.search import SearchRequest, SearchResponse
from src.search.service import search

app = FastAPI(title="Search API")


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/search", response_model=SearchResponse)
async def search_endpoint(request: SearchRequest):
    results = search(tenant=request.tenant, query=request.query, size=request.size)
    return results
