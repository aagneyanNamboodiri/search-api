from unittest.mock import patch

import pytest
from httpx import AsyncClient, ASGITransport

from src.main import app


@pytest.fixture
def mock_process():
    return {
        "results": {
            "common_brand": {
                "session_uuids": ["uuid-1"],
                "has_sessions": True,
                "total": 10,
                "page": 1,
                "page_size": 15,
            },
        },
        "es_results": None,
    }


@pytest.mark.asyncio
async def test_health():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}


@pytest.mark.asyncio
async def test_search_endpoint(mock_process):
    with patch(
        "src.main.SearchProcessor"
    ) as MockProcessor:
        MockProcessor.return_value.process.return_value = mock_process

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.post("/search", json={
                "tenant_name": "tenant_a",
                "search_query": "beer",
            })

    assert resp.status_code == 200
    data = resp.json()
    assert "common_brand" in data["results"]
    assert data["results"]["common_brand"]["has_sessions"] is True


@pytest.mark.asyncio
async def test_search_passes_all_params(mock_process):
    with patch(
        "src.main.SearchProcessor"
    ) as MockProcessor:
        MockProcessor.return_value.process.return_value = mock_process

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/search", json={
                "tenant_name": "tenant_a",
                "search_query": "beer",
                "page": 3,
                "page_size": 5,
                "entity": "common_brand",
                "debug": True,
            })

    MockProcessor.assert_called_once_with(
        tenant_name="tenant_a",
        search_query="beer",
        page=3,
        page_size=5,
        entity="common_brand",
        debug=True,
    )


@pytest.mark.asyncio
async def test_search_missing_required_field():
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/search", json={"tenant_name": "tenant_a"})
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_search_defaults(mock_process):
    with patch(
        "src.main.SearchProcessor"
    ) as MockProcessor:
        MockProcessor.return_value.process.return_value = mock_process

        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            await client.post("/search", json={
                "tenant_name": "tenant_a",
                "search_query": "beer",
            })

    MockProcessor.assert_called_once_with(
        tenant_name="tenant_a",
        search_query="beer",
        page=1,
        page_size=15,
        entity=None,
        debug=False,
    )
