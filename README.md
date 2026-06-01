# Search API

A small FastAPI service that accepts search parameters and queries an
Elasticsearch backend.

## Stack

- **FastAPI** — web framework
- **Elasticsearch (async client)** — search backend
- **pydantic-settings** — typed, env-driven configuration

## Project layout

```
app/
├── main.py              # App factory + ES client lifecycle (lifespan)
├── core/
│   ├── config.py        # Settings (loaded from .env)
│   └── elastic.py       # AsyncElasticsearch client factory
├── api/
│   ├── dependencies.py  # Shared DI: settings + ES client
│   └── routes/
│       ├── health.py    # /health, /health/elasticsearch
│       └── search.py    # /search (baseline multi_match)
└── schemas/
    └── search.py        # Request/response models
```

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # then edit values
```

## Configuration

All config is read from environment variables / `.env`. See `.env.example`.
Use **either** an API key **or** username/password for Elasticsearch auth.

## Run

```bash
uvicorn app.main:app --reload
```

- Docs: http://localhost:8000/docs
- Liveness: `GET /health`
- ES readiness: `GET /health/elasticsearch`
- Search: `GET /search?q=hello&page=1&size=10`

## Status / next steps

The `/search` endpoint is a baseline `multi_match` query. Next we'll design:

1. The request parameters (filters, sorting, facets, pagination strategy).
2. A performant ES query body tailored to the use case.
