# Search API

A small FastAPI service that accepts search parameters and queries an
Elasticsearch backend, plus a Vite + React frontend for searching.

## Stack

- **FastAPI** — web framework
- **Elasticsearch (async client)** — search backend
- **pydantic-settings** — typed, env-driven configuration
- **Vite + React + TypeScript** — frontend
- **Tailwind CSS v4 + shadcn/ui** — UI

## Project layout

```
app/                    # FastAPI backend
frontend/               # Vite React app (pnpm, Node 22)
scripts/                # Dev process helpers (used by Procfile)
Procfile                # honcho / foreman process definitions
```

## Setup

### Backend

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
cp .env.example .env   # then edit values
```

### Frontend

Node **22** is required (repo root `.nvmrc`). Use **pnpm**:

```bash
cd frontend
nvm use 22
pnpm install
```

## Run both servers

[Honcho](https://github.com/nickstenning/honcho) reads `Procfile` (same format as Foreman):

```bash
source .venv/bin/activate
honcho start
```

| Process | URL |
|---------|-----|
| API | http://127.0.0.1:8000 |
| Frontend | http://127.0.0.1:5173 |

The frontend proxies `/api/*` → `http://127.0.0.1:8000/*` during development.

### Run individually

```bash
# API only
uvicorn app.main:app --reload

# Frontend only (from frontend/, with Node 22)
pnpm dev
```

## API

- Docs: http://localhost:8000/docs
- Liveness: `GET /health`
- ES readiness: `GET /health/elasticsearch`
- Search: `GET /search?q=hello&page=1&size=10`

## Configuration

All config is read from environment variables / `.env`. See `.env.example`.
Use **either** an API key **or** username/password for Elasticsearch auth.

## Status / next steps

The `/search` endpoint is a baseline `multi_match` query. Next we'll design:

1. The request parameters (filters, sorting, facets, pagination strategy).
2. A performant ES query body tailored to the use case.
