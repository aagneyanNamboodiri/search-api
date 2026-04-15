# Search API

FastAPI service that queries Elasticsearch and enriches results from PostgreSQL.

## Prerequisites

- Python 3.10+
- Elasticsearch running in Docker on port `9200`
- PostgreSQL running locally with a database called `infiviz`

## Setup

### 1. Activate the virtual environment

```bash
source /opt/infilect/dev/envs/search_api_env/bin/activate
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Create your `.env` file

```bash
cp .env.example .env
```

Open `.env` and update the values if your local setup differs from the defaults:

```
SEARCH_API_ES_HOST=localhost
SEARCH_API_ES_PORT=9200

SEARCH_API_PG_HOST=localhost
SEARCH_API_PG_PORT=5432
SEARCH_API_PG_DATABASE=infiviz
SEARCH_API_PG_USER=postgres
SEARCH_API_PG_PASSWORD=postgres
```

### 4. Start the dev server

```bash
uvicorn src.main:app --reload
```

The server starts at [http://localhost:8000](http://localhost:8000).

Interactive API docs are available at [http://localhost:8000/docs](http://localhost:8000/docs).

## Usage

### Health check

```bash
curl http://localhost:8000/health
```

### Search

```bash
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{"tenant": "your_tenant_name", "query": "search text", "size": 10}'
```

- `tenant` — the tenant name, used as the Elasticsearch index and to resolve the correct Postgres schema/table
- `query` — the text to search for
- `size` — number of results to return (default `10`)

## Project structure

```
src/
├── main.py              # FastAPI app, endpoints
├── config/
│   └── settings.py      # Env-based config (ES + PG)
├── db/
│   ├── elasticsearch.py # ES client and search query
│   └── postgres.py      # PG client with PyPika queries
├── models/
│   └── search.py        # Request/Response schemas
└── search/
    └── service.py       # Orchestrates ES search → PG lookup
```
