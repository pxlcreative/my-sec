# SEC Adviser Database Platform

Self-hosted mirror of SEC-registered investment adviser data built on FastAPI, PostgreSQL 16, Elasticsearch 8, Redis 7, and Celery.

## Quick start

```bash
cp .env.example .env          # edit passwords / SECRET_KEY as needed
docker compose up --build
```

API will be available at `http://localhost:8000`.  
Health check: `GET http://localhost:8000/health`  
OpenAPI docs: `http://localhost:8000/docs`

## Services

| Service | Port | Description |
|---------|------|-------------|
| api | 8000 | FastAPI application |
| postgres | 5432 | Primary datastore |
| elasticsearch | 9200 | Full-text / fuzzy search |
| redis | 6379 | Celery broker + result backend |
| celery_worker | — | Async task worker |
| celery_beat | — | Scheduled task scheduler |

## Project layout

```
sec-adviser-platform/
  api/
    routes/          # FastAPI routers (one file per domain)
    models/          # SQLAlchemy ORM models
    services/        # Business logic, IAPD API clients
    celery_tasks/    # Celery app + task definitions
    config.py        # Pydantic settings (reads .env)
    main.py          # FastAPI app entry point
    Dockerfile
  alembic/           # DB migrations
  data/
    raw/csv/         # Downloaded bulk CSVs
    brochures/       # ADV Part 2 PDFs
    exports/         # Generated Excel / output files
  scripts/           # One-off CLI scripts
  tests/
  docker-compose.yml
  requirements.txt
  .env.example
```

## Database migrations

```bash
docker compose exec api alembic upgrade head
```
