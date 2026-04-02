.PHONY: up down restart logs migrate seed seed-schedules load-data test reindex shell ps

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

migrate:
	docker compose exec api alembic upgrade head

seed:
	docker compose exec api python scripts/seed_platforms.py
	docker compose exec api python scripts/seed_schedules.py

seed-schedules:
	docker compose exec api python scripts/seed_schedules.py

load-data:
	docker compose exec api python scripts/load_bulk_csv.py
	docker compose exec api python scripts/load_filing_data.py
	docker compose exec api python scripts/index_firms_to_es.py
	docker compose exec api python scripts/backfill_annual_aum.py

test:
	docker compose exec api pytest tests/ -v

reindex:
	docker compose exec api python scripts/index_firms_to_es.py

shell:
	docker compose exec api bash

ps:
	docker compose ps
