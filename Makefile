.PHONY: install up down restart logs logs-api logs-worker logs-beat \
        migrate seed seed-schedules load-data test test-frontend \
        reindex shell ps verify dlq-inspect env

# ── One-command install ─────────────────────────────────────────────────────
# Creates .env from .env.example if missing, brings services up, and blocks
# until /health returns 200 (i.e. entrypoint has finished migrate+seed).
install: env up
	@echo "Waiting for API to become healthy..."
	@for i in $$(seq 1 60); do \
	    if curl -sf http://localhost:8000/health > /dev/null 2>&1; then \
	        echo "API is healthy. Run 'make verify' to confirm."; \
	        exit 0; \
	    fi; \
	    sleep 2; \
	done; \
	echo "ERROR: API did not become healthy within 120s. Check 'make logs-api'."; \
	exit 1

# Copy .env from .env.example if missing; error if SECRET_KEY is still the placeholder.
env:
	@if [ ! -f .env ]; then \
	    echo "Creating .env from .env.example..."; \
	    cp .env.example .env; \
	    echo "Done. Edit .env to customize ports, SMTP, etc."; \
	fi
	@if grep -q "SECRET_KEY=change-me" .env 2>/dev/null; then \
	    echo ""; \
	    echo "WARNING: SECRET_KEY in .env is still the default placeholder."; \
	    echo "Generate a real one with: openssl rand -hex 32"; \
	    echo ""; \
	fi

# ── Service lifecycle ───────────────────────────────────────────────────────
up: env
	docker compose up -d

down:
	docker compose down

restart:
	docker compose down && docker compose up -d

logs:
	docker compose logs -f

logs-api:
	docker compose logs -f api

logs-worker:
	docker compose logs -f celery_worker

logs-beat:
	docker compose logs -f celery_beat

ps:
	docker compose ps

shell:
	docker compose exec api bash

# ── Schema + data (entrypoint runs these automatically; these are escape hatches) ──
migrate:
	docker compose exec api alembic upgrade head

seed:
	docker compose exec api python scripts/seed_platforms.py
	docker compose exec api python scripts/seed_schedules.py
	docker compose exec api python scripts/seed_questionnaires.py

seed-schedules:
	docker compose exec api python scripts/seed_schedules.py

load-data:
	docker compose exec api python scripts/load_bulk_csv.py
	docker compose exec api python scripts/load_filing_data.py
	docker compose exec api python scripts/index_firms_to_es.py
	docker compose exec api python scripts/backfill_annual_aum.py

reindex:
	docker compose exec api python scripts/index_firms_to_es.py

# ── Tests ───────────────────────────────────────────────────────────────────
test:
	docker compose exec api pytest tests/ -v

test-frontend:
	cd frontend && npm test

# ── Operator tools ──────────────────────────────────────────────────────────
# Five-check readiness table. Run after `make up` (or as a CI smoke check).
verify:
	@echo "=== sec-adv readiness check ==="
	@printf "%-20s " "API /health"
	@if curl -sf http://localhost:8000/health > /dev/null 2>&1; then echo "ok"; else echo "FAIL"; fi
	@printf "%-20s " "Postgres"
	@if docker compose exec -T postgres pg_isready -U secadv > /dev/null 2>&1; then echo "ok"; else echo "FAIL"; fi
	@printf "%-20s " "Elasticsearch"
	@if curl -sf http://localhost:9200/_cluster/health | grep -q '"status":"\(green\|yellow\)"'; then echo "ok"; else echo "FAIL"; fi
	@printf "%-20s " "Redis"
	@if docker compose exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then echo "ok"; else echo "FAIL"; fi
	@printf "%-20s " "Celery worker"
	@if docker compose exec -T celery_worker celery -A celery_tasks.app inspect ping -t 5 > /dev/null 2>&1; then echo "ok"; else echo "FAIL"; fi

# Show the last N failed Celery tasks sitting in the dead_letter queue.
dlq-inspect:
	docker compose exec api python scripts/dlq_inspect.py
