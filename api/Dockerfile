FROM python:3.12-slim

WORKDIR /project

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt requirements-dev.txt ./
# Install production + dev deps so `docker compose exec api pytest` works.
# To build a slim production image, pass --build-arg INSTALL_DEV=0.
ARG INSTALL_DEV=1
RUN pip install --no-cache-dir -r requirements.txt && \
    if [ "$INSTALL_DEV" = "1" ]; then pip install --no-cache-dir -r requirements-dev.txt; fi

# Copy source into image (overridden at runtime by volume mounts in dev)
COPY api/ api/
COPY alembic/ alembic/
COPY alembic.ini .
COPY scripts/ scripts/

# Shared init script for api / celery_worker / celery_beat containers.
# Waits for dependencies, then runs migrations + seeds under an advisory lock
# so the first-to-start container does the work and the rest no-op.
RUN chmod +x /project/api/entrypoint.sh
ENTRYPOINT ["/project/api/entrypoint.sh"]

EXPOSE 8000
