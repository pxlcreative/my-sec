FROM python:3.12-slim

WORKDIR /project

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source into image (overridden at runtime by volume mounts in dev)
COPY api/ api/
COPY alembic/ alembic/
COPY alembic.ini .

EXPOSE 8000
