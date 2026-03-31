import logging

from fastapi import FastAPI

from config import settings

logging.basicConfig(level=settings.log_level.upper())
logger = logging.getLogger(__name__)

app = FastAPI(
    title="SEC Adviser Database Platform",
    version="0.1.0",
    description="Self-hosted mirror of SEC-registered investment adviser data.",
)


@app.get("/health", tags=["meta"])
def health() -> dict:
    return {"status": "ok"}
