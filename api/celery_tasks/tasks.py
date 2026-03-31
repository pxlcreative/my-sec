import logging

from celery_tasks.app import app

logger = logging.getLogger(__name__)


@app.task
def ping() -> str:
    """Smoke-test task."""
    logger.info("ping task executed")
    return "pong"
