from celery import Celery

from config import settings

app = Celery(
    "sec_adv",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=["celery_tasks.tasks", "celery_tasks.match_tasks"],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
)
