from celery import Celery
from celery.schedules import crontab

from config import settings

app = Celery(
    "sec_adv",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "celery_tasks.tasks",
        "celery_tasks.match_tasks",
        "celery_tasks.export_tasks",
        "celery_tasks.monthly_sync",
    ],
)

app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    beat_schedule={
        "cleanup-expired-exports": {
            "task": "export_tasks.cleanup_expired_exports",
            # Runs every hour at :05
            "schedule": crontab(minute=5),
        },
        "monthly-pdf-sync": {
            "task": "monthly_sync.monthly_pdf_sync",
            # 2nd of every month at 06:00 UTC
            "schedule": crontab(minute=0, hour=6, day_of_month=2),
        },
    },
)
