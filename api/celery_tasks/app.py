import sys

# Ensure the volume-mounted source always takes precedence over the
# stale copy baked into the Docker image at /app. Forked pool workers
# may run with cwd=/app, which would otherwise shadow /project/api.
if "/project/api" not in sys.path:
    sys.path.insert(0, "/project/api")

from celery import Celery
from kombu import Queue

from config import settings

# Named queues. Keep this list in sync with the `-Q` flag on the worker
# command in docker-compose.yml. `dead_letter` holds tasks that exhausted
# their retries — inspect via `make dlq-inspect`.
#
# Queue priority (listed highest to lowest):
#   sync  — monthly data sync and brochure fetches; must not be blocked by
#            the large refresh_firm_task floods that fill the celery queue
#   match — interactive bulk-match jobs
#   celery — default: refresh_firm_task, alert evaluation, etc.
QUEUES = (
    Queue("sync"),
    Queue("celery"),
    Queue("match"),
    Queue("dead_letter"),
)

app = Celery(
    "sec_adv",
    broker=settings.redis_url,
    backend=settings.redis_url,
    include=[
        "celery_tasks.tasks",
        "celery_tasks.match_tasks",
        "celery_tasks.export_tasks",
        "celery_tasks.monthly_sync",
        "celery_tasks.refresh_tasks",
        "celery_tasks.brochure_tasks",
        "celery_tasks.alert_tasks",
    ],
)

app.conf.update(
    # Serialization
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,

    # Reliability
    task_acks_late=True,              # ack only after task succeeds (or gives up)
    task_reject_on_worker_lost=True,  # re-queue if worker crashes mid-task
    worker_prefetch_multiplier=1,     # don't hoard tasks; fair scheduling

    # Global task time limits. Per-task overrides welcome.
    task_soft_time_limit=3000,        # 50 min — raises SoftTimeLimitExceeded
    task_time_limit=3600,             # 60 min — kills the task hard

    # Broker reconnection
    broker_connection_retry_on_startup=True,

    # Beat scheduler reads schedules from the cron_schedules table
    beat_scheduler="celery_tasks.db_scheduler:DatabaseScheduler",
    beat_schedule_filename="/tmp/celerybeat-schedule",

    # Queues and routing
    task_queues=QUEUES,
    task_default_queue="celery",
    task_routes={
        "celery_tasks.match_tasks.run_bulk_match": {"queue": "match"},
        # Sync tasks get their own queue so refresh_firm_task floods don't
        # delay manually-triggered monthly syncs or brochure fetches.
        "monthly_sync.monthly_data_sync":                      {"queue": "sync"},
        "brochure_tasks.sync_all_platforms_brochures":         {"queue": "sync"},
        "brochure_tasks.sync_platform_brochures":              {"queue": "sync"},
        "brochure_tasks.fetch_firm_brochures":                 {"queue": "sync"},
    },
)
