from __future__ import annotations

from sqlalchemy.orm import Session

from models.cron_schedule import CronSchedule
from schemas.cron_schedule import CronSchedulePatch


def list_schedules(db: Session) -> list[CronSchedule]:
    return db.query(CronSchedule).order_by(CronSchedule.id).all()


def get_schedule(db: Session, schedule_id: int) -> CronSchedule | None:
    return db.get(CronSchedule, schedule_id)


def patch_schedule(db: Session, schedule_id: int, patch: CronSchedulePatch) -> CronSchedule | None:
    schedule = db.get(CronSchedule, schedule_id)
    if schedule is None:
        return None
    for field, value in patch.model_dump(exclude_none=True).items():
        setattr(schedule, field, value)
    db.commit()
    db.refresh(schedule)
    return schedule
