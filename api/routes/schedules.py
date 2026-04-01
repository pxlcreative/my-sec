from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from celery_tasks.app import app as celery_app
from db import get_db
from schemas.cron_schedule import CronScheduleOut, CronSchedulePatch
from services import schedule_service

router = APIRouter(prefix="/schedules", tags=["schedules"])


@router.get("", response_model=list[CronScheduleOut])
def list_schedules(db: Session = Depends(get_db)) -> list[CronScheduleOut]:
    return schedule_service.list_schedules(db)


@router.patch("/{schedule_id}", response_model=CronScheduleOut)
def update_schedule(
    schedule_id: int,
    patch: CronSchedulePatch,
    db: Session = Depends(get_db),
) -> CronScheduleOut:
    schedule = schedule_service.patch_schedule(db, schedule_id, patch)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    return schedule


@router.post("/{schedule_id}/trigger")
def trigger_schedule(schedule_id: int, db: Session = Depends(get_db)) -> dict:
    schedule = schedule_service.get_schedule(db, schedule_id)
    if schedule is None:
        raise HTTPException(status_code=404, detail="Schedule not found")
    result = celery_app.send_task(schedule.task)
    return {"status": "accepted", "task_id": result.id}
