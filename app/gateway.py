"""API Gateway. Accepts jobs, serves status, exposes the DLQ and metrics.

Ingestion is the textbook outbox flow: in ONE database transaction we create
the Job row (PENDING) and the JobCreated OutboxEvent. The manuscript bytes go
to MinIO first (object writes are idempotent by key). The relay publishes the
event afterwards — so we never have a committed job with a lost event, nor an
event for an uncommitted job.
"""
from __future__ import annotations

import uuid
from pathlib import Path
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
from sqlalchemy import func

from app import models, storage
from app.config import settings
from app.db import SessionLocal, init_db
from app.logging_conf import configure_logging
from app.semaphore import current_usage

log = configure_logging("gateway")
app = FastAPI(title="Distributed Multi-Modal GenAI Pipeline")


class CreateJob(BaseModel):
    manuscript: str = Field(min_length=1)
    webhook_url: str | None = None


@app.on_event("startup")
def _startup():
    init_db()
    storage.ensure_bucket()
    log.info("gateway online")


_DASHBOARD_HTML = (Path(__file__).parent / "dashboard.html").read_text(encoding="utf-8")


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return _DASHBOARD_HTML


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/jobs", status_code=201)
def create_job(req: CreateJob):
    job_id = uuid.uuid4().hex
    manuscript_key = f"manuscripts/{job_id}.txt"
    storage.put_text(manuscript_key, req.manuscript)

    session = SessionLocal()
    try:
        session.add(models.Job(
            id=job_id, status=models.PENDING,
            manuscript_key=manuscript_key, webhook_url=req.webhook_url,
        ))
        session.add(models.OutboxEvent(
            id=uuid.uuid4().hex, aggregate_id=job_id,
            event_type="JobCreated", routing_key="parse",
            payload={"job_id": job_id},
        ))
        session.commit()
    finally:
        session.close()

    log.info("job accepted", extra={"job_id": job_id, "event": "JobCreated"})
    return {"job_id": job_id, "status": models.PENDING}


@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    session = SessionLocal()
    try:
        job = session.get(models.Job, job_id)
        if job is None:
            raise HTTPException(404, "job not found")
        return _serialize(job)
    finally:
        session.close()


@app.get("/jobs")
def list_jobs(limit: int = 50):
    session = SessionLocal()
    try:
        jobs = session.query(models.Job).order_by(models.Job.created_at.desc()).limit(limit).all()
        return [_serialize(j) for j in jobs]
    finally:
        session.close()


@app.get("/dlq")
def list_dlq(limit: int = 50):
    session = SessionLocal()
    try:
        rows = session.query(models.DLQRecord).order_by(models.DLQRecord.created_at.desc()).limit(limit).all()
        return [{"job_id": r.job_id, "stage": r.stage, "attempts": r.attempts,
                 "reason": r.reason, "at": r.created_at.isoformat()} for r in rows]
    finally:
        session.close()


@app.get("/metrics")
def metrics():
    session = SessionLocal()
    try:
        by_status = dict(
            session.query(models.Job.status, func.count()).group_by(models.Job.status).all()
        )
        return {
            "jobs_by_status": by_status,
            "dlq_total": session.query(func.count(models.DLQRecord.id)).scalar(),
            "outbox_pending": session.query(func.count(models.OutboxEvent.id))
                .filter(models.OutboxEvent.published.is_(False)).scalar(),
            "tts_cache_entries": session.query(func.count(models.TTSCache.text_hash)).scalar(),
            "tts_slots_in_use": current_usage(),
            "tts_slots_limit": settings.tts_max_concurrency,
        }
    finally:
        session.close()


@app.get("/queues")
def queues():
    """Live message depth per queue, read from RabbitMQ's management API.
    Lets the dashboard show tickets actually flowing through the broker."""
    host = urlsplit(settings.rabbitmq_url).hostname or "rabbitmq"
    try:
        resp = httpx.get(
            f"http://{host}:15672/api/queues/%2F",
            auth=("guest", "guest"), timeout=3.0,
        )
        resp.raise_for_status()
        return {q["name"]: q.get("messages", 0) for q in resp.json()
                if q["name"].startswith("q.")}
    except Exception:  # noqa: BLE001 — management API momentarily unavailable
        return {}


def _serialize(job: models.Job) -> dict:
    return {
        "job_id": job.id,
        "status": job.status,
        "manuscript_key": job.manuscript_key,
        "parsed_key": job.parsed_key,
        "final_key": job.final_key,
        "error": job.error,
        "created_at": job.created_at.isoformat() if job.created_at else None,
        "updated_at": job.updated_at.isoformat() if job.updated_at else None,
        "completed_at": job.completed_at.isoformat() if job.completed_at else None,
    }
