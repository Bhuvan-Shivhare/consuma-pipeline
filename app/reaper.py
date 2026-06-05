"""Stuck-job reaper — the "resume upon restart" half of crash recovery.

RabbitMQ already redelivers messages that were never acked. The reaper covers
the remaining gap: a job whose in-flight message was somehow lost while the job
sits in a non-terminal state. It finds jobs that have not advanced for longer
than STUCK_JOB_SECONDS and re-emits the event for their current stage via the
outbox. Re-emission is safe because consumers dedupe on (job_id, stage): if the
stage already finished, the resume is a no-op; if it never finished, it runs.
"""
from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone

from app.config import settings
from app.db import SessionLocal, init_db
from app.logging_conf import configure_logging
from app import models
import uuid

log = configure_logging("reaper")
POLL_SECONDS = 15

# Current status -> (event_type, routing_key) needed to resume.
RESUME = {
    models.PENDING: ("JobCreated", "parse"),
    models.PARSING: ("JobCreated", "parse"),
    models.PARSED: ("TextParsed", "tts"),
    models.TTS_PROCESSING: ("TextParsed", "tts"),
    models.TTS_DONE: ("AudioGenerated", "stitch"),
    models.STITCHING: ("AudioGenerated", "stitch"),
}


def main():
    init_db()
    log.info("reaper online")
    while True:
        _sweep()
        time.sleep(POLL_SECONDS)


def _sweep():
    cutoff = datetime.now(timezone.utc) - timedelta(seconds=settings.stuck_job_seconds)
    session = SessionLocal()
    try:
        stuck = (
            session.query(models.Job)
            .filter(models.Job.status.in_(list(models.NON_TERMINAL)))
            .filter(models.Job.updated_at < cutoff)
            .limit(100)
            .all()
        )
        for job in stuck:
            # Don't resume a stage that already committed.
            done = {p.stage for p in session.query(models.ProcessedStage)
                    .filter_by(job_id=job.id).all()}
            event_type, rk = RESUME[job.status]
            if rk in done:
                continue
            session.add(models.OutboxEvent(
                id=uuid.uuid4().hex, aggregate_id=job.id,
                event_type=event_type, routing_key=rk, payload={"job_id": job.id},
            ))
            # Touch updated_at so we don't re-emit every cycle.
            job.updated_at = datetime.now(timezone.utc)
            log.warning("resuming stuck job", extra={"job_id": job.id, "stage": rk,
                                                      "event": event_type})
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()
        log.exception("reaper sweep failed")
    finally:
        session.close()


if __name__ == "__main__":
    main()
