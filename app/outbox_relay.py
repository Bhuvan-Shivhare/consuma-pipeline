"""Outbox relay — the second half of the transactional-outbox pattern.

Polls the `outbox` table for unpublished events and publishes them to the
broker, then marks them published. `SELECT ... FOR UPDATE SKIP LOCKED` lets
several relay replicas run safely without publishing the same event twice.
At-least-once publish here is fine: consumers are idempotent on (job_id, stage).
"""
from __future__ import annotations

import json
import time

from sqlalchemy import select

from app import broker
from app.db import SessionLocal, init_db
from app.logging_conf import configure_logging
from app.models import OutboxEvent
from datetime import datetime, timezone

log = configure_logging("relay")
BATCH = 50
POLL_SECONDS = 0.5


def main():
    init_db()
    conn = broker.connect()
    channel = conn.channel()
    broker.declare_topology(channel)
    log.info("outbox relay online")

    while True:
        published = _drain(channel)
        if published == 0:
            time.sleep(POLL_SECONDS)


def _drain(channel) -> int:
    session = SessionLocal()
    count = 0
    try:
        rows = session.execute(
            select(OutboxEvent)
            .where(OutboxEvent.published.is_(False))
            .order_by(OutboxEvent.created_at)
            .limit(BATCH)
            .with_for_update(skip_locked=True)
        ).scalars().all()

        for ev in rows:
            body = json.dumps(ev.payload).encode("utf-8")
            broker.publish_work(channel, ev.routing_key, body, message_id=ev.id, attempt=0)
            ev.published = True
            ev.published_at = datetime.now(timezone.utc)
            count += 1
            log.info("published", extra={"job_id": ev.aggregate_id, "event": ev.event_type})

        session.commit()
        return count
    except Exception:  # noqa: BLE001
        session.rollback()
        log.exception("relay batch failed")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    main()
