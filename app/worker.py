"""Worker node. Consumes from every stage queue, runs the matching handler,
and translates outcomes into broker actions:

  success / duplicate  -> ack
  transient failure    -> publish to retry queue (exp. backoff) or DLQ, then ack
  crash before ack     -> RabbitMQ redelivers to another worker automatically

Crash recovery rests on three things: manual acks, prefetch=1 (only one
in-flight message per worker, so a kill loses at most that one — and it is
unacked, hence redelivered), and the (job_id, stage) idempotency guard that
makes the redelivery safe.
"""
from __future__ import annotations

import json
import logging
import signal

from app import broker, storage
from app.config import settings
from app.db import SessionLocal, init_db
from app.errors import DuplicateStage, TransientError
from app.logging_conf import configure_logging
from app.models import DLQRecord
from app.pipeline import HANDLERS

log = configure_logging("worker")
_should_stop = False


def _on_message(channel, method, properties, body):
    stage = method.routing_key
    message_id = properties.message_id or "unknown"
    attempt = (properties.headers or {}).get("x-attempt", 0)
    redelivered = method.redelivered

    try:
        msg = json.loads(body)
    except json.JSONDecodeError:
        log.error("undecodable message -> DLQ", extra={"stage": stage})
        broker.publish_dlq(channel, stage, body, message_id, attempt)
        channel.basic_ack(method.delivery_tag)
        return

    job_id = msg.get("job_id", "?")
    log.info(
        "received", extra={"job_id": job_id, "stage": stage, "message_id": message_id,
                           "attempt": attempt, "event": "redelivered" if redelivered else "new"},
    )

    session = SessionLocal()
    try:
        HANDLERS[stage](session, msg, message_id)
        channel.basic_ack(method.delivery_tag)

    except DuplicateStage:
        log.info("duplicate -> ack & skip", extra={"job_id": job_id, "stage": stage})
        channel.basic_ack(method.delivery_tag)

    except TransientError as exc:
        next_attempt = attempt + 1
        if next_attempt <= settings.max_retries:
            log.warning(
                f"transient failure: {exc} -> retry {next_attempt}/{settings.max_retries}",
                extra={"job_id": job_id, "stage": stage, "attempt": next_attempt},
            )
            broker.publish_retry(channel, stage, next_attempt, body, message_id, next_attempt)
        else:
            log.error(
                f"retries exhausted: {exc} -> DLQ",
                extra={"job_id": job_id, "stage": stage, "attempt": attempt},
            )
            _record_dlq(session, job_id, stage, attempt, str(exc))
            broker.publish_dlq(channel, stage, body, message_id, attempt)
        channel.basic_ack(method.delivery_tag)

    except Exception as exc:  # noqa: BLE001 — unexpected: treat like transient
        next_attempt = attempt + 1
        if next_attempt <= settings.max_retries:
            log.exception("unexpected error -> retry", extra={"job_id": job_id, "stage": stage})
            broker.publish_retry(channel, stage, next_attempt, body, message_id, next_attempt)
        else:
            log.exception("unexpected error -> DLQ", extra={"job_id": job_id, "stage": stage})
            _record_dlq(session, job_id, stage, attempt, str(exc))
            broker.publish_dlq(channel, stage, body, message_id, attempt)
        channel.basic_ack(method.delivery_tag)
    finally:
        session.close()


def _record_dlq(session, job_id, stage, attempts, reason):
    try:
        from app.models import Job, FAILED
        session.add(DLQRecord(job_id=job_id, stage=stage, attempts=attempts, reason=reason))
        job = session.get(Job, job_id)
        if job is not None:
            job.status = FAILED
            job.error = f"{stage}: {reason}"
        session.commit()
    except Exception:  # noqa: BLE001
        session.rollback()


def _install_signal_handlers(channel):
    def _stop(signum, _frame):
        global _should_stop
        _should_stop = True
        log.info(f"signal {signum} received -> graceful shutdown")
        try:
            channel.stop_consuming()
        except Exception:  # noqa: BLE001
            pass

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)


def main():
    init_db()
    storage.ensure_bucket()
    conn = broker.connect()
    channel = conn.channel()
    broker.declare_topology(channel)
    channel.basic_qos(prefetch_count=1)  # one in-flight message per worker

    for stage in broker.STAGES:
        channel.basic_consume(f"q.{stage}", on_message_callback=_on_message)

    _install_signal_handlers(channel)
    log.info("worker online, consuming parse/tts/stitch")
    try:
        channel.start_consuming()
    finally:
        try:
            conn.close()
        except Exception:  # noqa: BLE001
            pass
        log.info("worker stopped")


if __name__ == "__main__":
    main()
