"""The four simulated pipeline stages and the idempotent-commit helper that
ties state, outbox and the exactly-once guard into one transaction.

Every stage follows the same contract:
    1. fast idempotency pre-check (skip obvious duplicates cheaply);
    2. do the (idempotent) work — may raise TransientError;
    3. commit ProcessedStage + Job update + next OutboxEvent atomically.
If step 3's unique (job_id, stage) insert conflicts, a concurrent/duplicate
delivery already finished the stage, so we roll back and raise DuplicateStage.
"""
from __future__ import annotations

import hashlib
import logging
import random
import time
import uuid
from datetime import datetime, timezone

import httpx
from sqlalchemy.exc import IntegrityError

from app import models, storage
from app.config import settings
from app.errors import DuplicateStage, TransientError
from app.redis_client import client as redis_client
from app.semaphore import SemaphoreTimeout, tts_slot

log = logging.getLogger("worker")


# --------------------------------------------------------------------------- #
# Idempotency helpers
# --------------------------------------------------------------------------- #
def _already_done(session, job_id: str, stage: str) -> bool:
    if redis_client.get(f"done:{job_id}:{stage}"):
        return True
    exists = (
        session.query(models.ProcessedStage)
        .filter_by(job_id=job_id, stage=stage)
        .first()
    )
    return exists is not None


def _commit_stage(session, job_id: str, stage: str, message_id: str, outbox_events: list[dict]) -> None:
    """Insert the exactly-once marker + emit next events. Caller has already
    mutated the Job on this session. Raises DuplicateStage on conflict."""
    session.add(models.ProcessedStage(job_id=job_id, stage=stage, message_id=message_id))
    for ev in outbox_events:
        session.add(
            models.OutboxEvent(
                id=uuid.uuid4().hex,
                aggregate_id=job_id,
                event_type=ev["event_type"],
                routing_key=ev["routing_key"],
                payload=ev["payload"],
            )
        )
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        raise DuplicateStage(f"{job_id}/{stage} already committed")
    redis_client.setex(f"done:{job_id}:{stage}", 3600, "1")


# --------------------------------------------------------------------------- #
# Stage: parse  (simulated LLM text parsing, 15% 500 error)
# --------------------------------------------------------------------------- #
def handle_parse(session, msg: dict, message_id: str) -> None:
    job_id = msg["job_id"]
    if _already_done(session, job_id, "parse"):
        raise DuplicateStage(job_id)

    job = session.get(models.Job, job_id)
    if job is None:
        raise DuplicateStage(job_id)  # nothing to do; ack and drop

    job.status = models.PARSING
    session.commit()

    # Simulated 500-Internal-Server-Error to exercise retry logic.
    if random.random() < settings.parse_failure_rate:
        raise TransientError("simulated 500 from parser")

    manuscript = storage.get_text(job.manuscript_key)
    blocks = [line.strip() for line in manuscript.splitlines() if line.strip()]
    if not blocks:
        blocks = [manuscript.strip() or "(empty manuscript)"]

    parsed_key = f"parsed/{job_id}.json"
    storage.put_json(parsed_key, {"blocks": blocks})

    job.parsed_key = parsed_key
    job.status = models.PARSED
    _commit_stage(
        session, job_id, "parse", message_id,
        [{"event_type": "TextParsed", "routing_key": "tts", "payload": {"job_id": job_id}}],
    )
    log.info("parsed manuscript", extra={"job_id": job_id, "stage": "parse"})


# --------------------------------------------------------------------------- #
# Stage: tts  (simulated vendor — 3 concurrent max, hash-cached, poison pill)
# --------------------------------------------------------------------------- #
def _fake_audio(block: str) -> bytes:
    # A tiny deterministic "wav-ish" payload so the object is real but cheap.
    return b"RIFFFAKE" + hashlib.sha256(block.encode()).digest() + block.encode("utf-8")[:64]


def _synthesize_block(block: str) -> str:
    text_hash = hashlib.sha256(block.encode("utf-8")).hexdigest()
    key = f"tts/{text_hash}.wav"
    # The vendor "call" — gated by the global semaphore (Constraint A).
    with tts_slot():
        if "POISON" in block:  # poison pill -> always fails -> DLQ after retries
            raise TransientError("poison pill block rejected by TTS vendor")
        time.sleep(random.uniform(0.4, 1.2))  # simulate vendor latency
        storage.put_bytes(key, _fake_audio(block), "audio/wav")
    return key


def handle_tts(session, msg: dict, message_id: str) -> None:
    job_id = msg["job_id"]
    if _already_done(session, job_id, "tts"):
        raise DuplicateStage(job_id)

    job = session.get(models.Job, job_id)
    if job is None or not job.parsed_key:
        raise DuplicateStage(job_id)

    job.status = models.TTS_PROCESSING
    session.commit()

    blocks = storage.get_json(job.parsed_key)["blocks"]
    audio_keys: list[str] = []
    for block in blocks:
        text_hash = hashlib.sha256(block.encode("utf-8")).hexdigest()
        # Constraint B: identical block already synthesized -> reuse, skip vendor.
        cached = session.get(models.TTSCache, text_hash)
        if cached:
            audio_keys.append(cached.minio_key)
            log.info("tts cache hit", extra={"job_id": job_id, "stage": "tts"})
            continue
        try:
            key = _synthesize_block(block)
        except SemaphoreTimeout as exc:
            raise TransientError(str(exc)) from exc
        # Cache hash -> key. Tolerate a race where another worker cached first.
        try:
            session.add(models.TTSCache(text_hash=text_hash, minio_key=key))
            session.commit()
        except IntegrityError:
            session.rollback()
        audio_keys.append(key)

    job.artifacts = {"audio_keys": audio_keys}
    job.status = models.TTS_DONE
    _commit_stage(
        session, job_id, "tts", message_id,
        [{"event_type": "AudioGenerated", "routing_key": "stitch", "payload": {"job_id": job_id}}],
    )
    log.info("tts generated", extra={"job_id": job_id, "stage": "tts"})


# --------------------------------------------------------------------------- #
# Stage: stitch  (combine -> upload final -> COMPLETED -> notify webhook)
# --------------------------------------------------------------------------- #
def _notify(job: "models.Job") -> None:
    url = job.webhook_url or settings.default_webhook_url
    body = {"job_id": job.id, "status": models.COMPLETED, "final_key": job.final_key}
    if not url:
        log.info("notification (local log only)", extra={"job_id": job.id, "event": "completed"})
        return
    try:
        httpx.post(url, json=body, timeout=5.0)
        log.info("webhook delivered", extra={"job_id": job.id, "event": "completed"})
    except Exception as exc:  # noqa: BLE001 — best-effort notify
        log.warning(f"webhook failed: {exc}", extra={"job_id": job.id})


def handle_stitch(session, msg: dict, message_id: str) -> None:
    job_id = msg["job_id"]
    if _already_done(session, job_id, "stitch"):
        raise DuplicateStage(job_id)

    job = session.get(models.Job, job_id)
    if job is None or not job.artifacts:
        raise DuplicateStage(job_id)

    job.status = models.STITCHING
    session.commit()

    combined = bytearray()
    for key in job.artifacts["audio_keys"]:
        combined += storage.get_bytes(key)
    final_key = f"final/{job_id}.wav"
    storage.put_bytes(final_key, bytes(combined), "audio/wav")

    job.final_key = final_key
    job.status = models.COMPLETED
    job.completed_at = datetime.now(timezone.utc)
    _commit_stage(session, job_id, "stitch", message_id, [])  # terminal: no further events
    _notify(job)
    log.info("job completed", extra={"job_id": job_id, "stage": "stitch"})


HANDLERS = {"parse": handle_parse, "tts": handle_tts, "stitch": handle_stitch}
