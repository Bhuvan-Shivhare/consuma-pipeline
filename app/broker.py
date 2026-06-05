"""RabbitMQ topology + publish helpers for hand-rolled choreography.

Topology (no managed orchestrator anywhere):

  pipeline.work  (direct) ── parse ──> q.parse
                          ── tts   ──> q.tts
                          ── stitch ─> q.stitch

  pipeline.retry (direct) ── "<stage>.<level>" ──> q.retry.<stage>.<level>
        each retry queue:  x-message-ttl = backoff[level],
                           x-dead-letter-exchange = pipeline.work,
                           x-dead-letter-routing-key = <stage>
        => after the TTL elapses the message is dead-lettered straight back
           into its own work queue. Per-stage/per-level queues mean every
           message in a queue shares one TTL, so there is NO head-of-line
           blocking (the classic single-delay-queue trap) and the rest of the
           pipeline keeps flowing while a poisoned job waits out its backoff.

  pipeline.dlx   (direct) ── <stage> ──> q.dlq   (terminal dead letters)
"""
from __future__ import annotations

import time

import pika

from app.config import settings

WORK_EXCHANGE = "pipeline.work"
RETRY_EXCHANGE = "pipeline.retry"
DLX_EXCHANGE = "pipeline.dlx"
DLQ_QUEUE = "q.dlq"

STAGES = ["parse", "tts", "stitch"]


def connect(retries: int = 30, delay: float = 2.0) -> pika.BlockingConnection:
    params = pika.URLParameters(settings.rabbitmq_url)
    # Generous heartbeat: a worker may sit inside a simulated vendor call.
    params.heartbeat = 600
    params.blocked_connection_timeout = 300
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            return pika.BlockingConnection(params)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"rabbitmq not reachable: {last_err}")


def declare_topology(channel: pika.adapters.blocking_connection.BlockingChannel) -> None:
    channel.exchange_declare(WORK_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(RETRY_EXCHANGE, exchange_type="direct", durable=True)
    channel.exchange_declare(DLX_EXCHANGE, exchange_type="direct", durable=True)

    channel.queue_declare(DLQ_QUEUE, durable=True)

    for stage in STAGES:
        channel.queue_declare(f"q.{stage}", durable=True)
        channel.queue_bind(f"q.{stage}", WORK_EXCHANGE, routing_key=stage)

        channel.queue_bind(DLQ_QUEUE, DLX_EXCHANGE, routing_key=stage)

        for level, ttl_ms in enumerate(settings.retry_backoffs_ms, start=1):
            rq = f"q.retry.{stage}.{level}"
            channel.queue_declare(
                rq,
                durable=True,
                arguments={
                    "x-message-ttl": ttl_ms,
                    "x-dead-letter-exchange": WORK_EXCHANGE,
                    "x-dead-letter-routing-key": stage,
                },
            )
            channel.queue_bind(rq, RETRY_EXCHANGE, routing_key=f"{stage}.{level}")


def _props(message_id: str, attempt: int) -> pika.BasicProperties:
    return pika.BasicProperties(
        message_id=message_id,
        delivery_mode=2,  # persistent
        headers={"x-attempt": attempt},
    )


def publish_work(channel, routing_key: str, body: bytes, message_id: str, attempt: int = 0) -> None:
    channel.basic_publish(WORK_EXCHANGE, routing_key, body, properties=_props(message_id, attempt))


def publish_retry(channel, stage: str, level: int, body: bytes, message_id: str, attempt: int) -> None:
    channel.basic_publish(
        RETRY_EXCHANGE, f"{stage}.{level}", body, properties=_props(message_id, attempt)
    )


def publish_dlq(channel, stage: str, body: bytes, message_id: str, attempt: int) -> None:
    channel.basic_publish(DLX_EXCHANGE, stage, body, properties=_props(message_id, attempt))
