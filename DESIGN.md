# Design & Architecture Decisions

This document explains the *why* behind the build — the part the assignment
actually grades ("architectural choices, state management across boundaries,
edge-case handling, and system reliability"). The AI steps are deliberately
simulated; the engineering is in the reliability guarantees.

## 1. Choreography, not orchestration (the hard constraint)

Managed orchestrators (Temporal/Airflow/Step Functions/Celery) are banned, so
each stage advances the pipeline by **publishing an event**, and the next stage
reacts to it. There is no central controller. The "state machine" lives in
Postgres and the transitions are driven by broker events:

```
PENDING --JobCreated--> PARSING --TextParsed--> TTS_PROCESSING
       --AudioGenerated--> STITCHING --> COMPLETED   (FAILED = dead-lettered)
```

Trade-off: choreography is harder to reason about globally than a single
orchestrator workflow, but it has no single point of failure and scales by
just adding workers. We recover global visibility via the `jobs` table +
`/metrics`.

## 2. The dual-write problem → Transactional Outbox

The naive ingestion is: write the job to the DB, then publish `JobCreated` to
the broker. If the process dies between those two steps you get either a job
with no event (stuck forever) or — with retries — an event for a job that was
never committed. **Two systems, no shared transaction.**

Fix: the gateway writes the `Job` row **and** an `outbox` row in **one** DB
transaction. A separate **relay** polls unpublished outbox rows and publishes
them, marking them sent. Now the event is durably tied to the state change.

- At-least-once publish is acceptable because consumers are idempotent (§3).
- The relay uses `SELECT ... FOR UPDATE SKIP LOCKED`, so multiple relay
  replicas can run without publishing the same event twice.

## 3. Idempotency keyed by (job_id, stage), not message_id

The obvious idempotency key is the message id. We deliberately use the
**composite `(job_id, stage)`** instead, enforced by a `UNIQUE` constraint on
`processed_stages`. This one decision solves three problems at once:

1. **Duplicate delivery** — the broker redelivers `JobCreated` twice → the
   second commit hits the unique constraint → rolled back, acked, no-op.
2. **Safe crash recovery** — a stage that *committed* can never run again; a
   stage that *never committed* always can (because the marker is written in
   the same transaction as the work's result).
3. **Safe reaper re-emission** — the reaper can re-fire a stage's event
   blindly; if it already ran, it's deduped.

The marker, the `Job` status update, and the next `outbox` event are all
written in a **single transaction** — so the system can never be left half-done.

## 4. Constraint A — global 3-concurrent TTS limit (distributed semaphore)

A per-process lock is useless when there are N worker replicas. We implement a
**distributed counting semaphore in Redis** via a Lua script (atomic): members
of a sorted set are acquisition tokens scored by expiry. Acquire = evict expired
holders, admit only if live holders < 3. Because slots carry a TTL, a worker
that **crashes while holding a slot cannot leak it** — the slot frees itself.
This is the Redlock idea generalised from a binary to a counting lock.

## 5. Constraint B — content-addressed TTS cache

Each text block is hashed (`sha256`). Before "calling the vendor" we check the
`tts_cache` table; a hit returns the existing MinIO object and **skips the
vendor entirely** — even across retries and across different jobs. This is
business-level idempotency layered on top of message-level idempotency.

## 6. DLQ with non-blocking exponential backoff

The trap people fall into: `sleep()` inside the consumer to back off. That
blocks the whole queue (head-of-line blocking) — one poisoned job stalls
everyone. Instead we use RabbitMQ's dead-letter mechanics:

- On transient failure, the message is published to a **per-stage, per-level
  retry queue** (`q.retry.<stage>.<level>`) with `x-message-ttl` = 5s / 30s /
  120s. When the TTL elapses, RabbitMQ dead-letters it **back into its work
  queue** (`x-dead-letter-routing-key`).
- Per-level queues mean every message in a queue shares one TTL → **no
  head-of-line blocking**. Healthy jobs keep flowing while a poison pill waits
  out its backoff.
- After `MAX_RETRIES` (3) the message goes to `q.dlq`, and we also write a
  `DLQRecord` row so the DLQ is queryable from the API (`/dlq`), not just
  inside RabbitMQ.

The retry counter rides in the `x-attempt` header, which survives
dead-lettering.

## 7. Crash recovery — three independent layers

1. **Manual acks + `prefetch=1`.** A worker acks only *after* the stage commits.
   `docker kill` mid-job leaves the message unacked → RabbitMQ redelivers it to
   another worker. `prefetch=1` bounds the blast radius to one message.
2. **The reaper.** Covers the rarer case where an in-flight message is lost
   while a job sits in a non-terminal state. It re-emits the current stage's
   event for jobs idle beyond `STUCK_JOB_SECONDS`; idempotency makes this safe.
3. **Restart policy + graceful shutdown.** `restart: unless-stopped` brings a
   crashed worker back; `SIGTERM` handling lets a *deploy* drain in-flight work
   cleanly rather than dropping it.

Verified: in `chaos_test.py --kill` a worker is force-killed mid-run and all
healthy jobs still reach `COMPLETED`.

## 8. Why these specific infra choices

- **RabbitMQ over Kafka** — first-class dead-letter exchanges, per-message TTL,
  and redelivery make the DLQ/backoff/crash-recovery requirements far cleaner
  than rebuilding them on Kafka offsets.
- **Postgres** — transactions are the backbone of both the outbox and the
  idempotency guard; we lean on `UNIQUE` constraints and `SKIP LOCKED`.
- **Redis** — the only store fast enough for a hot-path semaphore and an
  idempotency pre-check; Lua gives atomicity.
- **MinIO** — S3 semantics locally; object writes are idempotent by key, which
  is what lets stage handlers be safely retried.

## 9. Known limitations / what I'd add for production

- **Webhook delivery is best-effort.** For guaranteed notification I'd model it
  as another outbox event + consumer with its own retries/DLQ.
- **Reaper resume is at-least-once**, relying on the idempotency guard. Fine
  here; in production I'd add a per-stage lease/heartbeat for tighter control.
- **No auth / rate-limit on the gateway** — out of scope for the assessment.
- **Observability** is structured logs + `/metrics`. Production would add
  Prometheus scraping and distributed tracing (OpenTelemetry) across hops.
