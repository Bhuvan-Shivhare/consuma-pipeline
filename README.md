# Distributed Multi-Modal GenAI Pipeline
Demo : https://drive.google.com/file/d/1-c5PBdW8OQ75GTPpDjR-jqNT8n0ts3uo/view?usp=sharing
An asynchronous, **choreographed** (not orchestrated) microservices pipeline that
turns an uploaded text manuscript into a "produced audio drama." Every AI step
(LLM parsing, TTS vendor, audio stitching) is **simulated** — the real subject of
this project is **distributed-systems reliability**: state management across
service boundaries, idempotency, concurrency control, backoff/DLQ, and crash
recovery, built from raw infrastructure primitives.

> **Constraint honoured:** no managed workflow orchestrator (Temporal, Airflow,
> Step Functions, Celery). Choreography is hand-rolled on a broker + DB + Redis.

## Architecture

```
 client ──POST /jobs──> ┌──────────┐   outbox (same txn)   ┌──────────┐
                        │ Gateway  │──────────────────────>│ Postgres │
                        │ FastAPI  │                        │ (state)  │
                        └────┬─────┘                        └────┬─────┘
                             │ manuscript                        │ polls unpublished
                             ▼                                   ▼
                        ┌──────────┐                        ┌──────────┐
                        │  MinIO   │                        │  Relay   │ transactional
                        │ (objects)│                        │ (outbox) │ outbox publisher
                        └──────────┘                        └────┬─────┘
                                                                 │ publish
                                                                 ▼
   q.parse / q.tts / q.stitch   <──────  pipeline.work  <───────┘
        │ (manual ack, prefetch=1)             ▲
        ▼                                       │ TTL elapses -> dead-letter back
  ┌──────────┐  transient   pipeline.retry ── q.retry.<stage>.<level> (exp backoff)
  │ Worker(s)│ ───fail────>            └─ exhausted (3x) ─> pipeline.dlx -> q.dlq
  │ parse/tts│
  │ /stitch  │  Redis: idempotency pre-check · TTS semaphore (max 3) · cache flag
  └──────────┘
        ▲ resume stuck jobs
  ┌──────────┐
  │  Reaper  │  re-emits the current-stage event for jobs idle > STUCK_JOB_SECONDS
  └──────────┘
```

State machine: `PENDING → PARSING → PARSED → TTS_PROCESSING → TTS_DONE →
STITCHING → COMPLETED`, with `FAILED` for dead-lettered jobs.

## Components (all via `docker-compose`)

| Service | Role |
|---|---|
| **gateway** (FastAPI) | accept jobs, serve status, expose `/dlq` + `/metrics` |
| **worker** ×N | consume stage queues, run pipeline, retry/DLQ logic |
| **relay** | transactional-outbox publisher (DB → broker) |
| **reaper** | resume jobs stuck in a non-terminal state |
| **rabbitmq** | broker / choreography (+ management UI :15672) |
| **postgres** | pipeline state, outbox, idempotency, DLQ records |
| **redis** | idempotency pre-check, distributed TTS semaphore |
| **minio** | object storage for manuscript/parsed/audio/final (console :9001) |

## Quick start

```bash
cp .env.example .env          # every value is local — no external accounts/keys
docker compose up --build     # brings up infra + gateway + 2 workers + relay + reaper

# in another terminal:
python scripts/submit.py                      # submit a sample manuscript & watch it
python scripts/chaos_test.py                  # batch + poison-pill -> DLQ proof
python scripts/chaos_test.py --kill           # also docker-kill a worker mid-run
docker compose up --scale worker=4            # prove distributed choreography
```

Dashboards:
- **Live pipeline dashboard → `http://localhost:8000/`** (submit jobs, watch
  the state machine, DLQ and cache update in real time)
- API docs → `http://localhost:8000/docs`
- RabbitMQ → `http://localhost:15672` (guest/guest)
- MinIO → `http://localhost:9001` (minioadmin/minioadmin)

## Do I need to fill anything in manually?

**No external accounts, API keys, or paid services.** Every "vendor" is
simulated. The only values you provide are local passwords you invent yourself
in `.env` (Postgres user/pass, MinIO keys) — sensible defaults are already
there. Optionally paste a free [webhook.site](https://webhook.site) URL into
`DEFAULT_WEBHOOK_URL` if you want to *see* the completion notification fire;
otherwise it's logged locally.

## How each requirement is satisfied

**The pipeline steps**
1. **Ingestion** — `gateway.py`: manuscript → MinIO, `Job(PENDING)` + `JobCreated`
   written in one DB transaction (outbox), relay publishes it.
2. **Text Parsing (LLM sim)** — `pipeline.handle_parse`: downloads the file,
   splits into blocks, injects a **15 % 500-error** (`PARSE_FAILURE_RATE`).
3. **TTS (vendor sim)** — `pipeline.handle_tts`:
   - **Constraint A (concurrency):** a Redis Lua **counting semaphore**
     (`semaphore.py`) caps TTS calls at **3 globally**, across all worker
     replicas; a crashed holder's slot self-frees via TTL.
   - **Constraint B (idempotency/cost):** each block is content-addressed by
     `sha256`; a cache hit (`tts_cache`) returns the existing MinIO object and
     **never re-calls the vendor** — even across retries and across jobs.
4. **Stitch & Notify** — `pipeline.handle_stitch`: combines audio → final asset
   → `COMPLETED` → webhook (or local log).

**Critical resilience requirements**

| Requirement | Implementation |
|---|---|
| **Idempotent consumers** | `ProcessedStage` has a `UNIQUE(job_id, stage)` constraint; the marker is inserted in the *same transaction* as the state change + next event. A duplicate `JobCreated` (or any redelivery) hits the conflict, rolls back, and is acked as a no-op. A Redis flag short-circuits the common case cheaply. |
| **Dead Letter Queue** | Failures route to per-stage/per-level **retry queues** (`q.retry.<stage>.<level>`) with `x-message-ttl` = `5s/30s/120s` exponential backoff, dead-lettering back to the work queue. After **3 retries** the message goes to `q.dlq` (and a `DLQRecord` row, surfaced at `/dlq`). Per-level queues mean **no head-of-line blocking** — a poison pill waits out its backoff while every healthy job keeps flowing. |
| **Crash recovery** | **Manual acks + `prefetch=1`**: a `docker kill` mid-job leaves the message unacked, so RabbitMQ redelivers it to another worker; the `(job_id, stage)` guard makes the redelivery safe. The **reaper** additionally re-emits the current-stage event for any job idle beyond `STUCK_JOB_SECONDS`, covering "resume on restart." Workers also handle `SIGTERM` for graceful shutdown. |

## "Beyond-MVP" engineering choices

- **Transactional outbox** (`outbox` table + `relay`) eliminates the DB/broker
  dual-write race — no committed job with a lost event, no event for an
  uncommitted job. Relay uses `FOR UPDATE SKIP LOCKED` so it scales horizontally.
- **Composite `(job_id, stage)` idempotency key** (rather than message-id)
  unifies duplicate-delivery dedup *and* safe crash/reaper re-emission in one
  invariant.
- **Content-addressed TTS cache** gives business-level idempotency on top of
  message-level idempotency.
- **Structured JSON logs** correlate every line by `job_id`/`stage` across all
  services; `/metrics` exposes job counts, DLQ size, outbox backlog, cache size.
- **Reproducible chaos harness** (`scripts/chaos_test.py`) asserts the
  guarantees instead of just claiming them.

## Project layout

```
app/
  config.py        env-driven settings
  db.py            engine + session + startup wait
  models.py        Job, OutboxEvent, ProcessedStage, TTSCache, DLQRecord
  storage.py       MinIO helpers
  redis_client.py  shared Redis
  semaphore.py     distributed counting semaphore (Lua)
  broker.py        RabbitMQ topology + publish helpers
  errors.py        TransientError / DuplicateStage
  pipeline.py      the four stage handlers + idempotent commit
  gateway.py       FastAPI API
  worker.py        consumer loop (acks, retry/DLQ dispatch, graceful shutdown)
  outbox_relay.py  outbox publisher
  reaper.py        stuck-job resumer
scripts/
  submit.py        submit one manuscript and follow it
  chaos_test.py    batch + poison pill + optional worker kill, with assertions
```
