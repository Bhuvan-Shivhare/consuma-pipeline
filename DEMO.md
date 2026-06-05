# Demo Walkthrough & Video Script

A tight 4–6 minute screen recording that hits every graded requirement.
Record locally on macOS: **`Cmd + Shift + 5` → Record Entire Screen → Record**
(or QuickTime Player → File → New Screen Recording).

Have these tabs/windows ready before you hit record:
- Terminal (in the project folder)
- Browser tab: http://localhost:8000/docs  (API)
- Browser tab: http://localhost:15672      (RabbitMQ — guest/guest)
- Browser tab: http://localhost:9001       (MinIO — minioadmin/minioadmin)

---

## Scene 1 — The problem & the constraint (~30s, talking head or slide)

> "This is the async engine for a multi-modal GenAI platform: a manuscript goes
> in, a produced audio drama comes out. The AI steps are simulated — the real
> work is distributed-systems reliability. The hard rule: **no managed
> orchestrator**. So I choreograph the pipeline with a broker, Postgres and
> Redis."

Show `README.md` architecture diagram briefly.

## Scene 2 — One command brings it all up (~45s)

```bash
docker compose up -d
docker compose ps        # show all 8 services healthy
```

> "Gateway, two workers, the outbox relay, the reaper, plus RabbitMQ, Postgres,
> Redis and MinIO — all from one compose file. No external accounts or keys;
> it's fully self-contained."

## Scene 3 — Happy path end-to-end (~45s)

```bash
python3 scripts/submit.py
```

> "I submit a manuscript. The gateway stores it in MinIO and writes the job plus
> a JobCreated event in one transaction — the transactional outbox. Watch it
> move PENDING → PARSING → TTS → STITCHING → COMPLETED."

Then show the result in MinIO console (manuscripts/, parsed/, tts/, final/
folders) and the RabbitMQ queues.

## Scene 4 — The resilience proof (~2 min) — the centerpiece

```bash
cd scripts && python3 chaos_test.py --kill
```

Narrate while it runs:

> "This submits six healthy jobs plus one poison-pill manuscript, then
> **force-kills a worker mid-processing** with docker kill."

Point out as the output scrolls:
- `>> docker kill genai-pipeline-worker-1` — **crash recovery**: the killed
  worker's unacked message is redelivered; all six healthy jobs still COMPLETE.
- `poison=TTS_PROCESSING` repeating while healthy jobs finish — **no
  head-of-line blocking**: the poison pill is backing off (5s→30s→120s) without
  stalling anyone.
- `poison=FAILED` then `dlq: [... attempts: 3 ...]` — **DLQ after 3 retries
  with exponential backoff**.
- `tts_cache_entries` — **Constraint B**: the line shared across jobs was
  synthesized once and reused.
- Final line: **PASS**.

## Scene 5 — Show the guarantees in the infra (~45s)

- RabbitMQ UI (`:15672`): show `q.parse / q.tts / q.stitch`, the
  `q.retry.tts.*` retry queues, and `q.dlq` with the poisoned message.
- API: `curl localhost:8000/metrics` and `curl localhost:8000/dlq`.

> "The DLQ is even queryable from the API, not just inside RabbitMQ."

## Scene 6 — Wrap (~30s)

> "Every requirement is covered and verified: idempotent consumers via a
> (job_id, stage) unique guard, a transactional outbox for the dual-write
> problem, a Redis Lua semaphore for the global 3-concurrent TTS cap, a
> content-addressed cache, a non-blocking DLQ, and crash recovery proven by
> killing a worker live. Details and trade-offs are in DESIGN.md."

---

## Cheat-sheet of commands (paste these during recording)

```bash
docker compose up -d                       # start everything
docker compose ps                          # all services healthy
python3 scripts/submit.py                  # one job, watch it complete
cd scripts && python3 chaos_test.py --kill # full resilience proof (~3 min)
curl -s localhost:8000/metrics | python3 -m json.tool
curl -s localhost:8000/dlq     | python3 -m json.tool
docker compose logs -f worker              # live structured JSON logs
docker compose down -v                     # tear everything down
```

> Tip: the poison pill takes ~2.5 min to reach the DLQ (its 5s+30s+120s
> backoff). For a shorter recording, lower `RETRY_BACKOFFS_MS` in `.env` to
> e.g. `2000,4000,6000` and `docker compose up -d` before recording.
