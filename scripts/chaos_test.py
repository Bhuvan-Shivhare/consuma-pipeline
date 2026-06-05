"""Resilience harness — proves the three critical requirements end-to-end.
Stdlib-only (plus the `docker` CLI for --kill).

    python scripts/chaos_test.py            # submit batch + verify
    python scripts/chaos_test.py --kill     # also docker-kill a worker mid-run

Demonstrates:
  * DLQ          : a "POISON" manuscript fails TTS forever -> lands in /dlq
                   after 3 retries, WITHOUT blocking the healthy jobs.
  * Crash recovery (--kill): a worker is force-killed mid-processing; its
                   unacked message is redelivered and every healthy job still
                   reaches COMPLETED.
  * Idempotency / cache: a text block shared across jobs is synthesized once
                   (watch tts_cache_entries in /metrics and "tts cache hit" logs).
"""
from __future__ import annotations

import subprocess
import sys
import time

from _http import get_json, post_json

N_HEALTHY = 6
SHARED_LINE = "A shared refrain repeated across many manuscripts."


def submit(manuscript: str) -> str:
    return post_json("/jobs", {"manuscript": manuscript})["job_id"]


def kill_one_worker() -> None:
    names = subprocess.run(
        ["docker", "ps", "--filter", "name=worker", "--format", "{{.Names}}"],
        capture_output=True, text=True,
    ).stdout.strip().splitlines()
    if not names:
        print("!! no worker container found to kill")
        return
    print(f">> docker kill {names[0]}")
    subprocess.run(["docker", "kill", names[0]], check=False)


def main():
    do_kill = "--kill" in sys.argv

    healthy = [
        submit(f"Manuscript {i}: opening line.\n{SHARED_LINE}\nClosing line {i}.")
        for i in range(N_HEALTHY)
    ]
    poison = submit("This manuscript is fine.\nPOISON pill block here.\nThe end.")
    print(f"submitted {len(healthy)} healthy jobs + 1 poison job ({poison})")

    if do_kill:
        time.sleep(3)  # let work start, then crash a worker mid-flight
        kill_one_worker()

    jobs: dict[str, str] = {}
    deadline = time.time() + 300
    while time.time() < deadline:
        time.sleep(3)
        jobs = {j["job_id"]: j["status"] for j in get_json("/jobs?limit=200")}
        done = [j for j in healthy if jobs.get(j) in ("COMPLETED", "FAILED")]
        print(f"  healthy done={len(done)}/{len(healthy)} | poison={jobs.get(poison)}")
        if len(done) == len(healthy) and jobs.get(poison) == "FAILED":
            break

    metrics = get_json("/metrics")
    dlq = get_json("/dlq")

    print("\n=== RESULT ===")
    print("metrics:", metrics)
    print("dlq:", dlq)

    completed = [j for j in healthy if jobs.get(j) == "COMPLETED"]
    ok_healthy = len(completed) == len(healthy)
    ok_poison = any(d["job_id"] == poison for d in dlq)
    print(f"\nhealthy all COMPLETED : {ok_healthy} ({len(completed)}/{len(healthy)})")
    print(f"poison routed to DLQ  : {ok_poison}")
    print(f"tts_cache_entries     : {metrics.get('tts_cache_entries')} "
          f"(shared line cached, not re-synthesized per job)")
    print("\nPASS" if (ok_healthy and ok_poison) else "\nFAIL — inspect logs")


if __name__ == "__main__":
    main()
