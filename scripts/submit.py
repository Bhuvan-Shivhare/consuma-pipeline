"""Submit a manuscript to the gateway and poll until it finishes.
Stdlib-only — no pip install needed.

    python scripts/submit.py "Line one.\\nLine two.\\nLine three."
    python scripts/submit.py            # uses a built-in sample manuscript
"""
from __future__ import annotations

import sys
import time

from _http import get_json, post_json

SAMPLE = (
    "The lighthouse keeper lit the lamp at dusk.\n"
    "A storm rolled in from the grey Atlantic.\n"
    "She heard a knock against the iron door.\n"
    "Nothing was there but the howling wind."
)


def main():
    manuscript = sys.argv[1] if len(sys.argv) > 1 else SAMPLE
    job_id = post_json("/jobs", {"manuscript": manuscript})["job_id"]
    print(f"submitted job {job_id}")

    while True:
        time.sleep(1.5)
        job = get_json(f"/jobs/{job_id}")
        print(f"  status={job['status']}")
        if job["status"] in ("COMPLETED", "FAILED"):
            print(f"final: {job}")
            break


if __name__ == "__main__":
    main()
