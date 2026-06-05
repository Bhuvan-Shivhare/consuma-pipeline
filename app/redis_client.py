"""Shared Redis connection used for the fast idempotency pre-check and the
distributed TTS semaphore."""
from __future__ import annotations

import redis

from app.config import settings

client = redis.Redis.from_url(settings.redis_url, decode_responses=True)
