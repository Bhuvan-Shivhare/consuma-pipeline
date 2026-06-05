"""Distributed counting semaphore in Redis (Constraint A: max 3 concurrent
TTS vendor calls GLOBALLY, across every worker replica).

Implemented as a sorted set whose members are acquisition tokens scored by
their expiry time. The acquire script atomically (a) evicts expired holders
and (b) admits the caller only if live holders < limit. Expiry means a worker
that crashes WHILE holding a slot cannot leak it forever — the slot frees
itself after the TTL. This is the classic Redis Redlock-style pattern but for
a counting (not binary) lock.
"""
from __future__ import annotations

import time
import uuid
from contextlib import contextmanager

from app.config import settings
from app.redis_client import client

_KEY = "tts:semaphore"

# now and ttl are in SECONDS (float). Returns 1 on admit, 0 on full.
_ACQUIRE_LUA = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local token = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now)
if redis.call('ZCARD', key) < limit then
  redis.call('ZADD', key, now + ttl, token)
  redis.call('EXPIRE', key, math.ceil(ttl) + 1)
  return 1
end
return 0
"""

_RELEASE_LUA = """
return redis.call('ZREM', KEYS[1], ARGV[1])
"""

_acquire = client.register_script(_ACQUIRE_LUA)
_release = client.register_script(_RELEASE_LUA)


class SemaphoreTimeout(Exception):
    """Raised when a slot could not be acquired in time; the caller turns this
    into a transient failure so the message is retried later rather than lost."""


def current_usage() -> int:
    """Live count of held TTS slots (after evicting expired holders).
    Used by the dashboard to visualise Constraint A in real time."""
    client.zremrangebyscore(_KEY, "-inf", time.time())
    return client.zcard(_KEY)


@contextmanager
def tts_slot(ttl: float = 30.0, wait_timeout: float = 25.0, poll: float = 0.2):
    token = uuid.uuid4().hex
    deadline = time.time() + wait_timeout
    while True:
        granted = _acquire(keys=[_KEY], args=[time.time(), ttl, settings.tts_max_concurrency, token])
        if granted == 1:
            break
        if time.time() >= deadline:
            raise SemaphoreTimeout("TTS concurrency limit busy")
        time.sleep(poll)
    try:
        yield
    finally:
        _release(keys=[_KEY], args=[token])
