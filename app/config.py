"""Centralised, env-driven configuration. No value here requires an external
account — every default points at a docker-compose service."""
from __future__ import annotations

from functools import lru_cache
from typing import Annotated

from pydantic import field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+psycopg2://app:app@localhost:5432/pipeline"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/%2F"
    redis_url: str = "redis://localhost:6379/0"

    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "minioadmin"
    minio_secret_key: str = "minioadmin"
    minio_bucket: str = "pipeline"

    parse_failure_rate: float = 0.15
    tts_max_concurrency: int = 3
    max_retries: int = 3
    # NoDecode: skip pydantic-settings' JSON parsing so the validator below can
    # read the comma-separated env string (e.g. "5000,30000,120000").
    retry_backoffs_ms: Annotated[list[int], NoDecode] = [5000, 30000, 120000]
    stuck_job_seconds: int = 60

    default_webhook_url: str = ""

    @field_validator("retry_backoffs_ms", mode="before")
    @classmethod
    def _parse_backoffs(cls, v):
        if isinstance(v, str):
            return [int(x) for x in v.split(",") if x.strip()]
        return v


@lru_cache
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
