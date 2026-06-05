"""Database engine + session factory, with a small startup retry so the app
survives racing Postgres during `docker compose up`."""
from __future__ import annotations

import time

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker

from app.config import settings
from app.models import Base

engine = create_engine(settings.database_url, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, class_=Session)


def init_db(retries: int = 30, delay: float = 2.0) -> None:
    """Wait for Postgres, then create tables (idempotent)."""
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            Base.metadata.create_all(engine)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"database not reachable: {last_err}")
