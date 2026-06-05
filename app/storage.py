"""MinIO (S3-compatible) object storage helpers. Stores the manuscript, the
parsed blocks, each TTS audio fragment and the final stitched asset."""
from __future__ import annotations

import json
import time

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

from app.config import settings

_client = boto3.client(
    "s3",
    endpoint_url=settings.minio_endpoint,
    aws_access_key_id=settings.minio_access_key,
    aws_secret_access_key=settings.minio_secret_key,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)
BUCKET = settings.minio_bucket


def ensure_bucket(retries: int = 30, delay: float = 2.0) -> None:
    last_err: Exception | None = None
    for _ in range(retries):
        try:
            try:
                _client.head_bucket(Bucket=BUCKET)
            except ClientError:
                _client.create_bucket(Bucket=BUCKET)
            return
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            time.sleep(delay)
    raise RuntimeError(f"minio not reachable: {last_err}")


def put_bytes(key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    _client.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)
    return key


def put_text(key: str, text_value: str) -> str:
    return put_bytes(key, text_value.encode("utf-8"), "text/plain")


def put_json(key: str, obj: dict) -> str:
    return put_bytes(key, json.dumps(obj).encode("utf-8"), "application/json")


def get_bytes(key: str) -> bytes:
    return _client.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def get_text(key: str) -> str:
    return get_bytes(key).decode("utf-8")


def get_json(key: str) -> dict:
    return json.loads(get_bytes(key).decode("utf-8"))
