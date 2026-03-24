from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class MergeServerConfig:
    s3_bucket: str
    s3_prefix: str = ""
    s3_endpoint_url: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    merge_interval_seconds: int = 60
    max_concurrent_merges: int = 10
    max_runtime_seconds: int = 0
    lock_ttl_seconds: int = 300

    annotations_prefix: str = "annotations"
    lock_prefix: str = "merge_locks"

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> "MergeServerConfig":
        import os

        if env is None:
            env = dict(os.environ)

        s3_url = env.get("PIPELINE_S3_URL", "")
        if s3_url.startswith("s3://"):
            s3_url = s3_url[5:]
        parts = s3_url.split("/", 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        return cls(
            s3_bucket=bucket,
            s3_prefix=prefix.rstrip("/"),
            s3_endpoint_url=env.get("PIPELINE_S3_ENDPOINT_URL") or None,
            s3_access_key=env.get("PIPELINE_S3_ACCESS_KEY") or None,
            s3_secret_key=env.get("PIPELINE_S3_SECRET_KEY") or None,
            merge_interval_seconds=int(env.get("MERGE_INTERVAL_SECONDS", "60")),
            max_concurrent_merges=int(env.get("MERGE_MAX_CONCURRENT", "10")),
            max_runtime_seconds=int(env.get("MERGE_MAX_RUNTIME_SECONDS", "0")),
            lock_ttl_seconds=int(env.get("MERGE_LOCK_TTL_SECONDS", "300")),
            annotations_prefix=env.get("MERGE_ANNOTATIONS_PREFIX", "annotations"),
            lock_prefix=env.get("MERGE_LOCK_PREFIX", "merge_locks"),
        )

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "MergeServerConfig":
        return cls(
            s3_bucket=data.get("s3_bucket", ""),
            s3_prefix=data.get("s3_prefix", ""),
            s3_endpoint_url=data.get("s3_endpoint_url"),
            s3_access_key=data.get("s3_access_key"),
            s3_secret_key=data.get("s3_secret_key"),
            merge_interval_seconds=data.get("merge_interval_seconds", 60),
            max_concurrent_merges=data.get("max_concurrent_merges", 10),
            max_runtime_seconds=data.get("max_runtime_seconds", 0),
            lock_ttl_seconds=data.get("lock_ttl_seconds", 300),
            annotations_prefix=data.get("annotations_prefix", "annotations"),
            lock_prefix=data.get("lock_prefix", "merge_locks"),
        )
