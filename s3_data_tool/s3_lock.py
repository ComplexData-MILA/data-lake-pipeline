"""
S3-based long-running lock with TTL-based expiry.

Uses WSSMutex for atomic coordination during acquire/renew/release operations.
Lock files are stored as JSON in S3 with timestamp, hostname, and lock_id.

Example:
    async def merge(path: str, s3_client, bucket: str):
        async with S3Lock(path, ttl_ms=3_600_000, s3_client=s3_client, bucket=bucket) as lock:
            if not lock:
                return  # Lock held by another worker

            work_task = asyncio.create_task(do_merge_work())
            renewal_task = asyncio.create_task(_renew_lock_task(lock, work_task))

            try:
                await work_task
            finally:
                renewal_task.cancel()

    async def _renew_lock_task(lock: S3Lock, work_task: asyncio.Task):
        '''Renew lock periodically; cancel work task if renewal fails.'''
        while True:
            await asyncio.sleep((lock.ttl_ms / 1000) - 60)
            try:
                await lock.renew()
            except LockRenewalError:
                work_task.cancel()
                return
"""

import asyncio
import json
import logging
import re
import socket
import time
import uuid
from typing import Any

from mutex import WSSMutex

logger = logging.getLogger(__name__)


class LockRenewalError(Exception):
    pass


class S3Lock:
    def __init__(
        self,
        path: str,
        ttl_ms: int,
        s3_client: Any,
        bucket: str,
        prefix: str = "locks/",
    ):
        self._path = path
        self._ttl_ms = ttl_ms
        self._s3_client = s3_client
        self._bucket = bucket
        self._prefix = prefix
        self._s3_key = f"{prefix}{self._sanitize_path(path)}.lock"
        self._lock_id: str | None = None
        self._acquired = False
        self._mutex_name = self._sanitize_path(f"s3lock-{path}")

    def _sanitize_path(self, path: str) -> str:
        return re.sub(r'[^a-zA-Z0-9._-]', '-', path)

    async def acquire(self) -> bool:
        async with WSSMutex(self._mutex_name):
            existing = await self._read_lock_file()
            if existing:
                expires_at = existing["timestamp"] + (self._ttl_ms / 1000)
                if time.time() < expires_at:
                    logger.warning(
                        f"Lock acquisition failed: s3://{self._bucket}/{self._s3_key} "
                        f"(held by {existing.get('hostname', 'unknown')})"
                    )
                    return False

            self._lock_id = str(uuid.uuid4())
            lock_data = {
                "timestamp": time.time(),
                "hostname": socket.gethostname(),
                "lock_id": self._lock_id,
            }
            await self._s3_client.put_object(
                Bucket=self._bucket,
                Key=self._s3_key,
                Body=json.dumps(lock_data).encode(),
            )
            self._acquired = True
            return True

    async def renew(self) -> None:
        if not self._lock_id:
            raise LockRenewalError("Cannot renew: lock was never acquired")

        async with WSSMutex(self._mutex_name):
            existing = await self._read_lock_file()
            if not existing:
                raise LockRenewalError("Cannot renew: lock file does not exist")
            if existing.get("lock_id") != self._lock_id:
                raise LockRenewalError(
                    f"Cannot renew: lock ownership lost (held by {existing.get('hostname', 'unknown')})"
                )

            lock_data = {
                "timestamp": time.time(),
                "hostname": socket.gethostname(),
                "lock_id": self._lock_id,
            }
            await self._s3_client.put_object(
                Bucket=self._bucket,
                Key=self._s3_key,
                Body=json.dumps(lock_data).encode(),
            )

    async def release(self) -> None:
        if not self._lock_id:
            return

        async with WSSMutex(self._mutex_name):
            existing = await self._read_lock_file()
            if existing and existing.get("lock_id") == self._lock_id:
                await self._s3_client.delete_object(
                    Bucket=self._bucket,
                    Key=self._s3_key,
                )
        self._acquired = False

    async def _read_lock_file(self) -> dict | None:
        try:
            response = await self._s3_client.get_object(
                Bucket=self._bucket,
                Key=self._s3_key,
            )
            body = await response["Body"].read()
            return json.loads(body)
        except self._s3_client.exceptions.NoSuchKey:
            return None
        except Exception:
            return None

    async def __aenter__(self):
        await self.acquire()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.release()
        return False

    def __bool__(self) -> bool:
        return self._acquired
