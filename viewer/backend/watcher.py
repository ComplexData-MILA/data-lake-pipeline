"""S3 watcher: detects new objects and emits viewer events.

This is the PRIMARY event producer in the firewalled-VM topology (writers on
the Mila cluster cannot reach the VM's Redis): it polls the S3 listings of
datasets that have active SSE subscribers and diffs against the previous
snapshot. Watcher events carry ``row_count=None``, so the UI's "+N new rows"
counter is only fed by direct producer events when writers can publish —
never double-counted.

A subscriber registered as ``""`` (an SSE client without a dataset filter)
means "watch all datasets": the poll set is expanded via ``list_datasets_fn``
so the global activity stream stays live.

Leader election (Redis SET NX) keeps only one replica polling; with Redis
unavailable the single instance polls directly.
"""

import asyncio
import logging
import os
import socket
import uuid
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

WATCHER_INTERVAL = float(os.environ.get("VIEWER_WATCHER_INTERVAL", "5"))
LEADER_KEY = "viewer:watcher:leader"
LEADER_TTL = 10  # seconds; renewed each poll


class S3Watcher:
    def __init__(
        self,
        bucket: str,
        prefix: str,
        bus,
        list_fn,
        redis_client=None,
        interval: float = WATCHER_INTERVAL,
        list_datasets_fn=None,
    ):
        """*list_fn* is a blocking callable ``(dataset) -> {key: (etag, size, ts)}``.

        *list_datasets_fn* (blocking, ``() -> [dataset]``) expands a global
        ``""`` subscriber (watch-all) into the full dataset list.
        """
        self._bucket = bucket
        self._prefix = prefix
        self._bus = bus
        self._list_fn = list_fn
        self._redis = redis_client
        self._interval = interval
        self._list_datasets_fn = list_datasets_fn
        self._host_id = socket.gethostname()
        self._refcounts: dict[str, int] = {}
        self._snapshots: dict[str, dict] = {}
        self._task: asyncio.Task | None = None

    # -- subscribers ---------------------------------------------------------

    def add_subscriber(self, dataset: str) -> None:
        # "" is a valid subscriber meaning "watch all datasets".
        self._refcounts[dataset] = self._refcounts.get(dataset, 0) + 1

    def remove_subscriber(self, dataset: str) -> None:
        self._refcounts[dataset] = self._refcounts.get(dataset, 1) - 1
        if self._refcounts[dataset] <= 0:
            self._refcounts.pop(dataset, None)
            self._snapshots.pop(dataset, None)

    @property
    def watched_datasets(self) -> list[str]:
        return sorted(self._refcounts)

    async def _datasets_to_poll(self) -> list[str]:
        """Expand a global ('') subscription into the full dataset list."""
        if "" in self._refcounts:
            if self._list_datasets_fn is None:
                logger.warning(
                    "global subscriber but no list_datasets_fn; polling named datasets only"
                )
                return [d for d in self._refcounts if d]
            try:
                datasets = await asyncio.to_thread(self._list_datasets_fn)
                return sorted(set(datasets) | {d for d in self._refcounts if d})
            except Exception as e:  # noqa: BLE001
                logger.warning(f"list_datasets failed: {e}")
                return [d for d in self._refcounts if d]
        return sorted(self._refcounts)

    # -- lifecycle -----------------------------------------------------------

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            self._task = None

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval)
            try:
                await self.poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                logger.warning(f"watcher poll error: {e}")

    # -- polling -------------------------------------------------------------

    async def _acquire_leadership(self) -> bool:
        if self._redis is None:
            return True
        try:
            acquired = await self._redis.set(
                LEADER_KEY, self._host_id, nx=True, ex=LEADER_TTL
            )
            if acquired:
                return True
            # Already exists: renew only if we own it.
            owner = await self._redis.get(LEADER_KEY)
            if owner is not None and owner.decode() == self._host_id:
                await self._redis.expire(LEADER_KEY, LEADER_TTL)
                return True
            return False
        except Exception:  # noqa: BLE001
            return True

    async def poll_once(self) -> None:
        if not self._refcounts:
            return
        if not await self._acquire_leadership():
            return
        for dataset in await self._datasets_to_poll():
            snapshot = await asyncio.to_thread(self._list_fn, dataset)
            prev = self._snapshots.get(dataset)
            self._snapshots[dataset] = snapshot
            if prev is None:
                logger.info(
                    "watcher: baseline for %s (%d objects)", dataset, len(snapshot)
                )
                continue  # first poll: baseline only
            changed = [k for k, meta in snapshot.items() if prev.get(k) != meta]
            removed = [k for k in prev if k not in snapshot]
            if not changed and not removed:
                continue
            base = f"{self._prefix.rstrip('/')}/{dataset}/"
            batches = sorted(
                {
                    k[len(base):].split("/")[0]
                    for k in changed + removed
                    if k.startswith(base)
                    and k[len(base):].split("/")[0]
                    not in ("annotations", "_index", "_migration")
                }
            )
            await self._bus.publish(
                {
                    "event_id": uuid.uuid4().hex,
                    "type": "rows_ingested",
                    "dataset": dataset,
                    "batch": batches[0] if batches else None,
                    "annotator": None,
                    "run_id": None,
                    "row_count": None,
                    "prefix": self._prefix,
                    "bucket": self._bucket,
                    "ts": datetime.now(timezone.utc).isoformat(),
                    "source": "s3_watcher",
                }
            )
            logger.info(
                f"watcher: {dataset} changed ({len(changed)} new, "
                f"{len(removed)} removed objects)"
            )
