from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Generic, TypeVar

T = TypeVar("T")


@dataclass
class CacheEntry(Generic[T]):
    data: T
    fetched_at: datetime
    ttl_seconds: int

    def is_expired(self) -> bool:
        elapsed = (datetime.now(timezone.utc) - self.fetched_at).total_seconds()
        return elapsed > self.ttl_seconds


class StatusCache:
    def __init__(self, default_ttl_seconds: int = 60):
        self.default_ttl_seconds = default_ttl_seconds
        self._cache: dict[str, CacheEntry[Any]] = {}
        self._locks: dict[str, asyncio.Lock] = {}

    def _get_lock(self, key: str) -> asyncio.Lock:
        if key not in self._locks:
            self._locks[key] = asyncio.Lock()
        return self._locks[key]

    async def get_or_refresh(
        self,
        key: str,
        fetcher: Callable[[], T | Awaitable[T]],
        ttl_seconds: int | None = None,
        force_refresh: bool = False,
    ) -> T:
        ttl = ttl_seconds if ttl_seconds is not None else self.default_ttl_seconds
        lock = self._get_lock(key)

        async with lock:
            if not force_refresh and key in self._cache:
                entry = self._cache[key]
                if not entry.is_expired():
                    return entry.data

            result = fetcher()
            if asyncio.iscoroutine(result):
                data = await result
            else:
                data = result

            self._cache[key] = CacheEntry(
                data=data,
                fetched_at=datetime.now(timezone.utc),
                ttl_seconds=ttl,
            )
            return data

    def invalidate(self, key: str) -> None:
        if key in self._cache:
            del self._cache[key]

    def invalidate_all(self) -> None:
        self._cache.clear()

    def get_cache_info(self) -> dict:
        info = {}
        for key, entry in self._cache.items():
            info[key] = {
                "fetched_at": entry.fetched_at.isoformat(),
                "ttl_seconds": entry.ttl_seconds,
                "is_expired": entry.is_expired(),
            }
        return info
