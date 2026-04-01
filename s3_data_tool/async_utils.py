"""Async utility functions."""
import asyncio
from collections.abc import Awaitable, Callable
from typing import TypeVar

T = TypeVar('T')


async def with_semaphore(
    func: Callable[[], Awaitable[T]],
    semaphore: asyncio.Semaphore,
) -> T:
    """Execute async function with semaphore-limited concurrency."""
    async with semaphore:
        return await func()

