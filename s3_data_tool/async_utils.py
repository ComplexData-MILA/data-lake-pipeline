"""Async utility functions."""
import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import TypeVar

T = TypeVar('T')


async def with_semaphore(
    func: Callable[[], Awaitable[T]],
    semaphore: asyncio.Semaphore,
) -> T:
    """Execute async function with semaphore-limited concurrency."""
    async with semaphore:
        return await func()


async def chain_async_iterators(
    *iterators: AsyncIterator[T] | None,
) -> AsyncIterator[T]:
    """Chain multiple async iterators into one, skipping None values."""
    for it in iterators:
        if it is not None:
            async for item in it:
                yield item

