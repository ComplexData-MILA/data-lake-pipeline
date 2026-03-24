from __future__ import annotations

import asyncio
from asyncio import Semaphore
from typing import TYPE_CHECKING, AsyncIterator, List, Tuple

from data_lake_pipeline.protocols import (
    AsyncFilter,
    AsyncProcessor,
    FilterResult,
    ProcessorResult,
    StageContext,
)

if TYPE_CHECKING:
    pass


class StreamingStageProcessor:
    def __init__(
        self,
        handler: AsyncFilter | AsyncProcessor,
        max_concurrent: int = 100,
    ) -> None:
        self.handler = handler
        self.semaphore = Semaphore(max_concurrent)

    async def process_stream(
        self,
        input_stream: AsyncIterator[dict],
        context: StageContext,
    ) -> AsyncIterator[Tuple[dict, FilterResult | ProcessorResult]]:
        pending: List[Tuple[dict, asyncio.Task]] = []

        async for record in input_stream:
            task = asyncio.create_task(self._process_one(record, context))
            pending.append((record, task))

            completed, pending = self._collect_completed(pending)
            for rec, result in completed:
                yield rec, result

        for rec, task in pending:
            result = await task
            yield rec, result

    async def _process_one(
        self, record: dict, context: StageContext
    ) -> FilterResult | ProcessorResult:
        async with self.semaphore:
            results = await self.handler([record], context)
            return results[0]

    def _collect_completed(
        self, pending: List[Tuple[dict, asyncio.Task]]
    ) -> Tuple[
        List[Tuple[dict, FilterResult | ProcessorResult]],
        List[Tuple[dict, asyncio.Task]],
    ]:
        completed = []
        remaining = []
        for rec, task in pending:
            if task.done():
                result = task.result()
                completed.append((rec, result))
            else:
                remaining.append((rec, task))
        return completed, remaining
