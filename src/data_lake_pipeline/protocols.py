from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict


class FilterResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    passed: bool
    score: float | None = None
    reason: str | None = None
    output: dict[str, Any] = {}


class ProcessorResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    output: dict[str, Any] = {}
    metadata: dict[str, Any] | None = None


class StageContext(BaseModel):
    model_config = ConfigDict(frozen=True)

    stage_name: str
    batch_id: str
    metrics: dict[str, Any] = {}


@runtime_checkable
class AsyncFilter(Protocol):
    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[FilterResult]: ...


@runtime_checkable
class AsyncProcessor(Protocol):
    async def __call__(
        self, records: list[dict[str, Any]], context: StageContext
    ) -> list[ProcessorResult]: ...
