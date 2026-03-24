from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class StageAwareBatchManifest(BaseModel):
    model_config = ConfigDict(frozen=True)

    batch_id: str
    source: str
    original_key: str
    state: Literal["pending", "inflight", "completed", "failed"]
    pipeline_stage: str
    parent_batch_id: str | None = None
    created_at: str

    output_key_passed: str | None = None
    output_key_rejected: str | None = None

    checkpoint_interval: int = 1000
    passed_chunks: list[str] = []
    rejected_chunks: list[str] = []
    processed_ids_count: int = 0

    merged_passed_key: str | None = None
    merged_rejected_key: str | None = None

    passed_count: int | None = None
    rejected_count: int | None = None

    locked_by: str | None = None
    locked_at: str | None = None
    error: str | None = None
