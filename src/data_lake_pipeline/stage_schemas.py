from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict


class FilterCompletion(BaseModel):
    model_config = ConfigDict(frozen=True)

    filter_name: str
    completed_at: str
    output_key: str
    passed_count: int = 0
    rejected_count: int = 0


class StageAwareBatchManifest(BaseModel):
    model_config = ConfigDict(frozen=False)

    batch_id: str
    source: str
    original_key: str
    state: Literal["pending", "inflight", "completed", "failed"]
    pipeline_stage: str
    parent_batch_id: str | None = None
    created_at: str

    checkpoint_interval: int = 1000
    chunk_keys: list[str] = []
    processed_ids_count: int = 0

    locked_by: str | None = None
    locked_at: str | None = None
    error: str | None = None

    completed_filters: list[FilterCompletion] = []
    merged_annotations_key: str | None = None

    def is_filter_complete(self, name: str) -> bool:
        return any(f.filter_name == name for f in self.completed_filters)

    def get_filter_completion(self, name: str) -> FilterCompletion | None:
        for f in self.completed_filters:
            if f.filter_name == name:
                return f
        return None
