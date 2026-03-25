from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class FilterState(BaseModel):
    model_config = ConfigDict(frozen=False)

    locked_by: str
    locked_at: str
    chunk_keys: list[str] = []
    processed_ids_count: int = 0


class FilterError(BaseModel):
    model_config = ConfigDict(frozen=True)

    error: str
    failed_at: str
    attempt: int = 1


class FilterCompletion(BaseModel):
    model_config = ConfigDict(frozen=True)

    completed_at: str
    output_key: str
    passed_count: int = 0
    rejected_count: int = 0


class StageAwareBatchManifest(BaseModel):
    model_config = ConfigDict(frozen=False)

    batch_id: str
    source: str
    original_key: str
    pipeline_stage: str
    parent_batch_id: str | None = None
    created_at: str

    filter_states: dict[str, FilterState] = {}
    filter_errors: dict[str, list[FilterError]] = {}
    completed_filters: dict[str, FilterCompletion] = {}

    def is_filter_complete(self, filter_name: str) -> bool:
        return filter_name in self.completed_filters

    def is_filter_locked(self, filter_name: str, lock_timeout_seconds: int = 600) -> bool:
        state = self.filter_states.get(filter_name)
        if state is None:
            return False
        from datetime import datetime, timezone
        try:
            locked_time = datetime.fromisoformat(state.locked_at.replace("Z", "+00:00"))
            age = (datetime.now(timezone.utc) - locked_time).total_seconds()
            return age < lock_timeout_seconds
        except Exception:
            return False

    def get_filter_completion(self, filter_name: str) -> FilterCompletion | None:
        return self.completed_filters.get(filter_name)
