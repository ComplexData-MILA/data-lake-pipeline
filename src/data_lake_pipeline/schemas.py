from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class SourcePost(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str
    external_id: str
    text: str
    created_at: str
    url: str | None = None
    author: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    def to_landed_record(self) -> LandedRecord:
        return LandedRecord(
            source=self.source,
            external_id=self.external_id,
            text=self.text,
            created_at=self.created_at,
            url=self.url,
            author=self.author,
            score=self.score,
            metadata=self.metadata,
            ingested_at=datetime.now(timezone.utc).isoformat(),
        )


class LandedRecord(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str
    external_id: str
    text: str
    created_at: str
    url: str | None = None
    author: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    ingested_at: str


class AnnotationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: str
    external_id: str
    annotation: str
    model_name: str
    processor_backend: str
    source_file: str
    processed_at: str
    raw_text: str | None = None


class ProcessingSummary(BaseModel):
    model_config = ConfigDict(frozen=True)

    claimed_files: list[str] = Field(default_factory=list)
    archived_files: list[str] = Field(default_factory=list)
    failed_files: list[str] = Field(default_factory=list)
    output_file: str | None = None
    processed_records: int = 0


class BatchManifest(BaseModel):
    model_config = ConfigDict(frozen=False)

    batch_id: str
    source: str
    original_key: str
    state: Literal["pending", "inflight", "completed", "failed", "archived"]
    created_at: str
    locked_by: str | None = None
    locked_at: str | None = None
    row_count: int | None = None
    output_key: str | None = None
    error: str | None = None
