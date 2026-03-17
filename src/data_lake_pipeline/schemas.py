from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Literal


@dataclass
class SourcePost:
    """
    Output shape expected from each source adapter.
    """

    source: str
    external_id: str
    text: str
    created_at: str
    url: str | None = None
    author: str | None = None
    score: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_landed_record(self) -> "LandedRecord":
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


@dataclass
class LandedRecord:
    """
    Raw JSONL line shape written into 01_landing/.
    """

    source: str
    external_id: str
    text: str
    created_at: str
    url: str | None
    author: str | None
    score: float | None
    metadata: dict[str, Any]
    ingested_at: str

    def to_jsonl_line(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class AnnotationResult:
    """
    Per-post annotation output emitted by the batch processor.
    """

    source: str
    external_id: str
    annotation: str
    model_name: str
    processor_backend: str
    source_file: str
    processed_at: str
    raw_text: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ProcessingSummary:
    claimed_files: list[str]
    archived_files: list[str]
    failed_files: list[str]
    output_file: str | None
    processed_records: int

    def to_json(self) -> str:
        return json.dumps(
            {
                "claimed_files": self.claimed_files,
                "archived_files": self.archived_files,
                "failed_files": self.failed_files,
                "output_file": self.output_file,
                "processed_records": self.processed_records,
            },
            indent=2,
        )


@dataclass
class BatchManifest:
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

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "BatchManifest":
        return cls(
            batch_id=data["batch_id"],
            source=data["source"],
            original_key=data["original_key"],
            state=data["state"],
            created_at=data["created_at"],
            locked_by=data.get("locked_by"),
            locked_at=data.get("locked_at"),
            row_count=data.get("row_count"),
            output_key=data.get("output_key"),
            error=data.get("error"),
        )
