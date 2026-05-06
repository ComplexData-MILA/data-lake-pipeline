from datetime import datetime
from typing import Any

from pydantic import BaseModel


class StreamingConfigs(BaseModel):
    chunk_size: int = 100


class RunManifest(BaseModel):
    run_id: str
    deduplicate_on: list[str] | None = None
    streaming_configs: StreamingConfigs
    completed: bool = False
    created_at: datetime | None = None
    completed_at: datetime | None = None


class BatchManifest(BaseModel):
    name: str
    batch: str
    created_at: datetime
    updated_at: datetime
    runs: list[RunManifest]


class Annotation(BaseModel):
    """Annotation result from annotator function."""
    data: dict[str, Any]
    metadata: dict[str, Any] | None = None


class AnnotationManifest(BaseModel):
    """Marks that an annotator has finished annotating a batch.

    Written after a successful annotation run and persisted across
    clean-up merges. Only deleted when new base data arrives
    (merge_dataset_batch), which invalidates prior annotation results.
    """

    annotator_name: str
    dataset_name: str
    batch_name: str
    num_annotated: int
    completed_at: datetime


class DataItem(BaseModel):
    """Data item passed to annotation function."""
    data: dict[str, Any]
    id: str
    batch: str
