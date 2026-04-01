from datetime import datetime

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
