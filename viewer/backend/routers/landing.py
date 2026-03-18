from __future__ import annotations

from fastapi import APIRouter, Depends, Query, HTTPException

from data_lake_pipeline.storage import StorageBackend
from data_lake_pipeline.state import BatchState
from viewer.backend.dependencies import get_storage, get_batch_state
from viewer.backend.services.pipeline_status import PipelineStatusService

router = APIRouter()


def get_status_service(
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
) -> PipelineStatusService:
    return PipelineStatusService(storage=storage, batch_state=batch_state)


@router.get("/landing/status")
async def get_landing_status(
    source: str | None = Query(None),
    refresh: bool = Query(False),
    service: PipelineStatusService = Depends(get_status_service),
):
    status = await service.get_landing_status(source=source, force_refresh=refresh)
    return status


@router.get("/landing/{source}/{filename}")
async def get_landing_file(
    source: str,
    filename: str,
    limit: int = Query(100, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    storage: StorageBackend = Depends(get_storage),
):
    key = f"01_landing/{source}/{filename}"
    
    if not storage.object_exists(key):
        raise HTTPException(status_code=404, detail="File not found")
    
    records = []
    total = 0
    
    for record in storage.stream_jsonl(key):
        total += 1
        if offset <= total - 1 and len(records) < limit:
            records.append(record)
    
    return {
        "records": records,
        "total": total,
        "limit": limit,
        "offset": offset,
    }
