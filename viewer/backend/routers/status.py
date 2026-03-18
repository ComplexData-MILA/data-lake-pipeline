from __future__ import annotations

from fastapi import APIRouter, Depends, Query

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


@router.get("/status")
async def get_status(
    refresh: bool = Query(False),
    service: PipelineStatusService = Depends(get_status_service),
):
    status = await service.get_status(force_refresh=refresh)
    return status


@router.post("/cache/invalidate")
async def invalidate_cache(
    service: PipelineStatusService = Depends(get_status_service),
):
    service._cache.invalidate_all()
    return {"status": "ok"}
