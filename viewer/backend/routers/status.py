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


@router.get("/sources")
async def get_sources(
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
):
    sources = set()

    for key in storage.list_objects("01_landing"):
        parts = key.split("/")
        if len(parts) > 1:
            sources.add(parts[1])

    for manifest in batch_state.list_all():
        if manifest.source:
            sources.add(manifest.source)

    return sorted(list(sources))


@router.get("/schema/{stage}")
async def get_stage_schema(stage: str):
    schemas = {
        "landing": {
            "columns": [
                {"name": "source", "type": "string", "filterable": True},
                {"name": "external_id", "type": "string", "filterable": False},
                {"name": "text", "type": "string", "filterable": True},
                {"name": "created_at", "type": "datetime", "filterable": True},
                {"name": "url", "type": "string", "filterable": False},
                {"name": "author", "type": "string", "filterable": True},
                {"name": "score", "type": "number", "filterable": True},
                {"name": "metadata", "type": "json", "filterable": False},
                {"name": "ingested_at", "type": "datetime", "filterable": False},
            ]
        },
        "queue": {
            "columns": [
                {"name": "batch_id", "type": "string", "filterable": False},
                {"name": "source", "type": "string", "filterable": True},
                {"name": "state", "type": "string", "filterable": True},
                {"name": "created_at", "type": "datetime", "filterable": True},
                {"name": "locked_by", "type": "string", "filterable": False},
                {"name": "locked_at", "type": "datetime", "filterable": False},
                {"name": "row_count", "type": "number", "filterable": True},
                {"name": "error", "type": "string", "filterable": True},
            ]
        },
        "processed": {
            "columns": [
                {"name": "source", "type": "string", "filterable": True},
                {"name": "external_id", "type": "string", "filterable": False},
                {"name": "annotation", "type": "string", "filterable": True},
                {"name": "model_name", "type": "string", "filterable": False},
                {"name": "processor_backend", "type": "string", "filterable": False},
                {"name": "source_file", "type": "string", "filterable": False},
                {"name": "processed_at", "type": "datetime", "filterable": True},
                {"name": "raw_text", "type": "string", "filterable": True},
            ]
        },
    }

    if stage not in schemas:
        return {"columns": []}

    return schemas[stage]
