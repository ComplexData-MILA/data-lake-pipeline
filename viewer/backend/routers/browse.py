from __future__ import annotations

from fastapi import APIRouter, Depends, Query

from data_lake_pipeline.storage import StorageBackend
from viewer.backend.dependencies import get_storage

router = APIRouter()


@router.get("/browse")
async def browse_objects(
    prefix: str = Query(""),
    storage: StorageBackend = Depends(get_storage),
):
    objects = storage.list_objects_with_metadata(prefix)
    return {
        "objects": [
            {
                "key": obj.key,
                "size_bytes": obj.size_bytes,
                "age_seconds": obj.age_seconds,
            }
            for obj in objects
        ]
    }
