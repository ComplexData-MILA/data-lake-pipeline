from __future__ import annotations

import re

import duckdb
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from data_lake_pipeline.storage import StorageBackend
from viewer.backend.dependencies import get_storage

router = APIRouter()

PROCESSED_PREFIX = "03_processed"

SELECT_ONLY_PATTERN = re.compile(r"^\s*SELECT\s", re.IGNORECASE)


def validate_select_only(sql: str) -> bool:
    return bool(SELECT_ONLY_PATTERN.match(sql))


class QueryRequest(BaseModel):
    sql: str
    limit: int = 1000


@router.get("/processed")
async def list_processed(
    storage: StorageBackend = Depends(get_storage),
):
    objects = storage.list_objects_with_metadata(PROCESSED_PREFIX, ".parquet")
    return [
        {
            "key": obj.key,
            "size_bytes": obj.size_bytes,
            "age_seconds": obj.age_seconds,
            "last_modified": obj.last_modified.isoformat(),
        }
        for obj in objects
    ]


@router.post("/query")
async def execute_query(
    request: QueryRequest,
    storage: StorageBackend = Depends(get_storage),
):
    if not validate_select_only(request.sql):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    
    glob_path = f"{storage.prefix}/{PROCESSED_PREFIX}/*.parquet" if storage.prefix else f"{PROCESSED_PREFIX}/*.parquet"
    full_sql = f"SELECT * FROM ({request.sql}) LIMIT {request.limit}"
    
    try:
        conn = duckdb.connect()
        result = conn.execute(f"SELECT * FROM read_parquet('{glob_path}')").fetchall()
        columns = [desc[0] for desc in conn.description]
        
        result = conn.execute(full_sql).fetchall()
        columns = [desc[0] for desc in conn.description]
        
        return {
            "columns": columns,
            "rows": result,
            "row_count": len(result),
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
