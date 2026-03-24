from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, Depends
from pydantic import BaseModel

from data_lake_pipeline.storage import StorageBackend
from data_lake_pipeline.state import BatchState
from viewer.backend.dependencies import get_storage, get_batch_state

router = APIRouter()

PROCESSING_STAGE = "processed"
LANDING_PREFIX = "landing"
MANIFESTS_PREFIX = "manifests"


class RecordFilter(BaseModel):
    field: str
    operator: Literal["eq", "contains", "gt", "lt", "between"]
    value: str | list[str]


class RecordQuery(BaseModel):
    stage: Literal["landing", "queue", "processed"]
    page: int = 1
    page_size: int = 50
    filters: list[RecordFilter] = []
    sort_by: str | None = None
    sort_desc: bool = False


class RecordResponse(BaseModel):
    records: list[dict[str, Any]]
    total_count: int
    page: int
    page_size: int
    total_pages: int
    columns: list[str]


def matches_filter(record: dict[str, Any], f: RecordFilter) -> bool:
    value = record.get(f.field)
    if value is None:
        return False

    str_value = str(value).lower() if not isinstance(value, str) else value.lower()

    if f.operator == "eq":
        return str_value == str(f.value).lower()
    elif f.operator == "contains":
        return str(f.value).lower() in str_value
    elif f.operator == "gt":
        try:
            return float(str_value) > float(f.value)
        except (ValueError, TypeError):
            return str_value > str(f.value).lower()
    elif f.operator == "lt":
        try:
            return float(str_value) < float(f.value)
        except (ValueError, TypeError):
            return str_value < str(f.value).lower()
    elif f.operator == "between" and isinstance(f.value, list) and len(f.value) == 2:
        try:
            return float(f.value[0]) <= float(str_value) <= float(f.value[1])
        except (ValueError, TypeError):
            return str(f.value[0]).lower() <= str_value <= str(f.value[1]).lower()

    return True


def apply_filters(records: list[dict], filters: list[RecordFilter]) -> list[dict]:
    if not filters:
        return records
    return [r for r in records if all(matches_filter(r, f) for f in filters)]


def apply_sort(records: list[dict], sort_by: str | None, sort_desc: bool) -> list[dict]:
    if not sort_by or not records:
        return records
    return sorted(records, key=lambda r: str(r.get(sort_by, "")), reverse=sort_desc)


def paginate(records: list[dict], page: int, page_size: int) -> list[dict]:
    start = (page - 1) * page_size
    end = start + page_size
    return records[start:end]


def get_columns(records: list[dict]) -> list[str]:
    if not records:
        return []
    return list(records[0].keys())


async def query_landing_records(
    query: RecordQuery, storage: StorageBackend
) -> RecordResponse:
    all_records = []

    for obj_key in storage.list_objects(LANDING_PREFIX, ".jsonl"):
        for record in storage.stream_jsonl(obj_key):
            all_records.append(record)

    filtered = apply_filters(all_records, query.filters)
    sorted_records = apply_sort(filtered, query.sort_by, query.sort_desc)
    page_records = paginate(sorted_records, query.page, query.page_size)

    total_count = len(sorted_records)
    total_pages = max(1, (total_count + query.page_size - 1) // query.page_size)

    return RecordResponse(
        records=page_records,
        total_count=total_count,
        page=query.page,
        page_size=query.page_size,
        total_pages=total_pages,
        columns=get_columns(page_records) if page_records else get_columns(all_records),
    )


async def query_queue_records(
    query: RecordQuery, batch_state: BatchState
) -> RecordResponse:
    all_records = []

    for manifest in batch_state.list_all():
        record = manifest.model_dump(mode="json")
        all_records.append(record)

    filtered = apply_filters(all_records, query.filters)
    sorted_records = apply_sort(filtered, query.sort_by, query.sort_desc)
    page_records = paginate(sorted_records, query.page, query.page_size)

    total_count = len(sorted_records)
    total_pages = max(1, (total_count + query.page_size - 1) // query.page_size)

    return RecordResponse(
        records=page_records,
        total_count=total_count,
        page=query.page,
        page_size=query.page_size,
        total_pages=total_pages,
        columns=get_columns(page_records) if page_records else get_columns(all_records),
    )


async def query_processed_records(
    query: RecordQuery, storage: StorageBackend
) -> RecordResponse:
    import duckdb

    processed_prefix = (
        f"{storage.prefix}/{PROCESSING_STAGE}" if storage.prefix else PROCESSING_STAGE
    )
    glob_path = f"s3://{storage.bucket}/{processed_prefix}/*.parquet"

    conn = duckdb.connect()

    base_query = f"SELECT * FROM read_parquet('{glob_path}')"

    where_clauses = []
    for f in query.filters:
        if f.operator == "eq":
            where_clauses.append(f"{f.field} = '{f.value}'")
        elif f.operator == "contains":
            where_clauses.append(f"{f.field} ILIKE '%{f.value}%'")
        elif f.operator == "gt":
            where_clauses.append(f"{f.field} > '{f.value}'")
        elif f.operator == "lt":
            where_clauses.append(f"{f.field} < '{f.value}'")
        elif (
            f.operator == "between" and isinstance(f.value, list) and len(f.value) == 2
        ):
            where_clauses.append(f"{f.field} BETWEEN '{f.value[0]}' AND '{f.value[1]}'")

    if where_clauses:
        base_query += " WHERE " + " AND ".join(where_clauses)

    count_query = f"SELECT COUNT(*) FROM ({base_query})"
    total_count = conn.execute(count_query).fetchone()[0]

    order_clause = ""
    if query.sort_by:
        order_clause = (
            f" ORDER BY {query.sort_by} {'DESC' if query.sort_desc else 'ASC'}"
        )

    offset = (query.page - 1) * query.page_size
    paginated_query = f"SELECT * FROM ({base_query}){order_clause} LIMIT {query.page_size} OFFSET {offset}"

    result = conn.execute(paginated_query).fetchall()
    columns = [desc[0] for desc in conn.description] if conn.description else []

    records = [dict(zip(columns, row)) for row in result]

    total_pages = max(1, (total_count + query.page_size - 1) // query.page_size)

    return RecordResponse(
        records=records,
        total_count=total_count,
        page=query.page,
        page_size=query.page_size,
        total_pages=total_pages,
        columns=columns,
    )


@router.post("/records")
async def query_records(
    query: RecordQuery,
    storage: StorageBackend = Depends(get_storage),
    batch_state: BatchState = Depends(get_batch_state),
) -> RecordResponse:
    if query.stage == "landing":
        return await query_landing_records(query, storage)
    elif query.stage == "queue":
        return await query_queue_records(query, batch_state)
    else:
        return await query_processed_records(query, storage)
