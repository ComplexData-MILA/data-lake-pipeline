"""FastAPI backend for data viewer."""

import json
import logging
from typing import Any

import boto3
from botocore.config import Config as BotoConfig
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from .duckdb_query import (
    S3_ACCESS_KEY,
    S3_BUCKET,
    S3_ENDPOINT_URL,
    S3_PREFIX,
    S3_SECRET_KEY,
    FilterSpec,
    build_count_query,
    build_query,
    execute_query,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

s3_client = None


def get_s3_client():
    """Get or create the S3 client."""
    global s3_client
    if s3_client is None:
        use_ssl = S3_ENDPOINT_URL.startswith("https://")
        config = BotoConfig(
            s3={"addressing_style": "path"},
            signature_version="s3v4",
        )
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY,
            use_ssl=use_ssl,
            config=config,
        )
    return s3_client


app = FastAPI(title="Data Viewer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


class DatasetListResponse(BaseModel):
    datasets: list[str]


class AnnotationListResponse(BaseModel):
    annotators: list[str]


class SchemaColumn(BaseModel):
    name: str
    type: str


class SchemaResponse(BaseModel):
    columns: list[SchemaColumn]


class DataResponse(BaseModel):
    rows: list[dict[str, Any]]
    columns: list[str]
    annotator_columns: dict[str, list[str]] = {}


class CountResponse(BaseModel):
    count: int


def list_datasets_from_s3() -> list[str]:
    """List all datasets under the S3 prefix."""
    client = get_s3_client()
    datasets: set[str] = set()
    list_prefix = f"{S3_PREFIX}/"

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=list_prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            if (p := cp.get("Prefix")) is not None:
                dataset_name = p.rstrip("/").split("/")[-1]
                if not dataset_name.startswith("annotations"):
                    datasets.add(dataset_name)
    return sorted(datasets)


def list_annotators_from_s3(dataset_name: str) -> list[str]:
    """List all annotators for a dataset."""
    client = get_s3_client()
    annotators: set[str] = set()
    annotations_prefix = f"{S3_PREFIX}/{dataset_name}/annotations/"

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=annotations_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            if (p := cp.get("Prefix")) is not None:
                annotator_name = p.rstrip("/").split("/")[-1]
                annotators.add(annotator_name)
    return sorted(annotators)


def get_schema_with_types(
    dataset_name: str, annotators: list[str]
) -> list[SchemaColumn]:
    """Get combined schema from dataset and annotators."""
    columns: dict[str, str] = {}

    base_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/*/merged.parquet"
    query = f"SELECT * FROM read_parquet('{base_path}', union_by_name=true) LIMIT 1"

    try:
        results = execute_query(query)
        if results:
            for col_name in results[0].keys():
                if col_name and col_name not in columns:
                    columns[col_name] = "unknown"
    except Exception as e:
        logger.warning(f"Failed to get schema from base: {e}")

    for annotator in annotators:
        annot_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/annotations/{annotator}/*/merged.parquet"
        query = (
            f"SELECT * FROM read_parquet('{annot_path}', union_by_name=true) LIMIT 1"
        )
        try:
            results = execute_query(query)
            if results:
                for col_name in results[0].keys():
                    if col_name and col_name not in ["id", "_batch"]:
                        columns[f"{annotator}.{col_name}"] = "unknown"
        except Exception as e:
            logger.warning(f"Failed to get schema from annotator {annotator}: {e}")

    return [SchemaColumn(name=name, type=typ) for name, typ in sorted(columns.items())]


@app.get("/datasets", response_model=DatasetListResponse)
async def get_datasets():
    """List all available datasets."""
    datasets = list_datasets_from_s3()
    return DatasetListResponse(datasets=datasets)


@app.get("/datasets/{dataset_name}/annotations", response_model=AnnotationListResponse)
async def get_annotations(dataset_name: str):
    """List all available annotators for a dataset."""
    annotators = list_annotators_from_s3(dataset_name)
    return AnnotationListResponse(annotators=annotators)


@app.get("/datasets/{dataset_name}/annotations/{annotator}/columns")
async def get_annotator_columns(dataset_name: str, annotator: str):
    """Get available column names for a specific annotator (union across all batches)."""
    annot_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/annotations/{annotator}/*/merged.parquet"
    query = f"SELECT * FROM read_parquet('{annot_path}', union_by_name=true) LIMIT 100"
    try:
        results = execute_query(query)
        if results:
            columns = set()
            for row in results:
                for k in row.keys():
                    if k not in ["id", "_batch"]:
                        columns.add(k)
            return {"columns": sorted(columns)}
    except Exception as e:
        logger.error(f"Failed to get annotator columns: {e}")
    raise HTTPException(status_code=404, detail="Annotator not found")


@app.get("/datasets/{dataset_name}/schema", response_model=SchemaResponse)
async def get_schema(
    dataset_name: str,
    annotators: str = Query(default="", description="Comma-separated annotator names"),
):
    """Get schema (columns and types) for a dataset."""
    annotator_list = [a.strip() for a in annotators.split(",") if a.strip()]
    columns = get_schema_with_types(dataset_name, annotator_list)
    return SchemaResponse(columns=columns)


@app.get("/datasets/{dataset_name}/count")
async def get_count(
    dataset_name: str,
    annotator_columns: str = Query(
        default="{}", description="JSON dict mapping annotator to column names"
    ),
    filters: str = Query(default="{}", description="JSON-encoded filter spec"),
):
    """Get approximate row count."""
    try:
        annotator_cols = json.loads(annotator_columns) if annotator_columns else {}
    except json.JSONDecodeError:
        annotator_cols = {}
    try:
        filter_data = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        filter_data = {}

    annotator_list = list(annotator_cols.keys()) if annotator_cols else []

    filter_spec = FilterSpec(filter_data)
    query = build_count_query(dataset_name, annotator_list, filter_spec, annotator_cols)

    try:
        results = execute_query(query)
        count = results[0].get("cnt", 0) if results else 0
        return {"count": count}
    except Exception as e:
        logger.error(f"Count query failed: {e}")
        return {"count": 0}


@app.get("/datasets/{dataset_name}/data", response_model=DataResponse)
async def get_data(
    dataset_name: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=50, ge=1, le=1000),
    columns: str = Query(default="", description="Comma-separated column names"),
    annotator_columns: str = Query(
        default="{}", description="JSON dict mapping annotator to column names"
    ),
    filters: str = Query(default="{}", description="JSON-encoded filter spec"),
    sort: str | None = Query(default=None),
    sort_dir: str = Query(default="asc", pattern="^(asc|desc)$"),
    row_id: str | None = Query(default=None, description="Fetch single row by ID"),
):
    """Get paginated data rows."""
    column_list = [c.strip() for c in columns.split(",") if c.strip()]
    try:
        annotator_cols = json.loads(annotator_columns) if annotator_columns else {}
    except json.JSONDecodeError:
        annotator_cols = {}

    annotator_list = list(annotator_cols.keys()) if annotator_cols else []

    if not column_list:
        column_list = ["id", "_batch"]

    try:
        filter_data = json.loads(filters) if filters else {}
    except json.JSONDecodeError:
        filter_data = {}

    if row_id:
        filter_data["base"] = {"field": "id", "op": "eq", "value": row_id}
        page_size = 1
        page = 1

    filter_spec = FilterSpec(filter_data)
    offset = (page - 1) * page_size

    query, selected_columns, selected_annotator_columns = build_query(
        dataset_name,
        column_list,
        annotator_list,
        filter_spec,
        annotator_cols,
        sort,
        sort_dir,
        offset,
        page_size,
        row_id,
    )

    try:
        rows = execute_query(query)
        if row_id and not rows:
            raise HTTPException(status_code=404, detail=f"Row {row_id} not found")
        return DataResponse(
            rows=rows,
            columns=selected_columns,
            annotator_columns=selected_annotator_columns,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Data query failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


#
