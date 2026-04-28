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
from s3_data_tool.s3_utils import enumerate_parquet_paths_sync

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


def _prefix_contains_parquet(
    client,
    bucket: str,
    prefix: str,
) -> bool:
    """Return True if any key under *prefix* ends with ``/merged.parquet``."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/merged.parquet"):
                return True
    return False


def list_datasets_from_s3() -> list[str]:
    """List all datasets under the S3 prefix that have a merged.parquet."""
    client = get_s3_client()
    list_prefix = f"{S3_PREFIX.rstrip('/')}/"

    paginator = client.get_paginator("list_objects_v2")
    candidate_names: list[str] = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=list_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            prefix_val = cp.get("Prefix", "")
            name = prefix_val[len(list_prefix):].rstrip("/")
            if name and not name.startswith("annotations"):
                candidate_names.append(name)

    datasets: list[str] = []
    for name in candidate_names:
        if _prefix_contains_parquet(client, S3_BUCKET, f"{list_prefix}{name}/"):
            datasets.append(name)
    return sorted(datasets)


def list_annotators_from_s3(dataset_name: str) -> list[str]:
    """List all annotators for a dataset that have a merged.parquet."""
    client = get_s3_client()
    annotations_prefix = f"{S3_PREFIX}/{dataset_name}/annotations/"

    paginator = client.get_paginator("list_objects_v2")
    candidate_names: list[str] = []
    for page in paginator.paginate(
        Bucket=S3_BUCKET, Prefix=annotations_prefix, Delimiter="/"
    ):
        for cp in page.get("CommonPrefixes", []):
            prefix_val = cp.get("Prefix", "")
            name = prefix_val[len(annotations_prefix):].rstrip("/")
            if name:
                candidate_names.append(name)

    annotators: list[str] = []
    for name in candidate_names:
        if _prefix_contains_parquet(
            client, S3_BUCKET, f"{annotations_prefix}{name}/"
        ):
            annotators.append(name)
    return sorted(annotators)


def get_schema_with_types(
    dataset_name: str, annotators: list[str]
) -> list[SchemaColumn]:
    """Get combined schema from dataset and annotators."""
    columns: dict[str, str] = {}
    client = get_s3_client()

    base_paths = enumerate_parquet_paths_sync(
        client, S3_BUCKET, S3_PREFIX, dataset_name
    )
    if base_paths:
        base_paths_sql = "[" + ", ".join(f"'{p}'" for p in base_paths) + "]"
        query = (
            f"SELECT * FROM read_parquet({base_paths_sql}, union_by_name=true) LIMIT 1"
        )
        try:
            results = execute_query(query)
            if results:
                for col_name in results[0].keys():
                    if col_name and col_name not in columns:
                        columns[col_name] = "unknown"
        except Exception as e:
            logger.warning(f"Failed to get schema from base: {e}")

    for annotator in annotators:
        annot_paths = enumerate_parquet_paths_sync(
            client, S3_BUCKET, S3_PREFIX, dataset_name, annotator
        )
        if annot_paths:
            annot_paths_sql = "[" + ", ".join(f"'{p}'" for p in annot_paths) + "]"
            query = f"SELECT * FROM read_parquet({annot_paths_sql}, union_by_name=true) LIMIT 1"
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
    client = get_s3_client()
    annot_paths = enumerate_parquet_paths_sync(
        client, S3_BUCKET, S3_PREFIX, dataset_name, annotator
    )
    if not annot_paths:
        raise HTTPException(status_code=404, detail="Annotator not found")
    annot_paths_sql = "[" + ", ".join(f"'{p}'" for p in annot_paths) + "]"
    query = (
        f"SELECT * FROM read_parquet({annot_paths_sql}, union_by_name=true) LIMIT 100"
    )
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
    client = get_s3_client()

    base_parquet_paths = enumerate_parquet_paths_sync(
        client, S3_BUCKET, S3_PREFIX, dataset_name
    )
    annot_parquet_paths = {
        annotator: enumerate_parquet_paths_sync(
            client, S3_BUCKET, S3_PREFIX, dataset_name, annotator
        )
        for annotator in annotator_list
    }

    filter_spec = FilterSpec(filter_data)
    query = build_count_query(
        filter_spec,
        base_parquet_paths,
        annot_parquet_paths,
        annotator_list,
        annotator_cols,
    )

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

    client = get_s3_client()
    base_parquet_paths = enumerate_parquet_paths_sync(
        client, S3_BUCKET, S3_PREFIX, dataset_name
    )
    annot_parquet_paths = {
        annotator: enumerate_parquet_paths_sync(
            client, S3_BUCKET, S3_PREFIX, dataset_name, annotator
        )
        for annotator in annotator_list
    }

    query, selected_columns, selected_annotator_columns = build_query(
        column_list,
        annotator_list,
        filter_spec,
        base_parquet_paths,
        annot_parquet_paths,
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
