"""DuckDB query builder for the data viewer."""

import json
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_PREFIX"]
S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]


def _get_s3_settings() -> dict[str, str]:
    endpoint_host = S3_ENDPOINT_URL.removeprefix("https://").rstrip("/")
    endpoint_host = endpoint_host.removeprefix("http://").rstrip("/")
    use_ssl = "true" if S3_ENDPOINT_URL.startswith("https://") else "false"
    return {
        "endpoint": endpoint_host,
        "use_ssl": use_ssl,
    }


class FilterSpec:
    """Filter specification from frontend."""

    def __init__(self, data: dict[str, Any] | None = None):
        self.data = data or {}

    def get_base_filter(self) -> dict[str, Any] | None:
        return self.data.get("base")

    def get_annotator_filters(self) -> dict[str, dict[str, Any]]:
        return self.data.get("annotators", {})

    def compile(self, source: str, filter_spec: dict[str, Any] | None) -> str:
        """Compile filter spec to DuckDB WHERE clause."""
        if not filter_spec:
            return ""

        field = filter_spec.get("field")
        op = filter_spec.get("op")
        value = filter_spec.get("value")

        if not all([field, op]):
            return ""

        if op == "eq":
            if isinstance(value, bool):
                return f"{source}.{field} = {str(value).lower()}"
            elif isinstance(value, str):
                return f"{source}.{field} = '{value}'"
            else:
                return f"{source}.{field} = {value}"
        elif op == "neq":
            if isinstance(value, str):
                return f"{source}.{field} != '{value}'"
            else:
                return f"{source}.{field} != {value}"
        elif op == "gt":
            return f"{source}.{field} > {value}"
        elif op == "gte":
            return f"{source}.{field} >= {value}"
        elif op == "lt":
            return f"{source}.{field} < {value}"
        elif op == "lte":
            return f"{source}.{field} <= {value}"
        elif op == "contains":
            escaped = value.replace("'", "''")
            return f"{source}.{field} LIKE '%{escaped}%'"
        elif op == "startswith":
            escaped = value.replace("'", "''")
            return f"{source}.{field} LIKE '{escaped}%'"
        elif op == "endswith":
            escaped = value.replace("'", "''")
            return f"{source}.{field} LIKE '%{escaped}'"

        return ""


def build_query(
    dataset_name: str,
    columns: list[str],
    annotators: list[str],
    filters: FilterSpec,
    annotator_columns: dict[str, list[str]] = {},
    sort: str | None = None,
    sort_dir: str = "asc",
    offset: int = 0,
    limit: int = 50,
    row_id: str | None = None,
) -> tuple[str, list[str], dict[str, list[str]]]:
    """Build DuckDB query for paginated data with optional filters and joins."""
    base_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/*/merged.parquet"

    ctes = []

    base_filter = filters.get_base_filter()
    base_where = FilterSpec().compile("base", base_filter) if base_filter else ""
    base_where_clause = f" WHERE {base_where}" if base_where else ""

    base_cols = [c for c in columns if c != "id" and c != "_batch"]
    select_parts = ["id", "_batch"]

    if row_id:
        for col in base_cols:
            select_parts.append(col)
        ctes.append(
            f"base AS (SELECT DISTINCT ON (id) {', '.join(select_parts)} FROM read_parquet('{base_path}', union_by_name=true) AS base{base_where_clause} ORDER BY id, _batch)"
        )
    else:
        for col in base_cols:
            select_parts.append(f"ANY_VALUE({col}) AS {col}")
        ctes.append(
            f"base AS (SELECT {', '.join(select_parts)} FROM read_parquet('{base_path}', union_by_name=true) AS base{base_where_clause} GROUP BY id, _batch)"
        )

    joined_annotators: dict[str, bool] = {}
    selected_annotator_columns: dict[str, list[str]] = {}
    annotator_filters = filters.get_annotator_filters()

    for annotator in annotators:
        annot_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/annotations/{annotator}/*/merged.parquet"

        ann_filter = annotator_filters.get(annotator)
        ann_where = FilterSpec().compile(annotator, ann_filter) if ann_filter else ""
        ann_where_clause = f" WHERE {ann_where}" if ann_where else ""

        requested_cols = annotator_columns.get(annotator, [])

        try:
            cols_query = f"SELECT * FROM read_parquet('{annot_path}', union_by_name=true) LIMIT 1"
            col_results = execute_query(cols_query)
            available_cols = set(col_results[0].keys()) if col_results else set()
        except Exception:
            available_cols = set()

        if requested_cols:
            valid_cols = [c for c in requested_cols if c in available_cols] if available_cols else requested_cols
        else:
            valid_cols = [c for c in available_cols if c not in ["id", "_batch"]]

        if not valid_cols:
            continue

        ctes.append(
            f"{annotator} AS (SELECT * FROM read_parquet('{annot_path}', union_by_name=true) AS {annotator}{ann_where_clause})"
        )
        joined_annotators[annotator] = bool(ann_filter)
        selected_annotator_columns[annotator] = valid_cols

    select_cols = ["base.id", "base._batch"]
    for col in base_cols:
        select_cols.append(col)

    for ann in joined_annotators:
        for col in selected_annotator_columns[ann]:
            select_cols.append(f'{ann}.{col} AS "{ann}.{col}"')

    join_clause = ""
    if joined_annotators:
        join_parts = []
        for ann, has_filter in joined_annotators.items():
            join_type = "INNER JOIN" if has_filter else "LEFT JOIN"
            join_parts.append(f"{join_type} {ann} USING (id)")
        join_clause = " " + " ".join(join_parts)

    order_clause = " ORDER BY base.id ASC"
    if sort:
        available_base_cols = set(base_cols)
        if sort == "_batch" or sort in available_base_cols:
            direction = "DESC" if sort_dir == "desc" else "ASC"
            # When sorting by another column, add id as secondary sort for determinism
            order_clause = (
                f" ORDER BY base.{sort} {direction}, base.id ASC"
                if sort != "_batch"
                else f" ORDER BY base._batch {direction}, base.id ASC"
            )

    query = f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} FROM base{join_clause}{order_clause} LIMIT {limit} OFFSET {offset}"

    result_columns = ["id", "_batch"] + base_cols
    for ann in joined_annotators:
        for col in selected_annotator_columns[ann]:
            result_columns.append(f"{ann}.{col}")

    return query, result_columns, selected_annotator_columns


def build_count_query(
    dataset_name: str,
    annotators: list[str],
    filters: FilterSpec,
    annotator_columns: dict[str, list[str]] = {},
) -> str:
    """Build query to get approximate row count."""
    base_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/*/merged.parquet"

    base_filter = filters.get_base_filter()
    base_where = FilterSpec().compile("base", base_filter) if base_filter else ""
    base_where_clause = f" WHERE {base_where}" if base_where else ""

    cte = f"base AS (SELECT id FROM read_parquet('{base_path}', union_by_name=true) AS base{base_where_clause} GROUP BY id)"

    join_parts = []
    annotator_filters = filters.get_annotator_filters()
    active_annotators: dict[str, bool] = {}
    for annotator in annotators:
        if annotator not in annotator_columns or not annotator_columns[annotator]:
            continue
        annot_path = f"s3://{S3_BUCKET}/{S3_PREFIX}/{dataset_name}/annotations/{annotator}/*/merged.parquet"
        ann_filter = annotator_filters.get(annotator)
        ann_where = FilterSpec().compile(annotator, ann_filter) if ann_filter else ""
        ann_where_clause = f" WHERE {ann_where}" if ann_where else ""
        join_parts.append(
            f"{annotator} AS (SELECT id FROM read_parquet('{annot_path}', union_by_name=true) AS {annotator}{ann_where_clause})"
        )
        active_annotators[annotator] = bool(ann_filter)

    ctes = [cte] + join_parts

    if not active_annotators:
        return f"WITH {cte} SELECT COUNT(*) as cnt FROM base"

    join_clause_parts = []
    for ann, has_filter in active_annotators.items():
        join_type = "INNER JOIN" if has_filter else "LEFT JOIN"
        join_clause_parts.append(f"{join_type} {ann} USING (id)")
    join_clause = " ".join(join_clause_parts)
    return f"WITH {', '.join(ctes)} SELECT COUNT(DISTINCT base.id) as cnt FROM base {join_clause}"


def execute_query(query: str) -> list[dict[str, Any]]:
    """Execute DuckDB query and return results as list of dicts."""
    temp_dir = tempfile.mkdtemp()
    db_path = os.path.join(temp_dir, "query.duckdb")
    conn = duckdb.connect(db_path, read_only=False)

    s3_settings = _get_s3_settings()
    conn.execute(
        f"""
        SET s3_access_key_id='{S3_ACCESS_KEY}';
        SET s3_secret_access_key='{S3_SECRET_KEY}';
        SET s3_endpoint='{s3_settings['endpoint']}';
        SET s3_use_ssl={s3_settings['use_ssl']};
        SET s3_url_style='path';
    """
    )

    try:
        result = conn.execute(query)
        description = result.description
        rows = result.fetchall()
        return [dict(zip([d[0] for d in description], row)) for row in rows]
    finally:
        conn.close()
        shutil.rmtree(temp_dir, ignore_errors=True)
