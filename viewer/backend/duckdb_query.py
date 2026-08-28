"""DuckDB query builder for the data viewer.

All client-supplied values are bound as query parameters and all client-
supplied identifiers (column names, annotator names) are validated and
double-quoted, so client input is never interpolated into SQL.
"""

import json
import logging
import os
import re
from pathlib import Path
from typing import Any

import duckdb
from dotenv import load_dotenv

from . import db

logger = logging.getLogger(__name__)

# Load environment variables from .env file
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

S3_BUCKET = os.environ["S3_BUCKET"]
S3_PREFIX = os.environ["S3_PREFIX"]
S3_ENDPOINT_URL = os.environ["S3_ENDPOINT_URL"]
S3_ACCESS_KEY = os.environ["S3_ACCESS_KEY"]
S3_SECRET_KEY = os.environ["S3_SECRET_KEY"]


_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_COMPARISON_OPS = {"eq": "=", "neq": "!=", "gt": ">", "gte": ">=", "lt": "<", "lte": "<="}
_LIKE_OPS = {"contains", "startswith", "endswith"}


def _quote_identifier(source: str, name: str) -> str:
    """Validate *name* as an identifier and return it double-quoted with *source*."""
    if not _IDENTIFIER_RE.match(name):
        raise ValueError(f"Invalid identifier: {name!r}")
    return f'"{source}"."{name}"'


class FilterSpec:
    """Filter specification from frontend."""

    def __init__(self, data: dict[str, Any] | None = None):
        self.data = data or {}

    def get_base_filter(self) -> dict[str, Any] | None:
        return self.data.get("base")

    def get_annotator_filters(self) -> dict[str, dict[str, Any]]:
        return self.data.get("annotators", {})

    def compile(
        self, source: str, filter_spec: dict[str, Any] | None
    ) -> tuple[str, list[Any]]:
        """Compile a filter spec to a (WHERE clause, bound params) pair."""
        if not filter_spec:
            return "", []

        field = filter_spec.get("field")
        op = filter_spec.get("op")
        value = filter_spec.get("value")

        if not field or not op:
            return "", []

        col = _quote_identifier(source, field)

        if op in _COMPARISON_OPS:
            return f"{col} {_COMPARISON_OPS[op]} ?", [value]
        if op in _LIKE_OPS:
            escaped = (
                str(value).replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            pattern = {
                "contains": f"%{escaped}%",
                "startswith": f"{escaped}%",
                "endswith": f"%{escaped}",
            }[op]
            return f"{col} LIKE ? ESCAPE '\\'", [pattern]

        raise ValueError(f"Unsupported filter op: {op!r}")


def _format_parquet_paths(paths: list[str]) -> str:
    assert paths, "paths must not be empty"
    path_list = ", ".join(f"'{p}'" for p in paths)
    return f"[{path_list}]"


def _union_source(parquet_paths: list[str], jsonl_paths: list[str]) -> str:
    """Build a FROM source: merged parquet UNION ALL BY NAME live JSONL chunks.

    Returns an empty string when neither side has files. ``UNION ALL BY NAME``
    NULL-fills columns missing on one side and unifies types to a common type,
    and the downstream ``GROUP BY id`` / ``ANY_VALUE`` dedup absorbs a row that
    appears in both a chunk and its just-merged parquet.
    """
    # Both sides are parenthesized SELECTs so the set operation is legal and the
    # result is a valid standalone FROM source (e.g. ``FROM (...) AS base``).
    parts: list[str] = []
    if parquet_paths:
        parts.append(
            f"(SELECT * FROM read_parquet({_format_parquet_paths(parquet_paths)}, "
            "union_by_name=true))"
        )
    if jsonl_paths:
        parts.append(
            f"(SELECT * FROM read_json_auto({_format_parquet_paths(jsonl_paths)}, "
            "union_by_name=true, format='newline_delimited', ignore_errors=true, "
            "maximum_sample_files=-1))"
        )
    if not parts:
        return ""
    # Outer parens so a following alias (``AS base``) binds to the whole
    # set operation, not just its last operand.
    return f"({' UNION ALL BY NAME '.join(parts)})"


def build_query(
    columns: list[str],
    annotators: list[str],
    filters: FilterSpec,
    base_parquet_paths: list[str],
    annot_parquet_paths: dict[str, list[str]],
    annotator_columns: dict[str, list[str]] = {},
    sort: str | None = None,
    sort_dir: str = "asc",
    offset: int = 0,
    limit: int = 50,
    row_id: str | None = None,
    base_live_jsonl_paths: list[str] | None = None,
    annot_live_jsonl_paths: dict[str, list[str]] | None = None,
) -> tuple[str, list[Any], list[str], dict[str, list[str]]]:
    """Build a parameterized DuckDB query for paginated data with joins.

    ``base_live_jsonl_paths`` / ``annot_live_jsonl_paths`` add unmerged JSONL
    chunks to the read (live rows appear before clean-up merges them).

    Returns ``(query, params, result_columns, selected_annotator_columns)``.
    """
    params: list[Any] = []

    base_live_jsonl_paths = base_live_jsonl_paths or []
    annot_live_jsonl_paths = annot_live_jsonl_paths or {}

    base_src = _union_source(base_parquet_paths, base_live_jsonl_paths)
    if not base_src:
        return "", params, ["id", "_batch"], {}

    ctes = []

    base_filter = filters.get_base_filter()
    base_where, base_params = filters.compile("base", base_filter)
    params.extend(base_params)
    # NULL-id rows are corrupted JSONL lines that DuckDB's ignore_errors emits
    # as NULL-padded rows — drop them (all real rows carry an id).
    where_parts = [p for p in [base_where, '"base"."id" IS NOT NULL'] if p]
    base_where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    base_cols = [c for c in columns if c != "id" and c != "_batch"]
    select_parts = ['"id"', '"_batch"']

    if row_id:
        for col in base_cols:
            _quote_identifier("base", col)
            select_parts.append(f'"{col}"')
        ctes.append(
            f'base AS (SELECT DISTINCT ON ("id") {", ".join(select_parts)} '
            f"FROM {base_src} AS base"
            f'{base_where_clause} ORDER BY "id", "_batch")'
        )
    else:
        for col in base_cols:
            _quote_identifier("base", col)
            select_parts.append(f'ANY_VALUE("{col}") AS "{col}"')
        ctes.append(
            f"base AS (SELECT {', '.join(select_parts)} "
            f"FROM {base_src} AS base"
            f'{base_where_clause} GROUP BY "id", "_batch")'
        )

    joined_annotators: dict[str, bool] = {}
    selected_annotator_columns: dict[str, list[str]] = {}
    annotator_filters = filters.get_annotator_filters()

    for annotator in annotators:
        _quote_identifier("", annotator)  # validate
        annot_src = _union_source(
            annot_parquet_paths.get(annotator, []),
            annot_live_jsonl_paths.get(annotator, []),
        )
        if not annot_src:
            continue

        ann_filter = annotator_filters.get(annotator)
        ann_where, ann_params = filters.compile(annotator, ann_filter)
        params.extend(ann_params)
        ann_where_parts = [
            p for p in [ann_where, f'"{annotator}"."id" IS NOT NULL'] if p
        ]
        ann_where_clause = (
            f" WHERE {' AND '.join(ann_where_parts)}" if ann_where_parts else ""
        )

        requested_cols = annotator_columns.get(annotator, [])

        try:
            cols_query = f"SELECT * FROM {annot_src} LIMIT 1"
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

        for col in valid_cols:
            _quote_identifier(annotator, col)

        # GROUP BY id with ANY_VALUE dedups rows repeated across annotation
        # batches and chunk/parquet overlap, so joins never multiply rows.
        ann_select = ['"id"'] + [f'ANY_VALUE("{c}") AS "{c}"' for c in valid_cols]
        ctes.append(
            f'"{annotator}" AS (SELECT {", ".join(ann_select)} '
            f'FROM {annot_src} AS "{annotator}"{ann_where_clause} GROUP BY "id")'
        )
        joined_annotators[annotator] = bool(ann_filter)
        selected_annotator_columns[annotator] = valid_cols

    select_cols = ['"base"."id"', '"base"."_batch"']
    for col in base_cols:
        select_cols.append(f'"{col}"')

    for ann in joined_annotators:
        for col in selected_annotator_columns[ann]:
            select_cols.append(f'"{ann}"."{col}" AS "{ann}.{col}"')

    join_clause = ""
    if joined_annotators:
        join_parts = []
        for ann, has_filter in joined_annotators.items():
            join_type = "INNER JOIN" if has_filter else "LEFT JOIN"
            join_parts.append(f'{join_type} "{ann}" USING (id)')
        join_clause = " " + " ".join(join_parts)

    order_clause = ' ORDER BY "base"."id" ASC'
    if sort:
        _quote_identifier("base", sort)
        if sort == "_batch" or sort in base_cols:
            direction = "DESC" if sort_dir == "desc" else "ASC"
            order_clause = (
                f' ORDER BY "base"."{sort}" {direction}, "base"."id" ASC'
                if sort != "_batch"
                else f' ORDER BY "base"."_batch" {direction}, "base"."id" ASC'
            )

    query = (
        f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} "
        f"FROM base{join_clause}{order_clause} LIMIT {limit} OFFSET {offset}"
    )

    result_columns = ["id", "_batch"] + base_cols
    for ann in joined_annotators:
        for col in selected_annotator_columns[ann]:
            result_columns.append(f"{ann}.{col}")

    return query, params, result_columns, selected_annotator_columns


def build_count_query(
    filters: FilterSpec,
    base_parquet_paths: list[str],
    annot_parquet_paths: dict[str, list[str]],
    annotators: list[str],
    annotator_columns: dict[str, list[str]] = {},
    base_live_jsonl_paths: list[str] | None = None,
    annot_live_jsonl_paths: dict[str, list[str]] | None = None,
) -> tuple[str, list[Any]]:
    """Build a parameterized query for the row count. Returns (query, params)."""
    params: list[Any] = []

    base_live_jsonl_paths = base_live_jsonl_paths or []
    annot_live_jsonl_paths = annot_live_jsonl_paths or {}

    base_src = _union_source(base_parquet_paths, base_live_jsonl_paths)
    if not base_src:
        return "SELECT 0 AS cnt", params

    base_filter = filters.get_base_filter()
    base_where, base_params = filters.compile("base", base_filter)
    params.extend(base_params)
    where_parts = [p for p in [base_where, '"base"."id" IS NOT NULL'] if p]
    base_where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    cte = (
        f'base AS (SELECT "id" FROM {base_src} AS base'
        f'{base_where_clause} GROUP BY "id")'
    )

    join_parts = []
    annotator_filters = filters.get_annotator_filters()
    active_annotators: dict[str, bool] = {}
    for annotator in annotators:
        _quote_identifier("", annotator)  # validate
        if annotator not in annotator_columns or not annotator_columns[annotator]:
            continue
        annot_src = _union_source(
            annot_parquet_paths.get(annotator, []),
            annot_live_jsonl_paths.get(annotator, []),
        )
        if not annot_src:
            continue
        ann_filter = annotator_filters.get(annotator)
        ann_where, ann_params = filters.compile(annotator, ann_filter)
        params.extend(ann_params)
        ann_where_parts = [
            p for p in [ann_where, f'"{annotator}"."id" IS NOT NULL'] if p
        ]
        ann_where_clause = (
            f" WHERE {' AND '.join(ann_where_parts)}" if ann_where_parts else ""
        )
        join_parts.append(
            f'"{annotator}" AS (SELECT "id" FROM {annot_src} '
            f'AS "{annotator}"{ann_where_clause})'
        )
        active_annotators[annotator] = bool(ann_filter)

    ctes = [cte] + join_parts

    if not active_annotators:
        return f"WITH {cte} SELECT COUNT(*) as cnt FROM base", params

    join_clause_parts = []
    for ann, has_filter in active_annotators.items():
        join_type = "INNER JOIN" if has_filter else "LEFT JOIN"
        join_clause_parts.append(f'{join_type} "{ann}" USING (id)')
    join_clause = " ".join(join_clause_parts)
    return (
        f"WITH {', '.join(ctes)} SELECT COUNT(DISTINCT base.id) as cnt "
        f"FROM base {join_clause}",
        params,
    )


def keyset_eligible(
    filters: FilterSpec,
    sort: str | None,
    index_files: list[str],
) -> bool:
    """Whether the request can use the index-backed keyset pagination path."""
    return (
        bool(index_files)
        and (sort is None or sort == "id")
        and not filters.get_base_filter()
        and not filters.get_annotator_filters()
    )


def _batch_of_parquet_path(path: str) -> str | None:
    """Extract the batch name from a merged/index parquet path (segment before the filename)."""
    parts = path.rstrip("/").split("/")
    return parts[-2] if len(parts) >= 2 else None


def _plain_batch(stored: str) -> str:
    """Decode a stored _batch value (JSON-encoded) back to the plain batch name."""
    try:
        value = json.loads(stored)
        return value if isinstance(value, str) else stored
    except (json.JSONDecodeError, TypeError):
        return stored


def _prune_index_files(
    index_files: list[str],
    batch_meta: dict[str, dict[str, Any]],
    cursor_pair: tuple[str, str] | None,
    sort_dir: str,
) -> list[str]:
    """Keep index partitions whose id-range can intersect the keyset cursor."""
    cursor_id = cursor_pair[0] if cursor_pair else None
    kept = []
    for path in index_files:
        meta = batch_meta.get(_batch_of_parquet_path(path) or "")
        if not meta:
            kept.append(path)
            continue
        if sort_dir == "desc":
            if cursor_id is None or (
                meta.get("min_id") is not None and meta["min_id"] <= cursor_id
            ):
                kept.append(path)
        else:
            if cursor_id is None or (
                meta.get("max_id") is not None and meta["max_id"] >= cursor_id
            ):
                kept.append(path)
    return kept


def _prune_jsonl_blocks(
    jsonl_paths: list[str],
    batch_meta: dict[str, dict[str, Any]],
    window_min_id: str | None,
    window_max_id: str | None,
) -> list[str]:
    """Keep merged JSONL blocks whose id-range can intersect the window.

    Uses per-block min/max id ranges from ``_index/{batch}.meta.json``
    (``blocks`` entries); falls back to batch-level ranges when block meta
    is absent, and keeps the file when no meta is available at all.
    """
    kept = []
    for path in jsonl_paths:
        batch = _batch_of_parquet_path(path) or ""
        meta = batch_meta.get(batch) or {}
        blocks = meta.get("blocks")
        if blocks:
            filename = path.rsplit("/", 1)[-1]
            block_meta = next(
                (b for b in blocks if b.get("file") == filename), None
            )
            if block_meta is None:
                kept.append(path)
            elif _range_intersects(
                block_meta.get("min_id"), block_meta.get("max_id"),
                window_min_id, window_max_id,
            ):
                kept.append(path)
        elif meta and _range_intersects(
            meta.get("min_id"), meta.get("max_id"), window_min_id, window_max_id
        ):
            kept.append(path)
        elif not meta:
            kept.append(path)
    return kept


def _range_intersects(
    lo: str | None, hi: str | None, window_min: str | None, window_max: str | None
) -> bool:
    if lo is None or hi is None:
        return True
    if window_min is not None and hi < window_min:
        return False
    if window_max is not None and lo > window_max:
        return False
    return True


def parse_keyset_cursor(cursor: str | None) -> tuple[str, str] | None:
    """Parse the opaque cursor string into an (id, _batch) pair."""
    if not cursor:
        return None
    try:
        pair = json.loads(cursor)
        if not (isinstance(pair, list) and len(pair) == 2):
            return None
        if isinstance(pair[1], int):
            return None  # ordering cursor — belongs to the ordering path
        return (str(pair[0]), str(pair[1]))
    except (json.JSONDecodeError, TypeError):
        return None


def parse_ordering_cursor(cursor: str | None) -> tuple[str, int] | None:
    """Parse an ordering cursor (``[order_hash, position]``); None otherwise.

    The ``int`` second element distinguishes ordering cursors from the
    default-path ``[id, _batch]`` keyset cursors.
    """
    if not cursor:
        return None
    try:
        pair = json.loads(cursor)
        if isinstance(pair, list) and len(pair) == 2 and isinstance(pair[1], int):
            return (str(pair[0]), pair[1])
    except (json.JSONDecodeError, TypeError):
        return None
    return None


def build_count_query_fast(
    index_files: list[str],
    base_live_jsonl_paths: list[str],
) -> tuple[str, list[Any]]:
    """Index-backed distinct-id count (no filters). Returns (query, params)."""
    parts = [
        f'SELECT "id" FROM read_parquet({_format_parquet_paths(index_files)}, union_by_name=true)'
    ]
    live_src = _union_source([], base_live_jsonl_paths or [])
    if live_src:
        parts.append(f'SELECT "id" FROM {live_src}')
    union = " UNION ALL ".join(parts)
    query = (
        f'SELECT COUNT(DISTINCT "id") AS cnt FROM ({union}) WHERE "id" IS NOT NULL'
    )
    return query, []


def run_keyset_page(
    conn,
    columns: list[str],
    annotators: list[str],
    annot_parquet_paths: dict[str, list[str]],
    annot_jsonl_paths: dict[str, list[str]],
    annotator_columns: dict[str, list[str]],
    annot_live_jsonl_paths: dict[str, list[str]],
    base_merged_paths: list[str],
    base_jsonl_paths: list[str],
    base_live_jsonl_paths: list[str],
    index_files: list[str],
    batch_meta: dict[str, dict[str, Any]],
    cursor: str | None,
    sort_dir: str,
    limit: int,
) -> tuple[list[dict[str, Any]], list[str], dict[str, list[str]], str | None, bool]:
    """Index-backed keyset page executed on a single pooled connection.

    Step 1 (window): the page's (id, _batch) pairs come from the sorted index
    partitions UNION the live JSONL ids (so unmerged rows still page through),
    with the cursor filter pushed into each scan so per-file stats prune
    partitions outside the range.

    Step 2 (rows): full rows for exactly those pairs, reading only the merged
    files (parquet batches or JSONL blocks) whose id-range intersects the
    window plus the live chunks.

    Returns ``(rows, columns, annotator_columns, next_cursor, has_more)``.
    """
    params: list[Any] = []

    direction = "DESC" if sort_dir == "desc" else "ASC"
    # Composite (id, _batch) keyset with STRICT bounds: pages never re-include
    # the cursor pair itself (or any already-returned pair).
    cursor_pair = parse_keyset_cursor(cursor)
    row_cmp = "<" if sort_dir == "desc" else ">"

    index_paths = _prune_index_files(index_files, batch_meta, cursor_pair, sort_dir)

    # Window scans: index partitions first, live JSONL second. The cursor
    # filter is pushed into each scan so per-file stats prune partitions.
    window_parts: list[str] = []
    if index_paths:
        scan = (
            f'SELECT "id", "_batch" FROM read_parquet('
            f'{_format_parquet_paths(index_paths)}, union_by_name=true)'
        )
        if cursor_pair is not None:
            scan += f' WHERE ("id", "_batch") {row_cmp} (?, ?)'
            params.extend(cursor_pair)
        window_parts.append(scan)
    live_src = _union_source([], base_live_jsonl_paths or [])
    if live_src:
        scan = f'SELECT "id", "_batch" FROM {live_src}'
        if cursor_pair is not None:
            scan += f' WHERE ("id", "_batch") {row_cmp} (?, ?)'
            params.extend(cursor_pair)
        window_parts.append(scan)
    if not window_parts:
        return [], ["id", "_batch"], {}, cursor, False

    window_sql = (
        f"WITH pairs AS ({' UNION ALL '.join(window_parts)}) "
        f'SELECT "id", "_batch" FROM pairs '
        f'WHERE "id" IS NOT NULL AND "_batch" IS NOT NULL '
        f'GROUP BY "id", "_batch" '
        f'ORDER BY "id" {direction}, "_batch" {direction} LIMIT ?'
    )
    window_rows = conn.execute(window_sql, params + [limit + 1]).fetchall()

    has_more = len(window_rows) > limit
    window_rows = window_rows[:limit]
    if not window_rows:
        return [], ["id", "_batch"], {}, cursor, has_more

    next_cursor = json.dumps([window_rows[-1][0], window_rows[-1][1]])

    window_pairs = [(r[0], r[1], None) for r in window_rows]
    rows, result_columns, joined_annotators = fetch_window_rows(
        conn,
        window_pairs,
        columns,
        annotators,
        annot_parquet_paths,
        annot_jsonl_paths,
        annotator_columns,
        annot_live_jsonl_paths,
        base_merged_paths,
        base_jsonl_paths,
        base_live_jsonl_paths,
        batch_meta,
        position_order=False,
        sort_dir=sort_dir,
    )

    return rows, result_columns, joined_annotators, next_cursor, has_more


def fetch_window_rows(
    conn,
    window_rows: list[tuple[str, str, int | None]],
    columns: list[str],
    annotators: list[str],
    annot_parquet_paths: dict[str, list[str]],
    annot_jsonl_paths: dict[str, list[str]],
    annotator_columns: dict[str, list[str]],
    annot_live_jsonl_paths: dict[str, list[str]],
    base_parquet_paths: list[str],
    base_jsonl_paths: list[str],
    base_live_jsonl_paths: list[str],
    batch_meta: dict[str, dict[str, Any]],
    position_order: bool = False,
    sort_dir: str = "asc",
) -> tuple[list[dict[str, Any]], list[str], dict[str, list[str]]]:
    """Full rows for the window pairs, reading only intersecting merged files.

    Shared by the default keyset path (window from index partitions) and the
    ordering path (window from a materialized ordering file). *window_rows*
    are ``(id, _batch, position|None)`` tuples; when *position_order* is true
    the result is ordered by ``position`` (the ordering file's row order).

    Uses the *conn* argument for every query — callers already hold a pooled
    connection and nested pool acquires would deadlock.
    """
    # Temp table with the window pairs (parameterized inserts).
    conn.execute(
        'CREATE TEMP TABLE IF NOT EXISTS keyset_window '
        '("id" VARCHAR, "_batch" VARCHAR, "position" BIGINT)'
    )
    conn.execute("DELETE FROM keyset_window")
    conn.executemany("INSERT INTO keyset_window VALUES (?, ?, ?)", window_rows)

    # Only read merged files whose id-range intersects the window.
    window_batches = {_plain_batch(b) for _, b, _ in window_rows}
    window_min_id = min((r[0] for r in window_rows), default=None)
    window_max_id = max((r[0] for r in window_rows), default=None)
    pruned_merged = [
        p for p in base_parquet_paths if _batch_of_parquet_path(p) in window_batches
    ]
    pruned_blocks = _prune_jsonl_blocks(
        base_jsonl_paths, batch_meta, window_min_id, window_max_id
    )
    base_src = _union_source(pruned_merged, pruned_blocks + (base_live_jsonl_paths or []))
    if not base_src:
        return [], ["id", "_batch"], {}

    base_cols = [c for c in columns if c != "id" and c != "_batch"]
    for col in base_cols:
        _quote_identifier("base", col)

    if base_cols:
        # Schema drift: the window may intersect only files that lack some
        # requested columns (a column can exist solely in a batch this page
        # doesn't touch). When that happens, drop the pruning and read all
        # merged files so the column binds — the pruned read is an
        # optimization, not a correctness requirement.
        try:
            probe = conn.execute(f"SELECT * FROM {base_src} LIMIT 1")
            available_cols = {d[0] for d in probe.description}
            if any(col not in available_cols for col in base_cols):
                base_src = _union_source(
                    base_parquet_paths, base_jsonl_paths + (base_live_jsonl_paths or [])
                )
        except duckdb.Error:
            base_src = _union_source(
                base_parquet_paths, base_jsonl_paths + (base_live_jsonl_paths or [])
            )
    select_parts = ['"id"', '"_batch"']
    for col in base_cols:
        select_parts.append(f'ANY_VALUE("{col}") AS "{col}"')

    ctes = [
        f"base AS (SELECT {', '.join(select_parts)} FROM {base_src} AS base "
        'WHERE ("id", "_batch") IN (SELECT "id", "_batch" FROM keyset_window) '
        'GROUP BY "id", "_batch")'
    ]

    joined_annotators: dict[str, list[str]] = {}
    for annotator in annotators:
        _quote_identifier("", annotator)
        annot_src = _union_source(
            _prune_annotator_paths(
                annot_parquet_paths.get(annotator, []), window_batches
            ),
            _prune_jsonl_blocks(
                annot_jsonl_paths.get(annotator, []),
                batch_meta,
                window_min_id,
                window_max_id,
            )
            + annot_live_jsonl_paths.get(annotator, []),
        )
        if not annot_src:
            continue
        requested_cols = annotator_columns.get(annotator, [])
        try:
            # Use the held connection — acquiring another pooled connection
            # here deadlocks when the pool is exhausted.
            col_results = conn.execute(f"SELECT * FROM {annot_src} LIMIT 1")
            available_cols = set(col_results[0].keys()) if col_results else set()
        except Exception:
            available_cols = set()
        if requested_cols:
            valid_cols = [c for c in requested_cols if c in available_cols] if available_cols else requested_cols
        else:
            valid_cols = [c for c in available_cols if c not in ["id", "_batch"]]
        if not valid_cols:
            continue
        for col in valid_cols:
            _quote_identifier(annotator, col)
        ann_select = ['"id"'] + [f'ANY_VALUE("{c}") AS "{c}"' for c in valid_cols]
        ctes.append(
            f'"{annotator}" AS (SELECT {", ".join(ann_select)} '
            f'FROM {annot_src} AS "{annotator}" '
            'WHERE "id" IN (SELECT "id" FROM keyset_window) GROUP BY "id")'
        )
        joined_annotators[annotator] = valid_cols

    select_cols = ['"base"."id"', '"base"."_batch"']
    for col in base_cols:
        select_cols.append(f'"{col}"')
    for ann, valid_cols in joined_annotators.items():
        for col in valid_cols:
            select_cols.append(f'"{ann}"."{col}" AS "{ann}.{col}"')

    join_clause = " ".join(
        f'LEFT JOIN "{ann}" USING (id)' for ann in joined_annotators
    )

    if position_order:
        order_clause = '"position" ASC'
    else:
        direction = "DESC" if sort_dir == "desc" else "ASC"
        order_clause = f'"base"."id" {direction}, "base"."_batch" {direction}'

    row_sql = (
        f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} "
        f"FROM base{(' ' + join_clause) if join_clause else ''} "
        f'LEFT JOIN keyset_window ON ("base"."id", "base"."_batch") = '
        f'("keyset_window"."id", "keyset_window"."_batch") '
        f"ORDER BY {order_clause}"
    )
    result = conn.execute(row_sql)
    description = [d[0] for d in result.description]
    rows = [dict(zip(description, row)) for row in result.fetchall()]

    result_columns = ["id", "_batch"] + base_cols
    for ann, valid_cols in joined_annotators.items():
        for col in valid_cols:
            result_columns.append(f"{ann}.{col}")

    return rows, result_columns, joined_annotators


def _prune_annotator_paths(paths: list[str], window_batches: set[str]) -> list[str]:
    return [p for p in paths if _batch_of_parquet_path(p) in window_batches]


def build_ordering_query(
    conn,
    annotators: list[str],
    filters: FilterSpec,
    base_parquet_paths: list[str],
    base_jsonl_paths: list[str],
    annot_parquet_paths: dict[str, list[str]],
    annot_jsonl_paths: dict[str, list[str]],
    annotator_columns: dict[str, list[str]],
    sort: str | None,
    sort_dir: str,
    base_live_jsonl_paths: list[str] | None = None,
    annot_live_jsonl_paths: dict[str, list[str]] | None = None,
) -> tuple[str, list[Any]]:
    """Build the query that materializes an ordering for filtered/sorted views.

    Projects only ``id, _batch`` (plus the sort value when it is a base
    column), applies the same filters/joins as :func:`build_query` (so the
    row set matches what a scan would return), and orders rows per the same
    ORDER BY semantics — with no LIMIT. The caller writes the result (with a
    ``position`` column) to an ordering file for keyset paging.

    The *conn* argument runs the annotator column probe without acquiring
    another pooled connection (see :func:`fetch_window_rows`).
    """
    params: list[Any] = []

    base_live_jsonl_paths = base_live_jsonl_paths or []
    annot_live_jsonl_paths = annot_live_jsonl_paths or {}

    base_src = _union_source(base_parquet_paths, base_jsonl_paths + base_live_jsonl_paths)
    if not base_src:
        return "", params

    ctes = []

    base_filter = filters.get_base_filter()
    base_where, base_params = filters.compile("base", base_filter)
    params.extend(base_params)
    where_parts = [p for p in [base_where, '"base"."id" IS NOT NULL'] if p]
    base_where_clause = f" WHERE {' AND '.join(where_parts)}" if where_parts else ""

    sort_expr = '"base"."id"'
    select_parts = ['"id"', '"_batch"']
    if sort and sort not in ("id", "_batch"):
        _quote_identifier("base", sort)
        select_parts.append(f'ANY_VALUE("{sort}") AS "__sv"')
        sort_expr = '"__sv"'
    elif sort == "_batch":
        sort_expr = '"base"."_batch"'

    ctes.append(
        f"base AS (SELECT {', '.join(select_parts)} "
        f"FROM {base_src} AS base"
        f'{base_where_clause} GROUP BY "id", "_batch")'
    )

    joined_annotators: dict[str, bool] = {}
    annotator_filters = filters.get_annotator_filters()

    for annotator in annotators:
        _quote_identifier("", annotator)  # validate
        annot_src = _union_source(
            annot_parquet_paths.get(annotator, []),
            annot_jsonl_paths.get(annotator, [])
            + annot_live_jsonl_paths.get(annotator, []),
        )
        if not annot_src:
            continue

        ann_filter = annotator_filters.get(annotator)
        ann_where, ann_params = filters.compile(annotator, ann_filter)
        params.extend(ann_params)
        ann_where_parts = [
            p for p in [ann_where, f'"{annotator}"."id" IS NOT NULL'] if p
        ]
        ann_where_clause = (
            f" WHERE {' AND '.join(ann_where_parts)}" if ann_where_parts else ""
        )

        requested_cols = annotator_columns.get(annotator, [])
        try:
            col_results = conn.execute(f"SELECT * FROM {annot_src} LIMIT 1")
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
            f'"{annotator}" AS (SELECT "id" FROM {annot_src} '
            f'AS "{annotator}"{ann_where_clause} GROUP BY "id")'
        )
        joined_annotators[annotator] = bool(ann_filter)

    select_cols = ['"base"."id"', '"base"."_batch"']
    join_clause = ""
    if joined_annotators:
        join_parts = []
        for ann, has_filter in joined_annotators.items():
            join_type = "INNER JOIN" if has_filter else "LEFT JOIN"
            join_parts.append(f'{join_type} "{ann}" USING (id)')
        join_clause = " " + " ".join(join_parts)

    direction = "DESC" if sort_dir == "desc" else "ASC"
    order_clause = f' ORDER BY {sort_expr} {direction}, "base"."id" ASC'

    query = (
        f"WITH {', '.join(ctes)} SELECT {', '.join(select_cols)} "
        f"FROM base{join_clause}{order_clause}"
    )
    return query, params


def execute_query(
    query: str, params: list[Any] | None = None
) -> list[dict[str, Any]]:
    """Execute DuckDB query and return results as list of dicts.

    Delegates to the module-level default connection pool (persistent
    connections + httpfs cache), created on first use.
    """
    return db.get_pool().execute(query, params or [])


def init_pool(size: int | None = None, cache_dir: str | None = None) -> db.DuckDBPool:
    """Initialize the default connection pool (idempotent)."""
    return db.init_pool(size=size, cache_dir=cache_dir)


def shutdown_pool() -> None:
    """Close the default connection pool."""
    db.shutdown_pool()
