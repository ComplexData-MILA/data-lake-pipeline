"""SQL builders for the dashboard chart endpoints (activity + categorical).

The pipeline stores string values differently per format: parquet keeps raw
strings while JSONL chunks/blocks JSON-stringify them
(:func:`s3_data_tool.s3_utils.transform_row_for_jsonl`). Every expression
touching a string column here goes through :func:`_normalize_string`, which
collapses both encodings to the bare string via a JSON extraction roundtrip
(raw strings are not valid JSON, so they fall through to the plain cast).

Rows without a ``_created_at`` value (everything ingested before the field
was introduced) are excluded from all chart queries by design.
"""

from typing import Any

from .duckdb_query import _quote_identifier, _union_source

# Whitelisted bucket widths; only these literals are ever interpolated into SQL.
BUCKET_INTERVALS = {
    "1m": "1 minute",
    "5m": "5 minutes",
    "1h": "1 hour",
    "1d": "1 day",
}


def _normalize_string(expr: str) -> str:
    """Collapse raw and JSON-quoted string encodings to the bare string.

    ``CAST(json AS VARCHAR)`` keeps the quotes, so the JSON path must extract
    the scalar (``json_extract_string(..., '$')``); raw strings are not valid
    JSON, so ``TRY_CAST(AS JSON)`` yields NULL and they fall through to the
    plain cast.
    """
    return (
        f"COALESCE(json_extract_string(TRY_CAST({expr} AS JSON), '$'), "
        f"TRY_CAST({expr} AS VARCHAR))"
    )


def _created_at_expr(alias: str) -> str:
    """Normalized ``_created_at`` cast to TIMESTAMPTZ (NULL when absent/unparseable)."""
    col = _quote_identifier(alias, "_created_at")
    return f"TRY_CAST({_normalize_string(col)} AS TIMESTAMPTZ)"


def _value_expr(alias: str, column: str) -> str:
    """Normalized value expression for a categorical *column*.

    Raises ValueError for invalid identifiers (callers map to HTTP 400).
    """
    return _normalize_string(_quote_identifier(alias, column))


def _base_norm_cte(cte_name: str, src: str, extra_select: str = "") -> str:
    """CTE projecting id, _batch, normalized created-at (ct) [+ extra].

    The union source is aliased ``src`` inside the CTE; *cte_name* names the
    CTE itself (callers reference its columns as ``ct`` / ``v``).
    """
    extra = f", {extra_select}" if extra_select else ""
    return (
        f'{cte_name} AS (SELECT "id", "_batch", {_created_at_expr("src")} AS ct'
        f"{extra} FROM {src} AS src"
        f' WHERE "src"."id" IS NOT NULL AND "src"."_batch" IS NOT NULL)'
    )


def _window_sql(window_start: str | None, window_end: str | None) -> tuple[str, list[Any]]:
    """WHERE fragment (on ``ct``) + bound params for a [start, end) window."""
    if window_start is not None:
        return (
            "ct >= TRY_CAST(? AS TIMESTAMPTZ) AND ct < TRY_CAST(? AS TIMESTAMPTZ)",
            [window_start, window_end],
        )
    return "", []


def build_activity_query(
    parquet_paths: list[str],
    jsonl_paths: list[str],
    interval_literal: str,
    window_start: str | None,
    window_end: str | None,
) -> tuple[str, list[Any]]:
    """Rows created per time bucket for one dataset (id,_batch deduped).

    The merged+live overlap is absorbed by ``GROUP BY id, _batch``; a chunk row
    and its merged copy carry the same ``_created_at``, so ``MIN(ct)`` is exact.
    """
    src = _union_source(parquet_paths, jsonl_paths)
    if not src:
        return "", []
    window, params = _window_sql(window_start, window_end)
    where = f" WHERE ct IS NOT NULL" + (f" AND {window}" if window else "")
    query = f"""
        WITH {_base_norm_cte("base_norm", src)},
        dedup AS (
          SELECT MIN(ct) AS ct FROM base_norm{where}
          GROUP BY "id", "_batch"
        )
        SELECT time_bucket(INTERVAL '{interval_literal}', ct) AS ts, COUNT(*) AS cnt
        FROM dedup GROUP BY ts ORDER BY ts
    """
    return query, params


def build_categorical_counts_query(
    parquet_paths: list[str],
    jsonl_paths: list[str],
    column: str,
    limit: int,
    window_start: str | None,
    window_end: str | None,
) -> tuple[str, list[Any]]:
    """Top-*limit* value counts for a categorical *column* (+ total/distinct)."""
    v = _value_expr("src", column)
    src = _union_source(parquet_paths, jsonl_paths)
    if not src:
        return "", []
    window, params = _window_sql(window_start, window_end)
    where = "v IS NOT NULL AND ct IS NOT NULL" + (f" AND {window}" if window else "")
    query = f"""
        WITH {_base_norm_cte("base_norm", src, f"{v} AS v")},
        dedup AS (
          SELECT ANY_VALUE(v) AS v FROM base_norm
          WHERE {where}
          GROUP BY "id", "_batch"
        ),
        agg AS (SELECT v, COUNT(*) AS cnt FROM dedup GROUP BY v)
        SELECT v, cnt, SUM(cnt) OVER () AS total, COUNT(*) OVER () AS distinct_count
        FROM agg ORDER BY cnt DESC, v ASC LIMIT ?
    """
    return query, params + [limit]


def build_categorical_trend_query(
    parquet_paths: list[str],
    jsonl_paths: list[str],
    column: str,
    interval_literal: str,
    limit: int,
    window_start: str | None,
    window_end: str | None,
) -> tuple[str, list[Any]]:
    """Counts per (time bucket, category) for the top-*limit* values of *column*.

    Non-top values fold into a single ``other`` category. The caller derives
    the ordered top-value list from the returned series (each value's per-bucket
    counts summed) — no second scan needed.
    """
    v = _value_expr("src", column)
    src = _union_source(parquet_paths, jsonl_paths)
    if not src:
        return "", []
    window, params = _window_sql(window_start, window_end)
    where = "v IS NOT NULL AND ct IS NOT NULL" + (f" AND {window}" if window else "")
    query = f"""
        WITH {_base_norm_cte("base_norm", src, f"{v} AS v")},
        dedup AS (
          SELECT ANY_VALUE(v) AS v, MIN(ct) AS ct FROM base_norm
          WHERE {where}
          GROUP BY "id", "_batch"
        ),
        totals AS (SELECT v, COUNT(*) AS total_cnt FROM dedup GROUP BY v),
        topk AS (SELECT v FROM totals
                 QUALIFY row_number() OVER (ORDER BY total_cnt DESC, v ASC) <= ?),
        series AS (
          SELECT time_bucket(INTERVAL '{interval_literal}', ct) AS ts,
                 CASE WHEN v IN (SELECT v FROM topk) THEN v ELSE 'other' END AS cat
          FROM dedup
        )
        SELECT ts, cat, COUNT(*) AS cnt FROM series GROUP BY ts, cat ORDER BY ts, cat
    """
    return query, params + [limit]
