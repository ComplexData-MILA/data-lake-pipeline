"""Materialized ordering cache for filtered/sorted pagination.

Filtered queries (and non-id sorts) cannot use the ``_index`` keyset path.
Scanning + re-sorting the whole dataset for every page is what caused the
legacy path's memory spikes, so instead the first request for a given
``(filters, sort)`` combination materializes the ordered id list ONCE —
spilling to disk under DuckDB's ``memory_limit`` — and every page walks it
with a keyset cursor over ``position``.

Files: ``{DUCKDB_CACHE_DIR}/orderings/{dataset}/{rowset_hash}/{order_hash}.parquet``
with columns ``(id, _batch, position)``. Freshness is file mtime vs
``ORDERING_TTL`` (the count cache has the same semantics); a stale file is
rebuilt on the next request. Builds are serialized per file with an
in-process lock plus a best-effort Redis ``SET NX`` for multi-process
deployments — a request that loses the race falls back to the scan path.
"""

import asyncio
import json
import logging
import os
import tempfile
import threading
import time
from pathlib import Path
from typing import Any

import duckdb

from . import db
from .cache import _hash_key, create_sync_redis
from .duckdb_query import (
    FilterSpec,
    build_ordering_query,
    fetch_window_rows,
    parse_ordering_cursor,
)

logger = logging.getLogger(__name__)

ORDERING_TTL = int(os.environ.get("ORDERING_TTL", "30"))
ORDERING_CLEANUP_INTERVAL = int(os.environ.get("ORDERING_CLEANUP_INTERVAL", "300"))
ORDERING_LOCK_TTL = 60
# How long a request waits for another in-process build before falling back
# to the scan path (bounded wait — no request should block on a long build).
ORDERING_BUILD_WAIT = float(os.environ.get("ORDERING_BUILD_WAIT", "5"))

_locks_guard = threading.Lock()
_build_locks: dict[str, threading.Lock] = {}


class OrderingBusy(Exception):
    """Another worker is materializing this ordering; fall back to the scan."""


def rowset_hash(annotator_cols: dict[str, list[str]], filter_data: dict[str, Any]) -> str:
    """Hash of the inputs that define the filtered row set (matches count_key)."""
    return _hash_key(annotator_cols, filter_data)


def order_hash(rowset: str, sort: str | None, sort_dir: str) -> str:
    return _hash_key(rowset, sort, sort_dir)


def ordering_root() -> Path:
    return Path(db.DUCKDB_CACHE_DIR) / "orderings"


def ordering_dir(dataset: str, rowset: str) -> Path:
    return ordering_root() / dataset / rowset


def ordering_path(dataset: str, rowset: str, order: str) -> Path:
    return ordering_dir(dataset, rowset) / f"{order}.parquet"


def _is_fresh(path: Path) -> bool:
    try:
        return time.time() - path.stat().st_mtime < ORDERING_TTL
    except OSError:
        return False


def _configure(conn: duckdb.DuckDBPyConnection, tmp_dir: str) -> None:
    conn.execute(
        f"""
        SET s3_access_key_id={db._sql_string_literal(os.environ['S3_ACCESS_KEY'])};
        SET s3_secret_access_key={db._sql_string_literal(os.environ['S3_SECRET_KEY'])};
        SET s3_endpoint={db._sql_string_literal(db._s3_endpoint_host())};
        SET s3_use_ssl={str(db._s3_use_ssl()).lower()};
        SET s3_url_style='path';
        SET temp_directory={db._sql_string_literal(tmp_dir)};
        SET memory_limit={db._sql_string_literal(db.DUCKDB_MEMORY_LIMIT)};
    """
    )
    try:
        conn.execute(
            f"SET httpfs_cache_directory={db._sql_string_literal(str(Path(db.DUCKDB_CACHE_DIR) / 'http_cache'))};"
        )
    except duckdb.Error:
        logger.debug("httpfs_cache_directory not supported by this DuckDB version")


def _build_lock(path: Path) -> threading.Lock:
    key = str(path)
    with _locks_guard:
        if key not in _build_locks:
            _build_locks[key] = threading.Lock()
        return _build_locks[key]


def _redis_lock(dataset: str, order: str, redis_client) -> tuple[bool, str]:
    """Best-effort cross-process SET NX lock; returns (acquired, lock_key)."""
    lock_key = f"viewer:{db.DUCKDB_CACHE_DIR}:{dataset}:ordering:lock:{order}"
    if redis_client is None:
        return True, lock_key
    try:
        acquired = redis_client.set(
            lock_key, "1", nx=True, ex=ORDERING_LOCK_TTL
        )
        return bool(acquired), lock_key
    except Exception as e:  # noqa: BLE001
        logger.debug(f"redis lock failed for {lock_key}: {e}")
        return True, lock_key


def _build_ordering(
    dataset: str,
    manifest,
    filter_spec: FilterSpec,
    sort: str | None,
    sort_dir: str,
    annotators: list[str],
    annotator_cols: dict[str, list[str]],
) -> Path:
    """Materialize the ordering file for a (filters, sort) combination (blocking)."""
    rsh = rowset_hash(annotator_cols, filter_spec.data)
    oh = order_hash(rsh, sort, sort_dir)
    path = ordering_path(dataset, rsh, oh)

    if _is_fresh(path):
        return path

    lock = _build_lock(path)
    if not lock.acquire(timeout=ORDERING_BUILD_WAIT):
        raise OrderingBusy(f"ordering {oh} is being built by another worker")
    try:
        if _is_fresh(path):
            return path

        redis_client = create_sync_redis()
        acquired, lock_key = _redis_lock(dataset, oh, redis_client)
        if not acquired:
            raise OrderingBusy(f"ordering {oh} is being built by another worker")
        try:
            annot_parquet_paths = {
                a: manifest.annotators[a].merged_parquet
                for a in annotators
                if a in manifest.annotators
            }
            annot_jsonl_paths = {
                a: manifest.annotators[a].merged_jsonl
                for a in annotators
                if a in manifest.annotators
            }
            annot_live_paths = {
                a: manifest.annotators[a].live_jsonl
                for a in annotators
                if a in manifest.annotators
            }

            with tempfile.TemporaryDirectory() as tmp:
                conn = duckdb.connect(os.path.join(tmp, "ordering.duckdb"))
                try:
                    _configure(conn, tmp)
                    query, params = build_ordering_query(
                        conn,
                        annotators,
                        filter_spec,
                        manifest.merged_parquet,
                        manifest.merged_jsonl,
                        annot_parquet_paths,
                        annot_jsonl_paths,
                        annotator_cols,
                        sort,
                        sort_dir,
                        manifest.live_jsonl,
                        annot_live_paths,
                    )
                    if not query:
                        # No base files at all — write an empty ordering.
                        conn.execute(
                            'CREATE TABLE ord_tmp ("id" VARCHAR, "_batch" VARCHAR)'
                        )
                    else:
                        conn.execute(
                            f"CREATE TABLE ord_tmp AS {query}", list(params)
                        )
                    path.parent.mkdir(parents=True, exist_ok=True)
                    tmp_path = path.with_name(f".tmp-{os.getpid()}-{threading.get_ident()}.parquet")
                    conn.execute(
                        f"COPY (SELECT \"id\", \"_batch\", "
                        "(row_number() OVER () - 1)::BIGINT AS position "
                        f"FROM ord_tmp) TO '{tmp_path}' (FORMAT PARQUET)"
                    )
                finally:
                    conn.close()
            os.replace(tmp_path, path)
            logger.info(
                f"Built ordering {path.name} for {dataset} "
                f"(filters={bool(filter_spec.data)}, sort={sort} {sort_dir})"
            )
        except OrderingBusy:
            raise
        except Exception:
            try:
                path.unlink(missing_ok=True)
            except OSError:
                pass
            raise
        finally:
            if redis_client is not None:
                try:
                    redis_client.delete(lock_key)
                except Exception:  # noqa: BLE001
                    pass
    finally:
        lock.release()
    return path


def get_or_build_ordering(
    dataset: str,
    manifest,
    filter_spec: FilterSpec,
    sort: str | None,
    sort_dir: str,
    annotators: list[str],
    annotator_cols: dict[str, list[str]],
) -> Path:
    """Return a fresh ordering file, building it if needed (blocking)."""
    rsh = rowset_hash(annotator_cols, filter_spec.data)
    oh = order_hash(rsh, sort, sort_dir)
    path = ordering_path(dataset, rsh, oh)
    if _is_fresh(path):
        return path
    return _build_ordering(
        dataset, manifest, filter_spec, sort, sort_dir, annotators, annotator_cols
    )


def fresh_ordering_file(dataset: str, rsh: str) -> Path | None:
    """Any fresh ordering file for a row set (same rows regardless of sort)."""
    odir = ordering_dir(dataset, rsh)
    if not odir.exists():
        return None
    for f in odir.glob("*.parquet"):
        if _is_fresh(f):
            return f
    return None


def run_ordering_page(
    conn,
    ordering_file: Path,
    order: str,
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
    cursor: str | None,
    page: int,
    page_size: int,
) -> tuple[list[dict[str, Any]], list[str], dict[str, list[str]], str | None, bool]:
    """Page through a materialized ordering with a ``[order_hash, position]``
    cursor. A cursor from a different (filters, sort) request is treated as a
    missing cursor (first page), never an error.
    """
    params: list[Any] = []
    cursor_pair = parse_ordering_cursor(cursor)
    if cursor_pair is not None and cursor_pair[0] == order:
        where = "position > ?"
        params.append(cursor_pair[1])
    else:
        offset = (max(page, 1) - 1) * page_size
        # One extra row past the page so has_more can be detected.
        where = "position >= ? AND position < ?"
        params.extend([offset, offset + page_size + 1])

    window_rows = conn.execute(
        f"SELECT \"id\", \"_batch\", position FROM read_parquet('{ordering_file}') "
        f"WHERE {where} ORDER BY position LIMIT ?",
        params + [page_size + 1],
    ).fetchall()

    has_more = len(window_rows) > page_size
    window_rows = window_rows[:page_size]
    if not window_rows:
        return [], ["id", "_batch"], {}, cursor, has_more

    next_cursor = json.dumps([order, window_rows[-1][2]])

    rows, result_columns, joined_annotators = fetch_window_rows(
        conn,
        window_rows,
        columns,
        annotators,
        annot_parquet_paths,
        annot_jsonl_paths,
        annotator_columns,
        annot_live_jsonl_paths,
        base_parquet_paths,
        base_jsonl_paths,
        base_live_jsonl_paths,
        batch_meta,
        position_order=True,
    )

    return rows, result_columns, joined_annotators, next_cursor, has_more


def _cleanup_once() -> None:
    root = ordering_root()
    if not root.exists():
        return
    cutoff = time.time() - ORDERING_TTL * 2
    for f in root.rglob("*.parquet"):
        try:
            if f.stat().st_mtime < cutoff:
                f.unlink()
        except OSError:
            continue
    for d in sorted((p for p in root.rglob("*") if p.is_dir()), reverse=True):
        try:
            d.rmdir()
        except OSError:
            pass


async def cleanup_loop() -> None:
    """Periodically unlink expired ordering files (lifespan task)."""
    _cleanup_once()
    while True:
        await asyncio.sleep(ORDERING_CLEANUP_INTERVAL)
        _cleanup_once()
