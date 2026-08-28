"""Thread-safe DuckDB connection pool with a persistent httpfs cache.

DuckDB connections are not thread-safe, so the pool hands out one connection
per thread via a queue. All connections point at the same (effectively
read-only) database file and share one httpfs cache directory, so S3 object
downloads survive between requests instead of being re-fetched per query.
"""

import logging
import os
import queue
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence, TypeVar

import duckdb

logger = logging.getLogger(__name__)

DUCKDB_POOL_SIZE = int(os.environ.get("DUCKDB_POOL_SIZE", "4"))
DUCKDB_CACHE_DIR = os.environ.get("DUCKDB_CACHE_DIR", "/tmp/viewer-duckdb-cache")
DUCKDB_MEMORY_LIMIT = os.environ.get("DUCKDB_MEMORY_LIMIT", "2GB")
POOL_ACQUIRE_TIMEOUT = float(os.environ.get("VIEWER_POOL_ACQUIRE_TIMEOUT", "30"))
HTTP_CACHE_MAX_BYTES = int(
    os.environ.get("DUCKDB_CACHE_MAX_BYTES", "0")
)  # 0 = disabled

T = TypeVar("T")


class PoolTimeout(Exception):
    """Raised when a pooled connection cannot be acquired in time."""


def prune_httpfs_cache(cache_dir: Path, max_bytes: int) -> None:
    """Delete oldest httpfs cache entries until under *max_bytes* (best-effort)."""
    if max_bytes <= 0:
        return
    http_dir = cache_dir / "http_cache"
    if not http_dir.exists():
        return
    files = sorted(http_dir.rglob("*"), key=lambda p: p.stat().st_mtime, reverse=True)
    total = sum(p.stat().st_size for p in files if p.is_file())
    for path in files:
        if total <= max_bytes or not path.is_file():
            break
        try:
            total -= path.stat().st_size
            path.unlink()
        except OSError:
            continue


def _sql_string_literal(value: str) -> str:
    """Escape a string for embedding in a DuckDB SET statement."""
    return "'" + value.replace("'", "''") + "'"


class DuckDBPool:
    """Pool of persistent DuckDB connections configured for S3 reads."""

    def __init__(
        self,
        size: int = DUCKDB_POOL_SIZE,
        cache_dir: str = DUCKDB_CACHE_DIR,
    ):
        self._cache_dir = Path(cache_dir)
        self._cache_dir.mkdir(parents=True, exist_ok=True)
        self._queue: queue.Queue[duckdb.DuckDBPyConnection] = queue.Queue(maxsize=size)
        for i in range(size):
            conn = self._create_connection(i)
            self._queue.put(conn)
        prune_httpfs_cache(self._cache_dir, HTTP_CACHE_MAX_BYTES)

    def _create_connection(self, index: int) -> duckdb.DuckDBPyConnection:
        db_path = self._cache_dir / "viewer.duckdb"
        conn = duckdb.connect(str(db_path), read_only=False)
        try:
            self._configure(conn, index)
        except Exception:
            conn.close()
            raise
        return conn

    def _configure(self, conn: duckdb.DuckDBPyConnection, index: int) -> None:
        # Per-connection temp dir; shared httpfs cache dir so block downloads
        # are reused across requests and connections.
        conn.execute(f"SET temp_directory={_sql_string_literal(str(self._cache_dir / f'tmp_{index}'))};")
        # Cap memory so big aggregations/sorts spill to the temp dir instead
        # of OOMing the process.
        conn.execute(f"SET memory_limit={_sql_string_literal(DUCKDB_MEMORY_LIMIT)};")

        conn.execute(
            f"""
            SET s3_access_key_id={_sql_string_literal(os.environ['S3_ACCESS_KEY'])};
            SET s3_secret_access_key={_sql_string_literal(os.environ['S3_SECRET_KEY'])};
            SET s3_endpoint={_sql_string_literal(_s3_endpoint_host())};
            SET s3_use_ssl={str(_s3_use_ssl()).lower()};
            SET s3_url_style='path';
        """
        )
        try:
            conn.execute(
                f"SET httpfs_cache_directory={_sql_string_literal(str(self._cache_dir / 'http_cache'))};"
            )
        except duckdb.Error:
            # Setting only exists in newer DuckDB versions.
            logger.debug("httpfs_cache_directory not supported by this DuckDB version")

    @contextmanager
    def acquire(self) -> Iterator[duckdb.DuckDBPyConnection]:
        try:
            conn = self._queue.get(timeout=POOL_ACQUIRE_TIMEOUT)
        except queue.Empty:
            raise PoolTimeout(
                "All DuckDB connections are busy; retry shortly"
            ) from None
        try:
            yield conn
        finally:
            self._queue.put(conn)

    def execute(
        self, query: str, params: Sequence[Any] | None = None
    ) -> list[dict[str, Any]]:
        """Execute a query and return all rows as a list of dicts."""
        with self.acquire() as conn:
            result = conn.execute(query, list(params or []))
            description = result.description
            rows = result.fetchall()
            return [dict(zip([d[0] for d in description], row)) for row in rows]

    def execute_stream(
        self,
        query: str,
        params: Sequence[Any] | None = None,
        batch_size: int = 5000,
    ) -> Iterator[list[dict[str, Any]]]:
        """Execute a query, yielding row batches (connection held while iterating)."""
        with self.acquire() as conn:
            result = conn.execute(query, list(params or []))
            description = result.description
            names = [d[0] for d in description]
            while True:
                rows = result.fetchmany(batch_size)
                if not rows:
                    break
                yield [dict(zip(names, row)) for row in rows]

    def run(self, fn: Callable[[duckdb.DuckDBPyConnection], T]) -> T:
        """Run *fn* on a single pooled connection (for multi-step operations)."""
        with self.acquire() as conn:
            return fn(conn)

    def close(self) -> None:
        while True:
            try:
                conn = self._queue.get_nowait()
            except queue.Empty:
                break
            conn.close()


def _s3_endpoint_host() -> str:
    endpoint = os.environ["S3_ENDPOINT_URL"]
    return endpoint.removeprefix("https://").removeprefix("http://").rstrip("/")


def _s3_use_ssl() -> bool:
    return os.environ["S3_ENDPOINT_URL"].startswith("https://")


_pool: DuckDBPool | None = None


def init_pool(
    size: int | None = None, cache_dir: str | None = None
) -> DuckDBPool:
    """Create the module-level default pool (idempotent)."""
    global _pool
    if _pool is None:
        _pool = DuckDBPool(
            size=size or DUCKDB_POOL_SIZE,
            cache_dir=cache_dir or DUCKDB_CACHE_DIR,
        )
    return _pool


def get_pool() -> DuckDBPool:
    """Return the default pool, creating it on first use."""
    return init_pool()


def shutdown_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
