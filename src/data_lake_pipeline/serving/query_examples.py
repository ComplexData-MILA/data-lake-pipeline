from __future__ import annotations

import duckdb
from pathlib import Path


def query_processed_stash(processed_dir: str | Path, needle: str):
    processed_dir = Path(processed_dir)
    glob_expr = str(processed_dir / "*.parquet")
    sql = f"""
    SELECT source, external_id, annotation, processed_at
    FROM '{glob_expr}'
    WHERE annotation ILIKE ?
    ORDER BY processed_at DESC
    """
    return duckdb.execute(sql, [f"%{needle}%"]).df()
