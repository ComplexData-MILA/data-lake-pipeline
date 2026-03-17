"""Bloom filter cache for deduplicating X Community Notes."""

from __future__ import annotations

import io
import logging
from typing import TYPE_CHECKING, Iterator

from pybloom_live import ScalableBloomFilter
from tqdm import tqdm

from data_lake_pipeline.ingestion.stats import IngestStats

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend

BLOOM_FILENAME = "seen_notes.bloom"
INITIAL_CAPACITY = 3_000_000
ERROR_RATE = 0.001

logger = logging.getLogger(__name__)


def _bloom_key(cache_prefix: str) -> str:
    return f"{cache_prefix}/{BLOOM_FILENAME}"


def load_or_create_bloom(storage: StorageBackend, cache_prefix: str) -> ScalableBloomFilter:
    key = _bloom_key(cache_prefix)
    try:
        with tqdm(desc="Loading bloom filter from S3", unit="B", unit_scale=True) as pbar:
            data = storage.read_bytes(key)
            pbar.update(len(data))
        return ScalableBloomFilter.fromfile(io.BytesIO(data))
    except Exception:
        pass
    logger.info("Creating new bloom filter at %s", key)
    return ScalableBloomFilter(
        initial_capacity=INITIAL_CAPACITY,
        error_rate=ERROR_RATE,
        mode=ScalableBloomFilter.LARGE_SET_GROWTH,
    )


def save_bloom(bloom: ScalableBloomFilter, storage: StorageBackend, cache_prefix: str) -> None:
    key = _bloom_key(cache_prefix)
    buffer = io.BytesIO()
    bloom.tofile(buffer)
    data = buffer.getvalue()
    with tqdm(total=len(data), desc="Saving bloom filter to S3", unit="B", unit_scale=True) as pbar:
        storage.write_bytes(key, data)
        pbar.update(len(data))
    logger.info("Saved bloom filter to %s", key)


def is_new_note(note_id: str, bloom: ScalableBloomFilter) -> bool:
    if note_id in bloom:
        return False
    bloom.add(note_id)
    return True


def filter_new_notes(
    rows: Iterator[dict], bloom: ScalableBloomFilter, stats: IngestStats
) -> Iterator[dict]:
    for r in rows:
        if is_new_note(r["noteId"], bloom):
            stats.increment("deduplicated")
            yield r
