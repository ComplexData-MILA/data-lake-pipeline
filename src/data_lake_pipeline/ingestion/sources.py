from __future__ import annotations

import csv
import tempfile
import urllib.request
import zipfile
from datetime import datetime, timedelta, timezone
from itertools import islice
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterator
from zoneinfo import ZoneInfo

from data_lake_pipeline.ingestion.bloom_cache import (
    filter_new_notes,
    load_or_create_bloom,
    save_bloom,
)
from data_lake_pipeline.ingestion.stats import IngestStats
from data_lake_pipeline.schemas import SourcePost

if TYPE_CHECKING:
    from data_lake_pipeline.storage.base import StorageBackend

NOTES_BASE_URL = "https://ton.twimg.com/birdwatch-public-data"
SOURCE_NAME = "x_community_notes"
ET = ZoneInfo("America/New_York")


def _get_now_et() -> datetime:
    return datetime.now(ET)


def _example_posts(source: str, limit: int) -> list[SourcePost]:
    return [
        SourcePost(
            source=source,
            external_id=f"{source}-example-{i}",
            text=f"Example text payload {i} from {source}.",
            created_at="2026-03-13T12:00:00Z",
            url=f"https://example.com/{source}/{i}",
            author=f"{source}_author_{i}",
            score=float(100 - i),
            metadata={"rank": i + 1, "lang": "en"},
        )
        for i in range(limit)
    ]


def fetch_bluesky_top_posts(
    limit: int | None = 50, *, use_example_data: bool = True
) -> Iterator[SourcePost]:
    if use_example_data:
        example_limit = min(limit, 3) if limit else 3
        return iter(_example_posts("bluesky", example_limit))
    return iter([])


def fetch_reddit_top_posts(
    limit: int | None = 100, *, use_example_data: bool = True
) -> Iterator[SourcePost]:
    if use_example_data:
        example_limit = min(limit, 3) if limit else 3
        return iter(_example_posts("reddit", example_limit))
    return iter([])


def _construct_url(date: datetime) -> str:
    date_str = date.strftime("%Y/%m/%d")
    return f"{NOTES_BASE_URL}/{date_str}/notes/notes-00000.zip"


def _download_zip_to_temp(url: str) -> Path:
    with urllib.request.urlopen(url, timeout=120) as response:
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmp:
            tmp.write(response.read())
            return Path(tmp.name)


def _parse_tsv_from_zip(zip_path: Path, stats: IngestStats) -> Iterator[dict[str, Any]]:
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            tsv_name = next(n for n in zf.namelist() if n.endswith(".tsv"))
            with zf.open(tsv_name, "r") as f:
                text_reader = (line.decode("utf-8") for line in f)
                reader = csv.DictReader(text_reader, delimiter="\t")
                for row in reader:
                    stats.increment("total_rows")
                    if not row.get("noteId") or not row.get("createdAtMillis"):
                        stats.increment("skipped_rows")
                        continue
                    yield row
    finally:
        zip_path.unlink(missing_ok=True)


def _get_timestamp_threshold(backfill_days: int) -> int | None:
    if backfill_days == -1:
        return None
    threshold = _get_now_et() - timedelta(days=backfill_days)
    return int(threshold.timestamp() * 1000)


def _filter_by_timestamp(
    rows: Iterator[dict], threshold_ms: int | None, stats: IngestStats
) -> Iterator[dict]:
    if threshold_ms is None:
        for r in rows:
            stats.increment("timestamp_filtered")
            yield r
    else:
        for r in rows:
            if int(r["createdAtMillis"]) >= threshold_ms:
                stats.increment("timestamp_filtered")
                yield r


def _millis_to_iso(millis: str) -> str:
    ts = int(millis) / 1000
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _row_to_source_post(row: dict[str, Any]) -> SourcePost:
    return SourcePost(
        source=SOURCE_NAME,
        external_id=row["noteId"],
        text=row.get("summary", ""),
        created_at=_millis_to_iso(row["createdAtMillis"]),
        url=f"https://x.com/i/status/{row['tweetId']}",
        author=row.get("noteAuthorParticipantId"),
        score=None,
        metadata={
            "tweet_id": row["tweetId"],
            "classification": row.get("classification"),
            "believable": row.get("believable"),
            "harmful": row.get("harmful"),
            "validation_difficulty": row.get("validationDifficulty"),
            "is_media_note": bool(int(row.get("isMediaNote", 0))),
            "is_collaborative_note": bool(int(row.get("isCollaborativeNote", 0))),
            "misleading": {
                "other": bool(int(row.get("misleadingOther", 0))),
                "factual_error": bool(int(row.get("misleadingFactualError", 0))),
                "manipulated_media": bool(int(row.get("misleadingManipulatedMedia", 0))),
                "outdated_info": bool(int(row.get("misleadingOutdatedInformation", 0))),
                "missing_context": bool(int(row.get("misleadingMissingImportantContext", 0))),
                "unverified_claim": bool(int(row.get("misleadingUnverifiedClaimAsFact", 0))),
                "satire": bool(int(row.get("misleadingSatire", 0))),
            },
            "not_misleading": {
                "other": bool(int(row.get("notMisleadingOther", 0))),
                "factually_correct": bool(int(row.get("notMisleadingFactuallyCorrect", 0))),
                "outdated_but_not_when_written": bool(int(row.get("notMisleadingOutdatedButNotWhenWritten", 0))),
                "clearly_satire": bool(int(row.get("notMisleadingClearlySatire", 0))),
                "personal_opinion": bool(int(row.get("notMisleadingPersonalOpinion", 0))),
            },
            "trustworthy_sources": bool(int(row.get("trustworthySources", 0))),
        },
    )


def fetch_x_community_notes(
    limit: int | None = 1000,
    *,
    use_example_data: bool = True,
    storage: StorageBackend | None = None,
    cache_prefix: str = "00_cache/x_community_notes",
    incremental: bool = True,
    backfill_days: int = 0,
) -> tuple[Iterator[SourcePost], IngestStats]:
    """
    Fetch X Community Notes from today's cumulative snapshot.

    Args:
        limit: Maximum notes to return. None returns all.
        use_example_data: Return mock data if True.
        storage: StorageBackend for bloom filter persistence.
        cache_prefix: S3 prefix for bloom filter storage.
        incremental: Deduplicate against previously seen notes.
        backfill_days: Include notes from last N days (0=today, -1=all).

    Returns:
        (posts, stats) where stats has counts populated during iteration:
        - total_rows: Rows parsed from TSV
        - timestamp_filtered: Rows after backfill filter
        - deduplicated: Rows after bloom filter
        - skipped_rows: Rows missing required fields
    """
    stats = IngestStats()

    if use_example_data:
        example_limit = min(limit, 3) if limit else 3
        return iter(_example_posts(SOURCE_NAME, example_limit)), stats

    if storage is None:
        raise ValueError("storage required when use_example_data=False")

    fetch_date = _get_now_et()
    url = _construct_url(fetch_date)

    zip_path = _download_zip_to_temp(url)
    rows = _parse_tsv_from_zip(zip_path, stats)

    threshold_ms = _get_timestamp_threshold(backfill_days)
    rows = _filter_by_timestamp(rows, threshold_ms, stats)

    if incremental:
        bloom = load_or_create_bloom(storage, cache_prefix)
        rows = filter_new_notes(rows, bloom, stats)
        save_bloom(bloom, storage, cache_prefix)

    rows = islice(rows, limit)
    posts = (_row_to_source_post(r) for r in rows)

    return posts, stats
