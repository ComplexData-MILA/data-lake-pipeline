"""Unit tests for activity-window pruning and the datasets-list fast path.

These exercise :func:`viewer.backend.main._files_in_window` (mtime-based
window pruning) and :func:`viewer.backend.main._prefix_contains_data` (one
listing page instead of paginating every object) with fake S3 clients — no
live bucket needed, hence no ``integration`` marker.
"""

from datetime import datetime, timedelta, timezone

from viewer.backend.main import (
    _files_in_window,
    _is_data_object_key,
    _prefix_contains_data,
)
from viewer.backend.s3_files import FileManifest


def _mtime(minutes_ago: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()


def _manifest(mtimes: dict[str, str]) -> FileManifest:
    return FileManifest(
        dataset="ds",
        merged_parquet=[],
        merged_jsonl=[],
        live_jsonl=[],
        file_mtimes=mtimes,
    )


class TestIsDataObjectKey:
    def test_matches_merged_parquet(self):
        assert _is_data_object_key("datasets/ds/1/merged.parquet")

    def test_matches_merged_blocks(self):
        assert _is_data_object_key("datasets/ds/1/merged_00001.jsonl.gz")

    def test_matches_live_chunks(self):
        assert _is_data_object_key("datasets/ds/1/abc123_chunk_00000.jsonl")

    def test_rejects_manifests_and_status(self):
        assert not _is_data_object_key("datasets/ds/1/abc123.manifest.json")
        assert not _is_data_object_key("datasets/ds/_migration/status.json")
        assert not _is_data_object_key("datasets/ds/_index/1.meta.json")


class TestFilesInWindow:
    def test_no_window_keeps_all(self):
        files = ["s3://b/ds/1/merged.parquet"]
        assert _files_in_window(files, _manifest({}), None) == files

    def test_old_file_is_pruned(self):
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest({files[0]: _mtime(120)})
        window_start = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        assert _files_in_window(files, manifest, window_start) == []

    def test_recent_file_is_kept(self):
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest({files[0]: _mtime(5)})
        window_start = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        assert _files_in_window(files, manifest, window_start) == files

    def test_boundary_mtime_equal_to_window_start_is_kept(self):
        # [start, end) window: a file written exactly at the boundary can
        # still hold in-window rows, so it must not be pruned.
        start = datetime.now(timezone.utc) - timedelta(minutes=60)
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest({files[0]: start.isoformat()})
        assert _files_in_window(files, manifest, start.isoformat()) == files

    def test_s3_last_modified_format_parses(self):
        # S3 LastModified uses a space separator and microsecond precision.
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest(
            {files[0]: (datetime.now(timezone.utc) - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M:%S.%f+00:00")}
        )
        window_start = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        assert _files_in_window(files, manifest, window_start) == files

    def test_unknown_mtime_kept_fail_safe(self):
        files = ["s3://b/ds/1/merged.parquet"]
        window_start = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        assert _files_in_window(files, _manifest({}), window_start) == files

    def test_malformed_mtime_kept_fail_safe(self):
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest({files[0]: "not-a-timestamp"})
        window_start = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
        assert _files_in_window(files, manifest, window_start) == files

    def test_malformed_window_keeps_all(self):
        files = ["s3://b/ds/1/merged.parquet"]
        manifest = _manifest({files[0]: _mtime(120)})
        assert _files_in_window(files, manifest, "garbage") == files


class _FakePaginator:
    def __init__(self, pages):
        self.pages = pages

    def paginate(self, **kwargs):
        return iter([{"Contents": page} for page in self.pages])


class _FakeClient:
    """list_objects_v2 returns the first page once; paginate serves the rest."""

    def __init__(self, first_page, later_pages):
        self.first_page = first_page
        self.later_pages = later_pages
        self.direct_calls = 0

    def list_objects_v2(self, **kwargs):
        self.direct_calls += 1
        return {"Contents": list(self.first_page)}

    def get_paginator(self, _name):
        return _FakePaginator(self.later_pages)


def _page(*keys):
    return [{"Key": k} for k in keys]


class TestPrefixContainsData:
    def test_found_on_first_page_single_call(self):
        client = _FakeClient(
            _page("d/0/merged_00000.jsonl.gz", "d/0/merged_00001.jsonl.gz"),
            [],
        )
        assert _prefix_contains_data(client, "b", "d/") is True
        assert client.direct_calls == 1

    def test_data_after_index_objects_found_on_first_page(self):
        # _index/_migration objects sort before batch dirs; the fast path
        # must still find batch data on the same page.
        client = _FakeClient(
            _page("d/_index/1.parquet", "d/_migration/status.json", "d/0/merged_00000.jsonl.gz"),
            [],
        )
        assert _prefix_contains_data(client, "b", "d/") is True
        assert client.direct_calls == 1

    def test_falls_back_to_full_pagination(self):
        # First page has no data (e.g. a huge _index) — data on a later page.
        client = _FakeClient(
            _page("d/_index/1.parquet"),
            [_page("d/_index/2.parquet"), _page("d/0/merged_00000.jsonl.gz")],
        )
        assert _prefix_contains_data(client, "b", "d/") is True

    def test_no_data_anywhere(self):
        client = _FakeClient(
            _page("d/_migration/status.json"),
            [_page("d/config/x.json")],
        )
        assert _prefix_contains_data(client, "b", "d/") is False
