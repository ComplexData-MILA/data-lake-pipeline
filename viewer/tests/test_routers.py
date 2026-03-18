from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from viewer.tests.conftest import MockBatchManifest


class TestStatusRouter:
    def test_get_status_returns_200(self, test_client: TestClient, mock_batch_state):
        mock_batch_state.add_manifest(MockBatchManifest("batch1", state="completed"))
        mock_batch_state.add_manifest(MockBatchManifest("batch2", state="pending"))

        response = test_client.get("/api/status")

        assert response.status_code == 200
        data = response.json()
        assert "batches" in data
        assert "total_rows_processed" in data

    def test_invalidate_cache_returns_200(self, test_client: TestClient):
        response = test_client.post("/api/cache/invalidate")

        assert response.status_code == 200
        assert response.json() == {"status": "ok"}

    def test_get_sources_returns_list(
        self, test_client: TestClient, mock_batch_state, mock_storage
    ):
        mock_batch_state.add_manifest(MockBatchManifest("batch1", source="reddit"))
        mock_batch_state.add_manifest(MockBatchManifest("batch2", source="bluesky"))
        mock_storage.add_object("01_landing/twitter/2026-03-01.jsonl", b"")

        response = test_client.get("/api/sources")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "reddit" in data
        assert "bluesky" in data
        assert "twitter" in data

    def test_get_schema_landing(self, test_client: TestClient):
        response = test_client.get("/api/schema/landing")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        assert len(data["columns"]) > 0

    def test_get_schema_queue(self, test_client: TestClient):
        response = test_client.get("/api/schema/queue")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data
        col_names = [c["name"] for c in data["columns"]]
        assert "state" in col_names

    def test_get_schema_processed(self, test_client: TestClient):
        response = test_client.get("/api/schema/processed")

        assert response.status_code == 200
        data = response.json()
        assert "columns" in data

    def test_get_schema_invalid_stage(self, test_client: TestClient):
        response = test_client.get("/api/schema/invalid")

        assert response.status_code == 200
        data = response.json()
        assert data["columns"] == []


class TestQueryRouter:
    def test_valid_select_query_returns_200(self, test_client: TestClient):
        pass

    def test_non_select_query_returns_400(self, test_client: TestClient):
        response = test_client.post("/api/query", json={"sql": "DROP TABLE test"})

        assert response.status_code == 400
        assert "Only SELECT queries" in response.json()["detail"]


class TestManifestsRouter:
    def test_list_manifests_returns_list(
        self, test_client: TestClient, mock_batch_state
    ):
        mock_batch_state.add_manifest(MockBatchManifest("batch1", state="completed"))
        mock_batch_state.add_manifest(MockBatchManifest("batch2", state="pending"))

        response = test_client.get("/api/manifests")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 2

    def test_get_manifest_returns_404_for_missing(self, test_client: TestClient):
        response = test_client.get("/api/manifests/nonexistent-batch-id")

        assert response.status_code == 404


class TestRecordsRouter:
    def test_query_landing_records(self, test_client: TestClient, mock_storage):
        mock_storage.add_jsonl_object(
            "01_landing/reddit/2026-03-01.jsonl",
            [
                {
                    "source": "reddit",
                    "external_id": "1",
                    "text": "hello world",
                    "created_at": "2026-03-01T00:00:00Z",
                },
                {
                    "source": "reddit",
                    "external_id": "2",
                    "text": "foo bar",
                    "created_at": "2026-03-01T01:00:00Z",
                },
            ],
        )

        response = test_client.post(
            "/api/records",
            json={"stage": "landing", "page": 1, "page_size": 10, "filters": []},
        )

        assert response.status_code == 200
        data = response.json()
        assert "records" in data
        assert data["total_count"] == 2
        assert len(data["records"]) == 2
        assert data["page"] == 1

    def test_query_landing_records_with_filter(
        self, test_client: TestClient, mock_storage
    ):
        mock_storage.add_jsonl_object(
            "01_landing/reddit/2026-03-01.jsonl",
            [
                {
                    "source": "reddit",
                    "external_id": "1",
                    "text": "hello world",
                    "created_at": "2026-03-01T00:00:00Z",
                },
                {
                    "source": "reddit",
                    "external_id": "2",
                    "text": "foo bar",
                    "created_at": "2026-03-01T01:00:00Z",
                },
            ],
        )

        response = test_client.post(
            "/api/records",
            json={
                "stage": "landing",
                "page": 1,
                "page_size": 10,
                "filters": [
                    {"field": "text", "operator": "contains", "value": "hello"}
                ],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert "hello" in data["records"][0]["text"]

    def test_query_queue_records(self, test_client: TestClient, mock_batch_state):
        mock_batch_state.add_manifest(
            MockBatchManifest("batch1", source="reddit", state="pending")
        )
        mock_batch_state.add_manifest(
            MockBatchManifest("batch2", source="bluesky", state="completed")
        )

        response = test_client.post(
            "/api/records",
            json={"stage": "queue", "page": 1, "page_size": 10, "filters": []},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 2

    def test_query_queue_records_with_state_filter(
        self, test_client: TestClient, mock_batch_state
    ):
        mock_batch_state.add_manifest(
            MockBatchManifest("batch1", source="reddit", state="pending")
        )
        mock_batch_state.add_manifest(
            MockBatchManifest("batch2", source="bluesky", state="completed")
        )

        response = test_client.post(
            "/api/records",
            json={
                "stage": "queue",
                "page": 1,
                "page_size": 10,
                "filters": [{"field": "state", "operator": "eq", "value": "pending"}],
            },
        )

        assert response.status_code == 200
        data = response.json()
        assert data["total_count"] == 1
        assert data["records"][0]["state"] == "pending"

    def test_query_records_pagination(self, test_client: TestClient, mock_batch_state):
        for i in range(25):
            mock_batch_state.add_manifest(
                MockBatchManifest(f"batch{i}", state="pending")
            )

        response = test_client.post(
            "/api/records",
            json={"stage": "queue", "page": 1, "page_size": 10, "filters": []},
        )

        assert response.status_code == 200
        data = response.json()
        assert len(data["records"]) == 10
        assert data["total_pages"] == 3

        response2 = test_client.post(
            "/api/records",
            json={"stage": "queue", "page": 3, "page_size": 10, "filters": []},
        )

        assert response2.status_code == 200
        data2 = response2.json()
        assert len(data2["records"]) == 5
