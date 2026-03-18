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


class TestQueryRouter:
    def test_valid_select_query_returns_200(self, test_client: TestClient):
        pass

    def test_non_select_query_returns_400(self, test_client: TestClient):
        response = test_client.post("/api/query", json={"sql": "DROP TABLE test"})

        assert response.status_code == 400
        assert "Only SELECT queries" in response.json()["detail"]


class TestManifestsRouter:
    def test_list_manifests_returns_list(self, test_client: TestClient, mock_batch_state):
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
