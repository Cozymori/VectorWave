import pytest
from datetime import datetime, timezone

from vectorwave.database.dataset import VectorWaveDatasetManager
from vectorwave.database.db import (
    get_weaviate_client,
    create_execution_schema,
    create_golden_dataset_schema,
)
from vectorwave.models.db_config import WeaviateSettings


@pytest.fixture
def e2e_settings() -> WeaviateSettings:
    s = WeaviateSettings()
    s.IS_VECTORIZE_COLLECTION_NAME = False
    s.VECTORIZER = "none"
    return s


def _seed_execution(coll, *, function_name: str, return_value: str, vector, status="SUCCESS", uuid=None):
    """Insert one execution log with an explicit vector. Returns the assigned uuid (str)."""
    return coll.data.insert(
        properties={
            "function_name": function_name,
            "function_uuid": "00000000-0000-0000-0000-000000000001",
            "return_value": return_value,
            "status": status,
            "duration_ms": 10,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        },
        vector=vector,
        uuid=uuid,
    )


@pytest.mark.e2e
def test_register_as_golden_copies_vector_and_properties(clean_weaviate, e2e_settings):
    """register_as_golden fetches the source log, copies its vector, and inserts into golden."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_golden_dataset_schema(client, e2e_settings)
        exec_col = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        golden_col = client.collections.get(e2e_settings.GOLDEN_COLLECTION_NAME)

        log_uuid = str(_seed_execution(
            exec_col, function_name="calc", return_value="42", vector=[0.1, 0.2, 0.3]
        ))

        manager = VectorWaveDatasetManager()
        assert manager.register_as_golden(log_uuid, note="Best case", tags=["benchmark"]) is True

        golden_objs = list(golden_col.iterator(include_vector=True))
        assert len(golden_objs) == 1
        g = golden_objs[0]
        assert g.properties["original_uuid"] == log_uuid
        assert g.properties["function_name"] == "calc"
        assert g.properties["return_value"] == "42"
        assert g.properties["note"] == "Best case"
        assert g.properties["tags"] == ["benchmark"]
        assert g.vector["default"] == pytest.approx([0.1, 0.2, 0.3])
    finally:
        client.close()


@pytest.mark.e2e
def test_register_as_golden_fails_for_missing_log(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_golden_dataset_schema(client, e2e_settings)

        manager = VectorWaveDatasetManager()
        assert manager.register_as_golden("00000000-0000-0000-0000-000000000999") is False
    finally:
        client.close()


@pytest.mark.e2e
def test_register_as_golden_fails_when_log_has_no_vector(clean_weaviate, e2e_settings):
    """A log inserted without a vector cannot be promoted (capture_return_value missing)."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_golden_dataset_schema(client, e2e_settings)
        exec_col = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)

        log_uuid = str(exec_col.data.insert(
            properties={
                "function_name": "calc",
                "function_uuid": "00000000-0000-0000-0000-000000000001",
                "return_value": "42",
                "status": "SUCCESS",
                "duration_ms": 10,
                "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            },
        ))

        manager = VectorWaveDatasetManager()
        assert manager.register_as_golden(log_uuid) is False
    finally:
        client.close()


@pytest.mark.e2e
def test_recommend_candidates_classifies_steady_and_discovery(clean_weaviate, e2e_settings, monkeypatch):
    """Centroid math: candidates near the golden centroid are STEADY, slightly farther are DISCOVERY,
    far ones are dropped.
    """
    from vectorwave.models.db_config import get_weaviate_settings
    monkeypatch.setenv("RECOMMENDATION_STEADY_MARGIN", "0.1")
    monkeypatch.setenv("RECOMMENDATION_DISCOVERY_MARGIN", "0.2")
    get_weaviate_settings.cache_clear()

    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_golden_dataset_schema(client, e2e_settings)
        exec_col = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        golden_col = client.collections.get(e2e_settings.GOLDEN_COLLECTION_NAME)

        # Single golden point at [1,1] -> centroid=[1,1], avg_dist=0
        golden_col.data.insert(
            properties={
                "original_uuid": "origin-1",
                "function_name": "test_func",
                "return_value": "g",
                "note": "",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "tags": [],
            },
            vector=[1.0, 1.0],
        )

        # Three candidates with controlled distances to the centroid
        # cand_a: dist ≈ 0.071 (within steady_limit=0.1) -> STEADY
        # cand_b: dist ≈ 0.283 (within discovery_limit=0.3, > 0.1) -> DISCOVERY
        # cand_c: dist ≈ 1.414 (> discovery_limit) -> dropped
        _seed_execution(exec_col, function_name="test_func", return_value="A", vector=[1.05, 1.05])
        _seed_execution(exec_col, function_name="test_func", return_value="B", vector=[1.2, 1.2])
        _seed_execution(exec_col, function_name="test_func", return_value="C", vector=[2.0, 2.0])

        manager = VectorWaveDatasetManager()
        recs = manager.recommend_candidates("test_func")

        assert len(recs) == 2
        types = {r["return_value"]: r["type"] for r in recs}
        assert types["A"] == "STEADY"
        assert types["B"] == "DISCOVERY"
        assert "C" not in types
    finally:
        client.close()


@pytest.mark.e2e
def test_recommend_candidates_returns_empty_when_no_golden(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_golden_dataset_schema(client, e2e_settings)

        manager = VectorWaveDatasetManager()
        assert manager.recommend_candidates("nonexistent") == []
    finally:
        client.close()
