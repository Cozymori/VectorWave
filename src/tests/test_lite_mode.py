"""End-to-end coverage for VectorWave Lite mode (LanceDB backend).

These tests bypass the testcontainer Weaviate fixture entirely — Lite mode's
whole point is "no Docker required". Each test gets a fresh LanceDB
directory under tmp_path and verifies the @vectorize → batch → store flow
without ever touching Weaviate.
"""
from __future__ import annotations

import time

import pytest

from vectorwave.core.decorator import vectorize
from vectorwave.search.execution_search import find_executions


def _wait_until_count(query_fn, expected: int, timeout: float = 8.0) -> int:
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        last = len(query_fn())
        if last >= expected:
            return last
        time.sleep(0.1)
    raise AssertionError(f"expected at least {expected} rows, got {last}")


def _clear_caches():
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.store.factory import get_vector_store
    from vectorwave.vectorizer.factory import get_vectorizer
    for fn in (get_batch_manager, get_weaviate_settings, get_vector_store, get_vectorizer):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture
def lite_mode_env(monkeypatch, tmp_path):
    """Configure VectorWave for Lite mode against an isolated LanceDB dir."""
    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.setenv("VECTORWAVE_LITE_PATH", str(tmp_path / "lance"))
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    # Make sure WeaviateSettings doesn't try a real cloud key from .env
    monkeypatch.delenv("WEAVIATE_API_KEY", raising=False)
    monkeypatch.setenv("WEAVIATE_HOST", "lite-mode.invalid")
    _clear_caches()

    from vectorwave.store import get_vector_store
    store = get_vector_store()
    # Sanity: make sure we got the Lite backend, not a stale Pro singleton.
    assert store.backend_name == "lance", f"expected lance, got {store.backend_name}"

    # Pre-create the collection so the first @vectorize decoration succeeds
    # even though no auto-schema exists in Lite mode.
    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    for coll in (settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME):
        if not store.collection_exists(coll):
            store.ensure_collection(coll, properties=[])

    yield settings
    _clear_caches()


def test_lite_mode_logs_call_to_lance(lite_mode_env):
    """A decorated call lands in LanceDB without any Weaviate involvement."""
    settings = lite_mode_env

    @vectorize(search_description="Lite test", sequence_narrative="Lite test")
    def my_lite_fn(x):
        return x * 2

    assert my_lite_fn(21) == 42

    from vectorwave.store import get_vector_store
    store = get_vector_store()

    # Wait for the SUCCESS row to materialise; query the executions collection.
    _wait_until_count(
        lambda: store.query(settings.EXECUTION_COLLECTION_NAME, limit=10),
        expected=1,
    )

    records = store.query(settings.EXECUTION_COLLECTION_NAME, limit=10)
    assert len(records) >= 1
    success_records = [r for r in records if r.properties.get("status") == "SUCCESS"]
    assert len(success_records) == 1
    assert success_records[0].properties["function_name"] == "my_lite_fn"


def test_lite_mode_search_executions_filter(lite_mode_env):
    """`find_executions` works against the Lite backend."""
    settings = lite_mode_env

    @vectorize(search_description="Lite filter", sequence_narrative="Lite filter")
    def filtered_fn(query):
        if query == "fail":
            raise ValueError("oops")
        return "ok"

    assert filtered_fn("ok") == "ok"
    with pytest.raises(ValueError):
        filtered_fn("fail")

    from vectorwave.store import get_vector_store
    store = get_vector_store()
    _wait_until_count(
        lambda: store.query(settings.EXECUTION_COLLECTION_NAME, limit=10),
        expected=2,
    )

    successes = find_executions(filters={"status": "SUCCESS"}, limit=10)
    failures = find_executions(filters={"status": "ERROR"}, limit=10)
    assert any(r["function_name"] == "filtered_fn" for r in successes)
    assert any(r["function_name"] == "filtered_fn" for r in failures)
    # And the filter actually narrows results
    assert all(r["status"] == "SUCCESS" for r in successes)
    assert all(r["status"] == "ERROR" for r in failures)


def test_lite_mode_does_not_touch_weaviate(lite_mode_env, monkeypatch):
    """Sanity: a Lite-mode call must never call into the Weaviate connection helpers."""
    from vectorwave.database import db as db_mod
    calls = []
    real = db_mod.get_weaviate_client

    def _spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real(*args, **kwargs)

    monkeypatch.setattr(db_mod, "get_weaviate_client", _spy)

    @vectorize(search_description="No-weaviate", sequence_narrative="No-weaviate")
    def no_weaviate_fn(n):
        return n

    no_weaviate_fn(1)
    no_weaviate_fn(2)
    # Allow the batch worker thread to drain.
    time.sleep(0.5)
    assert calls == [], f"Lite mode unexpectedly called Weaviate: {calls}"


# ---------------------------------------------------------------------------
# Extended Lite coverage — proves the migrated paths (archiver, dataset,
# replayer, semantic-cache search helpers) work end-to-end without Weaviate.
# ---------------------------------------------------------------------------

import json
from datetime import datetime, timezone
from uuid import uuid4

from vectorwave.database.archiver import VectorWaveArchiver
from vectorwave.database.dataset import VectorWaveDatasetManager
from vectorwave.database.db_search import (
    search_similar_execution,
    check_semantic_drift,
)
from vectorwave.utils.replayer import VectorWaveReplayer


def _seed_row(store, settings, *, function_name, return_value, vector=None, status="SUCCESS"):
    return store.insert(
        collection=settings.EXECUTION_COLLECTION_NAME,
        properties={
            "function_uuid": str(uuid4()),
            "function_name": function_name,
            "status": status,
            "duration_ms": 1.0,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "return_value": json.dumps(return_value) if not isinstance(return_value, str) else return_value,
        },
        vector=vector,
    )


def test_lite_mode_archiver_export_and_clear(lite_mode_env, tmp_path):
    """archiver.export_and_clear writes JSONL and removes rows in Lite mode."""
    settings = lite_mode_env
    from vectorwave.store import get_vector_store
    store = get_vector_store()

    for v in ["a", "b"]:
        _seed_row(store, settings, function_name="lite_arch", return_value=v)

    out = tmp_path / "out" / "data.jsonl"
    archiver = VectorWaveArchiver()
    result = archiver.export_and_clear(
        function_name="lite_arch",
        output_file=str(out),
        clear_after_export=True,
    )
    assert result["exported"] == 2
    assert result["deleted"] == 2
    assert out.exists()


def test_lite_mode_dataset_register_as_golden(lite_mode_env):
    """register_as_golden copies a SUCCESS log + its vector into the golden collection."""
    settings = lite_mode_env
    from vectorwave.store import get_vector_store
    store = get_vector_store()
    store.ensure_collection(settings.GOLDEN_COLLECTION_NAME, properties=[])

    uuid_str = _seed_row(
        store, settings, function_name="lite_golden",
        return_value="r1", vector=[0.1] * 384,
    )

    manager = VectorWaveDatasetManager()
    assert manager.register_as_golden(uuid_str, note="lite test") is True

    golden_rows = store.query(settings.GOLDEN_COLLECTION_NAME, limit=10)
    assert len(golden_rows) == 1
    assert golden_rows[0].properties.get("original_uuid") == uuid_str


def test_lite_mode_search_similar_execution(lite_mode_env):
    """search_similar_execution finds the nearest SUCCESS row for a function."""
    settings = lite_mode_env
    from vectorwave.store import get_vector_store
    store = get_vector_store()

    # `return_value=` is passed as already-stringified, so _seed_row stores "hit"
    # verbatim (not JSON-encoded). That's the on-disk shape we read back.
    _seed_row(store, settings, function_name="lite_cache", return_value="hit", vector=[1.0] * 384)

    result = search_similar_execution(
        query_vector=[1.0] * 384,
        function_name="lite_cache",
        threshold=0.5,
        limit=1,
    )
    assert result is not None
    assert result["return_value"] == "hit"


def test_lite_mode_check_semantic_drift_returns_safely(lite_mode_env):
    """drift detection: returns False/None safely when no matching rows exist
    and runs through the store cleanly when rows are present."""
    settings = lite_mode_env
    from vectorwave.store import get_vector_store
    store = get_vector_store()

    is_drift, avg_dist, nearest = check_semantic_drift(
        vector=[0.0] * 384,
        function_name="nonexistent_fn",
        threshold=0.5,
        k=5,
    )
    assert is_drift is False
    assert avg_dist == 0.0
    assert nearest is None

    _seed_row(store, settings, function_name="lite_drift", return_value="ok", vector=[1.0] * 384)
    is_drift2, avg_dist2, nearest2 = check_semantic_drift(
        vector=[1.0] * 384,
        function_name="lite_drift",
        threshold=10.0,
        k=5,
    )
    assert is_drift2 is False
    assert nearest2 is not None
