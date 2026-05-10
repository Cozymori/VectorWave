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
