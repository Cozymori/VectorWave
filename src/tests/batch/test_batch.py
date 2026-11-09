"""End-to-end tests for the Weaviate batch manager.

The Rust-backed batch manager exposes only `add_object` to user code. We
exercise its real behaviour by calling `_flush_batch_core` directly (the
same callback the Rust worker thread invokes) and asserting items land in
Weaviate. Connection-failure paths stay mocked because they are tedious to
reproduce against a live container.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from vectorwave.batch.batch import get_batch_manager
from vectorwave.database.db import (
    create_execution_schema,
    create_vectorwave_schema,
    get_weaviate_client,
)
from vectorwave.exception.exceptions import WeaviateConnectionError
from vectorwave.models.db_config import WeaviateSettings


# ---------------------------------------------------------------------------
# Unit tests — singleton behaviour and connection-failure path
# ---------------------------------------------------------------------------

def test_batch_manager_init_marks_uninitialized_when_connect_fails(monkeypatch):
    monkeypatch.setattr(
        "vectorwave.batch.batch.get_weaviate_client",
        MagicMock(side_effect=WeaviateConnectionError("Test connection error")),
    )
    monkeypatch.setattr(
        "vectorwave.batch.batch.get_weaviate_settings",
        MagicMock(return_value=WeaviateSettings()),
    )

    get_batch_manager.cache_clear()
    manager = get_batch_manager()

    assert manager._initialized is False


def test_batch_manager_shutdown_is_idempotent(monkeypatch):
    """Repeated shutdown calls (atexit + explicit) must not double-close the
    Rust manager or the Weaviate client. Without the _shutdown_done guard,
    the second call would re-enter Rust shutdown / client.close()."""
    mock_client = MagicMock()
    mock_client.is_ready.return_value = True

    monkeypatch.setattr(
        "vectorwave.batch.batch.get_weaviate_client",
        MagicMock(return_value=mock_client),
    )
    monkeypatch.setattr(
        "vectorwave.batch.batch.get_weaviate_settings",
        MagicMock(return_value=WeaviateSettings()),
    )

    get_batch_manager.cache_clear()
    manager = get_batch_manager()

    manager.shutdown()
    manager.shutdown()
    manager.shutdown()

    # client.close() is called once during the first shutdown; subsequent
    # shutdowns should short-circuit before reaching it.
    assert mock_client.close.call_count == 1


# ---------------------------------------------------------------------------
# E2E tests
# ---------------------------------------------------------------------------

def _clear_all_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer):
        fn.cache_clear()


@pytest.fixture
def batch_e2e_env(weaviate_container):
    """Reset the batch manager and ensure the executions collection exists."""
    _clear_all_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        for name in (
            settings.COLLECTION_NAME,
            settings.EXECUTION_COLLECTION_NAME,
            settings.GOLDEN_COLLECTION_NAME,
            "VectorWaveTokenUsage",
        ):
            if client.collections.exists(name):
                client.collections.delete(name)
        create_vectorwave_schema(client, settings)
        create_execution_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_all_caches()


def _make_log_props(function_name: str = "fn") -> dict:
    return {
        "function_uuid": str(uuid4()),
        "function_name": function_name,
        "status": "SUCCESS",
        "duration_ms": 1.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }


@pytest.mark.e2e
def test_get_batch_manager_returns_a_ready_singleton(batch_e2e_env):
    m1 = get_batch_manager()
    m2 = get_batch_manager()
    assert m1 is m2
    assert m1._initialized is True


@pytest.mark.e2e
def test_flush_batch_writes_items_to_weaviate(batch_e2e_env):
    """Direct invocation of `_flush_batch_core` (the Rust callback) writes items."""
    settings = batch_e2e_env
    manager = get_batch_manager()

    items = [
        {
            "collection": settings.EXECUTION_COLLECTION_NAME,
            "properties": _make_log_props(function_name="alpha"),
            "uuid": str(uuid4()),
            "vector": None,
        },
        {
            "collection": settings.EXECUTION_COLLECTION_NAME,
            "properties": _make_log_props(function_name="beta"),
            "uuid": str(uuid4()),
            "vector": None,
        },
    ]

    manager._flush_batch_core(items)

    client = get_weaviate_client(settings)
    try:
        coll = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        names = sorted(o.properties["function_name"] for o in coll.iterator())
        assert names == ["alpha", "beta"]
    finally:
        client.close()


@pytest.mark.e2e
def test_flush_batch_reconnects_after_disconnect(batch_e2e_env):
    """If the manager is marked uninitialized, flush should reconnect and still write."""
    settings = batch_e2e_env
    manager = get_batch_manager()

    # Simulate a stale connection state
    manager._initialized = False
    manager.client = None

    items = [
        {
            "collection": settings.EXECUTION_COLLECTION_NAME,
            "properties": _make_log_props(function_name="reconnect"),
            "uuid": str(uuid4()),
            "vector": None,
        }
    ]

    manager._flush_batch_core(items)
    assert manager._initialized is True

    client = get_weaviate_client(settings)
    try:
        coll = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        names = [o.properties["function_name"] for o in coll.iterator()]
        assert "reconnect" in names
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Shutdown must write queued items, not drop them
# ---------------------------------------------------------------------------

def _queue_then_shutdown(count: int, collection: str):
    manager = get_batch_manager()
    for i in range(count):
        manager.add_object(collection, {**_make_log_props(f"fn_{i}")})
    manager.shutdown()


def test_shutdown_flushes_queued_items_in_lite_mode(monkeypatch, tmp_path):
    """Items below the batch threshold and inside the flush interval are only
    written by the shutdown flush. The Rust worker used to drop them on exit."""
    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.setenv("VECTORWAVE_LITE_PATH", str(tmp_path / "lance"))
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("BATCH_THRESHOLD", "100")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "60")
    monkeypatch.setenv("WEAVIATE_HOST", "lite-mode.invalid")
    from vectorwave.store.factory import get_vector_store
    _clear_all_caches()
    get_vector_store.cache_clear()
    try:
        store = get_vector_store()
        store.ensure_collection("VectorWaveExecutions", properties=[])

        _queue_then_shutdown(7, "VectorWaveExecutions")

        assert len(store.query("VectorWaveExecutions", limit=100)) == 7
    finally:
        _clear_all_caches()
        get_vector_store.cache_clear()


@pytest.mark.e2e
def test_shutdown_flushes_queued_items_to_weaviate(batch_e2e_env, monkeypatch):
    monkeypatch.setenv("BATCH_THRESHOLD", "100")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "60")
    _clear_all_caches()
    settings = batch_e2e_env

    _queue_then_shutdown(7, settings.EXECUTION_COLLECTION_NAME)

    client = get_weaviate_client(settings)
    try:
        coll = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        assert len(coll.query.fetch_objects(limit=100).objects) == 7
    finally:
        client.close()


def test_shutdown_returns_quickly_when_weaviate_is_unreachable(monkeypatch):
    """The shutdown flush must not hang process exit when the DB is gone."""
    import time
    monkeypatch.setenv("VECTORWAVE_MODE", "pro")
    monkeypatch.setenv("WEAVIATE_HOST", "127.0.0.1")
    monkeypatch.setenv("WEAVIATE_PORT", "1")
    monkeypatch.setenv("WEAVIATE_GRPC_PORT", "2")
    monkeypatch.setenv("BATCH_THRESHOLD", "100")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "60")
    _clear_all_caches()
    try:
        start = time.time()
        _queue_then_shutdown(5, "VectorWaveExecutions")
        assert time.time() - start < 15
    finally:
        _clear_all_caches()
