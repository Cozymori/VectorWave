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
