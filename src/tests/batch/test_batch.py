from unittest.mock import MagicMock, call, ANY

import pytest
from vectorwave.batch.batch import get_batch_manager
from vectorwave.exception.exceptions import WeaviateConnectionError
from vectorwave.models.db_config import WeaviateSettings


@pytest.fixture
def mock_deps(monkeypatch):
    """
    Fixture to mock dependencies for batch.py (db, config, atexit)
    """
    # Mock WeaviateClient
    mock_client = MagicMock()
    mock_client.batch = MagicMock()
    mock_client.batch.configure = MagicMock()
    mock_client.batch.add_object = MagicMock()
    mock_client.batch.flush = MagicMock()

    mock_collection_data = MagicMock()
    mock_collection = MagicMock()
    mock_collection.data = mock_collection_data
    mock_client.collections.get = MagicMock(return_value=mock_collection)

    mock_batch_context = MagicMock()
    mock_client.batch.dynamic.return_value.__enter__.return_value = mock_batch_context

    # Mock get_weaviate_client
    mock_get_client = MagicMock(return_value=mock_client)
    monkeypatch.setattr("vectorwave.batch.batch.get_weaviate_client", mock_get_client)

    # Mock get_weaviate_settings
    mock_settings = WeaviateSettings()
    mock_get_settings = MagicMock(return_value=mock_settings)
    monkeypatch.setattr("vectorwave.batch.batch.get_weaviate_settings", mock_get_settings)

    # Mock atexit.register
    mock_atexit_register = MagicMock()
    monkeypatch.setattr("atexit.register", mock_atexit_register)

    mock_thread = MagicMock()
    monkeypatch.setattr("threading.Thread", mock_thread)

    # Clear lru_cache
    get_batch_manager.cache_clear()

    return {
        "get_client": mock_get_client,
        "get_settings": mock_get_settings,
        "client": mock_client,
        "settings": mock_settings,
        "atexit": mock_atexit_register,
        "batch_context": mock_batch_context
    }

def test_get_batch_manager_is_singleton(mock_deps):
    """
    Case 1: Test if get_batch_manager() always returns the same instance (singleton)
    """
    manager1 = get_batch_manager()
    manager2 = get_batch_manager()
    assert manager1 is manager2

def test_batch_manager_initialization(mock_deps):
    """
    Case 2: Test if BatchManager correctly calls dependencies (configure, atexit) upon initialization
    """
    manager = get_batch_manager()

    mock_deps["get_settings"].assert_called_once()
    mock_deps["get_client"].assert_called_once_with(mock_deps["settings"])

    assert manager._initialized is True

def test_batch_manager_init_failure(monkeypatch):
    """
    Case 3: Test if _initialized remains False when DB connection (get_weaviate_client) fails
    """
    # Mock get_weaviate_client to raise an exception
    mock_get_client_fail = MagicMock(side_effect=WeaviateConnectionError("Test connection error"))
    monkeypatch.setattr("vectorwave.batch.batch.get_weaviate_client", mock_get_client_fail)

    get_batch_manager.cache_clear()
    manager = get_batch_manager()

    # The _initialized flag should be False if initialization fails
    assert manager._initialized is False

def test_add_object_enqueues_item(mock_deps):
    """
    [Updated] Case 4: Test if add_object() puts the item into the local queue (Non-blocking)
    Instead of calling client directly.
    """
    manager = get_batch_manager()
    props = {"key": "value"}

    assert manager.queue.empty()

    manager.add_object(collection="TestCollection", properties=props, uuid="test-uuid", vector=[0.1])

    # 1. Should NOT call DB directly
    mock_deps["client"].collections.get.assert_not_called()

    # 2. Should be in the Queue
    assert manager.queue.qsize() == 1
    item = manager.queue.get()

    assert item["collection"] == "TestCollection"
    assert item["properties"] == props
    assert item["uuid"] == "test-uuid"
    assert item["vector"] == [0.1]

def test_flush_batch_sends_to_weaviate(mock_deps):
    """
    [New] Case 5: Test if _flush_batch sends items using client.batch.dynamic context
    """
    manager = get_batch_manager()

    items = [
        {"collection": "C1", "properties": {"p": 1}, "uuid": "u1", "vector": None},
        {"collection": "C2", "properties": {"p": 2}, "uuid": "u2", "vector": [1.0]}
    ]

    # Manually trigger flush
    manager._flush_batch_core(items)

    # Check if dynamic batch context was entered
    mock_deps["client"].batch.dynamic.assert_called_once()

    # Check if batch.add_object was called for each item
    mock_batch_ctx = mock_deps["batch_context"]
    assert mock_batch_ctx.add_object.call_count == 2

    mock_batch_ctx.add_object.assert_has_calls([
        call(collection="C1", properties={"p": 1}, uuid="u1", vector=None),
        call(collection="C2", properties={"p": 2}, uuid="u2", vector=[1.0])
    ])

def test_flush_batch_reconnects_if_disconnected(mock_deps):
    """
    [New] Case 6: Test reconnection logic when client is not initialized
    """
    manager = get_batch_manager()

    # Simulate disconnection
    manager._initialized = False
    manager.client = None

    # Reset mocks
    mock_deps["get_client"].reset_mock()

    items = [{"collection": "C1", "properties": {}, "uuid": "u1", "vector": None}]

    # Trigger flush
    manager._flush_batch_core(items)

    # Should try to reconnect
    mock_deps["get_client"].assert_called_once()
    # And then send
    mock_deps["client"].batch.dynamic.assert_called_once()