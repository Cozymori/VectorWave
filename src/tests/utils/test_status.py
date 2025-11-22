import pytest
from unittest.mock import MagicMock, patch
from vectorwave.utils.status import get_db_status, get_registered_functions
from vectorwave.models.db_config import WeaviateSettings

# --- Fixtures ---

@pytest.fixture
def mock_status_deps(monkeypatch):
    """
    Mocks dependencies of status.py (get_cached_client, get_weaviate_settings).
    """
    # 1. Mock Weaviate Client
    mock_client = MagicMock()
    mock_get_client = MagicMock(return_value=mock_client)

    # 2. Mock Settings
    mock_settings = WeaviateSettings(COLLECTION_NAME="TestFunctions")
    mock_get_settings = MagicMock(return_value=mock_settings)

    # 3. Apply Patches
    monkeypatch.setattr("vectorwave.utils.status.get_cached_client", mock_get_client)
    monkeypatch.setattr("vectorwave.utils.status.get_weaviate_settings", mock_get_settings)

    return {
        "client": mock_client,
        "settings": mock_settings,
        "get_client": mock_get_client
    }

# --- Tests for get_db_status ---

def test_get_db_status_ready(mock_status_deps):
    """
    Case 1: Test if True is returned when DB is connected and ready.
    """
    mock_status_deps["client"].is_ready.return_value = True

    assert get_db_status() is True
    mock_status_deps["client"].is_ready.assert_called_once()

def test_get_db_status_not_ready(mock_status_deps):
    """
    Case 2: Test if False is returned when DB is connected but not ready.
    """
    mock_status_deps["client"].is_ready.return_value = False

    assert get_db_status() is False

def test_get_db_status_exception(mock_status_deps):
    """
    Case 3: Test if False is returned and error is logged when an exception occurs during DB connection.
    """
    # Set exception to occur on client call
    mock_status_deps["get_client"].side_effect = Exception("Connection Error")

    assert get_db_status() is False

# --- Tests for get_registered_functions ---

@patch("vectorwave.utils.status.get_db_status")
def test_get_registered_functions_db_offline(mock_get_db_status, mock_status_deps):
    """
    Case 4: Test if an empty list is returned when DB is offline (False).
    """
    mock_get_db_status.return_value = False

    result = get_registered_functions()

    assert result == []
    # Should not access client or collection if DB is offline
    mock_status_deps["client"].collections.get.assert_not_called()

@patch("vectorwave.utils.status.get_db_status")
def test_get_registered_functions_success(mock_get_db_status, mock_status_deps):
    """
    Case 5: Test if the list of registered functions is returned correctly.
    """
    # 1. Arrange
    mock_get_db_status.return_value = True

    # Mock collection and query results
    mock_collection = MagicMock()
    mock_status_deps["client"].collections.get.return_value = mock_collection

    # Create fake Weaviate objects
    obj1 = MagicMock()
    obj1.properties = {"function_name": "func_a", "module_name": "mod_a"}
    obj2 = MagicMock()
    obj2.properties = {"function_name": "func_b", "module_name": "mod_b"}

    # Mock result of fetch_objects call
    mock_collection.iterator.return_value = [obj1, obj2]

    # 2. Act
    result = get_registered_functions()

    # 3. Assert
    assert len(result) == 2
    assert result[0]["function_name"] == "func_a"
    assert result[1]["function_name"] == "func_b"

    # Verify correct name usage when retrieving collection
    mock_status_deps["client"].collections.get.assert_called_with("TestFunctions")

    mock_collection.iterator.assert_called_once()
    call_kwargs = mock_collection.iterator.call_args.kwargs
    assert "return_properties" in call_kwargs
    assert "function_name" in call_kwargs["return_properties"]

@patch("vectorwave.utils.status.get_db_status")
def test_get_registered_functions_exception(mock_get_db_status, mock_status_deps):
    """
    Case 6: Test if an empty list is returned and error is handled when an exception occurs during retrieval.
    """
    # 1. Arrange
    mock_get_db_status.return_value = True

    # Induce exception during collection retrieval
    mock_status_deps["client"].collections.get.side_effect = Exception("Query Failed")

    # 2. Act
    result = get_registered_functions()

    # 3. Assert
    assert result == []