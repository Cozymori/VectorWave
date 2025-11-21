import pytest
import json
from unittest.mock import MagicMock, patch
from vectorwave.utils.replayer import VectorWaveReplayer

# --- 1. Mock Fixtures (Mock Environment Setup) ---

@pytest.fixture
def mock_replayer_deps(monkeypatch):
    """
    Mocks the DB client and settings used by the Replayer.
    """
    # Settings Mock
    mock_settings = MagicMock()
    mock_settings.EXECUTION_COLLECTION_NAME = "VectorWaveExecutions"

    # Weaviate Client & Collection Mock
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Query Response Mock (Default: empty list)
    mock_query = MagicMock()
    mock_query.fetch_objects.return_value = MagicMock(objects=[])
    mock_collection.query = mock_query

    # Data Operation Mock (Update, etc.)
    mock_data = MagicMock()
    mock_collection.data = mock_data

    # Apply Patches
    monkeypatch.setattr("vectorwave.utils.replayer.get_cached_client", MagicMock(return_value=mock_client))
    monkeypatch.setattr("vectorwave.utils.replayer.get_weaviate_settings", MagicMock(return_value=mock_settings))

    return {
        "collection": mock_collection,
        "query": mock_query,
        "data": mock_data
    }

def create_mock_log(uuid_str, inputs, return_value):
    """Mimics a log object retrieved from the database."""
    mock_obj = MagicMock()
    mock_obj.uuid = uuid_str

    # Combine inputs and return_value into properties
    props = inputs.copy()
    props["return_value"] = json.dumps(return_value) if not isinstance(return_value, str) else return_value
    props["timestamp_utc"] = "2023-01-01T00:00:00Z"

    mock_obj.properties = props
    return mock_obj

# --- 2. Test Cases ---

def test_replay_success_match(mock_replayer_deps):
    """
    [Case 1] Successful Pass: Checks if the DB value matches the current function execution result.
    """
    # Arrange
    replayer = VectorWaveReplayer()

    # 1. DB Mock Data (Input: a=1, b=2 -> Expected: 3)
    mock_logs = [create_mock_log("uuid-1", {"a": 1, "b": 2}, 3)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    # 2. Target Function Mock (Dynamic Import Mocking)
    # If 'my_module.add' is called, this lambda function is executed
    mock_func = MagicMock(return_value=3) # Actual result is also 3

    # Set signature for replayer's inspect.signature check
    # (Mock objects normally lack signatures, so we overwrite it)
    import inspect
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "add", mock_func)
        mock_import.return_value = mock_module

        # Act
        result = replayer.replay("my_module.add", limit=1)

    # Assert
    assert result["passed"] == 1
    assert result["failed"] == 0
    mock_func.assert_called_with(a=1, b=2) # Verify call with correct arguments


def test_replay_failure_mismatch(mock_replayer_deps):
    """
    [Case 2] Failure: Checks for mismatch when the result value is different (Regression).
    """
    # Arrange
    replayer = VectorWaveReplayer()

    # DB: 1 + 2 = 3
    mock_logs = [create_mock_log("uuid-2", {"a": 1, "b": 2}, 3)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    # Func: 1 + 2 = 99 (Bug!)
    mock_func = MagicMock(return_value=99)

    import inspect
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "add", mock_func)
        mock_import.return_value = mock_module

        # Act
        result = replayer.replay("my_module.add")

    # Assert
    assert result["passed"] == 0
    assert result["failed"] == 1
    assert len(result["failures"]) == 1
    assert result["failures"][0]["expected"] == 3
    assert result["failures"][0]["actual"] == 99


def test_replay_update_baseline(mock_replayer_deps):
    """
    [Case 3] Update: Checks updating the baseline when the result is different but update_baseline=True.
    """
    # Arrange
    replayer = VectorWaveReplayer()

    # DB: Old value 'Old'
    mock_logs = [create_mock_log("uuid-3", {"msg": "Hi"}, "Old")]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    # Func: New value 'New'
    mock_func = MagicMock(return_value="New")

    import inspect
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('msg', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "greet", mock_func)
        mock_import.return_value = mock_module

        # Act
        # Set update_baseline=True
        result = replayer.replay("my_module.greet", update_baseline=True)

    # Assert
    assert result["updated"] == 1
    # Verify the DB update function was called
    mock_replayer_deps["data"].update.assert_called_once_with(
        uuid="uuid-3",
        properties={"return_value": '"New"'} # JSON serialized string
    )


def test_replay_argument_filtering(mock_replayer_deps):
    """
    [Case 4] Argument Filtering: Checks that unnecessary metadata (like user_id) not in the function signature is removed.
    """
    # Arrange
    replayer = VectorWaveReplayer()

    # DB contains extraneous data like 'team', 'timestamp', etc.
    inputs = {"a": 10, "team": "billing", "priority": 1}
    mock_logs = [create_mock_log("uuid-4", inputs, 100)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    # Function only accepts 'a' as an argument
    mock_func = MagicMock(return_value=100)

    import inspect
    # Only 'a' is defined in the signature
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "calc", mock_func)
        mock_import.return_value = mock_module

        # Act
        replayer.replay("my_module.calc")

    # Assert
    # Should only be called with 'a=10', excluding 'team' and 'priority'
    mock_func.assert_called_once_with(a=10)