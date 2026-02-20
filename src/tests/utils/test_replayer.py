import pytest
import json
import asyncio
import inspect
from unittest.mock import MagicMock, patch
from vectorwave.utils.replayer import VectorWaveReplayer
from vectorwave.models.db_config import WeaviateSettings


# --- Module-level helpers for mock injection tests ---

def _external_service(value):
    raise RuntimeError("Real _external_service must not be called in tests")


def _func_calling_external(x):
    return _external_service(x)


# --- 1. Mock Fixtures (Mock Environment Setup) ---

@pytest.fixture
def mock_replayer_deps(monkeypatch):
    """
    Mocks the DB client and settings used by the Replayer (Default Setup).
    """
    # Settings Mock
    mock_settings = MagicMock()
    mock_settings.EXECUTION_COLLECTION_NAME = "VectorWaveExecutions"
    mock_settings.GOLDEN_COLLECTION_NAME = "VectorWaveGoldenDataset"

    # Weaviate Client & Collection Mock
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection

    # Query Response Mock (Default: empty list)
    mock_query = MagicMock()
    mock_query.fetch_objects.return_value = MagicMock(objects=[])
    mock_collection.query = mock_query

    # Data Operation Mock
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

@pytest.fixture
def mock_replayer_deps_v2(monkeypatch):
    """
    Mock fixture separating Golden and Execution collections for Priority Testing.
    """
    # Settings
    mock_settings = WeaviateSettings(
        EXECUTION_COLLECTION_NAME="Executions",
        GOLDEN_COLLECTION_NAME="GoldenData"
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # Client & Collections
    mock_client = MagicMock()
    mock_exec_col = MagicMock()
    mock_golden_col = MagicMock()

    def get_collection_side_effect(name):
        if name == "Executions": return mock_exec_col
        if name == "GoldenData": return mock_golden_col
        return MagicMock()

    mock_client.collections.get.side_effect = get_collection_side_effect
    mock_get_client = MagicMock(return_value=mock_client)

    monkeypatch.setattr("vectorwave.utils.replayer.get_cached_client", mock_get_client)
    monkeypatch.setattr("vectorwave.utils.replayer.get_weaviate_settings", mock_get_settings)

    return {
        "golden_col": mock_golden_col,
        "exec_col": mock_exec_col
    }

def create_mock_log(uuid_str, inputs, return_value):
    """Mimics a log object retrieved from the database."""
    mock_obj = MagicMock()
    mock_obj.uuid = uuid_str
    props = inputs.copy()
    props["return_value"] = json.dumps(return_value) if not isinstance(return_value, str) else return_value
    props["timestamp_utc"] = "2023-01-01T00:00:00Z"
    mock_obj.properties = props
    return mock_obj

# --- 2. Test Cases ---

def test_replay_success_match(mock_replayer_deps):
    """[Case 1] Successful Pass"""
    replayer = VectorWaveReplayer()
    mock_logs = [create_mock_log("uuid-1", {"a": 1, "b": 2}, 3)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    mock_func = MagicMock(return_value=3)
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "add", mock_func)
        mock_import.return_value = mock_module

        result = replayer.replay("my_module.add", limit=1)

    assert result["passed"] == 1
    assert result["failed"] == 0
    mock_func.assert_called_with(a=1, b=2)

def test_replay_failure_mismatch(mock_replayer_deps):
    """[Case 2] Failure: Regression check"""
    replayer = VectorWaveReplayer()
    mock_logs = [create_mock_log("uuid-2", {"a": 1, "b": 2}, 3)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    mock_func = MagicMock(return_value=99) # Bug
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "add", mock_func)
        mock_import.return_value = mock_module
        result = replayer.replay("my_module.add")

    assert result["passed"] == 0
    assert result["failed"] == 1
    assert result["failures"][0]["expected"] == 3
    assert result["failures"][0]["actual"] == 99

def test_replay_update_baseline(mock_replayer_deps):
    """[Case 3] Update Baseline"""
    replayer = VectorWaveReplayer()
    mock_logs = [create_mock_log("uuid-3", {"msg": "Hi"}, "Old")]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    mock_func = MagicMock(return_value="New")
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('msg', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "greet", mock_func)
        mock_import.return_value = mock_module
        result = replayer.replay("my_module.greet", update_baseline=True)

    assert result["updated"] == 1
    mock_replayer_deps["data"].update.assert_called_once_with(
        uuid="uuid-3",
        properties={"return_value": '"New"'}
    )

def test_replay_argument_filtering(mock_replayer_deps):
    """[Case 4] Argument Filtering"""
    replayer = VectorWaveReplayer()
    inputs = {"a": 10, "team": "billing", "priority": 1}
    mock_logs = [create_mock_log("uuid-4", inputs, 100)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    mock_func = MagicMock(return_value=100)
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "calc", mock_func)
        mock_import.return_value = mock_module
        replayer.replay("my_module.calc")

    mock_func.assert_called_once_with(a=10)

def test_replay_async_function_execution_fixed(mock_replayer_deps):
    """[Case 5] Async Function Test"""
    replayer = VectorWaveReplayer()
    inputs = {"a": 1, "b": 2}
    expected_result = 3
    mock_logs = [create_mock_log("uuid-async-1", inputs, expected_result)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    async def real_async_add(a, b):
        await asyncio.sleep(0.001)
        return a + b

    setattr(real_async_add, '__signature__', inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ]))

    mock_module = MagicMock()
    mock_module.async_add = real_async_add

    with patch("vectorwave.utils.replayer.importlib.import_module", return_value=mock_module):
        result = replayer.replay("my_module.async_add", limit=1)

    assert result["passed"] == 1
    assert result["failed"] == 0

def test_replay_fetches_golden_first(mock_replayer_deps_v2):
    """
    [Case 6] Test if Replayer prioritizes fetching Golden Data
    """
    from vectorwave.utils.replayer import VectorWaveReplayer

    # Arrange
    # 1. Setup one Golden Data entry
    golden_obj = MagicMock()
    golden_obj.uuid = "golden-uuid"
    golden_obj.properties = {"original_uuid": "orig-1", "return_value": "3"}
    mock_replayer_deps_v2["golden_col"].query.fetch_objects.return_value.objects = [golden_obj]

    # Retrieve original log (to get input values)
    orig_log = MagicMock()
    orig_log.properties = {"a": 1, "b": 2}
    mock_replayer_deps_v2["exec_col"].query.fetch_object_by_id.return_value = orig_log

    # 2. Leave Standard Data empty
    mock_replayer_deps_v2["exec_col"].query.fetch_objects.return_value.objects = []

    # Function Mock
    mock_func = MagicMock(return_value=3)
    mock_func.__signature__ = inspect.Signature([
        inspect.Parameter('a', inspect.Parameter.POSITIONAL_OR_KEYWORD),
        inspect.Parameter('b', inspect.Parameter.POSITIONAL_OR_KEYWORD)
    ])

    # Act
    replayer = VectorWaveReplayer()
    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        setattr(mock_module, "add", mock_func)
        mock_import.return_value = mock_module

        result = replayer.replay("mod.add", limit=10)

    # Assert
    assert result["total"] == 1
    # Verify that the Golden Collection was queried
    mock_replayer_deps_v2["golden_col"].query.fetch_objects.assert_called_once()
    # Verify that fetch_object_by_id was called to retrieve the original log
    mock_replayer_deps_v2["exec_col"].query.fetch_object_by_id.assert_called_with("orig-1")


def test_replay_with_mocks_return_value(mock_replayer_deps):
    """[Case 7] mocks return_value: _external_service patched to return 42"""
    replayer = VectorWaveReplayer()
    mock_logs = [create_mock_log("uuid-mock-1", {"x": 10}, 42)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    result = replayer.replay(
        "tests.utils.test_replayer._func_calling_external",
        limit=1,
        mocks={"tests.utils.test_replayer._external_service": {"return_value": 42}}
    )

    assert result["passed"] == 1
    assert result["failed"] == 0


def test_replay_with_mocks_side_effect(mock_replayer_deps):
    """[Case 8] mocks side_effect: _external_service patched with lambda v: v * 2"""
    replayer = VectorWaveReplayer()
    mock_logs = [create_mock_log("uuid-mock-2", {"x": 7}, 14)]
    mock_replayer_deps["query"].fetch_objects.return_value.objects = mock_logs

    result = replayer.replay(
        "tests.utils.test_replayer._func_calling_external",
        limit=1,
        mocks={"tests.utils.test_replayer._external_service": {"side_effect": lambda v: v * 2}}
    )

    assert result["passed"] == 1
    assert result["failed"] == 0