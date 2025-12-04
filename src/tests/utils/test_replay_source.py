import pytest
import sys
from unittest.mock import MagicMock, patch
from vectorwave import vectorize, search_executions
from vectorwave.utils.replayer import VectorWaveReplayer
from vectorwave.models.db_config import WeaviateSettings

# 1. Dummy function for testing
@vectorize(unique_id="test_replay_tagging")
def calc_add(a: int, b: int):
    """Simple addition function for testing replay tags."""
    return a + b

@pytest.fixture
def mock_replay_deps(monkeypatch):
    """Mocks dependencies for Replay Source testing."""
    # Mock Settings
    mock_settings = WeaviateSettings(
        EXECUTION_COLLECTION_NAME="VectorWaveExecutions",
        GOLDEN_COLLECTION_NAME="VectorWaveGoldenDataset"
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # Mock Batch Manager (to verify logs)
    mock_batch = MagicMock()
    mock_batch.add_object = MagicMock()
    mock_get_batch = MagicMock(return_value=mock_batch)

    # Mock DB Client (for Replayer initialization)
    mock_client = MagicMock()
    mock_collection = MagicMock()
    mock_client.collections.get.return_value = mock_collection
    mock_get_client = MagicMock(return_value=mock_client)

    # --- [수정] Patch dependencies where they are USED, not just defined ---

    # 1. Patch for Replayer (vectorwave/utils/replayer.py)
    monkeypatch.setattr("vectorwave.utils.replayer.get_cached_client", mock_get_client)
    monkeypatch.setattr("vectorwave.utils.replayer.get_weaviate_settings", mock_get_settings)

    # 2. Patch for Database (vectorwave/database/db.py) - 안전장치
    monkeypatch.setattr("vectorwave.database.db.get_cached_client", mock_get_client)

    # 3. Patch for Config (vectorwave/models/db_config.py) - 안전장치
    monkeypatch.setattr("vectorwave.models.db_config.get_weaviate_settings", mock_get_settings)

    # 4. Patch batch manager for Decorator & Tracer
    monkeypatch.setattr("vectorwave.core.decorator.get_batch_manager", mock_get_batch)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_batch_manager", mock_get_batch)

    # 5. Patch search_executions in THIS test file
    mock_search = MagicMock()
    monkeypatch.setattr(sys.modules[__name__], "search_executions", mock_search)

    return {
        "batch": mock_batch,
        "search": mock_search,
        "client": mock_client
    }

def test_execution_source_tagging(mock_replay_deps):
    """
    Verification Scenario:
    1. Realtime Execution -> Check if saved to DB as 'REALTIME'
    2. Replay Execution -> Check if saved to DB as 'REPLAY'
    """
    mock_batch = mock_replay_deps["batch"]

    # ---------------------------------------------------------
    # Case 1: Realtime Execution
    # ---------------------------------------------------------
    print("\n🚀 1. Running Realtime Execution...")
    calc_add(10, 20)

    # Check if log was sent to BatchManager
    assert mock_batch.add_object.called

    # Get the latest call args
    args, kwargs = mock_batch.add_object.call_args
    props = kwargs['properties']

    print(f"   [Log Check] Source: {props.get('exec_source')}")

    # Verify exec_source is 'REALTIME'
    assert props.get("exec_source") == "REALTIME"
    assert props.get("status") == "SUCCESS"

    # ---------------------------------------------------------
    # Case 2: Replay Execution
    # ---------------------------------------------------------
    print("\n🔄 2. Running Replay...")

    # Prepare dummy log for Replayer to fetch
    dummy_log_obj = MagicMock()
    dummy_log_obj.uuid = "old-uuid-123"
    dummy_log_obj.properties = {
        "function_name": "calc_add",
        "a": 10,
        "b": 20,
        "return_value": "30",
        "exec_source": "REALTIME"
    }

    # Replayer uses client.collections.get().query.fetch_objects()
    mock_collection = mock_replay_deps["client"].collections.get.return_value
    mock_collection.query.fetch_objects.return_value.objects = [dummy_log_obj]

    # Also need to mock import_module since Replayer imports the function dynamically
    with patch("vectorwave.utils.replayer.importlib.import_module") as mock_import:
        mock_module = MagicMock()
        # Set the function on the mock module
        setattr(mock_module, "calc_add", calc_add)
        mock_import.return_value = mock_module

        replayer = VectorWaveReplayer()

        # Reset batch mock to capture only replay log
        mock_batch.add_object.reset_mock()

        # Execute Replay
        result = replayer.replay(
            function_full_name="src.tests.utils.test_replay_source.calc_add",
            limit=1
        )

    assert result['total'] > 0, "Replay should have executed at least 1 test."

    # Check if new log was sent to BatchManager
    assert mock_batch.add_object.called

    # Get the latest call args (from Replay execution)
    args, kwargs = mock_batch.add_object.call_args
    replay_props = kwargs['properties']

    print(f"   [Log Check] Source: {replay_props.get('exec_source')}")

    # Verify exec_source is 'REPLAY'
    assert replay_props.get("exec_source") == "REPLAY"

    print("\n✅ All tests passed! Execution Source tagging is working.")