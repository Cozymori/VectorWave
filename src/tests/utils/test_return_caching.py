import pytest
from unittest.mock import MagicMock, patch
import json
from vectorwave.utils.return_caching_utils import _check_and_return_cached_result
from vectorwave.models.db_config import WeaviateSettings


# --- Mock Fixtures ---

@pytest.fixture
def mock_caching_utils_deps(monkeypatch):
    """
    Mocks external dependencies of return_caching_utils.py (BatchManager, Tracer, DB search, etc.).
    """
    # 1. Mock Settings
    mock_settings = WeaviateSettings(
        EXECUTION_COLLECTION_NAME="TestExecutions",
        global_custom_values={"run_id": "test-run-123"}
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # 2. Mock Batch Manager (Key verification target)
    mock_batch_manager = MagicMock()
    mock_batch_manager.add_object = MagicMock()
    mock_get_batch = MagicMock(return_value=mock_batch_manager)

    # 3. Mock Vectorizer
    mock_vectorizer = MagicMock()
    mock_vectorizer.embed.return_value = [0.1, 0.2, 0.3]  # Dummy Vector
    mock_get_vectorizer = MagicMock(return_value=mock_vectorizer)

    # 4. Mock Tracer Context (Provides current Trace ID)
    mock_tracer = MagicMock()
    mock_tracer.trace_id = "existing-trace-id-abc"

    # 5. Apply Monkeypatches
    TARGET_MODULE = "vectorwave.utils.return_caching_utils"

    monkeypatch.setattr(f"{TARGET_MODULE}.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr(f"{TARGET_MODULE}.get_batch_manager", mock_get_batch)
    monkeypatch.setattr(f"{TARGET_MODULE}.get_vectorizer", mock_get_vectorizer)

    return {
        "batch_manager": mock_batch_manager,
        "vectorizer": mock_vectorizer,
        "tracer_obj": mock_tracer
    }


def test_check_and_return_cached_result_cache_hit_logging(mock_caching_utils_deps):
    """
    [Case 1] Verify that DB logging is correctly performed with 'CACHE_HIT' status upon a cache hit.
    """
    # Arrange
    # 1. Mock cache search result (Log found in DB)
    mock_cached_log = {
        "return_value": json.dumps({"result": "cached_data"}),
        "metadata": {"distance": 0.1},
        "uuid": "cached-log-uuid"
    }

    # 2. Mock search_similar_execution to return the above result (Simulate Cache Hit)
    with patch("vectorwave.utils.return_caching_utils.search_similar_execution", return_value=mock_cached_log):
        # [Fix] Instead of directly patching the .get method of the ContextVar object, replace the variable itself with a Mock object.
        with patch("vectorwave.utils.return_caching_utils.current_tracer_var") as mock_tracer_var:
            with patch("vectorwave.utils.return_caching_utils.current_span_id_var") as mock_span_var:
                # Set return value for .get() call on the Mock object
                mock_tracer_var.get.return_value = mock_caching_utils_deps["tracer_obj"]
                mock_span_var.get.return_value = "parent-span-123"

                # 4. Execute the target function
                def dummy_func(a, b): pass  # Target function

                result = _check_and_return_cached_result(
                    func=dummy_func,
                    args=(10,),
                    kwargs={"b": 20},
                    function_name="dummy_func",
                    cache_threshold=0.9,
                    is_async=False
                )

    # Assert
    # 1. Was the cached result returned correctly?
    assert result == {"result": "cached_data"}

    # 2. [Core] Was BatchManager.add_object called? (Check if logging occurred)
    mock_batch = mock_caching_utils_deps["batch_manager"]
    mock_batch.add_object.assert_called_once()

    # 3. [Core] Verify properties of the saved log
    call_kwargs = mock_batch.add_object.call_args.kwargs
    props = call_kwargs["properties"]

    assert props["status"] == "CACHE_HIT"  # Check if status is CACHE_HIT
    assert props["duration_ms"] == 0.0  # Check if duration is 0
    assert props["trace_id"] == "existing-trace-id-abc"  # Check if existing Trace ID is maintained
    assert props["parent_span_id"] == "parent-span-123"  # Check for parent Span ID maintenance
    assert props["function_name"] == "dummy_func"
    assert props["run_id"] == "test-run-123"  # Check for inclusion of global tags


def test_check_and_return_cached_result_cache_miss(mock_caching_utils_deps):
    """
    [Case 2] Verify that None is returned without logging upon a cache miss.
    """
    # Arrange
    # Set search_similar_execution to return None (Cache Miss)
    with patch("vectorwave.utils.return_caching_utils.search_similar_execution", return_value=None):
        def dummy_func(): pass

        result = _check_and_return_cached_result(
            func=dummy_func,
            args=(),
            kwargs={},
            function_name="dummy_func",
            cache_threshold=0.9,
            is_async=False
        )

    # Assert
    # 1. Result should be None (To proceed to actual function execution)
    assert result is None

    # 2. BatchManager.add_object should not be called
    mock_caching_utils_deps["batch_manager"].add_object.assert_not_called()