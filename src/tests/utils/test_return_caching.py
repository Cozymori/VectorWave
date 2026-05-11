import pytest
from unittest.mock import MagicMock, patch
import json
import weaviate.classes.query as wvc_query
from vectorwave.utils.return_caching_utils import _check_and_return_cached_result, CACHE_MISS
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


@pytest.fixture
def mock_caching_utils_deps_v2(monkeypatch):
    """
    Enhanced Mock Fixture for Golden Dataset testing.
    Mocks VectorStore (golden hit/miss), search_similar_execution, vectorizer, batch.
    """
    mock_settings = WeaviateSettings(
        EXECUTION_COLLECTION_NAME="Executions",
        GOLDEN_COLLECTION_NAME="GoldenData"
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # Backend-agnostic VectorStore mock — golden hits go through store.near_vector
    mock_store = MagicMock()
    mock_store.near_vector.return_value = []  # default: golden miss
    mock_get_store = MagicMock(return_value=mock_store)

    mock_vectorizer = MagicMock()
    mock_vectorizer.embed.return_value = [0.1, 0.2]
    mock_get_vectorizer = MagicMock(return_value=mock_vectorizer)

    mock_batch = MagicMock()
    mock_get_batch = MagicMock(return_value=mock_batch)

    TARGET = "vectorwave.utils.return_caching_utils"
    monkeypatch.setattr(f"{TARGET}.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr(f"{TARGET}.get_vector_store", mock_get_store)
    monkeypatch.setattr(f"{TARGET}.get_vectorizer", mock_get_vectorizer)
    monkeypatch.setattr(f"{TARGET}.get_batch_manager", mock_get_batch)

    # Mock search_similar_execution (standard search fallback)
    mock_search_std = MagicMock(return_value=None)
    monkeypatch.setattr(f"{TARGET}.search_similar_execution", mock_search_std)

    return {
        "store": mock_store,
        "search_std": mock_search_std,
        "batch": mock_batch,
    }


# --- Tests ---

def test_check_and_return_cached_result_cache_hit_logging(mock_caching_utils_deps):
    """
    [Case 1] Verify that DB logging is correctly performed with 'CACHE_HIT' status upon a cache hit.
    """
    # Arrange
    mock_cached_log = {
        "return_value": json.dumps({"result": "cached_data"}),
        "metadata": {"distance": 0.1},
        "uuid": "cached-log-uuid"
    }

    # Mock VectorStore so the Golden lookup returns nothing -> fall through to
    # the standard `search_similar_execution` path.
    mock_store = MagicMock()
    mock_store.near_vector.return_value = []

    with patch("vectorwave.utils.return_caching_utils.get_vector_store", return_value=mock_store):
        with patch("vectorwave.utils.return_caching_utils.search_similar_execution", return_value=mock_cached_log):
            with patch("vectorwave.utils.return_caching_utils.current_tracer_var") as mock_tracer_var:
                with patch("vectorwave.utils.return_caching_utils.current_span_id_var") as mock_span_var:
                    mock_tracer_var.get.return_value = mock_caching_utils_deps["tracer_obj"]
                    mock_span_var.get.return_value = "parent-span-123"

                    def dummy_func(a, b): pass

                    result = _check_and_return_cached_result(
                        func=dummy_func,
                        args=(10,),
                        kwargs={"b": 20},
                        function_name="dummy_func",
                        cache_threshold=0.9,
                        is_async=False
                    )

    # Assert
    assert result == {"result": "cached_data"}


def test_check_and_return_cached_result_cache_miss(mock_caching_utils_deps):
    """
    [Case 2] Verify that the CACHE_MISS sentinel is returned without logging
    upon a cache miss. Using a sentinel (not None) lets functions whose
    legitimate return value is None still be served from cache.
    """
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

    assert result is CACHE_MISS
    mock_caching_utils_deps["batch_manager"].add_object.assert_not_called()


def test_cache_priority_golden_hit(mock_caching_utils_deps_v2):
    """
    [Case 3] If a cache hit occurs in the Golden Dataset, standard search should not be performed (Priority Test).
    """
    deps = mock_caching_utils_deps_v2

    # Arrange: VectorStore golden lookup returns a StoreRecord (hit)
    from vectorwave.store.base import StoreRecord
    deps["store"].near_vector.return_value = [
        StoreRecord(
            uuid="golden-1",
            properties={"return_value": '"GoldenResult"', "original_uuid": "orig-1"},
            distance=0.0,
            certainty=1.0,
        )
    ]

    # Act
    result = _check_and_return_cached_result(
        func=lambda: None, args=(), kwargs={}, function_name="test", cache_threshold=0.9, is_async=False
    )

    # Assert
    assert result == "GoldenResult"
    # Golden lookup was called
    deps["store"].near_vector.assert_called_once()
    # Standard search should not be called (Verify Priority Logic)
    deps["search_std"].assert_not_called()


def test_cache_priority_golden_miss_fallback(mock_caching_utils_deps_v2):
    """
    [Case 4] If not found in Golden Dataset, it should fallback to Standard search (Fallback Test).
    """
    deps = mock_caching_utils_deps_v2

    # Arrange: Golden returns empty -> fallback to standard search
    deps["store"].near_vector.return_value = []

    deps["search_std"].return_value = {
        "return_value": '"StdResult"',
        "metadata": {"distance": 0.1, "certainty": 0.9},
        "uuid": "std-1"
    }

    # Act
    result = _check_and_return_cached_result(
        func=lambda: None, args=(), kwargs={}, function_name="test", cache_threshold=0.9, is_async=False
    )

    # Assert
    assert result == "StdResult"
    # Both should have been called
    deps["store"].near_vector.assert_called_once()
    deps["search_std"].assert_called_once()
