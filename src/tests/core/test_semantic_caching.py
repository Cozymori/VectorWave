import pytest
from unittest.mock import patch, MagicMock, call
import asyncio
import json

# Import the core components to test
from vectorwave.core.decorator import vectorize
from vectorwave.models.db_config import WeaviateSettings
# [MODIFIED: Reflecting the correct path]
from vectorwave.utils.return_caching_utils import _check_and_return_cached_result

# Import all real functions needed for mocking the cache
from vectorwave.batch.batch import get_batch_manager as real_get_batch_manager
from vectorwave.database.db import get_cached_client as real_get_cached_client
from vectorwave.models.db_config import get_weaviate_settings as real_get_settings
from vectorwave.vectorizer.factory import get_vectorizer as real_get_vectorizer

# --- Fixture Setup ---

@pytest.fixture
def mock_caching_deps(monkeypatch):
    """
    Mocks core dependencies for semantic caching testing.
    Mocking path: vectorwave.utils.return_caching_utils
    """
    # 1. Mock BatchManager (Used to verify log saving)
    mock_batch_manager = MagicMock()
    mock_batch_manager.add_object = MagicMock()
    mock_get_batch_manager = MagicMock(return_value=mock_batch_manager)

    # 2. Mock Vectorizer (Provides dummy vector)
    mock_vectorizer = MagicMock()
    mock_vectorizer.embed.return_value = [0.1, 0.2, 0.3] # Dummy vector
    mock_get_vectorizer = MagicMock(return_value=mock_vectorizer)

    # 3. Mock Settings
    mock_settings = WeaviateSettings(
        COLLECTION_NAME="TestFunctions",
        EXECUTION_COLLECTION_NAME="TestExecutions",
        global_custom_values={"run_id": "test-run-cache"},
        sensitive_keys={"secret_key"} # For sensitive data masking test
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # 4. Mock Weaviate Client (For BatchManager initialization)
    mock_client = MagicMock()
    mock_get_client = MagicMock(return_value=mock_client)

    # 5. Mock DB Search (Core of caching)
    mock_search_similar_execution = MagicMock(return_value=None) # Default value: Cache Miss

    # --- Apply Mocking ---

    # Core/Decorator Dependencies
    monkeypatch.setattr("vectorwave.core.decorator.get_batch_manager", mock_get_batch_manager)
    monkeypatch.setattr("vectorwave.core.decorator.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr("vectorwave.core.decorator.get_vectorizer", mock_get_vectorizer)

    # Caching Utils Dependencies (Using the correct path)
    MOCK_PATH = "vectorwave.utils.return_caching_utils"
    monkeypatch.setattr(f"{MOCK_PATH}.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr(f"{MOCK_PATH}.get_vectorizer", mock_get_vectorizer)
    monkeypatch.setattr(f"{MOCK_PATH}.search_similar_execution", mock_search_similar_execution)

    # Tracer Dependencies (For successful log saving)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_batch_manager", mock_get_batch_manager)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_vectorizer", mock_get_vectorizer)

    # Batch Dependencies
    monkeypatch.setattr("vectorwave.batch.batch.get_weaviate_client", mock_get_client)
    monkeypatch.setattr("vectorwave.batch.batch.get_weaviate_settings", mock_get_settings)

    # Clear caches
    real_get_batch_manager.cache_clear()
    real_get_cached_client.cache_clear()
    real_get_settings.cache_clear()
    real_get_vectorizer.cache_clear()


    return {
        "batch": mock_batch_manager,
        "vectorizer": mock_vectorizer,
        "search_cache": mock_search_similar_execution,
        "settings": mock_settings
    }


# --- Test Cases ---

def test_cache_miss_and_log_saving(mock_caching_deps):
    """
    Test 1: The first call (Cache Miss) should execute the function and save the input vector and result.
    """
    mock_batch = mock_caching_deps["batch"]
    mock_vectorizer = mock_caching_deps["vectorizer"]
    mock_search = mock_caching_deps["search_cache"]

    # Set Mock search_cache to None (Miss)
    mock_search.return_value = None

    # Mock function body
    mock_func = MagicMock(return_value={"data": 100, "secret_key": "abc"})

    @vectorize(
        search_description="Test Cache Miss",
        sequence_narrative="Cache Test",
        semantic_cache=True,
        cache_threshold=0.9
    )
    def my_cache_func(user_id, amount):
        return mock_func(user_id, amount)

    # Reset Mock to exclude static log calls
    mock_batch.add_object.reset_mock()

    # --- First Call (Cache Miss) ---
    result = my_cache_func(user_id="user_X", amount=5000)

    # 1. Verification: Check function execution
    mock_func.assert_called_once_with("user_X", 5000)
    assert result == {"data": 100, "secret_key": "abc"}

    # 2. Verification: Cache search was performed and Miss returned
    mock_search.assert_called_once()

    # 3. Verification: Execution log was saved
    mock_batch.add_object.assert_called_once()

    # 4. Verification: Check if the input vector was used in the log (SUCCESS case)
    args, kwargs = mock_batch.add_object.call_args
    assert kwargs["collection"] == mock_caching_deps["settings"].EXECUTION_COLLECTION_NAME
    assert kwargs["properties"]["status"] == "SUCCESS"
    assert kwargs["vector"] == mock_vectorizer.embed.return_value # Check if input vector was saved

    # 5. Verification: Check that the return value was logged and sensitive data was masked
    logged_return = json.loads(kwargs["properties"]["return_value"])
    assert logged_return["data"] == 100
    assert logged_return["secret_key"] == "[MASKED]"


@patch('vectorwave.utils.return_caching_utils.search_similar_execution')
def test_cache_hit_identical_input_sync(mock_search_cache, mock_caching_deps):
    """
    Test 2: The second call (Cache Hit) should skip function execution and return the cached value.
    """
    mock_batch = mock_caching_deps["batch"]

    # Mock function body (Should NOT be called)
    mock_func = MagicMock(return_value="Original Result")

    # Mock cached result (Format retrieved from DB)
    cached_return_value = json.dumps({"status": "cached_success", "amount": 10000})
    mock_cached_log = {
        "return_value": cached_return_value,
        "metadata": {"distance": 0.01, "certainty": 0.99},
        "uuid": "cached-uuid-1234"
    }

    # Set Mock search_cache to Hit
    mock_search_cache.return_value = mock_cached_log

    @vectorize(
        search_description="Test Cache Hit",
        sequence_narrative="Cache Test",
        semantic_cache=True,
        cache_threshold=0.9
    )
    def my_cache_func_hit(user_id, amount):
        return mock_func(user_id, amount)

    # Reset Mock
    mock_func.reset_mock()
    mock_batch.add_object.reset_mock()
    mock_search_cache.reset_mock()

    # --- Second Call (Cache Hit) ---
    result = my_cache_func_hit(user_id="user_X", amount=5000)

    # 1. Verification: Function was NOT executed
    mock_func.assert_not_called()

    # 2. Verification: Cache search was performed and Hit returned
    mock_search_cache.assert_called_once()

    # 3. Verification: Execution log was NOT saved (Execution skipped)
    mock_batch.add_object.assert_not_called()

    # 4. Verification: Check if the result is the deserialized cached value
    assert result == {"status": "cached_success", "amount": 10000}


@pytest.mark.asyncio
@patch('vectorwave.utils.return_caching_utils.search_similar_execution')
async def test_cache_hit_async(mock_search_cache, mock_caching_deps):
    """
    Test 6: Tests the Cache Hit flow for an asynchronous (Async) function.
    """
    mock_batch = mock_caching_deps["batch"]

    # Mock function body (Should NOT be called)
    mock_func = MagicMock(return_value="Original Async Result")

    # Mock cached result
    cached_return_value = json.dumps({"status": "cached_async_success", "amount": 999})
    mock_cached_log = {
        "return_value": cached_return_value,
        "metadata": {"distance": 0.001, "certainty": 0.999},
        "uuid": "cached-async-1234"
    }
    mock_search_cache.return_value = mock_cached_log

    @vectorize(
        search_description="Test Async Cache Hit",
        sequence_narrative="Async Cache Test",
        semantic_cache=True,
        cache_threshold=0.9
    )
    async def my_async_cache_func_hit(user_id, amount):
        await asyncio.sleep(0.001)
        return mock_func(user_id, amount)

    # Reset Mock
    mock_func.reset_mock()
    mock_batch.add_object.reset_mock()
    mock_search_cache.reset_mock()

    # --- Call (Cache Hit) ---
    result = await my_async_cache_func_hit(user_id="user_ASYNC", amount=100)

    # 1. Verification: Function was NOT executed
    mock_func.assert_not_called()

    # 2. Verification: Cache search was performed and Hit returned
    mock_search_cache.assert_called_once()

    # 3. Verification: Execution log was NOT saved
    mock_batch.add_object.assert_not_called()

    # 4. Verification: Check if the result is the deserialized cached value
    assert result == {"status": "cached_async_success", "amount": 999}


@patch('vectorwave.utils.return_caching_utils.search_similar_execution')
def test_cache_miss_low_certainty(mock_search_cache, mock_caching_deps, caplog):
    """
    Test 3: Tests the Cache Miss scenario when similarity is below the threshold.
    """
    mock_batch = mock_caching_deps["batch"]
    mock_func = MagicMock(return_value="Executed Result")

    # Set Mock search_cache to return None (Cache Miss)
    mock_search_cache.return_value = None

    @vectorize(
        search_description="Test Miss Threshold",
        sequence_narrative="Cache Test",
        semantic_cache=True,
        cache_threshold=0.99 # Set high threshold
    )
    def my_threshold_func(input_data):
        return mock_func(input_data)

    # Reset Mock
    mock_func.reset_mock()
    mock_batch.add_object.reset_mock()

    # --- Call (Miss due to threshold) ---
    result = my_threshold_func(input_data=1)

    # 1. Verification: Function was executed
    mock_func.assert_called_once()
    assert result == "Executed Result"

    # 2. Verification: Cache search was performed
    mock_search_cache.assert_called_once()

    # 3. Verification: Log was saved (because the function was executed)
    mock_batch.add_object.assert_called_once()


@patch('vectorwave.utils.return_caching_utils.search_similar_execution')
def test_cache_disabled_when_vectorizer_is_none(mock_search_cache, mock_caching_deps, caplog):
    """
    Test 5: Tests that caching is disabled when the Python vectorizer is not configured.
    """
    import logging
    caplog.set_level(logging.WARNING)

    # 1. Mock: Set get_vectorizer to return None
    with patch('vectorwave.core.decorator.get_vectorizer', return_value=None):
        with patch('vectorwave.utils.return_caching_utils.get_vectorizer', return_value=None):

            @vectorize(
                search_description="Test Caching Disabled",
                sequence_narrative="Disabled Test",
                semantic_cache=True, # Request is True
                capture_return_value=False
            )
            def my_disabled_cache_func(x):
                return x

            # 2. Act: Function call
            mock_func = MagicMock(return_value=10)
            mock_caching_deps["batch"].add_object.reset_mock()

            # The decorator pre-check will disable semantic_cache before the function is executed.
            result = my_disabled_cache_func(1)

            # 3. Verification: Check if a warning was logged (at decorator load time)
            warning_logs = [r for r in caplog.records if "Semantic caching requested" in r.message]
            assert len(warning_logs) == 1
            assert "no Python vectorizer is configured" in warning_logs[0].message

            # 4. Verification: Cache search logic was NOT called
            mock_search_cache.assert_not_called()

            # 5. Verification: Result was returned normally
            assert result == 1

            # 6. Verification: Execution log was saved
            mock_caching_deps["batch"].add_object.assert_called()


def test_tracer_input_vector_data_and_masking(mock_caching_deps):
    """
    Tests that the _create_input_vector_data function masks sensitive keys.
    """
    from vectorwave.monitoring.tracer import _create_input_vector_data

    # Arrange: settings has sensitive_keys={"secret_key"}
    settings = mock_caching_deps["settings"]

    # Act
    input_data = _create_input_vector_data(
        func_name="test_func",
        args=(1, 2),
        kwargs={"amount": 100, "secret_key": "my_top_secret"},
        sensitive_keys=settings.sensitive_keys
    )

    # 1. Verification: Check if masking is included in the canonical text
    text_content = input_data["text"]
    assert "test_func" in text_content
    assert "amount" in text_content
    assert "[MASKED]" in text_content
    assert "my_top_secret" not in text_content

    # 2. Verification: Check if masking is included in the properties dictionary
    props = input_data["properties"]
    assert props["function"] == "test_func"
    assert props["kwargs"]["amount"] == 100
    assert props["kwargs"]["secret_key"] == "[MASKED]"