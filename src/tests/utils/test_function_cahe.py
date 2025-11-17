import pytest
import json
from unittest.mock import patch, mock_open, ANY, MagicMock
from vectorwave.utils.function_cache import FunctionCacheManager

# --- 1. Tests for calculate_content_hash static method ---

def test_calculate_content_hash_consistency():
    """Tests that identical inputs always return the same hash."""
    props1 = {"a": 1, "b": "test", "c": [1, 2, 3]}
    props2 = {"a": 1, "b": "test", "c": [1, 2, 3]}

    hash1 = FunctionCacheManager.calculate_content_hash("id1", props1)
    hash2 = FunctionCacheManager.calculate_content_hash("id1", props2)

    assert hash1 == hash2
    assert len(hash1) == 64 # SHA256

def test_calculate_content_hash_sensitivity_props():
    """Tests that different properties return different hashes."""
    props1 = {"a": 1, "b": "test"}
    props2 = {"a": 1, "b": "changed"}

    hash1 = FunctionCacheManager.calculate_content_hash("id1", props1)
    hash2 = FunctionCacheManager.calculate_content_hash("id1", props2)

    assert hash1 != hash2

def test_calculate_content_hash_sensitivity_id():
    """Tests that different identifiers return different hashes."""
    props1 = {"a": 1}

    hash1 = FunctionCacheManager.calculate_content_hash("id1", props1)
    hash2 = FunctionCacheManager.calculate_content_hash("id2", props1)

    assert hash1 != hash2

def test_calculate_content_hash_order_invariance():
    """
    Tests that different dictionary key orders return the same hash,
    due to json.dumps(sort_keys=True).
    """
    props1 = {"a": 1, "b": 2}
    props2 = {"b": 2, "a": 1} # Changed order

    hash1 = FunctionCacheManager.calculate_content_hash("id1", props1)
    hash2 = FunctionCacheManager.calculate_content_hash("id1", props2)

    assert hash1 == hash2


# --- 2. Tests for __init__ and _load_cache ---

@patch("os.path.exists", return_value=False)
def test_init_cache_file_not_found(mock_exists):
    """Tests that self.cache is initialized as an empty dict ({}) when the cache file is not found."""
    manager = FunctionCacheManager()

    mock_exists.assert_called_once_with("./.vectorwave_functions_cache.json")
    assert manager.cache == {}

@patch("os.path.exists", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data='{"uuid_abc": "hash_123"}')
def test_init_cache_file_loaded(mock_file, mock_exists):
    """Tests that the cache file is loaded successfully."""
    manager = FunctionCacheManager()

    mock_exists.assert_called_once_with("./.vectorwave_functions_cache.json")
    mock_file.assert_called_once_with("./.vectorwave_functions_cache.json", 'r', encoding='utf-8')
    assert manager.cache == {"uuid_abc": "hash_123"}

@patch("os.path.exists", return_value=True)
@patch("builtins.open", new_callable=mock_open, read_data='{invalid_json')
@patch("logging.Logger.warning") # Mock the logger
def test_init_cache_invalid_json(mock_logger, mock_file, mock_exists):
    """Tests that a warning is logged and cache is an empty dict ({}) if JSON parsing fails."""
    manager = FunctionCacheManager()

    assert manager.cache == {}
    mock_logger.assert_called_once()
    assert "Failed to load static function cache" in mock_logger.call_args[0][0]


# --- 3. Tests for logic and saving ---

@patch("os.path.exists", return_value=False)
def test_is_cached_and_unchanged_logic(mock_exists):
    """Tests the True/False logic of is_cached_and_unchanged."""
    manager = FunctionCacheManager()
    manager.cache = {
        "uuid_A": "hash_A_123",
        "uuid_B": "hash_B_123"
    }

    # 1. Cache Hit (Both UUID and hash match)
    assert manager.is_cached_and_unchanged("uuid_A", "hash_A_123") == True

    # 2. Cache Miss (UUID matches, but hash is different)
    assert manager.is_cached_and_unchanged("uuid_B", "hash_B_456_NEW") == False

    # 3. Cache Miss (UUID does not exist)
    assert manager.is_cached_and_unchanged("uuid_C_NEW", "hash_C_123") == False

@patch("os.path.exists", return_value=False)
@patch("json.dump") # Mock json.dump
@patch("builtins.open", new_callable=mock_open)
def test_update_cache_calls_save(mock_file, mock_json_dump, mock_exists):
    """Tests if update_cache updates self.cache and calls _save_cache."""
    manager = FunctionCacheManager()
    assert manager.cache == {}

    manager.update_cache("uuid_new", "hash_new_123")

    # 1. Check if the internal cache was updated
    assert manager.cache == {"uuid_new": "hash_new_123"}

    # 2. Check if _save_cache was called and attempted to write to the file
    mock_file.assert_called_once_with("./.vectorwave_functions_cache.json", 'w', encoding='utf-8')

    # 3. Check if json.dump was called with the correct arguments (indent, sort_keys)
    mock_json_dump.assert_called_once_with(
        {"uuid_new": "hash_new_123"}, # 1. The updated cache
        ANY, # 2. The mock_file handle
        indent=4,
        sort_keys=True
    )