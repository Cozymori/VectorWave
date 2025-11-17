import pytest
import os
import logging

CACHE_FILE_PATH = ".vectorwave_functions_cache.json"
logger = logging.getLogger(__name__)


def _delete_cache():
    """Helper function to remove the cache file if it exists."""
    if os.path.exists(CACHE_FILE_PATH):
        try:
            os.remove(CACHE_FILE_PATH)
        except OSError as e:
            print(f"\n[CacheFixture Error] Failed to remove {CACHE_FILE_PATH}: {e}")


@pytest.fixture(autouse=True, scope="function")
def atomic_function_cache():
    # --- SETUP (Before Test) ---
    _delete_cache()

    # --- Run the test ---
    yield

    # --- TEARDOWN (After Test) ---
    _delete_cache()
