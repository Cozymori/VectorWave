import hashlib
import json
import logging
import os
import threading
from typing import Dict, Any, Optional
from ..models.db_config import get_weaviate_settings

logger = logging.getLogger(__name__)

CACHE_FILE_NAME = ".vectorwave_functions_cache.json"


class FunctionCacheManager:
    """Manages the local file cache for VectorWave function definitions."""

    def __init__(self, cache_dir: str = "."):
        self.cache_path = os.path.join(cache_dir, CACHE_FILE_NAME)
        # Guards both the in-memory dict and the on-disk JSON. Concurrent
        # @vectorize decorations from parallel imports would otherwise
        # interleave json.dump calls and corrupt the file.
        self._lock = threading.Lock()
        self.cache: Dict[str, Any] = self._load_cache()
        logger.info(f"FunctionCacheManager initialized. Cache file: {self.cache_path}")

    def _load_cache(self) -> Dict[str, Any]:
        if not os.path.exists(self.cache_path):
            return {}
        try:
            with open(self.cache_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (IOError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to load cache. Starting clean. Error: {e}")
            return {}

    def _save_cache_locked(self):
        """Caller must hold self._lock."""
        try:
            with open(self.cache_path, 'w', encoding='utf-8') as f:
                json.dump(self.cache, f, indent=4, sort_keys=True)
        except IOError as e:
            logger.error(f"Failed to save cache. Error: {e}")

    @staticmethod
    def calculate_content_hash(func_identifier: str, static_properties: Dict[str, Any]) -> str:
        """Calculates SHA256 hash based on function identifier and static properties."""
        content_data = {
            "identifier": func_identifier,
            "static_props": static_properties
        }
        content_str = json.dumps(content_data, sort_keys=True, ensure_ascii=True)
        return hashlib.sha256(content_str.encode('utf-8')).hexdigest()

    def get_cached_metadata(self, func_uuid: str, current_hash: str) -> Optional[Dict[str, Any]]:
        """
        Returns cached metadata (description/narrative) if hash matches.
        """
        entry = self.cache.get(func_uuid)
        if isinstance(entry, dict) and entry.get("hash") == current_hash:
            return entry.get("metadata")
        return None

    def is_cached_and_unchanged(self, func_uuid: str, current_hash: str) -> bool:
        """Checks if function is cached and unchanged (supports both old string format and new dict format)."""
        entry = self.cache.get(func_uuid)
        if entry is None:
            return False

        # Support old format (string hash)
        if isinstance(entry, str):
            return entry == current_hash

        # Support new format (dict with hash)
        if isinstance(entry, dict):
            return entry.get("hash") == current_hash

        return False

    def update_cache(self, func_uuid: str, current_hash: str):
        """Legacy update: saves only hash."""
        with self._lock:
            self.cache[func_uuid] = {"hash": current_hash, "metadata": None}
            self._save_cache_locked()

    def update_cache_with_metadata(self, func_uuid: str, current_hash: str, metadata: Dict[str, Any]):
        """[NEW] Updates cache with hash and generated metadata."""
        with self._lock:
            self.cache[func_uuid] = {
                "hash": current_hash,
                "metadata": metadata
            }
            self._save_cache_locked()


def initialize_cache_manager() -> FunctionCacheManager:

    settings = get_weaviate_settings()

    cache_dir = os.path.dirname(settings.CUSTOM_PROPERTIES_FILE_PATH) or "."

    return FunctionCacheManager(cache_dir=cache_dir)


function_cache_manager = initialize_cache_manager()
