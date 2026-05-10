"""Factory for the configured VectorWave backend.

The mode is selected by the ``VECTORWAVE_MODE`` env var:

- ``pro`` (default) — Weaviate via the existing client
- ``lite`` — LanceDB local file store, no Docker

The cached singleton matches the rest of the package's ``@lru_cache``
pattern. Tests that need a fresh backend should call
``get_vector_store.cache_clear()`` after toggling env vars.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache

from .base import VectorStore

logger = logging.getLogger(__name__)


@lru_cache()
def get_vector_store() -> VectorStore:
    mode = os.environ.get("VECTORWAVE_MODE", "pro").lower()

    if mode == "lite":
        from .lance_store import LanceVectorStore

        db_path = os.environ.get("VECTORWAVE_LITE_PATH", ".vectorwave/lance")
        logger.info("[VectorWave] Lite mode active — LanceDB at %s", db_path)
        return LanceVectorStore(db_path=db_path)

    if mode in ("pro", "weaviate"):
        from ..database.db import get_cached_client
        from .weaviate_store import WeaviateVectorStore

        logger.info("[VectorWave] Pro mode active — Weaviate")
        return WeaviateVectorStore(client=get_cached_client())

    raise ValueError(
        f"Unknown VECTORWAVE_MODE='{mode}'. Expected 'pro' (default) or 'lite'."
    )
