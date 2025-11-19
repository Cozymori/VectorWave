import logging
from typing import Optional, Tuple, Any, Dict, Callable
import json
import inspect
from functools import wraps

from weaviate.util import generate_uuid5

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from ..monitoring.tracer import _create_input_vector_data, _deserialize_return_value
from ..database.db_search import search_similar_execution
from ..vectorizer.factory import get_vectorizer

logger = logging.getLogger(__name__)


def _check_and_return_cached_result(
        func: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        function_name: str,
        cache_threshold: float,
        is_async: bool
) -> Optional[Any]:
    """
    Checks the VectorWaveExecutions database for a semantically similar cached result.
    If found, returns the deserialized result; otherwise, returns None.

    This function should be called from the outer_wrapper of the @vectorize decorator.
    """
    if not cache_threshold: # Check if caching is effectively disabled (threshold 0 or None)
        return None

    settings: WeaviateSettings = get_weaviate_settings()
    vectorizer = get_vectorizer()

    if vectorizer is None:
        # This case should ideally be caught in decorator setup, but defensive check remains.
        logger.error(f"Cannot perform vectorization for caching on '{function_name}': Vectorizer is None.")
        return None

    try:
        # (A) Create vectorization data
        input_vector_data = _create_input_vector_data(
            func_name=function_name,
            args=args, # Original positional args
            kwargs=kwargs, # Original keyword args
            sensitive_keys=settings.sensitive_keys
        )

        # (B) Vectorize (Synchronous call is fine, as vectorizer.embed is sync)
        input_vector = vectorizer.embed(input_vector_data['text'])

        # (C) Search Cache (Needs to handle the fact that search_similar_execution is sync)
        cached_log = search_similar_execution(
            query_vector=input_vector,
            function_name=function_name,
            threshold=cache_threshold
        )

        if cached_log:
            distance = cached_log['metadata'].get('distance')
            logger.info(
                f"[Cache Hit] '{function_name}' skipped. "
                f"Distance: {distance:.4f}, "
                f"Trace ID: {cached_log['uuid'][:8]}..."
            )
            # (D) Deserialize and return the cached result
            return _deserialize_return_value(cached_log.get('return_value'))

        return None

    except Exception as e:
        logger.error(f"Failed to check semantic cache for '{function_name}': {e}", exc_info=True)
        # Cache failure must not stop execution, so return None to proceed to actual function call.
        return None