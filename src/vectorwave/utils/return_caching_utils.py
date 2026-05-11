import logging
from typing import Optional, Tuple, Any, Dict, Callable
import json
from datetime import datetime, timezone
from uuid import uuid4

from weaviate.util import generate_uuid5

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from ..monitoring.tracer import _create_input_vector_data, current_tracer_var, \
    current_span_id_var
from .serialization import deserialize_return_value as _deserialize_return_value
from ..database.db_search import search_similar_execution
from ..vectorizer.factory import get_vectorizer
from ..batch.batch import get_batch_manager
from ..store import get_vector_store

logger = logging.getLogger(__name__)

# Sentinel returned by _check_and_return_cached_result on miss.
# A bare None is reserved for legitimate cache hits whose stored return value
# is None — using None for both would mask functions that legitimately return
# None as if they had no cache entry.
CACHE_MISS = object()


def _check_and_return_cached_result(
        func: Callable,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        function_name: str,
        cache_threshold: float,
        is_async: bool,
        filters: Optional[Dict[str, Any]] = None  # [NEW] 인자 추가
) -> Any:
    """
    Checks for a cached result. Returns the cached return value (which may be
    None) on hit, or the `CACHE_MISS` sentinel on miss/error.
    Priority 1: VectorWaveGoldenDataset (Golden Data)
    Priority 2: VectorWaveExecutions (Standard Logs)
    """
    if not cache_threshold:
        return CACHE_MISS

    settings: WeaviateSettings = get_weaviate_settings()
    vectorizer = get_vectorizer()

    if vectorizer is None:
        logger.error(f"Cannot perform vectorization for caching on '{function_name}': Vectorizer is None.")
        return CACHE_MISS

    try:
        # (A) Create vectorization data
        input_vector_data = _create_input_vector_data(
            func_name=function_name,
            args=args,
            kwargs=kwargs,
            sensitive_keys=settings.sensitive_keys
        )

        # (B) Vectorize
        input_vector = vectorizer.embed(input_vector_data['text'])

        # (C) Priority 1: Search Golden Dataset
        store = get_vector_store()
        golden_match = None

        golden_filters: Dict[str, Any] = {"function_name": function_name}
        if filters:
            golden_filters.update(filters)

        try:
            golden_records = store.near_vector(
                collection=settings.GOLDEN_COLLECTION_NAME,
                vector=input_vector,
                filters=golden_filters,
                certainty=cache_threshold,
                limit=1,
                return_properties=["return_value", "original_uuid"],
            )

            if golden_records:
                golden_match = golden_records[0]
                logger.info(
                    f"🌟 [Golden Cache Hit] '{function_name}' found in Golden Dataset. "
                    f"(Distance: {golden_match.distance:.4f})"
                )
        except Exception as e:
            logger.warning(f"Golden cache search failed: {e}")

        # (D) Decide Source (Golden vs Standard)
        cached_log = None
        is_golden_hit = False

        if golden_match:
            cached_log = {
                "return_value": golden_match.properties.get("return_value"),
                "metadata": {
                    "distance": golden_match.distance,
                    "certainty": golden_match.certainty,
                },
                "uuid": golden_match.uuid,
            }
            is_golden_hit = True
        else:
            # [NEW] filters 전달
            cached_log = search_similar_execution(
                query_vector=input_vector,
                function_name=function_name,
                threshold=cache_threshold,
                filters=filters
            )

        # (E) Process Cache Hit
        if cached_log:
            if not is_golden_hit:
                distance = cached_log['metadata'].get('distance')
                logger.info(
                    f"[Cache Hit] '{function_name}' skipped (Standard Log). "
                    f"Distance: {distance:.4f}"
                )

            # (F) Log CACHE_HIT event
            try:
                batch_manager = get_batch_manager()

                tracer = current_tracer_var.get()
                parent_span_id = current_span_id_var.get()
                trace_id = tracer.trace_id if tracer else str(uuid4())

                module_name = getattr(func, "__module__", "__main__")
                func_uuid = generate_uuid5(f"{module_name}.{function_name}")

                hit_properties = {
                    "trace_id": trace_id,
                    "span_id": str(uuid4()),
                    "parent_span_id": parent_span_id,
                    "function_name": function_name,
                    "function_uuid": func_uuid,
                    "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                    "duration_ms": 0.0,
                    "status": "CACHE_HIT",
                    "return_value": cached_log.get('return_value'),
                    "is_golden_source": is_golden_hit
                }

                if settings.global_custom_values:
                    hit_properties.update(settings.global_custom_values)

                # Add to batch
                batch_manager.add_object(
                    collection=settings.EXECUTION_COLLECTION_NAME,
                    properties=hit_properties,
                    vector=input_vector
                )

            except Exception as log_e:
                logger.error(f"Failed to log CACHE_HIT: {log_e}")

            return _deserialize_return_value(cached_log.get('return_value'))

        return CACHE_MISS

    except Exception as e:
        logger.error(f"Failed to check semantic cache for '{function_name}': {e}", exc_info=True)
        return CACHE_MISS