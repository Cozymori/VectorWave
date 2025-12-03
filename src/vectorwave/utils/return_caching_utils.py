import logging
from typing import Optional, Tuple, Any, Dict, Callable
import json
from datetime import datetime, timezone
from uuid import uuid4

from weaviate.util import generate_uuid5
import weaviate.classes.query as wvc_query

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from ..monitoring.tracer import _create_input_vector_data, _deserialize_return_value, current_tracer_var, \
    current_span_id_var
from ..database.db_search import search_similar_execution
from ..vectorizer.factory import get_vectorizer
from ..batch.batch import get_batch_manager
from ..database.db import get_cached_client  # [NEW] 클라이언트 직접 접근

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
    Checks for a cached result.
    Priority 1: VectorWaveGoldenDataset (Golden Data)
    Priority 2: VectorWaveExecutions (Standard Logs)
    """
    if not cache_threshold:
        return None

    settings: WeaviateSettings = get_weaviate_settings()
    vectorizer = get_vectorizer()

    if vectorizer is None:
        logger.error(f"Cannot perform vectorization for caching on '{function_name}': Vectorizer is None.")
        return None

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

        # (C) [NEW] Priority 1: Search Golden Dataset
        client = get_cached_client()
        golden_match = None

        try:
            golden_col = client.collections.get(settings.GOLDEN_COLLECTION_NAME)
            # Golden Data search(vector similarity based)
            response = golden_col.query.near_vector(
                near_vector=input_vector,
                limit=1,
                filters=wvc_query.Filter.by_property("function_name").equal(function_name),
                certainty=cache_threshold,
                return_properties=["return_value", "original_uuid"],
                return_metadata=wvc_query.MetadataQuery(distance=True, certainty=True)
            )

            if response.objects:
                golden_match = response.objects[0]
                logger.info(f"🌟 [Golden Cache Hit] '{function_name}' found in Golden Dataset. (Distance: {golden_match.metadata.distance:.4f})")

        except Exception as e:
            logger.warning(f"Golden cache search failed: {e}")

        # (D) Decide Source (Golden vs Standard)
        cached_log = None
        is_golden_hit = False

        if golden_match:
            cached_log = {
                'return_value': golden_match.properties.get('return_value'),
                'metadata': {
                    'distance': golden_match.metadata.distance,
                    'certainty': golden_match.metadata.certainty,
                },
                'uuid': str(golden_match.uuid)
            }
            is_golden_hit = True
        else:
            cached_log = search_similar_execution(
                query_vector=input_vector,
                function_name=function_name,
                threshold=cache_threshold
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

        return None

    except Exception as e:
        logger.error(f"Failed to check semantic cache for '{function_name}': {e}", exc_info=True)
        return None