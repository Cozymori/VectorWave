import logging
import re
import weaviate
import weaviate.classes as wvc
from typing import Dict, Any, Optional, List, Tuple

from weaviate.collections.classes.filters import _Filters
from weaviate.classes.query import Filter

# Conservative GraphQL/Weaviate property name shape — letters, digits,
# underscores, must start with a letter or underscore. Filter keys outside
# this shape are rejected so user-controlled input can't target arbitrary
# server-side identifiers.
_PROP_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from ..exception.exceptions import WeaviateConnectionError
from ..vectorizer.factory import get_vectorizer
from weaviate.classes.aggregate import Metrics

import uuid
from datetime import datetime

# Create module-level logger
logger = logging.getLogger(__name__)


def _build_weaviate_filters(filters: Optional[Dict[str, Any]]) -> _Filters | None:
    if not filters:
        return None

    filter_list = []

    for key, value in filters.items():
        parts = key.split('__')
        prop_name = parts[0]
        operator = parts[1] if len(parts) > 1 else 'equal'

        if not _PROP_NAME_PATTERN.match(prop_name):
            logger.warning(
                "Rejecting filter on unsafe property name '%s'; expected "
                "[A-Za-z_][A-Za-z0-9_]*. This filter clause will be skipped.",
                prop_name,
            )
            continue

        try:
            prop = Filter.by_property(prop_name)

            if operator == 'equal':
                if isinstance(value, list) and value:
                    # Use contains_any for matching any value in the list (equivalent to SQL IN)
                    filter_list.append(prop.contains_any(value))
                else:
                    filter_list.append(prop.equal(value))
            elif operator == 'not_equal':
                filter_list.append(prop.not_equal(value))
            elif operator == 'gte':  # Greater than or equal
                filter_list.append(prop.greater_or_equal(value))
            elif operator == 'gt':  # Greater than
                filter_list.append(prop.greater_than(value))
            elif operator == 'lte':  # Less than or equal
                filter_list.append(prop.less_or_equal(value))
            elif operator == 'lt':  # Less than
                filter_list.append(prop.less_than(value))
            elif operator == 'like':
                filter_list.append(prop.like(f"*{value}*"))
            else:
                logger.warning(f"Unsupported filter operator: {operator}. Defaulting to 'equal'.")
                filter_list.append(prop.equal(value))

        except Exception as e:
            logger.error(f"Failed to build filter for {key}: {e}")

    if not filter_list:
        return None

    return Filter.all_of(filter_list)


def search_errors_by_message(
        query: str,
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None
) -> List[Dict[str, Any]]:
    """
    Searches the executions collection for semantically similar error logs.
    Routes through the VectorStore so it works in both Pro and Lite modes.
    """
    try:
        from ..store import get_vector_store
        settings: WeaviateSettings = get_weaviate_settings()
        store = get_vector_store()

        base_filters = {"status": "ERROR"}
        if filters:
            base_filters.update(filters)

        vectorizer = get_vectorizer()
        if vectorizer is None:
            logger.error(
                "Cannot perform vector search: No Python vectorizer (huggingface / openai_client) configured."
            )
            raise WeaviateConnectionError("Cannot perform vector search: No Python vectorizer configured.")

        try:
            logger.info("Vectorizing error query...")
            query_vector = vectorizer.embed(query)
        except Exception as e:
            logger.error(f"Query vectorization failed: {e}")
            raise WeaviateConnectionError(f"Query vectorization failed: {e}")

        records = store.near_vector(
            collection=settings.EXECUTION_COLLECTION_NAME,
            vector=query_vector,
            filters=base_filters,
            limit=limit,
            return_properties=[
                "function_name", "error_message", "error_code",
                "timestamp_utc", "trace_id", "parent_span_id", "span_id",
            ],
        )
        return [
            {"properties": r.properties, "metadata": {"distance": r.distance}, "uuid": r.uuid}
            for r in records
        ]

    except Exception as e:
        logger.error("Error during error-message search: %s", e)
        raise WeaviateConnectionError(f"Failed to execute 'search_errors_by_message': {e}")


def search_functions(query: str, limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Searches the functions collection. Requires a Python-side vectorizer
    (HuggingFace / OpenAI client) so we always go through near_vector — the
    legacy `near_text` path needed Weaviate's text2vec module, which Lite
    mode doesn't have. Pro users without a Python vectorizer should set
    VECTORIZER=huggingface (default) to keep using this function.
    """
    try:
        from ..store import get_vector_store
        settings: WeaviateSettings = get_weaviate_settings()
        store = get_vector_store()

        vectorizer = get_vectorizer()
        if vectorizer is None:
            raise WeaviateConnectionError(
                "search_functions requires a Python-side vectorizer. "
                "Set VECTORIZER=huggingface (default) or 'openai_client'."
            )

        try:
            query_vector = vectorizer.embed(query)
        except Exception as e:
            raise WeaviateConnectionError(f"Query vectorization failed: {e}")

        records = store.near_vector(
            collection=settings.COLLECTION_NAME,
            vector=query_vector,
            filters=filters,
            limit=limit,
        )
        return [
            {"properties": r.properties, "metadata": {"distance": r.distance}, "uuid": r.uuid}
            for r in records
        ]

    except Exception as e:
        logger.error("Error during function search: %s", e)
        raise WeaviateConnectionError(f"Failed to execute 'search_functions': {e}")


def search_executions(
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = "timestamp_utc",
        sort_ascending: bool = False
) -> List[Dict[str, Any]]:
    """
    Searches execution logs from the [VectorWaveExecutions] collection using filtering and sorting.

    Routes through the VectorStore abstraction so it works in both Pro
    (Weaviate) and Lite (LanceDB) modes.
    """
    try:
        from ..store import get_vector_store
        settings: WeaviateSettings = get_weaviate_settings()
        store = get_vector_store()
        records = store.query(
            collection=settings.EXECUTION_COLLECTION_NAME,
            filters=filters,
            sort_by=sort_by,
            sort_ascending=sort_ascending,
            limit=limit,
        )
        results = []
        for rec in records:
            props = dict(rec.properties)
            props["uuid"] = rec.uuid
            for key, value in list(props.items()):
                if isinstance(value, uuid.UUID) or isinstance(value, datetime):
                    props[key] = str(value)
            results.append(props)
        return results

    except Exception as e:
        raise WeaviateConnectionError(f"Failed to execute 'search_executions': {e}")


def search_similar_execution(
        query_vector: List[float],
        function_name: str,
        threshold: float = 0.9,
        limit: int = 1,
        filters: Optional[Dict[str, Any]] = None
) -> Optional[Dict[str, Any]]:
    """
    Searches the executions collection for the nearest SUCCESS log to a query
    vector. Routes through the VectorStore so it works in both Pro and Lite.
    """
    try:
        from ..store import get_vector_store
        settings: WeaviateSettings = get_weaviate_settings()
        store = get_vector_store()

        base_filters = {
            "status": "SUCCESS",
            "function_name": function_name,
        }
        if filters:
            base_filters.update(filters)

        logger.info(
            f"Performing near_vector cache search for '{function_name}' with certainty >= {threshold}"
        )

        records = store.near_vector(
            collection=settings.EXECUTION_COLLECTION_NAME,
            vector=query_vector,
            filters=base_filters,
            certainty=threshold,
            limit=limit,
            return_properties=["return_value", "timestamp_utc"],
        )

        if records:
            best = records[0]
            return {
                "return_value": best.properties.get("return_value"),
                "metadata": {
                    "distance": best.distance,
                    "certainty": best.certainty,
                },
                "uuid": best.uuid,
            }
        return None

    except Exception as e:
        logger.error(f"Error during cache search for '{function_name}': {e}", exc_info=True)
        return None


def search_functions_hybrid(
        query: str,
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        alpha: float = 0.5
) -> List[Dict[str, Any]]:
    """
    Performs Hybrid Search (Keyword + Vector) on function definitions.

    **Pro-only.** Hybrid search (BM25 + vector with a tunable alpha) is a
    Weaviate-specific feature; Lite mode (LanceDB) has no equivalent yet.
    Lite users should call `search_functions` (pure vector) instead.
    """
    import os
    if os.environ.get("VECTORWAVE_MODE", "pro").lower() == "lite":
        raise WeaviateConnectionError(
            "search_functions_hybrid is a Pro-only feature. "
            "Use search_functions for pure vector search in Lite mode."
        )
    try:
        from ..database.db import get_cached_client
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.COLLECTION_NAME)
        weaviate_filter = _build_weaviate_filters(filters)

        vectorizer = get_vectorizer()

        # 1. Python Vectorizer
        if vectorizer is not None:
            logger.info(f"[Hybrid] Vectorizing query with Python client... (alpha={alpha})")
            try:
                query_vector = vectorizer.embed(query)
            except Exception as e:
                logger.error(f"Query vectorization failed: {e}")
                raise WeaviateConnectionError(f"Query vectorization failed: {e}")

            # Hybrid Search with explicit vector
            response = collection.query.hybrid(
                query=query,
                vector=query_vector,
                alpha=alpha,
                limit=limit,
                filters=weaviate_filter,
                return_metadata=wvc.query.MetadataQuery(score=True, distance=True)
            )


        else:
            logger.info(f"[Hybrid] Searching with Weaviate module... (alpha={alpha})")
            # Hybrid Search letting Weaviate handle vectorization (if module enabled)
            response = collection.query.hybrid(
                query=query,
                alpha=alpha,
                limit=limit,
                filters=weaviate_filter,
                return_metadata=wvc.query.MetadataQuery(score=True, distance=True)
            )

        results = [
            {
                "properties": obj.properties,
                "metadata": obj.metadata,
                "uuid": obj.uuid
            }
            for obj in response.objects
        ]
        return results

    except Exception as e:
        logger.error("Error during Weaviate Hybrid search: %s", e)
        raise WeaviateConnectionError(f"Failed to execute 'search_functions_hybrid': {e}")


def check_semantic_drift(
        vector: List[float],
        function_name: str,
        threshold: float,
        k: int = 5
) -> Tuple[bool, float, Optional[str]]:
    """
    KNN-based semantic drift check. Routes through the VectorStore so it works
    in both Pro and Lite modes.
    """
    try:
        from ..store import get_vector_store
        settings = get_weaviate_settings()
        store = get_vector_store()

        records = store.near_vector(
            collection=settings.EXECUTION_COLLECTION_NAME,
            vector=vector,
            filters={"function_name": function_name, "status": "SUCCESS"},
            limit=k,
            return_properties=[],
        )

        if not records:
            return False, 0.0, None

        distances = [r.distance for r in records if r.distance is not None]
        if not distances:
            return False, 0.0, None

        avg_distance = sum(distances) / len(distances)
        nearest_uuid = records[0].uuid
        is_drift = avg_distance > threshold

        if is_drift:
            logger.warning(
                f"🚨 [Semantic Drift] '{function_name}' detected anomaly! "
                f"Avg Distance (k={len(distances)}): {avg_distance:.4f} (Threshold: {threshold})"
            )

        return is_drift, avg_distance, nearest_uuid

    except Exception as e:
        logger.error(f"Failed to check semantic drift: {e}")
        return False, 0.0, None


def simulate_drift_check(
        text: str,
        function_name: str,
        threshold: Optional[float] = None,
        k: Optional[int] = None
) -> Dict[str, Any]:
    # ... (No changes needed here)
    """
    Simulates drift detection for a hypothetical input string without executing the function.
    Useful for 'Drift Radar' or debugging.
    """
    try:
        settings = get_weaviate_settings()
        vectorizer = get_vectorizer()

        if vectorizer is None:
            return {"error": "No vectorizer configured."}

        # 1. Set defaults from settings if not provided
        if threshold is None:
            threshold = settings.DRIFT_DISTANCE_THRESHOLD
        if k is None:
            k = settings.DRIFT_NEIGHBOR_AMOUNT

        # 2. Vectorize the input text
        try:
            vector = vectorizer.embed(text)
        except Exception as e:
            return {"error": f"Vectorization failed: {e}"}

        # 3. Perform the check using the existing logic
        is_drift, avg_distance, nearest_uuid = check_semantic_drift(
            vector=vector,
            function_name=function_name,
            threshold=threshold,
            k=k
        )

        return {
            "function_name": function_name,
            "input_text": text,
            "is_drift": is_drift,
            "avg_distance": avg_distance,
            "threshold": threshold,
            "nearest_neighbor_uuid": nearest_uuid,
            "status": "ANOMALY" if is_drift else "NORMAL"
        }

    except Exception as e:
        logger.error(f"Simulation failed: {e}")
        return {"error": str(e)}


def get_token_usage_stats() -> Dict[str, int]:
    """VectorWaveTokenUsage collection based analysis. Works in both modes."""
    try:
        from ..store import get_vector_store
        store = get_vector_store()
        if not store.collection_exists("VectorWaveTokenUsage"):
            logger.warning("VectorWaveTokenUsage collection does not exist.")
            return {"total_tokens": 0}

        total_tokens = 0
        stats: Dict[str, int] = {}

        for rec in store.iterate("VectorWaveTokenUsage"):
            props = rec.properties
            tokens = int(props.get("tokens", 0))
            category = props.get("category", "unknown")

            total_tokens += tokens

            cat_key = f"{category}_tokens"
            stats[cat_key] = stats.get(cat_key, 0) + tokens

        stats["total_tokens"] = total_tokens
        return stats

    except Exception as e:
        logger.error(f"Stats error: {e}", exc_info=True)
        return {}