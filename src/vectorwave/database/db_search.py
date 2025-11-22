import logging
import weaviate
import weaviate.classes as wvc
from typing import Dict, Any, Optional, List

from weaviate.collections.classes.filters import _Filters
from weaviate.classes.query import Filter

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from .db import get_cached_client
from ..exception.exceptions import WeaviateConnectionError
from ..vectorizer.factory import get_vectorizer

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
    [NEW] Searches the 'VectorWaveExecutions' collection for
    semantically similar error logs using a natural language query.
    """
    try:
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.EXECUTION_COLLECTION_NAME)

        # [NEW] By default, only search for logs with "ERROR" status
        base_filters = {"status": "ERROR"}
        if filters:
            base_filters.update(filters)

        weaviate_filter = _build_weaviate_filters(base_filters)

        vectorizer = get_vectorizer()
        if not vectorizer:
            logger.error(
                "Cannot perform vector search: No Python vectorizer (e.g., 'huggingface' or 'openai_client') is configured in .env.")
            raise WeaviateConnectionError("Cannot perform vector search: No Python vectorizer configured.")

        try:
            logger.info("Vectorizing error query...")
            query_vector = vectorizer.embed(query)
        except Exception as e:
            logger.error(f"Query vectorization failed: {e}")
            raise WeaviateConnectionError(f"Query vectorization failed: {e}")

        logger.info(f"Performing near_vector search for errors matching: '{query}'")
        response = collection.query.near_vector(
            near_vector=query_vector,
            limit=limit,
            filters=weaviate_filter,
            # [NEW] Return metadata (distance) along with properties useful for error analysis
            return_metadata=wvc.query.MetadataQuery(distance=True),
            return_properties=[
                "function_name", "error_message", "error_code",
                "timestamp_utc", "trace_id", "parent_span_id", "span_id"
            ]
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
        logger.error("Error during Weaviate error search: %s", e)
        raise WeaviateConnectionError(f"Failed to execute 'search_errors_by_message': {e}")


def search_functions(query: str, limit: int = 5, filters: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Searches function definitions from the [VectorWaveFunctions] collection using natural language (nearText).
    """
    try:
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.COLLECTION_NAME)
        weaviate_filter = _build_weaviate_filters(filters)

        vectorizer = get_vectorizer()

        if vectorizer:
            print("[VectorWave] Searching with Python client (near_vector)...")
            try:
                query_vector = vectorizer.embed(query)
            except Exception as e:
                print(f"Error vectorizing query with Python client: {e}")
                raise WeaviateConnectionError(f"Query vectorization failed: {e}")

            response = collection.query.near_vector(
                near_vector=query_vector,
                limit=limit,
                filters=weaviate_filter,
                return_metadata=wvc.query.MetadataQuery(distance=True)
            )

        else:
            print("[VectorWave] Searching with Weaviate module (near_text)...")
            response = collection.query.near_text(
                query=query,
                limit=limit,
                filters=weaviate_filter,
                return_metadata=wvc.query.MetadataQuery(distance=True)
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
        logger.error("Error during Weaviate search: %s", e)
        raise WeaviateConnectionError(f"Failed to execute 'search_functions': {e}")


def search_executions(
        limit: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = "timestamp_utc",
        sort_ascending: bool = False
) -> List[Dict[str, Any]]:
    """
    Searches execution logs from the [VectorWaveExecutions] collection using filtering and sorting.
    """
    try:
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        weaviate_filter = _build_weaviate_filters(filters)
        weaviate_sort = None

        if sort_by:
            weaviate_sort = wvc.query.Sort.by_property(
                name=sort_by,
                ascending=sort_ascending
            )

        response = collection.query.fetch_objects(
            limit=limit,
            filters=weaviate_filter,
            sort=weaviate_sort
        )
        results = []
        for obj in response.objects:
            props = obj.properties.copy()
            for key, value in props.items():
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
        limit: int = 1
) -> Optional[Dict[str, Any]]:
    """
    Searches the 'VectorWaveExecutions' collection for a successful log
    with a semantically similar input vector, used for Semantic Caching.

    Args:
        query_vector: The vector of the serialized function input.
        function_name: The name of the function to filter by.
        threshold: The similarity threshold (0.0 to 1.0). Only returns results
                   where certainty >= threshold.
        limit: The maximum number of results to return (default 1 for caching).

    Returns:
        The properties of the best matching execution log if found, otherwise None.
    """
    try:
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.EXECUTION_COLLECTION_NAME)

        # 1. Build Base Filters: Must be a successful execution of the target function
        base_filters = {
            "status": "SUCCESS",
            "function_name": function_name
        }
        weaviate_filter = _build_weaviate_filters(base_filters)

        # Weaviate's near_vector uses `certainty` for similarity thresholding.
        certainty_threshold = threshold

        logger.info(
            f"Performing near_vector cache search for '{function_name}' with certainty >= {certainty_threshold}")

        response = collection.query.near_vector(
            near_vector=query_vector,
            limit=limit,
            filters=weaviate_filter,
            certainty=certainty_threshold,
            # We only need the return value and metadata
            return_metadata=wvc.query.MetadataQuery(distance=True, certainty=True),
            return_properties=["return_value", "timestamp_utc"]
        )

        if response.objects:
            # Found a match. Extract the properties and metadata.
            best_match = response.objects[0]

            result = {
                'return_value': best_match.properties.get('return_value'),
                'metadata': {
                    'distance': best_match.metadata.distance,
                    'certainty': best_match.metadata.certainty,
                },
                'uuid': str(best_match.uuid)
            }

            return result

        return None

    except Exception as e:
        logger.error(f"Error during Weaviate cache search for '{function_name}': {e}", exc_info=True)
        # Fail gracefully: a cache search failure should not prevent execution.
        return None


def search_functions_hybrid(
        query: str,
        limit: int = 5,
        filters: Optional[Dict[str, Any]] = None,
        alpha: float = 0.5
) -> List[Dict[str, Any]]:
    """
    Performs Hybrid Search (Keyword + Vector) on function definitions.

    Args:
        query: The search query string.
        limit: Max number of results.
        filters: Dictionary of filters.
        alpha: Weight for hybrid search (0.0 = Keyword only, 1.0 = Vector only, 0.5 = Balanced).
    """
    try:
        settings: WeaviateSettings = get_weaviate_settings()
        client: weaviate.WeaviateClient = get_cached_client()

        collection = client.collections.get(settings.COLLECTION_NAME)
        weaviate_filter = _build_weaviate_filters(filters)

        vectorizer = get_vectorizer()

        # 1. Python Vectorizer
        if vectorizer:
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
