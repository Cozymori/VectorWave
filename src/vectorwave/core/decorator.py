import inspect
import logging
from functools import wraps
from typing import List, Optional, Dict, Any

from weaviate.util import generate_uuid5

from ..batch.batch import get_batch_manager
from ..models.db_config import get_weaviate_settings
from ..monitoring.tracer import trace_root, trace_span
from ..utils.function_cache import function_cache_manager
from ..utils.return_caching_utils import _check_and_return_cached_result
from ..vectorizer.factory import get_vectorizer
from ..utils.context import execution_source_context

logger = logging.getLogger(__name__)

PENDING_FUNCTIONS: List[Dict[str, Any]] = []


def vectorize(search_description: Optional[str] = None,
              sequence_narrative: Optional[str] = None,
              auto: bool = False,
              capture_return_value: bool = False,
              semantic_cache: bool = False,
              cache_threshold: float = 0.9,
              replay: bool = False,
              attributes_to_capture: Optional[List[str]] = None,
              **execution_tags):
    """
    VectorWave Decorator with Auto-Generation support.
    """

    if semantic_cache:
        if get_vectorizer() is None:
            logger.warning(
                f"Semantic caching requested for '{search_description}' but no Python vectorizer is configured. "
                f"Disabling semantic_cache."
            )
            semantic_cache = False

    if semantic_cache and not capture_return_value:
        capture_return_value = True

    if replay and not capture_return_value:
        capture_return_value = True

    def decorator(func):
        is_async_func = inspect.iscoroutinefunction(func)

        module_name = func.__module__
        function_name = func.__name__
        func_identifier = f"{module_name}.{function_name}"
        func_uuid = generate_uuid5(func_identifier)

        # Prepare attributes to capture
        final_attributes = ['function_uuid', 'team', 'priority', 'run_id', 'exec_source']
        if attributes_to_capture:
            for attr in attributes_to_capture:
                if attr not in final_attributes:
                    final_attributes.append(attr)

        if replay:
            try:
                sig = inspect.signature(func)
                for param_name in sig.parameters:
                    if param_name not in ('self', 'cls') and param_name not in final_attributes:
                        final_attributes.append(param_name)
            except Exception as e:
                logger.warning(f"Failed to inspect signature for replay auto-capture in '{function_name}': {e}")

        # Extract Execution Tags
        valid_execution_tags = {}
        settings = get_weaviate_settings()
        if execution_tags and settings.custom_properties:
            allowed_keys = set(settings.custom_properties.keys())
            for key, value in execution_tags.items():
                if key in allowed_keys:
                    valid_execution_tags[key] = value
                else:
                    logger.warning(
                        "Function '%s' has undefined execution_tag: '%s'. "
                        "This tag will be IGNORED. Please add it to your .weaviate_properties file.",
                        function_name,
                        key
                    )

        try:
            # define static properties
            docstring = inspect.getdoc(func) or ""
            source_code = inspect.getsource(func)

            static_properties = {
                "function_name": function_name,
                "module_name": module_name,
                "docstring": docstring,
                "source_code": source_code,
                "search_description": search_description,
                "sequence_narrative": sequence_narrative
            }
            static_properties.update(valid_execution_tags)

            if auto:
                logger.info(f"Function '{function_name}' registered for auto-metadata generation.")
                PENDING_FUNCTIONS.append({
                    "func_name": function_name,
                    "func_uuid": func_uuid,
                    "func_identifier": func_identifier,
                    "static_properties": static_properties
                })
            else:
                # [Existing] Immediate Registration Mode
                current_content_hash = function_cache_manager.calculate_content_hash(func_identifier, static_properties)

                if function_cache_manager.is_cached_and_unchanged(func_uuid, current_content_hash):
                    logger.info(f"Function '{function_name}' is UNCHANGED. Skipping DB write.")
                else:
                    logger.info(f"Function '{function_name}' is NEW or CHANGED. Writing to DB.")
                    batch = get_batch_manager()
                    vectorizer = get_vectorizer()
                    vector_to_add = None

                    if vectorizer and search_description:
                        try:
                            vector_to_add = vectorizer.embed(search_description)
                        except Exception as e:
                            logger.warning(f"Failed to vectorize '{function_name}': {e}")

                    batch.add_object(
                        collection=settings.COLLECTION_NAME,
                        properties=static_properties,
                        uuid=func_uuid,
                        vector=vector_to_add
                    )
                    function_cache_manager.update_cache(func_uuid, current_content_hash)

        except Exception as e:
            logger.error("Error in @vectorize setup for '%s': %s", func.__name__, e)

        # --- Wrapper Logic ---

        if is_async_func:
            @trace_root()
            @trace_span(attributes_to_capture=final_attributes, capture_return_value=capture_return_value)
            @wraps(func)
            async def inner_wrapper(*args, **kwargs):
                # Remove injected tags from kwargs before calling original func
                clean_kwargs = {k: v for k, v in kwargs.items() if
                                k not in valid_execution_tags and k != 'function_uuid' and k != 'exec_source'}
                return await func(*args, **clean_kwargs)

            @wraps(func)
            async def outer_wrapper(*args, **kwargs):
                if semantic_cache:
                    cached = _check_and_return_cached_result(func, args, kwargs, function_name, cache_threshold, True)
                    if cached is not None: return cached

                full_kwargs = kwargs.copy()
                full_kwargs.update(valid_execution_tags)
                full_kwargs['function_uuid'] = func_uuid
                full_kwargs['exec_source'] = execution_source_context.get()
                return await inner_wrapper(*args, **full_kwargs)

            outer_wrapper._is_vectorized = True
            return outer_wrapper

        else:  # Sync wrapper
            @trace_root()
            @trace_span(attributes_to_capture=final_attributes, capture_return_value=capture_return_value)
            @wraps(func)
            def inner_wrapper(*args, **kwargs):
                clean_kwargs = {k: v for k, v in kwargs.items() if
                                k not in valid_execution_tags and k != 'function_uuid' and k != 'exec_source'}
                return func(*args, **clean_kwargs)

            @wraps(func)
            def outer_wrapper(*args, **kwargs):
                if semantic_cache:
                    cached = _check_and_return_cached_result(func, args, kwargs, function_name, cache_threshold, False)
                    if cached is not None: return cached

                full_kwargs = kwargs.copy()
                full_kwargs.update(valid_execution_tags)
                full_kwargs['function_uuid'] = func_uuid
                full_kwargs['exec_source'] = execution_source_context.get()
                return inner_wrapper(*args, **full_kwargs)

            outer_wrapper._is_vectorized = True
            return outer_wrapper

    return decorator