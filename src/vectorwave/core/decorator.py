# src/vectorwave/core/decorator.py

import logging
import inspect
from functools import wraps

from weaviate.util import generate_uuid5

from ..batch.batch import get_batch_manager
from ..models.db_config import get_weaviate_settings
from ..monitoring.tracer import trace_root, trace_span
from ..vectorizer.factory import get_vectorizer
from ..utils.function_cache import function_cache_manager

# Create module-level logger
logger = logging.getLogger(__name__)

def vectorize(search_description: str,
              sequence_narrative: str,
              capture_return_value: bool = False,
              **execution_tags):
    """
    VectorWave Decorator
    ...
    """

    def decorator(func):

        is_async_func = inspect.iscoroutinefunction(func)

        func_uuid = None
        valid_execution_tags = {}

        try:
            module_name = func.__module__
            function_name = func.__name__

            func_identifier = f"{module_name}.{function_name}"
            func_uuid = generate_uuid5(func_identifier)

            static_properties = {
                "function_name": function_name,
                "module_name": module_name,
                "docstring": inspect.getdoc(func) or "",
                "source_code": inspect.getsource(func),
                "search_description": search_description,
                "sequence_narrative": sequence_narrative
            }

            settings = get_weaviate_settings()

            if execution_tags:
                if not settings.custom_properties:
                    logger.warning(
                        f"Function '{function_name}' provided execution_tags {list(execution_tags.keys())} "
                        f"but no .weaviate_properties file was loaded. These tags will be IGNORED."
                    )
                else:
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

            static_properties.update(valid_execution_tags)

            # 2. contents hash
            current_content_hash = function_cache_manager.calculate_content_hash(func_identifier, static_properties)

            # 3. check hash
            if function_cache_manager.is_cached_and_unchanged(func_uuid, current_content_hash):
                # if not changed jump db writing.
                logger.info(f"Function '{function_name}' (UUID: {func_uuid[:8]}...) is UNCHANGED. Skipping DB write.")
            else:
                # if hash changed or new function db write
                logger.info(f"Function '{function_name}' (UUID: {func_uuid[:8]}...) is NEW or CHANGED. Writing to DB.")

                batch = get_batch_manager()
                vectorizer = get_vectorizer()
                vector_to_add = None

                if vectorizer:
                    try:
                        logger.info(f"Vectorizing '{function_name}' using Python vectorizer...")
                        vector_to_add = vectorizer.embed(search_description)
                    except Exception as e:
                        logger.warning(f"Failed to vectorize '{function_name}' with Python client: {e}")

                batch.add_object(
                    collection=settings.COLLECTION_NAME,
                    properties=static_properties,
                    uuid=func_uuid,
                    vector=vector_to_add
                )

                # 4. update cash
                function_cache_manager.update_cache(func_uuid, current_content_hash)

        except Exception as e:
            logger.error("Error in @vectorize setup for '%s': %s", func.__name__, e)

            if is_async_func:
                @wraps(func)
                async def original_async_func_wrapper(*args, **kwargs):
                    return await func(*args, **kwargs)
                return original_async_func_wrapper
            else:
                @wraps(func)
                def original_sync_func_wrapper(*args, **kwargs):
                    return func(*args, **kwargs)
                return original_sync_func_wrapper

        if is_async_func:

            @trace_root()
            @trace_span(attributes_to_capture=['function_uuid', 'team', 'priority', 'run_id'],
                        capture_return_value=capture_return_value)
            @wraps(func)
            async def inner_wrapper(*args, **kwargs):
                original_kwargs = kwargs.copy()
                keys_to_remove = list(valid_execution_tags.keys())
                keys_to_remove.append('function_uuid')
                for key in execution_tags.keys():
                    if key not in keys_to_remove:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    original_kwargs.pop(key, None)

                return await func(*args, **original_kwargs)

            @wraps(func)
            async def outer_wrapper(*args, **kwargs):
                full_kwargs = kwargs.copy()
                full_kwargs.update(valid_execution_tags)
                full_kwargs['function_uuid'] = func_uuid

                return await inner_wrapper(*args, **full_kwargs)

            return outer_wrapper

        else:  # original sync logic

            @trace_root()
            @trace_span(attributes_to_capture=['function_uuid', 'team', 'priority', 'run_id'],
                        capture_return_value=capture_return_value)
            @wraps(func)
            def inner_wrapper(*args, **kwargs):
                original_kwargs = kwargs.copy()
                keys_to_remove = list(valid_execution_tags.keys())
                keys_to_remove.append('function_uuid')
                for key in execution_tags.keys():
                    if key not in keys_to_remove:
                        keys_to_remove.append(key)
                for key in keys_to_remove:
                    original_kwargs.pop(key, None)

                return func(*args, **original_kwargs)

            @wraps(func)
            def outer_wrapper(*args, **kwargs):
                full_kwargs = kwargs.copy()
                full_kwargs.update(valid_execution_tags)
                full_kwargs['function_uuid'] = func_uuid

                return inner_wrapper(*args, **full_kwargs)

            return outer_wrapper

    return decorator