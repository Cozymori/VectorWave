import logging
import inspect
import time
import traceback
import json
from functools import wraps
from contextvars import ContextVar
from typing import Optional, List, Dict, Any, Callable
from uuid import uuid4
from datetime import datetime, timezone

from .alert.base import BaseAlerter
from ..batch.batch import get_batch_manager
from ..models.db_config import get_weaviate_settings, WeaviateSettings
from .alert.factory import get_alerter
from ..vectorizer.factory import get_vectorizer
from ..database.db_search import check_semantic_drift
from ..utils.context import execution_source_context

# Create module-level logger
logger = logging.getLogger(__name__)


class TraceCollector:
    def __init__(self, trace_id: str):
        self.trace_id = trace_id
        self.settings: WeaviateSettings = get_weaviate_settings()
        self.batch = get_batch_manager()
        self.alerter: BaseAlerter = get_alerter()
        self.alert_sent: bool = False


current_tracer_var: ContextVar[Optional[TraceCollector]] = ContextVar('current_tracer', default=None)
current_span_id_var: ContextVar[Optional[str]] = ContextVar('current_span_id', default=None)


def _mask_and_serialize(data: Any, sensitive_keys: set) -> Any:
    """
    Recursively masks sensitive data in dicts/lists and prepares for serialization.
    """
    if isinstance(data, dict):
        new_dict = {}
        for k, v in data.items():
            if str(k).lower() in sensitive_keys:
                new_dict[k] = "[MASKED]"
            else:
                new_dict[k] = _mask_and_serialize(v, sensitive_keys)
        return new_dict

    if isinstance(data, list):
        return [_mask_and_serialize(item, sensitive_keys) for item in data]

    if not isinstance(data, (str, int, float, bool, type(None))):
        try:
            return str(data)
        except Exception:
            return "[SERIALIZATION_ERROR]"

    return data


def _capture_span_attributes(
        attributes_to_capture: Optional[List[str]],
        kwargs: Dict[str, Any],
        func_name: str,
        sensitive_keys: set
) -> Dict[str, Any]:
    captured_attributes = {}
    if not attributes_to_capture:
        return captured_attributes

    try:
        for attr_name in attributes_to_capture:
            if attr_name in kwargs:
                if attr_name.lower() in sensitive_keys:
                    processed_value = "[MASKED]"
                else:
                    raw_value = kwargs[attr_name]
                    processed_value = _mask_and_serialize(raw_value, sensitive_keys)

                captured_attributes[attr_name] = processed_value

    except Exception as e:
        logger.warning("Failed to capture attributes for '%s': %s", func_name, e)

    return captured_attributes


def _determine_error_code(tracer: "TraceCollector", e: Exception) -> str:
    error_code = None
    try:
        if hasattr(e, 'error_code'):
            error_code = str(e.error_code)
        elif tracer.settings.failure_mapping:
            exception_class_name = type(e).__name__
            if exception_class_name in tracer.settings.failure_mapping:
                error_code = tracer.settings.failure_mapping[exception_class_name]

        if not error_code:
            error_code = type(e).__name__

    except Exception as e_code:
        logger.warning(f"Failed to determine error_code: {e_code}")
        error_code = "UNKNOWN_ERROR_CODE_FAILURE"

    return error_code


def _create_span_properties(
        tracer: "TraceCollector",
        func: Callable,
        start_time: float,
        status: str,
        error_msg: Optional[str],
        error_code: Optional[str],
        captured_attributes: Dict[str, Any],
        my_span_id: str,
        parent_span_id: Optional[str],
        capture_return_value: bool,
        result: Optional[Any],
) -> Dict[str, Any]:
    duration_ms = (time.perf_counter() - start_time) * 1000

    return_value_to_log = None
    if capture_return_value and status == "SUCCESS" and result is not None:
        if not isinstance(result, (str, int, float, bool, list, dict, type(None))):
            return_value_to_log = str(result)
        else:
            return_value_to_log = result

    span_properties = {
        "trace_id": tracer.trace_id,
        "span_id": my_span_id,
        "parent_span_id": parent_span_id,
        "function_name": func.__name__,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "duration_ms": duration_ms,
        "status": status,
        "error_message": error_msg,
        "error_code": error_code,
        "return_value": return_value_to_log,
        "exec_source": execution_source_context.get()
    }

    if tracer.settings.global_custom_values:
        span_properties.update(tracer.settings.global_custom_values)

    span_properties.update(captured_attributes)
    return span_properties


def _create_input_vector_data(
        func_name: str,
        args: tuple,
        kwargs: Dict[str, Any],
        sensitive_keys: set
) -> Dict[str, Any]:
    """
    To enhance the quality of SC and DD, we extract only
    the core arguments and vectorize them
    """
    # 1. Process/Mask positional and keyword arguments
    processed_args = _mask_and_serialize(list(args), sensitive_keys)
    processed_kwargs = _mask_and_serialize(kwargs, sensitive_keys)


    texts_for_vector = [f"Function Context: {func_name}"]

    for val in processed_args:
        if val != "[MASKED]":
            texts_for_vector.append(str(val))

    for key, val in processed_kwargs.items():
        if val != "[MASKED]":
            texts_for_vector.append(f"{key}: {val}")

    vector_text = " ".join(texts_for_vector)

    canonical_data = {
        "function": func_name,
        "args": processed_args,
        "kwargs": processed_kwargs
    }

    return {
        "text": vector_text,
        "properties": canonical_data
    }


def _deserialize_return_value(return_value_str: Optional[str]) -> Any:
    """
    Attempts to deserialize a return value string (stored in DB)
    back to a Python object.
    [FIX] Now attempts json.loads for ALL strings to correctly unquote simple strings.
    """
    if return_value_str is None:
        return None

    try:
        # Try to deserialize everything (dicts, lists, and quoted strings like '"result"')
        return json.loads(return_value_str)
    except (json.JSONDecodeError, TypeError):
        # Fallback: If it's a raw string that wasn't JSON encoded or failed
        # e.g., "Plain String" without quotes
        return return_value_str


def trace_root() -> Callable:
    """
    Decorator factory for the workflow's entry point function.
    Creates and sets the TraceCollector in ContextVar.
    """

    def decorator(func: Callable) -> Callable:

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                if current_tracer_var.get() is not None:
                    return await func(*args, **kwargs)

                trace_id = kwargs.pop('trace_id', str(uuid4()))
                tracer = TraceCollector(trace_id=trace_id)
                token = current_tracer_var.set(tracer)
                token_span = current_span_id_var.set(None)

                try:
                    return await func(*args, **kwargs)
                finally:
                    current_tracer_var.reset(token)

            return async_wrapper

        else:  # original sync logic
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                if current_tracer_var.get() is not None:
                    return func(*args, **kwargs)

                trace_id = kwargs.pop('trace_id', str(uuid4()))
                tracer = TraceCollector(trace_id=trace_id)
                token = current_tracer_var.set(tracer)
                token_span = current_span_id_var.set(None)

                try:
                    return func(*args, **kwargs)
                finally:
                    current_tracer_var.reset(token)

            return sync_wrapper

    return decorator


def trace_span(
        _func: Optional[Callable] = None,
        *,
        attributes_to_capture: Optional[List[str]] = None,
        capture_return_value: bool = False
) -> Callable:
    """
    Decorator to capture function execution as a 'span'.
    Can be used as @trace_span or @trace_span(attributes_to_capture=[...]).
    """

    def decorator(func: Callable) -> Callable:

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                tracer = current_tracer_var.get()
                if not tracer:
                    return await func(*args, **kwargs)

                parent_span_id = current_span_id_var.get()
                my_span_id = str(uuid4())
                token = current_span_id_var.set(my_span_id)

                start_time = time.perf_counter()
                status = "SUCCESS"
                error_msg = None
                error_code = None
                result = None
                span_properties = None
                vector_to_add: Optional[List[float]] = None
                return_value_log: Optional[str] = None

                captured_attributes = _capture_span_attributes(
                    attributes_to_capture, kwargs, func.__name__, tracer.settings.sensitive_keys
                )

                if capture_return_value:
                    vectorizer = get_vectorizer()
                    if vectorizer:
                        input_vector_data = _create_input_vector_data(
                            func_name=func.__name__,
                            args=args,
                            kwargs=kwargs,
                            sensitive_keys=tracer.settings.sensitive_keys
                        )
                        try:
                            # If successful, this vector will be saved to the DB
                            vector_to_add = vectorizer.embed(input_vector_data['text'])
                        except Exception as ve:
                            logger.warning(f"Failed to vectorize input for '{func.__name__}' (Async): {ve}")

                try:
                    result = await func(*args, **kwargs)

                    if capture_return_value:
                        processed_result = _mask_and_serialize(result, tracer.settings.sensitive_keys)
                        try:
                            return_value_log = json.dumps(processed_result)
                        except TypeError:
                            return_value_log = str(processed_result)

                except Exception as e:
                    status = "ERROR"
                    error_msg = traceback.format_exc()
                    error_code = _determine_error_code(tracer, e)

                    span_properties = _create_span_properties(
                        tracer, func, start_time, status, error_msg, error_code, captured_attributes,
                        my_span_id=my_span_id,
                        parent_span_id=parent_span_id,
                        capture_return_value=capture_return_value,
                        result=None,
                    )

                    try:
                        vectorizer = get_vectorizer()
                        if vectorizer:
                            simple_error_msg = str(e)
                            vector_to_add = vectorizer.embed(simple_error_msg)
                    except Exception as ve:
                        logger.warning(f"Failed to vectorize error message for '{func.__name__}': {ve}")

                    try:
                        if not tracer.alert_sent:
                            tracer.alerter.notify(span_properties)
                            tracer.alert_sent = True
                    except Exception as alert_e:
                        logger.warning(f"Alerter failed to notify: {alert_e}")

                    raise e

                finally:
                    if status == "SUCCESS":
                        span_properties = _create_span_properties(
                            tracer, func, start_time, status, error_msg, error_code, captured_attributes,
                            my_span_id=my_span_id,
                            parent_span_id=parent_span_id,
                            capture_return_value=capture_return_value,
                            result=return_value_log
                        )

                    if tracer.settings.DRIFT_DETECTION_ENABLED and vector_to_add:
                        is_drift, dist, nearest_id = check_semantic_drift(
                            vector=vector_to_add,
                            function_name=func.__name__,
                            threshold=tracer.settings.DRIFT_DISTANCE_THRESHOLD,
                            k=tracer.settings.DRIFT_NEIGHBOR_AMOUNT
                        )

                        if is_drift:
                            drift_alert_props = span_properties.copy()
                            drift_alert_props["status"] = "WARNING" # 상태 변경
                            drift_alert_props["error_code"] = "SEMANTIC_DRIFT"
                            drift_alert_props["error_message"] = (
                                f"Anomaly detected in data distribution.\n"
                                f"Distance to nearest neighbor: {dist:.4f} (Threshold: {tracer.settings.DRIFT_DISTANCE_THRESHOLD})\n"
                                f"Nearest Log UUID: {nearest_id}"
                            )

                            try:
                                tracer.alerter.notify(drift_alert_props)
                            except Exception as e:
                                logger.warning(f"Failed to send drift alert: {e}")

                            span_properties["status"] = "ANOMALY"
                            span_properties["error_code"] = "SEMANTIC_DRIFT"
                            span_properties["error_message"] = drift_alert_props["error_message"]

                    if span_properties:
                        try:
                            tracer.batch.add_object(
                                collection=tracer.settings.EXECUTION_COLLECTION_NAME,
                                properties=span_properties,
                                vector=vector_to_add
                            )
                        except Exception as e:
                            logger.error("Failed to log span for '%s' (trace_id: %s): %s", func.__name__,
                                         tracer.trace_id, e)

                    current_span_id_var.reset(token)

                return result

            return async_wrapper

        else:  # (Sync Function)
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                tracer = current_tracer_var.get()
                if not tracer:
                    return func(*args, **kwargs)

                parent_span_id = current_span_id_var.get()
                my_span_id = str(uuid4())
                token = current_span_id_var.set(my_span_id)

                start_time = time.perf_counter()
                status = "SUCCESS"
                error_msg = None
                error_code = None
                result = None
                span_properties = None
                vector_to_add: Optional[List[float]] = None
                return_value_log: Optional[str] = None

                captured_attributes = _capture_span_attributes(
                    attributes_to_capture, kwargs, func.__name__, tracer.settings.sensitive_keys
                )

                if capture_return_value:
                    vectorizer = get_vectorizer()
                    if vectorizer:
                        input_vector_data = _create_input_vector_data(
                            func_name=func.__name__,
                            args=args,
                            kwargs=kwargs,
                            sensitive_keys=tracer.settings.sensitive_keys
                        )
                        try:
                            # If successful, this vector will be saved to the DB
                            vector_to_add = vectorizer.embed(input_vector_data['text'])
                        except Exception as ve:
                            logger.warning(f"Failed to vectorize input for '{func.__name__}': {ve}")

                try:
                    result = func(*args, **kwargs)

                    if capture_return_value:
                        processed_result = _mask_and_serialize(result, tracer.settings.sensitive_keys)
                        try:
                            return_value_log = json.dumps(processed_result)
                        except TypeError:
                            return_value_log = str(processed_result)

                except Exception as e:
                    status = "ERROR"
                    error_msg = traceback.format_exc()
                    error_code = _determine_error_code(tracer, e)

                    span_properties = _create_span_properties(
                        tracer, func, start_time, status, error_msg, error_code, captured_attributes,
                        my_span_id=my_span_id,
                        parent_span_id=parent_span_id,
                        capture_return_value=capture_return_value,
                        result=None
                    )

                    try:
                        vectorizer = get_vectorizer()
                        if vectorizer:
                            simple_error_msg = str(e)
                            vector_to_add = vectorizer.embed(simple_error_msg)
                    except Exception as ve:
                        logger.warning(f"Failed to vectorize error message for '{func.__name__}': {ve}")

                    try:
                        if not tracer.alert_sent:
                            tracer.alerter.notify(span_properties)
                            tracer.alert_sent = True
                    except Exception as alert_e:
                        logger.warning(f"Alerter failed to notify: {alert_e}")

                    raise e

                finally:
                    if status == "SUCCESS":
                        span_properties = _create_span_properties(
                            tracer, func, start_time, status, error_msg, error_code, captured_attributes,
                            my_span_id=my_span_id,
                            parent_span_id=parent_span_id,
                            capture_return_value=capture_return_value,
                            result=return_value_log
                        )

                    if tracer.settings.DRIFT_DETECTION_ENABLED and vector_to_add:
                        is_drift, dist, nearest_id = check_semantic_drift(
                            vector=vector_to_add,
                            function_name=func.__name__,
                            threshold=tracer.settings.DRIFT_DISTANCE_THRESHOLD,
                            k=tracer.settings.DRIFT_NEIGHBOR_AMOUNT
                        )

                        if is_drift:
                            drift_alert_props = span_properties.copy()
                            drift_alert_props["status"] = "WARNING" # 상태 변경
                            drift_alert_props["error_code"] = "SEMANTIC_DRIFT"
                            drift_alert_props["error_message"] = (
                                f"Anomaly detected in data distribution.\n"
                                f"Distance to nearest neighbor: {dist:.4f} (Threshold: {tracer.settings.DRIFT_DISTANCE_THRESHOLD})\n"
                                f"Nearest Log UUID: {nearest_id}"
                            )

                            try:
                                tracer.alerter.notify(drift_alert_props)
                            except Exception as e:
                                logger.warning(f"Failed to send drift alert: {e}")

                            span_properties["status"] = "ANOMALY"
                            span_properties["error_code"] = "SEMANTIC_DRIFT"
                            span_properties["error_message"] = drift_alert_props["error_message"]

                    if span_properties:
                        try:
                            tracer.batch.add_object(
                                collection=tracer.settings.EXECUTION_COLLECTION_NAME,
                                properties=span_properties,
                                vector=vector_to_add
                            )
                        except Exception as e:
                            logger.error("Failed to log span for '%s' (trace_id: %s): %s", func.__name__,
                                         tracer.trace_id, e)

                    current_span_id_var.reset(token)

                return result

            return sync_wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)
