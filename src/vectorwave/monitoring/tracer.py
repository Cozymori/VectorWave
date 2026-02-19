import logging
import inspect
import time
import traceback
import json
from dataclasses import dataclass
from functools import wraps, lru_cache
from contextvars import ContextVar
from typing import Optional, List, Dict, Any, Callable
from uuid import uuid4
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import vectorwave.vectorwave_core as vectorwave_core
from .alert.base import BaseAlerter
from ..batch.batch import get_batch_manager
from ..models.db_config import get_weaviate_settings, WeaviateSettings
from .alert.factory import get_alerter
from ..vectorizer.factory import get_vectorizer
from ..database.db_search import check_semantic_drift
from ..utils.context import execution_source_context
from ..utils.serialization import deserialize_return_value as _deserialize_return_value

logger = logging.getLogger(__name__)

# Global executor for background logging
_background_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="VectorWaveLogger")


class TraceCollector:
    def __init__(self, trace_id: str):
        self.trace_id = trace_id
        self.settings: WeaviateSettings = get_weaviate_settings()
        self.batch = get_batch_manager()
        self.alerter: BaseAlerter = get_alerter()
        self.alert_sent: bool = False


current_tracer_var: ContextVar[Optional[TraceCollector]] = ContextVar('current_tracer', default=None)
current_span_id_var: ContextVar[Optional[str]] = ContextVar('current_span_id', default=None)


@dataclass
class SpanContext:
    """Bundles all per-span logging data to avoid long parameter lists."""
    tracer: TraceCollector
    func: Callable
    start_time: float
    status: str
    error_msg: Optional[str]
    error_code: Optional[str]
    my_span_id: str
    parent_span_id: Optional[str]
    capture_return_value: bool
    result: Any
    attributes_to_capture: Optional[List[str]]
    args: tuple
    kwargs: Dict[str, Any]  # shallow-copied at creation to avoid race conditions
    exec_source: Optional[str]


@lru_cache(maxsize=2048)
def _get_cached_signature(func: Callable) -> inspect.Signature:
    return inspect.signature(func)


def _capture_span_attributes(
        attributes_to_capture: Optional[List[str]],
        args: tuple,
        kwargs: Dict[str, Any],
        func: Callable,
        sensitive_keys: set
) -> Dict[str, Any]:
    """
    Captures attribute values using cached function signature for performance.
    """
    captured_attributes = {}
    if not attributes_to_capture:
        return captured_attributes

    try:
        # 1. Use Cached Signature (Fast)
        sig = _get_cached_signature(func)
        valid_param_names = set(sig.parameters.keys())

        # Filter kwargs
        sig_kwargs = {k: v for k, v in kwargs.items() if k in valid_param_names}

        # Bind arguments
        bound = sig.bind(*args, **sig_kwargs)
        bound.apply_defaults()

        all_values = bound.arguments.copy()

        # 2. Merge extra tags (e.g., 'team', 'run_id')
        for key, value in kwargs.items():
            if key not in all_values and key in attributes_to_capture:
                all_values[key] = value

        # 3. Process & Mask
        for attr_name in attributes_to_capture:
            if attr_name in all_values:
                raw_value = all_values[attr_name]

                if attr_name.lower() in sensitive_keys:
                    processed_value = "[MASKED]"
                else:
                    processed_value = vectorwave_core.mask_and_serialize(raw_value, list(sensitive_keys))

                captured_attributes[attr_name] = processed_value

    except Exception as e:
        logger.warning("Failed to capture attributes for '%s': %s", func.__name__, e)

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
        exec_source: Optional[str]
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
        "exec_source": exec_source
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
    processed_args = vectorwave_core.mask_and_serialize(list(args), list(sensitive_keys))
    processed_kwargs = vectorwave_core.mask_and_serialize(kwargs, list(sensitive_keys))

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


def _perform_background_logging(ctx: SpanContext):
    """
    Executes logging tasks (Vectorization, DB Insert, Drift Check) in the background.
    Receives a SpanContext whose kwargs is already shallow-copied (race-condition safe).
    """
    try:
        # 1. Capture Attributes (Parsing inputs)
        captured_attributes = _capture_span_attributes(
            ctx.attributes_to_capture, ctx.args, ctx.kwargs, ctx.func,
            ctx.tracer.settings.sensitive_keys
        )

        vector_to_add: Optional[List[float]] = None
        return_value_log: Optional[str] = None
        vectorizer = get_vectorizer()

        # 2. Vectorize Inputs (If enabled)
        if ctx.capture_return_value and vectorizer is not None:
            try:
                input_vector_data = _create_input_vector_data(
                    func_name=ctx.func.__name__,
                    args=ctx.args,
                    kwargs=ctx.kwargs,
                    sensitive_keys=ctx.tracer.settings.sensitive_keys
                )
                vector_to_add = vectorizer.embed(input_vector_data['text'])
            except Exception as ve:
                logger.warning(f"Failed to vectorize input for '{ctx.func.__name__}': {ve}")

        # 3. Process Result
        if ctx.status == "SUCCESS" and ctx.capture_return_value:
            processed_result = vectorwave_core.mask_and_serialize(
                ctx.result, list(ctx.tracer.settings.sensitive_keys)
            )
            try:
                return_value_log = json.dumps(processed_result)
            except TypeError:
                return_value_log = str(processed_result)

        # 4. Vectorize Error (If needed)
        if ctx.status != "SUCCESS" and vectorizer is not None:
            try:
                vector_to_add = vectorizer.embed(str(ctx.error_msg))
            except Exception as ve:
                logger.warning(f"Failed to vectorize error message: {ve}")

        # 5. Create Span Properties
        span_properties = _create_span_properties(
            tracer=ctx.tracer,
            func=ctx.func,
            start_time=ctx.start_time,
            status=ctx.status,
            error_msg=ctx.error_msg,
            error_code=ctx.error_code,
            captured_attributes=captured_attributes,
            my_span_id=ctx.my_span_id,
            parent_span_id=ctx.parent_span_id,
            capture_return_value=ctx.capture_return_value,
            result=return_value_log if ctx.status == "SUCCESS" else None,
            exec_source=ctx.exec_source
        )

        # 6. Alerting (If Failure)
        if ctx.status != "SUCCESS":
            try:
                if not ctx.tracer.alert_sent:
                    ctx.tracer.alerter.notify(span_properties)
                    ctx.tracer.alert_sent = True
            except Exception as alert_e:
                logger.warning(f"Alerter failed: {alert_e}")

        # 7. Semantic Drift Detection
        if ctx.tracer.settings.DRIFT_DETECTION_ENABLED and vector_to_add and ctx.status == "SUCCESS":
            try:
                is_drift, dist, nearest_id = check_semantic_drift(
                    vector=vector_to_add, function_name=ctx.func.__name__,
                    threshold=ctx.tracer.settings.DRIFT_DISTANCE_THRESHOLD,
                    k=ctx.tracer.settings.DRIFT_NEIGHBOR_AMOUNT
                )
                if is_drift:
                    drift_alert_props = span_properties.copy()
                    drift_alert_props["status"] = "WARNING"
                    drift_alert_props["error_code"] = "SEMANTIC_DRIFT"
                    drift_alert_props["error_message"] = (
                        f"Anomaly detected.\nDistance: {dist:.4f} "
                        f"(Threshold: {ctx.tracer.settings.DRIFT_DISTANCE_THRESHOLD})\nNearest: {nearest_id}"
                    )
                    ctx.tracer.alerter.notify(drift_alert_props)

                    span_properties["status"] = "ANOMALY"
                    span_properties["error_code"] = "SEMANTIC_DRIFT"
                    span_properties["error_message"] = drift_alert_props["error_message"]
            except Exception as e:
                logger.warning(f"Failed to check semantic drift: {e}")

        # 8. Batch Insert
        if span_properties:
            try:
                ctx.tracer.batch.add_object(
                    collection=ctx.tracer.settings.EXECUTION_COLLECTION_NAME,
                    properties=span_properties,
                    vector=vector_to_add
                )
            except Exception as e:
                logger.error("Failed to log span: %s", e)

    except Exception as e:
        logger.error(f"Background logging failed for '{ctx.func.__name__}': {e}")


def _init_trace_root(kwargs):
    """Setup for trace_root. Returns token or None if already inside a trace."""
    if current_tracer_var.get() is not None:
        return None
    trace_id = kwargs.pop('trace_id', str(uuid4()))
    tracer = TraceCollector(trace_id=trace_id)
    token = current_tracer_var.set(tracer)
    current_span_id_var.set(None)
    return token


def trace_root() -> Callable:
    def decorator(func: Callable) -> Callable:
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                token = _init_trace_root(kwargs)
                if token is None:
                    return await func(*args, **kwargs)
                try:
                    return await func(*args, **kwargs)
                finally:
                    current_tracer_var.reset(token)
            return async_wrapper
        else:
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                token = _init_trace_root(kwargs)
                if token is None:
                    return func(*args, **kwargs)
                try:
                    return func(*args, **kwargs)
                finally:
                    current_tracer_var.reset(token)
            return sync_wrapper
    return decorator


def _dispatch_span_logging(ctx: SpanContext, use_async: bool, token):
    """Dispatches logging (sync or async) and resets the span context."""
    try:
        if use_async:
            _background_executor.submit(_perform_background_logging, ctx)
        else:
            _perform_background_logging(ctx)
    except Exception as log_e:
        logger.error(f"Error dispatching log for {ctx.func.__name__}: {log_e}")
    finally:
        current_span_id_var.reset(token)


def trace_span(
        _func: Optional[Callable] = None,
        *,
        attributes_to_capture: Optional[List[str]] = None,
        capture_return_value: bool = False,
        force_sync: bool = False
) -> Callable:
    def decorator(func: Callable) -> Callable:

        def should_use_async(tracer):
            return tracer.settings.ASYNC_LOGGING and not force_sync

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                tracer = current_tracer_var.get()
                if tracer is None:
                    return await func(*args, **kwargs)

                parent_span_id = current_span_id_var.get()
                my_span_id = str(uuid4())
                token = current_span_id_var.set(my_span_id)
                exec_source = execution_source_context.get()

                start_time = time.perf_counter()
                status = "SUCCESS"
                error_msg = None
                error_code = None
                result = None

                try:
                    result = await func(*args, **kwargs)
                except Exception as e:
                    status = "ERROR"
                    error_msg = traceback.format_exc()
                    error_code = _determine_error_code(tracer, e)
                    if error_code in tracer.settings.ignored_error_codes:
                        status = "FAILURE"
                        tracer.alert_sent = True
                    raise e
                finally:
                    ctx = SpanContext(
                        tracer=tracer, func=func, start_time=start_time,
                        status=status, error_msg=error_msg, error_code=error_code,
                        my_span_id=my_span_id, parent_span_id=parent_span_id,
                        capture_return_value=capture_return_value, result=result,
                        attributes_to_capture=attributes_to_capture,
                        args=args, kwargs=kwargs.copy(),  # shallow copy guards against caller mutation
                        exec_source=exec_source
                    )
                    _dispatch_span_logging(ctx, should_use_async(tracer), token)
                return result
            return async_wrapper

        else:  # Sync
            @wraps(func)
            def sync_wrapper(*args, **kwargs):
                tracer = current_tracer_var.get()
                if tracer is None:
                    return func(*args, **kwargs)

                parent_span_id = current_span_id_var.get()
                my_span_id = str(uuid4())
                token = current_span_id_var.set(my_span_id)
                exec_source = execution_source_context.get()

                start_time = time.perf_counter()
                status = "SUCCESS"
                error_msg = None
                error_code = None
                result = None

                try:
                    result = func(*args, **kwargs)
                except Exception as e:
                    status = "ERROR"
                    error_msg = traceback.format_exc()
                    error_code = _determine_error_code(tracer, e)
                    if error_code in tracer.settings.ignored_error_codes:
                        status = "FAILURE"
                        tracer.alert_sent = True
                    raise e
                finally:
                    ctx = SpanContext(
                        tracer=tracer, func=func, start_time=start_time,
                        status=status, error_msg=error_msg, error_code=error_code,
                        my_span_id=my_span_id, parent_span_id=parent_span_id,
                        capture_return_value=capture_return_value, result=result,
                        attributes_to_capture=attributes_to_capture,
                        args=args, kwargs=kwargs.copy(),  # shallow copy guards against caller mutation
                        exec_source=exec_source
                    )
                    _dispatch_span_logging(ctx, should_use_async(tracer), token)
                return result
            return sync_wrapper

    if _func is None:
        return decorator
    else:
        return decorator(_func)