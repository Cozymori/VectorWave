"""OpenTelemetry export for VectorWave spans (issue #29).

Opt-in OTel emitter that runs alongside the existing Weaviate batch
logging. Each VectorWave span produces:

- a Weaviate row in VectorWaveExecutions (search / replay / cache lookup)
- and, when `OTEL_ENABLED=true`, an OTel span exported via OTLP / stdout
  with the same timing, status, and attributes (so users can pipe the
  data into Jaeger, Tempo, DataDog, Honeycomb, etc.)

We deliberately do not try to mirror the parent/child hierarchy or
override OTel's trace/span ids — VectorWave's ids are carried as
attributes (``vectorwave.trace_id``, ``vectorwave.span_id``,
``vectorwave.parent_span_id``) so users can correlate. A future PR can
add proper hierarchy plumbing once we settle on a context-propagation
strategy.
"""
from __future__ import annotations

import logging
import os
from functools import lru_cache
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


def is_otel_enabled() -> bool:
    """Whether to emit OTel spans this process. Defaults off."""
    return os.environ.get("OTEL_ENABLED", "").lower() in ("1", "true", "yes")


@lru_cache(maxsize=1)
def _get_tracer():
    """Lazy-initialised global tracer. Returns None if OTel SDK is missing
    or initialisation fails — callers must handle the None case so a
    misconfigured exporter doesn't crash the request hot path."""
    try:
        from opentelemetry import trace
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
    except ImportError:
        logger.warning(
            "OTEL_ENABLED=true but `opentelemetry-sdk` isn't installed. "
            "Install with `pip install 'vectorwave[otel]'` or pip install "
            "opentelemetry-sdk to use OTel export."
        )
        return None

    service_name = os.environ.get("OTEL_SERVICE_NAME", "vectorwave")
    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    # Wire the configured exporter. Order of preference: explicit OTLP
    # endpoint > stdout (default for dev / tests).
    if os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.trace.export import BatchSpanProcessor

            provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        except ImportError:
            logger.warning(
                "OTLP exporter unavailable; install `opentelemetry-exporter-otlp` "
                "or remove OTEL_EXPORTER_OTLP_ENDPOINT."
            )
        except Exception as e:
            logger.error(f"OTLP exporter init failed: {e}")
    else:
        # Default: stdout. Useful for `dev` / quick verification. Override
        # via the standard OTel env vars.
        try:
            from opentelemetry.sdk.trace.export import (
                BatchSpanProcessor,
                ConsoleSpanExporter,
            )

            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        except Exception as e:
            logger.error(f"OTel console exporter init failed: {e}")

    trace.set_tracer_provider(provider)
    return trace.get_tracer("vectorwave", "0.3.0")


# OTel attribute values must be primitives (str/int/float/bool) or a
# sequence of one of those. We coerce everything we send.
def _coerce_attr(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)) and all(isinstance(v, (str, int, float, bool)) for v in value):
        return list(value)
    return str(value)


def emit_span(
    span_properties: Dict[str, Any],
    start_time_ns: int,
    end_time_ns: int,
    error: Optional[BaseException] = None,
) -> None:
    """Emit one completed OTel span mirroring the VectorWave span.

    No-op if OTel isn't enabled or SDK init failed. Errors during emission
    are logged and swallowed — telemetry must never break the wrapped
    user function.
    """
    if not is_otel_enabled():
        return
    tracer = _get_tracer()
    if tracer is None:
        return

    try:
        from opentelemetry.trace import Status, StatusCode

        function_name = span_properties.get("function_name", "vectorwave_span")
        span = tracer.start_span(name=function_name, start_time=start_time_ns)

        attribute_skip = {"function_name", "trace_id", "span_id", "parent_span_id"}
        for key, value in span_properties.items():
            if value is None:
                continue
            if key in attribute_skip:
                # carry under a vectorwave.* namespace below
                continue
            span.set_attribute(f"vectorwave.{key}", _coerce_attr(value))

        # Original VectorWave identifiers — useful for cross-referencing
        # Weaviate logs against OTel traces.
        if span_properties.get("trace_id"):
            span.set_attribute("vectorwave.trace_id", str(span_properties["trace_id"]))
        if span_properties.get("span_id"):
            span.set_attribute("vectorwave.span_id", str(span_properties["span_id"]))
        if span_properties.get("parent_span_id"):
            span.set_attribute(
                "vectorwave.parent_span_id", str(span_properties["parent_span_id"])
            )

        status = span_properties.get("status")
        if status and status != "SUCCESS":
            span.set_status(
                Status(
                    StatusCode.ERROR,
                    span_properties.get("error_message") or status,
                )
            )
            if error is not None:
                span.record_exception(error)

        span.end(end_time=end_time_ns)
    except Exception as e:
        logger.warning(f"OTel emit failed (non-fatal): {e}")


def shutdown_otel() -> None:
    """Flush + shut down the OTel provider. Safe to call multiple times."""
    try:
        from opentelemetry import trace
        provider = trace.get_tracer_provider()
        shutdown = getattr(provider, "shutdown", None)
        if callable(shutdown):
            shutdown()
    except Exception:
        pass
