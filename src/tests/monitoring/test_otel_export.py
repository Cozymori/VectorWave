"""End-to-end tests for the OpenTelemetry mirror (issue #29).

Uses the OTel SDK's InMemorySpanExporter so we can assert on emitted
spans without standing up a real Jaeger/Tempo backend. The VectorWave
batch manager is stubbed (Lite mode + LanceDB tmp dir) so the test
only measures what the OTel layer emits, not what Weaviate sees.
"""
from __future__ import annotations

import pytest

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from vectorwave.core.decorator import vectorize


def _clear_caches():
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.store.factory import get_vector_store
    from vectorwave.vectorizer.factory import get_vectorizer
    from vectorwave.monitoring.otel import _get_tracer
    for fn in (get_batch_manager, get_weaviate_settings, get_vector_store, get_vectorizer, _get_tracer):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


# OTel only allows a single global TracerProvider per process. Install it once
# at session scope; tests just clear the exporter between runs.
@pytest.fixture(scope="session")
def _otel_session_exporter():
    exporter = InMemorySpanExporter()
    provider = TracerProvider(resource=Resource.create({"service.name": "vectorwave-test"}))
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    yield exporter


@pytest.fixture
def otel_capture(monkeypatch, tmp_path, _otel_session_exporter):
    """Per-test view of the session OTel exporter, plus a fresh Lite-mode env."""
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "vectorwave-test")
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.setenv("VECTORWAVE_LITE_PATH", str(tmp_path / "lance"))
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    monkeypatch.setenv("WEAVIATE_HOST", "otel-test.invalid")

    _clear_caches()

    # Route the production `_get_tracer` to the session TracerProvider so we
    # capture its spans without re-installing (OTel forbids re-setting).
    import vectorwave.monitoring.otel as otel_mod
    monkeypatch.setattr(otel_mod, "_get_tracer", lambda: trace.get_tracer("vectorwave", "0.3.0"))

    from vectorwave.store import get_vector_store
    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    store = get_vector_store()
    for coll in (settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME):
        if not store.collection_exists(coll):
            store.ensure_collection(coll, properties=[])

    _otel_session_exporter.clear()
    yield _otel_session_exporter
    _clear_caches()


def _force_flush() -> None:
    """SimpleSpanProcessor exports synchronously, but the dual-emit runs on
    the background tracer's thread pool when ASYNC_LOGGING=true. We use
    force_sync defaults so calls are synchronous, but flush the provider
    for safety."""
    provider = trace.get_tracer_provider()
    flush = getattr(provider, "force_flush", None)
    if callable(flush):
        flush()


def test_vectorize_success_emits_otel_span(otel_capture):
    @vectorize(search_description="otel success", sequence_narrative="otel success")
    def add(a, b):
        return a + b

    assert add(3, 4) == 7
    _force_flush()

    spans = otel_capture.get_finished_spans()
    fn_spans = [s for s in spans if s.name == "add"]
    assert len(fn_spans) >= 1, f"expected an 'add' span, got names: {[s.name for s in spans]}"
    span = fn_spans[-1]
    attrs = dict(span.attributes or {})
    assert attrs.get("vectorwave.status") == "SUCCESS"
    assert attrs.get("vectorwave.trace_id"), "trace_id must be carried as an attribute"
    assert attrs.get("vectorwave.span_id"), "span_id must be carried as an attribute"
    assert attrs.get("vectorwave.duration_ms") is not None
    # Status code from OTel SpanStatus
    assert span.status.status_code.name in ("UNSET", "OK"), span.status


def test_vectorize_error_emits_otel_span_with_error_status(otel_capture):
    @vectorize(search_description="otel error", sequence_narrative="otel error")
    def boom():
        raise ValueError("nope")

    with pytest.raises(ValueError):
        boom()
    _force_flush()

    spans = otel_capture.get_finished_spans()
    fn_spans = [s for s in spans if s.name == "boom"]
    assert len(fn_spans) >= 1
    span = fn_spans[-1]
    attrs = dict(span.attributes or {})
    assert attrs.get("vectorwave.status") == "ERROR"
    assert "nope" in (attrs.get("vectorwave.error_message") or "")
    assert span.status.status_code.name == "ERROR", span.status


def test_otel_disabled_emits_nothing(monkeypatch, tmp_path, _otel_session_exporter):
    """Sanity: with OTEL_ENABLED unset, no spans should reach the exporter."""
    monkeypatch.delenv("OTEL_ENABLED", raising=False)
    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.setenv("VECTORWAVE_LITE_PATH", str(tmp_path / "lance"))
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    monkeypatch.setenv("WEAVIATE_HOST", "otel-off.invalid")

    _clear_caches()

    from vectorwave.store import get_vector_store
    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    store = get_vector_store()
    for coll in (settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME):
        if not store.collection_exists(coll):
            store.ensure_collection(coll, properties=[])

    _otel_session_exporter.clear()

    @vectorize(search_description="off", sequence_narrative="off")
    def quiet(x):
        return x

    quiet(1)
    _force_flush()

    assert _otel_session_exporter.get_finished_spans() == ()
    _clear_caches()
