"""Fixtures shared by VectorWave benchmarks.

The benchmarks measure pure Python overhead in `@vectorize`, so we stub the
batch manager / alerter / vectorizer to take their cost out of the numbers.
The point isn't to micro-optimise infra; it's to know how much CPU
VectorWave adds to a wrapped function call before deciding what (if
anything) to port to Rust.
"""
from __future__ import annotations

from typing import Any, List

import pytest

from vectorwave.models.db_config import WeaviateSettings


class _StubBatchManager:
    """No-op batch manager that just counts calls. Avoids the Rust queue and
    the real Weaviate connection so we measure only the wrapper / tracer cost."""

    def __init__(self):
        self.add_calls = 0
        self._initialized = True
        self._shutdown_done = False
        self.settings = WeaviateSettings()
        self.client = None

    def add_object(self, **kwargs):
        self.add_calls += 1

    def shutdown(self):
        self._shutdown_done = True


class _StubAlerter:
    def __init__(self):
        self.notify_calls = 0

    def notify(self, _props):
        self.notify_calls += 1


@pytest.fixture
def stubbed_vectorize_env(monkeypatch, tmp_path):
    """Patches the heavy collaborators so `@vectorize` runs purely in Python.

    Returns the stub batch manager so tests can assert on call counts if they
    want to. Each test gets its own batch + alerter instance.
    """
    batch = _StubBatchManager()
    alerter = _StubAlerter()

    # Force a no-op Python vectorizer (None means "skip vectorisation").
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("WEAVIATE_HOST", "stub.invalid")
    monkeypatch.setenv("ASYNC_LOGGING", "false")

    # Use an isolated cache file under tmp so concurrent benchmarks don't
    # collide on .vectorwave_functions_cache.json in the repo root.
    monkeypatch.setenv("CUSTOM_PROPERTIES_FILE_PATH", str(tmp_path / ".vw_props"))

    # Clear and re-stub all cached singletons.
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    from vectorwave.monitoring.alert.factory import get_alerter
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer, get_alerter):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()

    monkeypatch.setattr("vectorwave.core.decorator.get_batch_manager", lambda *a, **k: batch)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_batch_manager", lambda *a, **k: batch)
    monkeypatch.setattr("vectorwave.monitoring.tracer.get_alerter", lambda *a, **k: alerter)

    yield batch
