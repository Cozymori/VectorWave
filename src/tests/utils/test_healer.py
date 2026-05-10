"""End-to-end test for VectorWaveHealer.diagnose_and_heal.

Seeds VectorWaveFunctions with the buggy source code (vectorised by the local
HuggingFace vectorizer so search_functions_hybrid can find it via near_vector)
and VectorWaveExecutions with a mix of ERROR + SUCCESS logs, then calls
diagnose_and_heal. The LLM response is replayed from a committed VCR cassette,
so no API key is required at replay time.
"""
import time
from datetime import datetime, timezone, timedelta
from uuid import uuid4

import pytest

from vectorwave.utils.healer import VectorWaveHealer
from vectorwave.database.db import (
    create_execution_schema,
    create_vectorwave_schema,
    get_weaviate_client,
)


def _wait_for_count(coll, expected: int, timeout: float = 8.0) -> int:
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        last = len(coll.query.fetch_objects(limit=200).objects)
        if last >= expected:
            return last
        time.sleep(0.1)
    raise AssertionError(f"expected at least {expected} rows, got {last}")


def _clear_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    from vectorwave.core.llm.factory import get_llm_client
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer, get_llm_client):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture
def healer_e2e_env(weaviate_container, monkeypatch):
    """E2E setup: HF vectorizer for hybrid search, OpenAI key for the cassette LLM client,
    fresh function + execution collections."""
    monkeypatch.setenv("VECTORIZER", "huggingface")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-cassette-replay")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    _clear_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        for name in (settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME):
            if client.collections.exists(name):
                client.collections.delete(name)
        create_vectorwave_schema(client, settings)
        create_execution_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_caches()


def _seed_function_def(coll, *, name: str, source_code: str, vectorizer):
    description = f"function {name} that divides two numbers"
    return coll.data.insert(
        properties={
            "function_name": name,
            "module_name": "tests.utils.test_healer",
            "file_path": f"tests/utils/test_healer.py",
            "docstring": "Buggy divider",
            "source_code": source_code,
            "search_description": description,
            "sequence_narrative": "Receives a and b, returns a / b",
        },
        vector=vectorizer.embed(description),
    )


def _seed_execution(coll, *, function_name, status, error_message=None, error_code=None,
                    a=None, b=None, return_value=None, minutes_ago=1):
    ts = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()
    props = {
        "function_uuid": str(uuid4()),
        "function_name": function_name,
        "status": status,
        "duration_ms": 1.0,
        "timestamp_utc": ts,
    }
    if error_message:
        props["error_message"] = error_message
    if error_code:
        props["error_code"] = error_code
    if a is not None:
        props["a"] = a
    if b is not None:
        props["b"] = b
    if return_value is not None:
        props["return_value"] = str(return_value)
    return coll.data.insert(properties=props)


@pytest.mark.e2e
@pytest.mark.vcr
def test_diagnose_and_heal_returns_fixed_code(healer_e2e_env, monkeypatch):
    """Healer pulls the function source + recent error logs, asks the LLM for a
    fix (cassette), strips markdown, and returns the corrected code."""
    settings = healer_e2e_env

    # Allow extra input columns on the executions schema (Weaviate auto-schema is on by default)
    monkeypatch.setenv("AUTOSCHEMA_ENABLED", "true")

    from vectorwave.vectorizer.factory import get_vectorizer
    vectorizer = get_vectorizer()
    assert vectorizer is not None, "HuggingFace vectorizer must be initialised for this test"

    buggy_source = "def mybuggy(a, b):\n    return a / b\n"

    client = get_weaviate_client(settings)
    try:
        funcs = client.collections.get(settings.COLLECTION_NAME)
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_function_def(funcs, name="mybuggy", source_code=buggy_source, vectorizer=vectorizer)
        _seed_execution(
            execs, function_name="mybuggy", status="ERROR",
            error_message="ZeroDivisionError: division by zero", error_code="ZeroDivisionError",
            a=10, b=0,
        )
        _seed_execution(
            execs, function_name="mybuggy", status="ERROR",
            error_message="ZeroDivisionError: division by zero", error_code="ZeroDivisionError",
            a=5, b=0,
        )
        _seed_execution(
            execs, function_name="mybuggy", status="SUCCESS",
            a=10, b=2, return_value=5,
        )
        _wait_for_count(funcs, 1)
        _wait_for_count(execs, 3)
    finally:
        client.close()

    healer = VectorWaveHealer(model="gpt-4-turbo")
    suggestion = healer.diagnose_and_heal("mybuggy", lookback_minutes=120, create_pr=False)

    # The cassette replies with a fenced code block; healer strips the fence
    # and returns the inner source. The fix guards against b == 0.
    assert "def mybuggy" in suggestion
    assert "if b == 0" in suggestion
    assert "return a / b" in suggestion
    assert "```" not in suggestion, "markdown fences must be stripped from the response"


@pytest.mark.e2e
def test_diagnose_and_heal_reports_no_errors_when_logs_clean(healer_e2e_env):
    """If the function has no recent errors, healer returns an early-out
    message without calling the LLM at all."""
    settings = healer_e2e_env

    from vectorwave.vectorizer.factory import get_vectorizer
    vectorizer = get_vectorizer()

    client = get_weaviate_client(settings)
    try:
        funcs = client.collections.get(settings.COLLECTION_NAME)
        _seed_function_def(funcs, name="cleanfunc",
                           source_code="def cleanfunc(): return 42\n", vectorizer=vectorizer)
        _wait_for_count(funcs, 1)
    finally:
        client.close()

    result = VectorWaveHealer().diagnose_and_heal("cleanfunc", lookback_minutes=60, create_pr=False)
    assert "No errors found" in result
