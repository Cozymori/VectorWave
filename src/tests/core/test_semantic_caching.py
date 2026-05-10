"""End-to-end tests for the semantic-cache path of @vectorize.

The first call seeds Weaviate with the input vector + return value, the second
call vectorises the same input, finds the seeded row via near_vector, and
returns the cached value without re-executing the function. We use a local
HuggingFace vectorizer so no OpenAI key is required.
"""
import asyncio
import json
import time

import pytest

from vectorwave.core.decorator import vectorize
from vectorwave.database.db import (
    create_golden_dataset_schema,
    create_vectorwave_schema,
    create_execution_schema,
    get_weaviate_client,
)
from vectorwave.monitoring.tracer import _create_input_vector_data


# ---------------------------------------------------------------------------
# Unit test — pure masking logic in input-vector preparation
# ---------------------------------------------------------------------------

def test_create_input_vector_data_masks_sensitive_keys():
    """`_create_input_vector_data` must scrub sensitive keys from both the
    embedding text and the canonical properties dict."""
    data = _create_input_vector_data(
        func_name="test_func",
        args=(1, 2),
        kwargs={"amount": 100, "secret_key": "my_top_secret"},
        sensitive_keys={"secret_key"},
    )

    text = data["text"]
    assert "test_func" in text
    assert "amount" in text
    assert "[MASKED]" not in text
    assert "secret_key" not in text
    assert "my_top_secret" not in text

    props = data["properties"]
    assert props["function"] == "test_func"
    assert props["kwargs"]["amount"] == 100
    assert props["kwargs"]["secret_key"] == "[MASKED]"


# ---------------------------------------------------------------------------
# E2E helpers
# ---------------------------------------------------------------------------

def _wait_for_count(coll, expected: int, timeout: float = 8.0) -> int:
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        last = len(coll.query.fetch_objects(limit=200).objects)
        if last >= expected:
            return last
        time.sleep(0.1)
    raise AssertionError(f"expected at least {expected} rows, got {last}")


def _wait_until_indexed(settings, expected_count: int, timeout: float = 5.0) -> None:
    """Poll the executions collection until at least `expected_count` rows are visible.
    Used to guarantee that a previous call's log is searchable before the next call.
    """
    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, expected_count, timeout=timeout)
    finally:
        client.close()


def _clear_all_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer):
        fn.cache_clear()


@pytest.fixture
def semantic_cache_env(weaviate_container, monkeypatch):
    """E2E setup: HuggingFace vectorizer + fast batch flush + clean schemas."""
    monkeypatch.setenv("VECTORIZER", "huggingface")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    _clear_all_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        for name in (
            settings.COLLECTION_NAME,
            settings.EXECUTION_COLLECTION_NAME,
            settings.GOLDEN_COLLECTION_NAME,
            "VectorWaveTokenUsage",
        ):
            if client.collections.exists(name):
                client.collections.delete(name)
        create_vectorwave_schema(client, settings)
        create_execution_schema(client, settings)
        create_golden_dataset_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_all_caches()


@pytest.fixture
def no_vectorizer_env(weaviate_container, monkeypatch):
    """E2E setup with VECTORIZER=none — semantic_cache should auto-disable."""
    monkeypatch.setenv("VECTORIZER", "none")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    _clear_all_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        for name in (
            settings.COLLECTION_NAME,
            settings.EXECUTION_COLLECTION_NAME,
            settings.GOLDEN_COLLECTION_NAME,
            "VectorWaveTokenUsage",
        ):
            if client.collections.exists(name):
                client.collections.delete(name)
        create_vectorwave_schema(client, settings)
        create_execution_schema(client, settings)
        create_golden_dataset_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_all_caches()


# ---------------------------------------------------------------------------
# E2E — first call seeds, identical second call hits cache
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_first_call_misses_and_writes_execution_log(semantic_cache_env):
    """A cold cache: function executes, success log lands in VectorWaveExecutions."""
    settings = semantic_cache_env
    call_count = {"n": 0}

    @vectorize(
        search_description="Cache miss test",
        sequence_narrative="Cold cache",
        semantic_cache=True,
        cache_threshold=0.5,
    )
    def my_cache_func(input_data):
        call_count["n"] += 1
        return {"result": input_data * 2}

    result = my_cache_func(input_data=10)
    assert result == {"result": 20}
    assert call_count["n"] == 1

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        objs = list(execs.iterator(include_vector=True))
        assert len(objs) == 1
        props = objs[0].properties
        assert props["status"] == "SUCCESS"
        assert props["function_name"] == "my_cache_func"
        assert json.loads(props["return_value"]) == {"result": 20}
        # Vectorized input must be stored alongside the log
        assert objs[0].vector["default"] is not None
        assert len(objs[0].vector["default"]) > 0
    finally:
        client.close()


@pytest.mark.e2e
def test_identical_second_call_hits_cache_and_skips_execution(semantic_cache_env):
    """Two calls with identical inputs: the second must short-circuit via the cache."""
    settings = semantic_cache_env
    call_count = {"n": 0}

    @vectorize(
        search_description="Cache hit test",
        sequence_narrative="Hit",
        semantic_cache=True,
        cache_threshold=0.5,
    )
    def my_cache_func(query):
        call_count["n"] += 1
        return {"answer": f"computed-for-{query}", "n": call_count["n"]}

    first = my_cache_func(query="how to compute sales tax")
    assert first == {"answer": "computed-for-how to compute sales tax", "n": 1}

    # Allow the first execution log to be flushed and indexed
    _wait_until_indexed(settings, expected_count=1)

    second = my_cache_func(query="how to compute sales tax")
    assert call_count["n"] == 1, "cache hit should have skipped the function body"
    assert second == {"answer": "computed-for-how to compute sales tax", "n": 1}

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        # 1 SUCCESS log from call 1 + 1 CACHE_HIT log from call 2
        _wait_for_count(execs, 2)
        statuses = sorted(o.properties["status"] for o in execs.iterator())
        assert statuses == ["CACHE_HIT", "SUCCESS"]
    finally:
        client.close()


@pytest.mark.e2e
def test_different_input_misses_cache_with_high_threshold(semantic_cache_env):
    """A semantically distant second call must not match at high threshold."""
    settings = semantic_cache_env
    call_count = {"n": 0}

    @vectorize(
        search_description="Cache threshold test",
        sequence_narrative="Strict",
        semantic_cache=True,
        cache_threshold=0.95,
    )
    def my_strict_func(query):
        call_count["n"] += 1
        return {"answer": query.upper(), "n": call_count["n"]}

    my_strict_func(query="how do I compute sales tax for an order")
    _wait_until_indexed(settings, expected_count=1)
    second = my_strict_func(query="render a react component into the dom tree")

    # Two semantically distant inputs: the second should re-execute
    assert call_count["n"] == 2
    assert second["n"] == 2

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 2)
        statuses = [o.properties["status"] for o in execs.iterator()]
        assert statuses.count("SUCCESS") == 2
        assert "CACHE_HIT" not in statuses
    finally:
        client.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_async_function_hits_cache_on_second_call(semantic_cache_env):
    settings = semantic_cache_env
    call_count = {"n": 0}

    @vectorize(
        search_description="Async cache test",
        sequence_narrative="Async",
        semantic_cache=True,
        cache_threshold=0.5,
    )
    async def my_async_func(query):
        call_count["n"] += 1
        await asyncio.sleep(0.01)
        return {"answer": f"async-{query}", "n": call_count["n"]}

    first = await my_async_func(query="how to compute sales tax")
    assert first["n"] == 1

    _wait_until_indexed(settings, expected_count=1)

    second = await my_async_func(query="how to compute sales tax")
    assert call_count["n"] == 1
    assert second["n"] == 1


@pytest.mark.e2e
def test_semantic_cache_disables_when_no_vectorizer(no_vectorizer_env, caplog):
    """semantic_cache=True with VECTORIZER=none is downgraded to plain logging
    with a warning at decoration time."""
    settings = no_vectorizer_env
    call_count = {"n": 0}

    import logging
    caplog.set_level(logging.WARNING)

    @vectorize(
        search_description="Disabled cache",
        sequence_narrative="Disabled",
        semantic_cache=True,
        cache_threshold=0.5,
    )
    def my_disabled_cache_func(x):
        call_count["n"] += 1
        return x

    assert my_disabled_cache_func(1) == 1
    assert my_disabled_cache_func(1) == 1
    assert call_count["n"] == 2, "without a vectorizer both calls must execute"

    warnings = [r for r in caplog.records if "Semantic caching requested" in r.message]
    assert len(warnings) == 1
    assert "no Python vectorizer" in warnings[0].message
