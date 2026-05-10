"""End-to-end tests for the @vectorize decorator.

These tests exercise the full decoration → trace → batch → Weaviate path. We
configure the batch manager to flush almost immediately (BATCH_THRESHOLD=1,
FLUSH_INTERVAL_SECONDS=0.1) so each call materialises in Weaviate within a
second, then poll the collection to assert what landed there.
"""
import asyncio
import json
import time

import pytest

from vectorwave.core.decorator import vectorize
from vectorwave.monitoring.tracer import trace_span
from vectorwave.database.db import (
    get_weaviate_client,
    create_vectorwave_schema,
    create_execution_schema,
)


def _wait_for_count(coll, expected: int, timeout: float = 8.0) -> int:
    """Polls until the collection has at least `expected` rows or times out."""
    deadline = time.time() + timeout
    last_count = -1
    while time.time() < deadline:
        last_count = len(coll.query.fetch_objects(limit=200).objects)
        if last_count >= expected:
            return last_count
        time.sleep(0.1)
    raise AssertionError(f"expected at least {expected} rows, got {last_count} after {timeout}s")


def _read_all(coll):
    return list(coll.iterator())


def _clear_all_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer):
        fn.cache_clear()


def _setup_env(monkeypatch, props_path: str):
    """Common env setup: fast batch flush, point at the given custom_properties file."""
    monkeypatch.setenv("CUSTOM_PROPERTIES_FILE_PATH", props_path)
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    _clear_all_caches()


def _wipe_and_recreate_schemas(settings):
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
    finally:
        client.close()


@pytest.fixture
def vectorize_e2e_env(weaviate_container, monkeypatch, tmp_path):
    """E2E setup with custom_properties run_id/team/priority and run_id=test-run-abc as global."""
    props_path = tmp_path / "test.weaviate_properties"
    props_path.write_text(json.dumps({
        "run_id": {"data_type": "TEXT"},
        "team": {"data_type": "TEXT"},
        "priority": {"data_type": "INT"},
        "user_id": {"data_type": "TEXT"},
        "amount": {"data_type": "INT"},
        "receipt_id": {"data_type": "TEXT"},
    }))
    monkeypatch.setenv("RUN_ID", "test-run-abc")
    _setup_env(monkeypatch, str(props_path))

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    _wipe_and_recreate_schemas(settings)

    yield settings
    _clear_all_caches()


@pytest.fixture
def vectorize_e2e_env_no_props(weaviate_container, monkeypatch, tmp_path):
    """E2E setup with no custom_properties file (path points at a nonexistent file)."""
    monkeypatch.delenv("RUN_ID", raising=False)
    _setup_env(monkeypatch, str(tmp_path / "does_not_exist.json"))

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    assert settings.custom_properties is None
    _wipe_and_recreate_schemas(settings)

    yield settings
    _clear_all_caches()


# ---------------------------------------------------------------------------
# Static metadata: function definition is registered to VectorWaveFunctions
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_vectorize_writes_function_metadata_to_weaviate(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(search_description="Test search desc", sequence_narrative="Test sequence narr")
    def my_test_function_static():
        """My test docstring"""
        pass

    client = get_weaviate_client(settings)
    try:
        funcs = client.collections.get(settings.COLLECTION_NAME)
        _wait_for_count(funcs, 1)
        objs = _read_all(funcs)
        assert len(objs) == 1
        props = objs[0].properties
        assert props["function_name"] == "my_test_function_static"
        assert props["docstring"] == "My test docstring"
        assert "def my_test_function_static" in props["source_code"]
        assert props["search_description"] == "Test search desc"
        assert props["sequence_narrative"] == "Test sequence narr"
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Dynamic logs: each call lands in VectorWaveExecutions
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_vectorize_logs_successful_call_to_executions(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(search_description="Test", sequence_narrative="Test")
    def my_success_function():
        return "Success"

    assert my_success_function() == "Success"

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        objs = _read_all(execs)
        assert len(objs) == 1
        props = objs[0].properties
        assert props["status"] == "SUCCESS"
        assert props["error_message"] is None
        assert props["duration_ms"] >= 0
        assert props["function_name"] == "my_success_function"
        # Global custom value populated from RUN_ID env
        assert props["run_id"] == "test-run-abc"
    finally:
        client.close()


@pytest.mark.e2e
def test_vectorize_logs_failed_call_with_error_status(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(search_description="FailTest", sequence_narrative="FailTest")
    def my_failing_function():
        raise ValueError("This is a test error")

    with pytest.raises(ValueError, match="This is a test error"):
        my_failing_function()

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        objs = _read_all(execs)
        assert len(objs) == 1
        props = objs[0].properties
        assert props["status"] == "ERROR"
        assert "ValueError: This is a test error" in props["error_message"]
        assert "Traceback" in props["error_message"]
        assert props["run_id"] == "test-run-abc"
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Custom execution tags: function-specific tags merge with global tags
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_vectorize_merges_global_and_function_specific_tags(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(
        search_description="Test with specific tags",
        sequence_narrative="Tags should be merged",
        team="backend",
        priority=1,
    )
    def my_tagged_function():
        return "Tagged success"

    assert my_tagged_function() == "Tagged success"

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        props = _read_all(execs)[0].properties
        assert props["status"] == "SUCCESS"
        assert props["run_id"] == "test-run-abc"
        assert props["team"] == "backend"
        assert props["priority"] == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_vectorize_function_specific_tag_overrides_global(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(
        search_description="Test override",
        sequence_narrative="Next",
        run_id="override-run-xyz",
    )
    def my_override_function():
        return None

    my_override_function()

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        props = _read_all(execs)[0].properties
        assert props["run_id"] == "override-run-xyz"
    finally:
        client.close()


@pytest.mark.e2e
def test_vectorize_filters_unknown_tags_not_in_custom_properties(vectorize_e2e_env):
    """A tag absent from custom_properties is dropped before reaching Weaviate."""
    settings = vectorize_e2e_env

    @vectorize(
        search_description="Test tag filtering",
        sequence_narrative="Next",
        team="data-science",
        priority=2,
        unknown_tag="should-be-ignored",
    )
    def my_mixed_tags_function():
        return None

    my_mixed_tags_function()

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        props = _read_all(execs)[0].properties
        assert props["run_id"] == "test-run-abc"
        assert props["team"] == "data-science"
        assert props["priority"] == 2
        assert "unknown_tag" not in props
    finally:
        client.close()


@pytest.mark.e2e
def test_vectorize_drops_all_tags_when_no_properties_file(vectorize_e2e_env_no_props):
    """With no custom_properties configured, every passed tag is filtered out."""
    settings = vectorize_e2e_env_no_props

    @vectorize(
        search_description="Test no props",
        sequence_narrative="Next",
        team="should-be-ignored",
    )
    def my_no_props_function():
        return None

    my_no_props_function()

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        props = _read_all(execs)[0].properties
        assert props["status"] == "SUCCESS"
        # `team` is not even a property in the schema, so it cannot leak through
        assert "team" not in props
    finally:
        client.close()


# ---------------------------------------------------------------------------
# Async @vectorize coverage (moved from monitoring/test_async_trace.py)
# ---------------------------------------------------------------------------

@pytest.mark.e2e
@pytest.mark.asyncio
async def test_vectorize_logs_successful_async_call(vectorize_e2e_env):
    settings = vectorize_e2e_env

    @vectorize(
        search_description="Async test",
        sequence_narrative="Next",
        team="async-team",
    )
    async def my_async_vectorized_func(x):
        await asyncio.sleep(0.01)
        return f"async result {x}"

    result = await my_async_vectorized_func(x=5)
    assert result == "async result 5"

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 1)
        props = _read_all(execs)[0].properties
        assert props["status"] == "SUCCESS"
        assert props["function_name"] == "my_async_vectorized_func"
        assert props["duration_ms"] > 0
        assert props["run_id"] == "test-run-abc"
        assert props["team"] == "async-team"
    finally:
        client.close()


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_async_vectorize_root_with_async_child_spans(vectorize_e2e_env):
    """Root @vectorize plus two @trace_span async children should land 3 rows
    that share the same trace_id."""
    settings = vectorize_e2e_env

    @trace_span(attributes_to_capture=["user_id", "amount"])
    async def async_step_1_validate(user_id: str, amount: int):
        await asyncio.sleep(0.01)
        return True

    @trace_span(attributes_to_capture=["user_id", "receipt_id"])
    async def async_step_2_send_receipt(user_id: str, receipt_id: str):
        await asyncio.sleep(0.01)
        return "sent"

    @vectorize(
        search_description="Async payment workflow",
        sequence_narrative="root + 2 spans",
        team="billing",
    )
    async def async_process_payment(user_id: str, amount: int):
        await async_step_1_validate(user_id=user_id, amount=amount)
        receipt_id = f"async_receipt_{user_id}"
        await async_step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)
        return {"status": "success", "receipt_id": receipt_id}

    result = await async_process_payment("useraab", 500)
    assert result["status"] == "success"

    client = get_weaviate_client(settings)
    try:
        execs = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _wait_for_count(execs, 3)
        props_by_name = {o.properties["function_name"]: o.properties for o in _read_all(execs)}
        assert set(props_by_name.keys()) == {
            "async_step_1_validate",
            "async_step_2_send_receipt",
            "async_process_payment",
        }
        trace_ids = {p["trace_id"] for p in props_by_name.values()}
        assert len(trace_ids) == 1, f"expected one shared trace_id, got {trace_ids}"
        assert props_by_name["async_step_1_validate"]["user_id"] == "useraab"
        assert props_by_name["async_step_1_validate"]["amount"] == 500
        assert props_by_name["async_step_2_send_receipt"]["receipt_id"] == "async_receipt_useraab"
        assert props_by_name["async_process_payment"]["team"] == "billing"
    finally:
        client.close()
