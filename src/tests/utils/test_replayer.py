"""End-to-end tests for VectorWaveReplayer.

Logs are seeded directly into Weaviate and the replayer is exercised against
real fetch/insert/update paths. Function names are deliberately single tokens
(no underscores) because Weaviate's default `word` tokenization on TEXT
properties affects equality filters.
"""
import asyncio
import json

import pytest
from datetime import datetime, timezone
from uuid import uuid4

from vectorwave.utils.replayer import VectorWaveReplayer
from vectorwave.database.db import (
    create_execution_schema,
    create_golden_dataset_schema,
    get_weaviate_client,
)


# ---------------------------------------------------------------------------
# Module-level functions exercised by the replayer
# ---------------------------------------------------------------------------

def radd(a, b):
    return a + b


def rbuggy(a, b):
    """Intentionally wrong implementation: produces a regression."""
    return a + b + 100


def rgreet(msg):
    return f"Hello {msg}"


def rcalc(a):
    return a * 10


async def rasync(a, b):
    await asyncio.sleep(0.001)
    return a + b


def _ext(value):
    raise RuntimeError("Real _ext must not be called in tests; it should be mocked")


def rcallsx(x):
    return _ext(x)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _clear_all_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer):
        fn.cache_clear()


@pytest.fixture
def replayer_e2e_env(weaviate_container, monkeypatch, tmp_path):
    """E2E setup: register input-arg properties as custom_properties so they survive
    the schema's strict typing, then create fresh executions + golden collections."""
    props_path = tmp_path / "test.weaviate_properties"
    props_path.write_text(json.dumps({
        "a": {"data_type": "INT"},
        "b": {"data_type": "INT"},
        "x": {"data_type": "INT"},
        "msg": {"data_type": "TEXT"},
        "team": {"data_type": "TEXT"},
        "priority": {"data_type": "INT"},
    }))
    monkeypatch.setenv("CUSTOM_PROPERTIES_FILE_PATH", str(props_path))
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
        create_execution_schema(client, settings)
        create_golden_dataset_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_all_caches()


def _seed_exec_log(coll, *, function_name, inputs, return_value, status="SUCCESS"):
    encoded_return = json.dumps(return_value)
    props = {
        "function_uuid": str(uuid4()),
        "function_name": function_name,
        "status": status,
        "duration_ms": 1.0,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "return_value": encoded_return,
    }
    props.update(inputs)
    return coll.data.insert(properties=props)


def _seed_golden(coll, *, function_name, original_uuid, return_value):
    return coll.data.insert(
        properties={
            "original_uuid": original_uuid,
            "function_name": function_name,
            "return_value": json.dumps(return_value),
            "note": "",
            "created_at": datetime.now(timezone.utc).isoformat(),
            "tags": [],
        },
        vector=[0.0] * 8,
    )


# ---------------------------------------------------------------------------
# E2E tests
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_replay_passes_when_outputs_match(replayer_e2e_env):
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(exec_col, function_name="radd", inputs={"a": 1, "b": 2}, return_value=3)

        result = VectorWaveReplayer().replay("tests.utils.test_replayer.radd", limit=1)
        assert result["total"] == 1
        assert result["passed"] == 1
        assert result["failed"] == 0
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_detects_regression(replayer_e2e_env):
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(exec_col, function_name="rbuggy", inputs={"a": 1, "b": 2}, return_value=3)

        result = VectorWaveReplayer().replay("tests.utils.test_replayer.rbuggy", limit=1)
        assert result["total"] == 1
        assert result["passed"] == 0
        assert result["failed"] == 1
        failure = result["failures"][0]
        assert failure["expected"] == 3
        assert failure["actual"] == 103
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_update_baseline_writes_new_value(replayer_e2e_env):
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        log_uuid = str(_seed_exec_log(
            exec_col, function_name="rgreet", inputs={"msg": "World"}, return_value="OldGreeting"
        ))

        result = VectorWaveReplayer().replay(
            "tests.utils.test_replayer.rgreet", update_baseline=True
        )
        assert result["updated"] == 1

        updated = exec_col.query.fetch_object_by_id(log_uuid)
        assert json.loads(updated.properties["return_value"]) == "Hello World"
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_filters_extra_arguments_to_function_signature(replayer_e2e_env):
    """Properties on the log that aren't in the function signature must be ignored."""
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(
            exec_col,
            function_name="rcalc",
            inputs={"a": 10, "team": "billing", "priority": 1},
            return_value=100,
        )

        result = VectorWaveReplayer().replay("tests.utils.test_replayer.rcalc", limit=1)
        assert result["total"] == 1
        assert result["passed"] == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_handles_async_function(replayer_e2e_env):
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(exec_col, function_name="rasync", inputs={"a": 1, "b": 2}, return_value=3)

        result = VectorWaveReplayer().replay("tests.utils.test_replayer.rasync", limit=1)
        assert result["total"] == 1
        assert result["passed"] == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_loads_golden_dataset_entries(replayer_e2e_env):
    """A Golden Dataset entry resolves its inputs from the referenced execution log."""
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        golden_col = client.collections.get(settings.GOLDEN_COLLECTION_NAME)

        ref_uuid = str(_seed_exec_log(
            exec_col, function_name="radd", inputs={"a": 5, "b": 7}, return_value=12
        ))
        _seed_golden(golden_col, function_name="radd", original_uuid=ref_uuid, return_value=12)

        # Limit=1 keeps the run focused on the Golden entry that comes first.
        result = VectorWaveReplayer().replay("tests.utils.test_replayer.radd", limit=1)
        assert result["total"] == 1
        assert result["passed"] == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_with_mocks_return_value(replayer_e2e_env):
    """`mocks={target: {return_value: V}}` patches a dependency for the replay run."""
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(exec_col, function_name="rcallsx", inputs={"x": 10}, return_value=42)

        result = VectorWaveReplayer().replay(
            "tests.utils.test_replayer.rcallsx",
            limit=1,
            mocks={"tests.utils.test_replayer._ext": {"return_value": 42}},
        )
        assert result["passed"] == 1
        assert result["failed"] == 0
    finally:
        client.close()


@pytest.mark.e2e
def test_replay_with_mocks_side_effect(replayer_e2e_env):
    """`mocks={target: {side_effect: callable}}` is honored for input-dependent dependencies."""
    settings = replayer_e2e_env
    client = get_weaviate_client(settings)
    try:
        exec_col = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
        _seed_exec_log(exec_col, function_name="rcallsx", inputs={"x": 7}, return_value=14)

        result = VectorWaveReplayer().replay(
            "tests.utils.test_replayer.rcallsx",
            limit=1,
            mocks={"tests.utils.test_replayer._ext": {"side_effect": lambda v: v * 2}},
        )
        assert result["passed"] == 1
        assert result["failed"] == 0
    finally:
        client.close()
