from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest
from uuid import uuid4

from vectorwave.search.execution_search import (
    find_executions,
    find_recent_errors,
    find_slowest_executions,
    find_by_trace_id,
    find_replay_executions,
)
from vectorwave.database.db import get_weaviate_client, create_execution_schema
from vectorwave.models.db_config import WeaviateSettings


# ---------------------------------------------------------------------------
# Unit tests — verify the high-level wrappers build the right filter dict
# (the wrappers contain only argument-shaping logic; the actual querying is
#  exercised end-to-end below)
# ---------------------------------------------------------------------------

def test_find_executions_passes_arguments_through(monkeypatch):
    captured = MagicMock(return_value=[])
    monkeypatch.setattr("vectorwave.search.execution_search.search_executions", captured)

    find_executions(filters={"status": "OK"}, limit=5, sort_by="duration_ms", sort_ascending=True)

    captured.assert_called_once_with(
        filters={"status": "OK"},
        limit=5,
        sort_by="duration_ms",
        sort_ascending=True,
    )


def test_find_recent_errors_builds_status_and_time_filter(monkeypatch):
    captured = MagicMock(return_value=[])
    monkeypatch.setattr("vectorwave.search.execution_search.find_executions", captured)

    fixed_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    fake_dt = MagicMock()
    fake_dt.now.return_value = fixed_now
    fake_dt.fromisoformat.side_effect = datetime.fromisoformat
    monkeypatch.setattr("vectorwave.search.execution_search.datetime", fake_dt)

    find_recent_errors(minutes_ago=10, error_codes=["INVALID_INPUT"])

    filters = captured.call_args.kwargs["filters"]
    assert filters["status"] == "ERROR"
    assert filters["error_code"] == ["INVALID_INPUT"]
    assert filters["timestamp_utc__gte"] == (fixed_now - timedelta(minutes=10)).isoformat()


def test_find_slowest_executions_sorts_by_duration_descending(monkeypatch):
    captured = MagicMock(return_value=[])
    monkeypatch.setattr("vectorwave.search.execution_search.find_executions", captured)

    find_slowest_executions(limit=3, min_duration_ms=100.5)

    kwargs = captured.call_args.kwargs
    assert kwargs["sort_by"] == "duration_ms"
    assert kwargs["sort_ascending"] is False
    assert kwargs["limit"] == 3
    assert kwargs["filters"]["duration_ms__gte"] == 100.5


def test_find_by_trace_id_filters_and_sorts_chronologically(monkeypatch):
    captured = MagicMock(return_value=[])
    monkeypatch.setattr("vectorwave.search.execution_search.find_executions", captured)

    find_by_trace_id("my-trace-123")

    kwargs = captured.call_args.kwargs
    assert kwargs["filters"] == {"trace_id": "my-trace-123"}
    assert kwargs["sort_by"] == "timestamp_utc"
    assert kwargs["sort_ascending"] is True
    assert kwargs["limit"] == 100


# ---------------------------------------------------------------------------
# E2E — exercise the full search path against a real Weaviate
# ---------------------------------------------------------------------------

@pytest.fixture
def e2e_settings() -> WeaviateSettings:
    s = WeaviateSettings()
    s.IS_VECTORIZE_COLLECTION_NAME = False
    s.VECTORIZER = "none"
    return s


def _seed(coll, *, function_name="fn", status="SUCCESS", duration_ms=10,
          timestamp=None, error_code=None, trace_id=None, exec_source=None):
    props = {
        "function_uuid": str(uuid4()),
        "function_name": function_name,
        "status": status,
        "duration_ms": duration_ms,
        "timestamp_utc": (timestamp or datetime.now(timezone.utc)).isoformat(),
    }
    if error_code is not None:
        props["error_code"] = error_code
    if trace_id is not None:
        props["trace_id"] = trace_id
    if exec_source is not None:
        props["exec_source"] = exec_source
    return coll.data.insert(properties=props)


@pytest.mark.e2e
def test_find_recent_errors_excludes_old_and_wrong_code(clean_weaviate, e2e_settings):
    """Time and error_code filters are applied at the DB layer, not in Python."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        now = datetime.now(timezone.utc)
        _seed(coll, status="ERROR", error_code="INVALID_INPUT", timestamp=now - timedelta(minutes=1))
        _seed(coll, status="ERROR", error_code="TIMEOUT", timestamp=now - timedelta(minutes=2))
        _seed(coll, status="ERROR", error_code="INVALID_INPUT", timestamp=now - timedelta(minutes=30))
        _seed(coll, status="SUCCESS", timestamp=now - timedelta(minutes=1))

        results = find_recent_errors(minutes_ago=10, error_codes=["INVALID_INPUT"])

        assert len(results) == 1
        assert results[0]["error_code"] == "INVALID_INPUT"
        assert results[0]["status"] == "ERROR"
    finally:
        client.close()


@pytest.mark.e2e
def test_find_recent_errors_accepts_multiple_codes(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        now = datetime.now(timezone.utc)
        _seed(coll, status="ERROR", error_code="INVALID_INPUT", timestamp=now)
        _seed(coll, status="ERROR", error_code="TIMEOUT_ERROR", timestamp=now)
        _seed(coll, status="ERROR", error_code="OTHER", timestamp=now)

        results = find_recent_errors(
            minutes_ago=20, limit=5, error_codes=["INVALID_INPUT", "TIMEOUT_ERROR"]
        )

        codes = {r["error_code"] for r in results}
        assert codes == {"INVALID_INPUT", "TIMEOUT_ERROR"}
    finally:
        client.close()


@pytest.mark.e2e
def test_find_slowest_executions_returns_top_n_by_duration(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        for d in [50, 200, 100, 500, 30]:
            _seed(coll, duration_ms=d)

        results = find_slowest_executions(limit=3)
        durations = [r["duration_ms"] for r in results]
        assert durations == [500, 200, 100]
    finally:
        client.close()


@pytest.mark.e2e
def test_find_slowest_executions_respects_min_duration(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        for d in [50, 99, 100, 200]:
            _seed(coll, duration_ms=d)

        results = find_slowest_executions(limit=10, min_duration_ms=100)
        assert sorted(r["duration_ms"] for r in results) == [100, 200]
    finally:
        client.close()


@pytest.mark.e2e
def test_find_by_trace_id_groups_and_orders_chronologically(clean_weaviate, e2e_settings):
    """Filter narrows to one trace and the result is sorted by timestamp ascending.

    Trace ids are deliberately hyphen-free so the default `word` tokenization on
    the TEXT property does not collapse `trace-A` and `trace-B` to the shared
    `trace` token.
    """
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        base = datetime.now(timezone.utc)
        _seed(coll, trace_id="alphatrace", timestamp=base + timedelta(seconds=2))
        _seed(coll, trace_id="alphatrace", timestamp=base + timedelta(seconds=1))
        _seed(coll, trace_id="betatrace", timestamp=base)

        results = find_by_trace_id("alphatrace")
        assert len(results) == 2
        assert all(r["trace_id"] == "alphatrace" for r in results)
        timestamps = [r["timestamp_utc"] for r in results]
        assert timestamps == sorted(timestamps)
    finally:
        client.close()


@pytest.mark.e2e
def test_find_replay_executions_filters_by_exec_source(clean_weaviate, e2e_settings):
    """find_replay_executions narrows to exec_source='REPLAY' rows and optional status/function."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="f1", status="SUCCESS", exec_source="REPLAY")
        _seed(coll, function_name="f2", status="ERROR", exec_source="REPLAY")
        _seed(coll, function_name="f1", status="SUCCESS", exec_source="LIVE")

        all_replays = find_replay_executions(limit=10)
        assert len(all_replays) == 2
        assert all(r["exec_source"] == "REPLAY" for r in all_replays)

        only_success = find_replay_executions(limit=10, status="SUCCESS")
        assert len(only_success) == 1
        assert only_success[0]["status"] == "SUCCESS"

        only_f2 = find_replay_executions(limit=10, function_name="f2")
        assert len(only_f2) == 1
        assert only_f2[0]["function_name"] == "f2"
    finally:
        client.close()
