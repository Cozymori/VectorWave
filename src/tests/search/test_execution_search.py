from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch

import pytest
from vectorwave.search.execution_search import (
    find_executions,
    find_recent_errors,
    find_slowest_executions,
    find_by_trace_id
)


@pytest.fixture
def mock_low_level_search(monkeypatch):
    """
    Mocks 'vectorwave.database.db_search.search_executions'
    Used when testing the find_executions function.
    """
    mock = MagicMock(return_value=["dummy_log_data"])
    monkeypatch.setattr("vectorwave.search.execution_search.search_executions", mock)
    return mock


def test_find_executions(mock_low_level_search):
    """
    Tests if find_executions correctly passes arguments (dict) to the low-level function.
    """
    filters_dict = {"status": "OK"}  # Use dict

    find_executions(
        filters=filters_dict,
        limit=5,
        sort_by="duration_ms",
        sort_ascending=True
    )

    # Check if the low-level search_executions function was called with the correct arguments
    mock_low_level_search.assert_called_once_with(
        filters=filters_dict,  # Check if dict was passed
        limit=5,
        sort_by="duration_ms",
        sort_ascending=True
    )


# --- Setup for test_find_recent_errors ---
mock_now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
expected_time_limit_iso = (mock_now - timedelta(minutes=10)).isoformat()

# Mock error logs returned by the DB
mock_db_errors = [
    {"timestamp_utc": (mock_now - timedelta(minutes=1)).isoformat(), "error_code": "INVALID_INPUT"},
    {"timestamp_utc": (mock_now - timedelta(minutes=2)).isoformat(), "error_code": "TIMEOUT"},
    {"timestamp_utc": (mock_now - timedelta(minutes=15)).isoformat(), "error_code": "INVALID_INPUT"},  # Too old
]


# [Core Fix] Create mock datetime class
# Replaces the datetime.datetime class itself.
MockDateTime = MagicMock()
# Set .now() to return a fixed time (mock_now) when called
MockDateTime.now = MagicMock(return_value=mock_now)
# Set .fromisoformat() to call the real datetime.fromisoformat function
# (If this is omitted, a TypeError occurs in the log_time <= time_limit comparison)
MockDateTime.fromisoformat = MagicMock(side_effect=datetime.fromisoformat)


@patch('vectorwave.search.execution_search.datetime', MockDateTime)
@patch('vectorwave.search.execution_search.find_executions')
def test_find_recent_errors(mock_find_executions):
    """
    Tests if find_recent_errors calls find_executions with the correct filters.
    (The test no longer checks manual filtering, as filtering is delegated to the DB).
    """
    # 1. Run test
    find_recent_errors(minutes_ago=10, error_codes=["INVALID_INPUT"])

    # 2. Verify find_executions was called correctly
    call_args = mock_find_executions.call_args
    filters_arg = call_args.kwargs['filters']

    assert filters_arg['status'] == 'ERROR'
    assert filters_arg['error_code'] == 'INVALID_INPUT'
    assert filters_arg['timestamp_utc__gte'] == expected_time_limit_iso


@patch('vectorwave.search.execution_search.find_executions')
def test_find_slowest_executions(mock_find_executions):
    """
    Tests if find_slowest_executions sorts correctly by 'duration_ms'.
    """
    find_slowest_executions(limit=3, min_duration_ms=100.5)

    call_args = mock_find_executions.call_args

    # [Fix] Check if filters_arg is a dict (or None)
    filters_arg = call_args.kwargs['filters']
    assert (filters_arg is None) or isinstance(filters_arg, dict)

    # 2. Verify sort order
    assert call_args.kwargs['sort_by'] == 'duration_ms'
    assert call_args.kwargs['sort_ascending'] == False
    assert call_args.kwargs['limit'] == 3


@patch('vectorwave.search.execution_search.find_executions')
def test_find_by_trace_id(mock_find_executions):
    """
    Tests if find_by_trace_id filters by 'trace_id' and sorts by time.
    """
    find_by_trace_id("my-test-trace-123")

    call_args = mock_find_executions.call_args

    # [Fix] Check if filters_arg is a dict
    filters_arg = call_args.kwargs['filters']
    assert isinstance(filters_arg, dict)

    # 1. Verify trace_id filter
    assert filters_arg['trace_id'] == 'my-test-trace-123'

    # 2. Verify sort order (time ascending)
    assert call_args.kwargs['sort_by'] == 'timestamp_utc'
    assert call_args.kwargs['sort_ascending'] == True
    assert call_args.kwargs['limit'] == 100