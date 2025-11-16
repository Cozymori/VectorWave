import pytest
from unittest.mock import MagicMock
import time

from vectorwave.monitoring.tracer import trace_root, trace_span
from vectorwave.models.db_config import WeaviateSettings

# --- Import real functions for cache clearing ---
from vectorwave.batch.batch import get_batch_manager as real_get_batch_manager
from vectorwave.database.db import get_cached_client as real_get_cached_client
from vectorwave.models.db_config import get_weaviate_settings as real_get_settings
from vectorwave.monitoring.tracer import TraceCollector, current_tracer_var
from vectorwave.monitoring.alert.base import BaseAlerter

# Module paths to mock (adjust to your project structure if needed)
TRACER_MODULE_PATH = "vectorwave.monitoring.tracer"
BATCH_MODULE_PATH = "vectorwave.batch.batch"


class CustomErrorWithCode(Exception):
    def __init__(self, message, error_code):
        super().__init__(message)
        self.error_code = error_code


@pytest.fixture
def mock_tracer_deps(monkeypatch):
    """
    Mocks dependencies for tracer.py (batch, settings).
    """
    # 1. Mock BatchManager
    mock_batch_instance = MagicMock()
    mock_batch_instance.add_object = MagicMock()
    mock_get_batch_manager = MagicMock(return_value=mock_batch_instance)

    # 2. Mock Settings (including global tags)
    mock_settings = WeaviateSettings(
        COLLECTION_NAME="TestFunctions",
        EXECUTION_COLLECTION_NAME="TestExecutions",
        custom_properties=None,  # Not important for this test
        global_custom_values={"run_id": "global-run-abc", "env": "test"},
        failure_mapping={"ValueError": "INVALID_INPUT"}
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    mock_alerter_instance = MagicMock(spec=BaseAlerter)
    mock_alerter_instance.notify = MagicMock()
    mock_get_alerter = MagicMock(return_value=mock_alerter_instance)

    mock_client = MagicMock()
    mock_get_client = MagicMock(return_value=mock_client)

    # --- Patch dependencies for tracer.py ---
    monkeypatch.setattr(f"{TRACER_MODULE_PATH}.get_batch_manager", mock_get_batch_manager)
    monkeypatch.setattr(f"{TRACER_MODULE_PATH}.get_weaviate_settings", mock_get_settings)
    monkeypatch.setattr(f"{TRACER_MODULE_PATH}.get_alerter", mock_get_alerter)

    # Patch dependencies inside batch.py to prevent BatchManager init failure
    monkeypatch.setattr(f"{BATCH_MODULE_PATH}.get_weaviate_client", mock_get_client)
    monkeypatch.setattr(f"{BATCH_MODULE_PATH}.get_weaviate_settings", mock_get_settings)

    # 5. Clear caches
    real_get_batch_manager.cache_clear()
    real_get_cached_client.cache_clear()
    real_get_settings.cache_clear()

    return {
        "batch": mock_batch_instance,
        "settings": mock_settings,
        "alerter": mock_alerter_instance
    }


def test_trace_root_and_span_success(mock_tracer_deps):
    """
    Case 1: Success (Root + Span) - The span should be recorded successfully.
    """
    mock_batch = mock_tracer_deps["batch"]
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span
    def my_inner_span(x):
        return f"result: {x}"

    @trace_root()
    def my_workflow_root():
        return my_inner_span(x=10)

    # --- Act ---
    result = my_workflow_root()

    # --- Assert ---
    assert result == "result: 10"
    mock_batch.add_object.assert_called_once()
    mock_alerter.notify.assert_not_called()

    args, kwargs = mock_batch.add_object.call_args
    props = kwargs["properties"]

    assert kwargs["collection"] == "TestExecutions"
    assert props["status"] == "SUCCESS"
    assert props["function_name"] == "my_inner_span"
    assert props["error_message"] is None
    assert kwargs["properties"]["error_code"] is None
    assert "trace_id" in props
    assert props["run_id"] == "global-run-abc"
    assert props["env"] == "test"


def test_trace_span_failure(mock_tracer_deps):
    """
    Case 2: Failure (Root + Failing Span) - The span should be recorded with an ERROR status.
    """
    mock_batch = mock_tracer_deps["batch"]
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span
    def my_failing_span():
        raise ValueError("This is a test error")

    @trace_root()
    def my_workflow_root_fail():
        my_failing_span()

    # --- Act & Assert (Exception) ---
    with pytest.raises(ValueError, match="This is a test error"):
        my_workflow_root_fail()

    # --- Assert (Log) ---
    mock_batch.add_object.assert_called_once()
    db_props = mock_batch.add_object.call_args.kwargs["properties"]
    assert db_props["status"] == "ERROR"
    assert "ValueError: This is a test error" in db_props["error_message"]
    assert db_props["error_code"] == "INVALID_INPUT"

    mock_alerter.notify.assert_called_once()
    alert_props = mock_alerter.notify.call_args.args[0]

    args, kwargs = mock_batch.add_object.call_args
    props = kwargs["properties"]


    assert alert_props == db_props
    assert alert_props["status"] == "ERROR"
    assert alert_props["error_code"] == "INVALID_INPUT"
    assert props["status"] == "ERROR"
    assert "ValueError: This is a test error" in props["error_message"]
    assert kwargs["properties"]["error_code"] == "INVALID_INPUT"
    assert props["function_name"] == "my_failing_span"
    assert props["run_id"] == "global-run-abc"


def test_span_without_root_does_nothing(mock_tracer_deps):
    """
    Case 3: Tracing disabled (Span only) - If there's no Root, nothing should be recorded.
    """
    mock_batch = mock_tracer_deps["batch"]

    @trace_span
    def my_lonely_span():
        return "lonely_result"

    # --- Act ---
    result = my_lonely_span()

    # --- Assert ---
    assert result == "lonely_result"
    mock_batch.add_object.assert_not_called()


def test_span_captures_attributes_and_overrides_globals(mock_tracer_deps):
    """
    Case 4/5: Attribute Capturing and Overriding
    """
    mock_batch = mock_tracer_deps["batch"]

    class MyObject:
        def __str__(self): return "MyObjectInstance"

    @trace_span(attributes_to_capture=["team", "priority", "run_id", "user_obj"])
    def my_span_with_attrs(team, priority, run_id, user_obj, other_arg="default"):
        return "captured"

    @trace_root()
    def my_workflow_root_attrs():
        return my_span_with_attrs(
            team="backend",
            priority=1,
            run_id="override-run-xyz",  # <-- This should override "global-run-abc"
            user_obj=MyObject(),
            other_arg="should-be-ignored"
        )

    # --- Act ---
    my_workflow_root_attrs()

    # --- Assert ---
    mock_batch.add_object.assert_called_once()
    props = mock_batch.add_object.call_args.kwargs["properties"]

    assert props["team"] == "backend"
    assert props["priority"] == 1
    assert props["user_obj"] == "MyObjectInstance"
    assert props["run_id"] == "override-run-xyz"  # Overridden
    assert props["env"] == "test"  # Non-overridden global remains
    assert "other_arg" not in props


def test_root_accepts_custom_trace_id(mock_tracer_deps):
    """
    Bonus: Test case for manually providing a 'trace_id'.
    (This is the test that was fixed)
    """
    mock_batch = mock_tracer_deps["batch"]

    @trace_span
    def my_inner_span():
        pass

    @trace_root()
    def my_workflow_root_custom_id():  # <-- ✅ FIXED: Removed 'trace_id' arg
        my_inner_span()

    # --- Act ---
    # The decorator wrapper still receives 'trace_id' from this call
    my_workflow_root_custom_id(trace_id="my-custom-trace-id-123")

    # --- Assert ---
    mock_batch.add_object.assert_called_once()
    props = mock_batch.add_object.call_args.kwargs["properties"]

    # Check if the trace_id was popped and injected correctly
    assert props["trace_id"] == "my-custom-trace-id-123"


def test_trace_span_error_code_priority_1_custom_attr(mock_tracer_deps):
    """Test (Priority 1) custom e.error_code attribute."""
    mock_batch = mock_tracer_deps["batch"]
    tracer = TraceCollector(trace_id="test_trace_p1")
    tracer.settings = mock_tracer_deps["settings"]

    @trace_span()
    def my_custom_fail_function():
        raise CustomErrorWithCode("Test", "PAYMENT_FAILED_001")

    token = current_tracer_var.set(tracer)
    try:
        with pytest.raises(CustomErrorWithCode):
            my_custom_fail_function()
    finally:
        current_tracer_var.reset(token)

    args, kwargs = mock_batch.add_object.call_args
    assert kwargs["properties"]["status"] == "ERROR"
    assert kwargs["properties"]["error_code"] == "PAYMENT_FAILED_001"


def test_trace_span_error_code_priority_3_default_class_name(mock_tracer_deps):
    """Test (Priority 3) default class name (KeyError is not in mapping)."""
    mock_batch = mock_tracer_deps["batch"]
    tracer = TraceCollector(trace_id="test_trace_p3")
    tracer.settings = mock_tracer_deps["settings"]

    @trace_span()
    def my_key_error_function():
        _ = {}["missing_key"]  # Raises KeyError

    token = current_tracer_var.set(tracer)
    try:
        with pytest.raises(KeyError):
            my_key_error_function()
    finally:
        current_tracer_var.reset(token)

    args, kwargs = mock_batch.add_object.call_args
    assert kwargs["properties"]["status"] == "ERROR"
    assert kwargs["properties"]["error_code"] == "KeyError"
