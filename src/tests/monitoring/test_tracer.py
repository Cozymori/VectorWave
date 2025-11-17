import pytest
from unittest.mock import MagicMock
import time

from vectorwave.monitoring.tracer import trace_root, trace_span
from vectorwave.models.db_config import WeaviateSettings

# --- Import real functions for cache clearing ---
from vectorwave.batch.batch import get_batch_manager as real_get_batch_manager
from vectorwave.database.db import get_cached_client as real_get_cached_client
from vectorwave.models.db_config import get_weaviate_settings as real_get_settings
from vectorwave.monitoring.tracer import TraceCollector, current_tracer_var, current_span_id_var
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
    Case 1: Success (Root + Span)
    [MODIFIED] Now also checks for parent_span_id hierarchy.
    """
    mock_batch = mock_tracer_deps["batch"].add_object  # Get the add_object mock
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span  # Child
    def my_inner_span(x):
        return f"result: {x}"

    @trace_root()  # Root (sets up context)
    @trace_span  # Root Span (logs itself)
    def my_workflow_root():
        return my_inner_span(x=10)

    # --- Act ---
    result = my_workflow_root()

    # --- Assert ---
    assert result == "result: 10"
    # 2 calls: 1 (inner_span) + 1 (workflow_root)
    assert mock_batch.call_count == 2
    mock_alerter.notify.assert_not_called()

    call_args_list = mock_batch.call_args_list
    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in call_args_list
    }

    assert "my_inner_span" in props_map
    assert "my_workflow_root" in props_map

    root_props = props_map["my_workflow_root"]
    inner_props = props_map["my_inner_span"]

    # --- [NEW] Hierarchy Validation ---
    assert root_props["parent_span_id"] is None  # Root span has no parent
    assert inner_props["parent_span_id"] is not None
    assert inner_props["parent_span_id"] == root_props["span_id"]  # Child's parent is Root

    assert root_props["trace_id"] == inner_props["trace_id"]
    assert root_props["status"] == "SUCCESS"
    assert inner_props["status"] == "SUCCESS"
    assert root_props["run_id"] == "global-run-abc"
    assert inner_props["run_id"] == "global-run-abc"


def test_trace_span_failure(mock_tracer_deps):
    """
    Case 2: Failure (Root + Failing Span)
    Now checks hierarchy and logs both root (success) and child (error).
    """
    mock_batch = mock_tracer_deps["batch"].add_object  # Get the add_object mock
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span  # Child (Failing)
    def my_failing_span():
        raise ValueError("This is a test error")

    @trace_root()  # Root (sets up context)
    @trace_span  # Root Span (Success)
    def my_workflow_root_fail():
        my_failing_span()

    # --- Act & Assert (Exception) ---
    with pytest.raises(ValueError, match="This is a test error"):
        my_workflow_root_fail()

    # --- Assert (Log) ---
    # 2 calls: 1 (root_fail - SUCCESS) + 1 (failing_span - ERROR)
    assert mock_batch.call_count == 2

    call_args_list = mock_batch.call_args_list
    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in call_args_list
    }

    assert "my_workflow_root_fail" in props_map
    assert "my_failing_span" in props_map

    root_props = props_map["my_workflow_root_fail"]
    failing_props = props_map["my_failing_span"]

    # Root Span Asserts (SUCCESS)
    assert root_props["status"] == "ERROR"
    assert root_props["parent_span_id"] is None

    # Failing Span Asserts (ERROR)
    assert failing_props["status"] == "ERROR"
    assert "ValueError: This is a test error" in failing_props["error_message"]
    assert failing_props["error_code"] == "INVALID_INPUT"
    assert failing_props["parent_span_id"] == root_props["span_id"]
    assert failing_props["run_id"] == "global-run-abc"

    # Alerter Asserts
    mock_alerter.notify.assert_called_once()
    alert_props = mock_alerter.notify.call_args.args[0]

    assert alert_props == failing_props  # Alerter was called with the failing span's properties
    assert alert_props["status"] == "ERROR"
    assert alert_props["error_code"] == "INVALID_INPUT"


def test_span_without_root_does_nothing(mock_tracer_deps):
    """
    Case 3: Tracing disabled (Span only) - If there's no Root, nothing should be recorded.
    """
    mock_batch = mock_tracer_deps["batch"].add_object

    @trace_span
    def my_lonely_span():
        return "lonely_result"

    # --- Act ---
    result = my_lonely_span()

    # --- Assert ---
    assert result == "lonely_result"
    mock_batch.assert_not_called()


def test_span_captures_attributes_and_overrides_globals(mock_tracer_deps):
    """
    Case 4/5: Attribute Capturing and Overriding
    Checks hierarchy.
    """
    mock_batch = mock_tracer_deps["batch"].add_object

    class MyObject:
        def __str__(self): return "MyObjectInstance"

    @trace_span(attributes_to_capture=["team", "priority", "run_id", "user_obj"])
    def my_span_with_attrs(team, priority, run_id, user_obj, other_arg="default"):
        return "captured"

    @trace_root()
    @trace_span  # Root span
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
    assert mock_batch.call_count == 2

    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in mock_batch.call_args_list
    }
    root_props = props_map["my_workflow_root_attrs"]
    child_props = props_map["my_span_with_attrs"]

    # Hierarchy
    assert root_props["parent_span_id"] is None
    assert child_props["parent_span_id"] == root_props["span_id"]

    # Child properties (existing test)
    assert child_props["team"] == "backend"
    assert child_props["priority"] == 1
    assert child_props["user_obj"] == "MyObjectInstance"
    assert child_props["run_id"] == "override-run-xyz"  # Overridden
    assert child_props["env"] == "test"  # Non-overridden global remains
    assert "other_arg" not in child_props


def test_root_accepts_custom_trace_id(mock_tracer_deps):
    """
    Bonus: Test case for manually providing a 'trace_id'.
    Checks hierarchy and trace_id propagation.
    """
    mock_batch = mock_tracer_deps["batch"].add_object

    @trace_span
    def my_inner_span():
        pass

    @trace_root()  # Sets up context
    @trace_span  # Root span
    def my_workflow_root_custom_id():
        my_inner_span()

    # --- Act ---
    # The decorator wrapper still receives 'trace_id' from this call
    my_workflow_root_custom_id(trace_id="my-custom-trace-id-123")

    # --- Assert ---
    assert mock_batch.call_count == 2

    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in mock_batch.call_args_list
    }
    root_props = props_map["my_workflow_root_custom_id"]
    child_props = props_map["my_inner_span"]

    # Check if the trace_id was popped and injected correctly
    assert root_props["trace_id"] == "my-custom-trace-id-123"
    assert child_props["trace_id"] == "my-custom-trace-id-123"

    # Check hierarchy
    assert root_props["parent_span_id"] is None
    assert child_props["parent_span_id"] == root_props["span_id"]


def test_trace_span_error_code_priority_1_custom_attr(mock_tracer_deps):
    """
    Test (Priority 1) custom e.error_code attribute.
    Also sets and checks parent_span_id.
    """
    mock_batch = mock_tracer_deps["batch"].add_object
    tracer = TraceCollector(trace_id="test_trace_p1")
    tracer.settings = mock_tracer_deps["settings"]

    @trace_span()
    def my_custom_fail_function():
        raise CustomErrorWithCode("Test", "PAYMENT_FAILED_001")

    # Set up mock parent context
    tracer_token = current_tracer_var.set(tracer)
    parent_span_token = current_span_id_var.set("mock-parent-id-123")

    try:
        with pytest.raises(CustomErrorWithCode):
            my_custom_fail_function()
    finally:
        current_span_id_var.reset(parent_span_token)
        current_tracer_var.reset(tracer_token)

    args, kwargs = mock_batch.call_args
    assert kwargs["properties"]["status"] == "ERROR"
    assert kwargs["properties"]["error_code"] == "PAYMENT_FAILED_001"
    assert kwargs["properties"]["parent_span_id"] == "mock-parent-id-123"


def test_trace_span_error_code_priority_3_default_class_name(mock_tracer_deps):
    """
    Test (Priority 3) default class name (KeyError is not in mapping).
    Also sets and checks parent_span_id.
    """
    mock_batch = mock_tracer_deps["batch"].add_object
    tracer = TraceCollector(trace_id="test_trace_p3")
    tracer.settings = mock_tracer_deps["settings"]

    @trace_span()
    def my_key_error_function():
        _ = {}["missing_key"]  # Raises KeyError

    # [NEW] Set up mock parent context
    tracer_token = current_tracer_var.set(tracer)
    parent_span_token = current_span_id_var.set("mock-parent-id-456")

    try:
        with pytest.raises(KeyError):
            my_key_error_function()
    finally:
        current_span_id_var.reset(parent_span_token)
        current_tracer_var.reset(tracer_token)

    args, kwargs = mock_batch.call_args
    assert kwargs["properties"]["status"] == "ERROR"
    assert kwargs["properties"]["error_code"] == "KeyError"
    assert kwargs["properties"]["parent_span_id"] == "mock-parent-id-456"
