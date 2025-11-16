# src/tests/test_async_support.py
import pytest
import asyncio
from unittest.mock import patch, MagicMock

# --- Fixtures ---
from ..monitoring.test_tracer import mock_tracer_deps
from ..core.test_decorator import mock_decorator_deps

# --- Imports from the library to test ---
from vectorwave.monitoring.tracer import trace_root, trace_span
from vectorwave.core.decorator import vectorize


# ==================================
# 1. tracer.py async tests
# ==================================

@pytest.mark.asyncio
async def test_trace_root_and_span_async_success(mock_tracer_deps):
    '''
    Case 1: Async workflow (Root + Span) success test
    - ContextVar (trace_id) must be correctly propagated across 'await' boundaries
    '''
    mock_batch = mock_tracer_deps["batch"]
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span(attributes_to_capture=['x'])
    async def my_async_inner_span(x):
        await asyncio.sleep(0.01)  # Simulate IO
        return f"result: {x}"

    @trace_root()
    @trace_span()
    async def my_async_workflow_root():
        result = await my_async_inner_span(x=10)
        await asyncio.sleep(0.01)  # Additional IO simulation
        return result

    mock_batch.reset_mock()
    mock_alerter.reset_mock()

    # --- Execution ---
    result = await my_async_workflow_root()

    # --- Verification ---
    assert result == "result: 10"

    # 2 calls expected: 1 (inner_span) + 1 (workflow_root)
    assert mock_batch.add_object.call_count == 2

    mock_alerter.notify.assert_not_called()

    call_args_list = mock_batch.add_object.call_args_list
    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in call_args_list
    }

    assert "my_async_inner_span" in props_map
    assert "my_async_workflow_root" in props_map

    inner_props = props_map["my_async_inner_span"]
    root_props = props_map["my_async_workflow_root"]

    # Verify status and captured attributes
    assert inner_props["status"] == "SUCCESS"
    assert inner_props["x"] == 10
    assert root_props["status"] == "SUCCESS"

    # --- Core verification ---
    # Both spans must have the same trace_id
    assert inner_props["trace_id"] is not None
    assert inner_props["trace_id"] == root_props["trace_id"]

    # Global tags must also be applied identically
    assert inner_props["run_id"] == "global-run-abc"
    assert root_props["run_id"] == "global-run-abc"


@pytest.mark.asyncio
async def test_trace_span_async_failure(mock_tracer_deps):
    '''
    Case 2: Async span failure test
    '''
    mock_batch = mock_tracer_deps["batch"]
    mock_alerter = mock_tracer_deps["alerter"]

    class AsyncTestError(Exception):
        @property
        def error_code(self):
            return "ASYNC_TEST_FAILURE"

    @trace_span
    async def my_failing_async_span():
        await asyncio.sleep(0.01)
        raise AsyncTestError("Async failure test")

    @trace_root()
    async def my_async_workflow_fail():
        await my_failing_async_span()

    # --- Execution and Verification (Exception) ---
    with pytest.raises(AsyncTestError, match="Async failure test"):
        await my_async_workflow_fail()

    mock_alerter.notify.assert_called_once()
    alert_props = mock_alerter.notify.call_args.args[0]

    assert alert_props["status"] == "ERROR"
    assert "AsyncTestError: Async failure test" in alert_props["error_message"]
    assert alert_props["function_name"] == "my_failing_async_span"
    assert alert_props["error_code"] == "ASYNC_TEST_FAILURE"

    mock_batch.add_object.assert_called_once()
    db_props = mock_batch.add_object.call_args.kwargs["properties"]

    assert db_props == alert_props
    assert db_props["span_id"] == alert_props["span_id"]

    # --- Verification (Log) ---
    # Check if the failed span log was recorded
    failing_span_props = None
    for call in mock_batch.add_object.call_args_list:
        if call.kwargs["properties"]["function_name"] == "my_failing_async_span":
            failing_span_props = call.kwargs["properties"]
            break

    assert failing_span_props is not None
    assert failing_span_props["status"] == "ERROR"
    assert "AsyncTestError: Async failure test" in failing_span_props["error_message"]


@pytest.mark.asyncio
async def test_span_without_root_async_does_nothing(mock_tracer_deps):
    '''
    Case 3: Async span called without a root (@trace_root) should not be logged
    '''
    mock_batch = mock_tracer_deps["batch"]
    mock_alerter = mock_tracer_deps["alerter"]

    @trace_span
    async def my_lonely_async_span():
        await asyncio.sleep(0.01)
        return "lonely_result"

    result = await my_lonely_async_span()

    assert result == "lonely_result"
    mock_batch.add_object.assert_not_called()
    mock_alerter.notify.assert_not_called()


# ==================================
# 2. decorator.py async tests
# ==================================

@pytest.mark.asyncio
async def test_vectorize_async_dynamic_data_logging_success(mock_decorator_deps):
    '''
    Case 4: Test successful execution of @vectorize applied to an async function
    '''
    mock_batch = mock_decorator_deps["batch"]
    mock_settings = mock_decorator_deps["settings"]

    # Reset mock call count for this test
    mock_batch.reset_mock()

    @vectorize(
        search_description="Async test",
        sequence_narrative="Next",
        team="async-team"  # Execution tag
    )
    async def my_async_vectorized_func(x):
        await asyncio.sleep(0.01)
        return f"async result {x}"

    # --- Verification (Decoration time) ---
    # Static log (function definition) must be recorded once when the decorator is loaded
    mock_batch.add_object.assert_called_once()
    static_call_args = mock_batch.add_object.call_args
    assert static_call_args.kwargs["collection"] == mock_settings.COLLECTION_NAME
    assert static_call_args.kwargs["properties"]["function_name"] == "my_async_vectorized_func"

    # --- Execution ---
    result = await my_async_vectorized_func(x=5)

    # --- Verification (Execution time) ---
    assert result == "async result 5"

    # A second call (dynamic execution log) must occur (2 total)
    assert mock_batch.add_object.call_count == 2

    dynamic_call_args = mock_batch.add_object.call_args
    props = dynamic_call_args.kwargs["properties"]

    assert dynamic_call_args.kwargs["collection"] == mock_settings.EXECUTION_COLLECTION_NAME
    assert props["status"] == "SUCCESS"
    assert props["duration_ms"] > 0
    # Both global tag (run_id) and function tag (team) must be included
    assert props["run_id"] == "test-run-abc"
    assert props["team"] == "async-team"


@pytest.mark.asyncio
async def test_vectorize_async_with_async_child_spans(mock_decorator_deps):
    '''
    Case 5: End-to-End async workflow test for @vectorize (root)
            and async @trace_span (child)
    '''
    mock_batch = mock_decorator_deps["batch"]
    mock_batch.reset_mock()  # Reset for test

    # --- Workflow Definition ---
    @trace_span(attributes_to_capture=['user_id', 'amount'])
    async def async_step_1_validate(user_id: str, amount: int):
        await asyncio.sleep(0.01)
        return True

    @trace_span(attributes_to_capture=['user_id', 'receipt_id'])
    async def async_step_2_send_receipt(user_id: str, receipt_id: str):
        await asyncio.sleep(0.01)
        return "sent"

    @vectorize(
        search_description="Async payment workflow",
        sequence_narrative="...",
        team="billing"
    )
    async def async_process_payment(user_id: str, amount: int):
        await async_step_1_validate(user_id=user_id, amount=amount)
        receipt_id = f"async_receipt_{user_id}"
        await async_step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)
        return {"status": "success", "receipt_id": receipt_id}

    # --- Verification (Decoration time) ---
    # 1 static log recorded
    assert mock_batch.add_object.call_count == 1

    # --- Execution ---
    result = await async_process_payment("user_async_123", 500)

    # --- Verification (Execution time) ---
    assert result["status"] == "success"

    # Total calls = 1 (static) + 3 (dynamic: 1 root, 2 children) = 4
    assert mock_batch.add_object.call_count == 4

    all_calls = mock_batch.add_object.call_args_list
    dynamic_calls = all_calls[1:]  # Exclude static log

    assert len(dynamic_calls) == 3

    props_map = {
        call.kwargs["properties"]["function_name"]: call.kwargs["properties"]
        for call in dynamic_calls
    }

    # Check if all 3 dynamic spans were recorded
    assert "async_step_1_validate" in props_map
    assert "async_step_2_send_receipt" in props_map
    assert "async_process_payment" in props_map

    props1 = props_map["async_step_1_validate"]
    props2 = props_map["async_step_2_send_receipt"]
    props_root = props_map["async_process_payment"]

    # --- Core verification ---
    # All 3 dynamic spans must share the same trace_id
    trace_id = props_root["trace_id"]
    assert trace_id is not None
    assert props1["trace_id"] == trace_id
    assert props2["trace_id"] == trace_id

    # Verify captured attributes
    assert props1["user_id"] == "user_async_123"
    assert props1["amount"] == 500
    assert props2["receipt_id"] == "async_receipt_user_async_123"

    # Verify tags (root's tag and child's global tag)
    assert props_root["team"] == "billing"
    assert props1["run_id"] == "test-run-abc"