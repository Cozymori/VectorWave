# Async trace_root + trace_span tests. The @vectorize-async tests have been
# moved to tests.core.test_decorator (where the e2e fixture lives).
import pytest
import asyncio

from ..monitoring.test_tracer import mock_tracer_deps  # noqa: F401  (used by tests below)

from vectorwave.monitoring.tracer import trace_root, trace_span


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

