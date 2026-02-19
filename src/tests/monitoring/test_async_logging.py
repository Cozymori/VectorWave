import pytest
from unittest.mock import MagicMock, patch
from contextvars import ContextVar

from vectorwave.monitoring.tracer import trace_span, TraceCollector
from vectorwave.models.db_config import WeaviateSettings
from vectorwave.utils.context import execution_source_context

# --- Fixtures ---

@pytest.fixture
def mock_settings():
    """Settings object for testing"""
    settings = MagicMock(spec=WeaviateSettings)
    settings.ASYNC_LOGGING = False  # Default value
    settings.SENSITIVE_FIELD_NAMES = "password,secret"
    settings.sensitive_keys = {"password", "secret"}
    settings.ignored_error_codes = set()
    settings.DRIFT_DETECTION_ENABLED = False
    return settings

@pytest.fixture
def mock_tracer(mock_settings):
    """TraceCollector object for testing"""
    tracer = MagicMock(spec=TraceCollector)
    tracer.settings = mock_settings
    tracer.batch = MagicMock()
    tracer.alerter = MagicMock()
    tracer.alert_sent = False
    return tracer

# --- Tests ---

def test_async_logging_enabled(mock_tracer):
    """
    [Case 1] When ASYNC_LOGGING=True,
    _perform_background_logging should be called via executor.submit.
    """
    mock_tracer.settings.ASYNC_LOGGING = True

    # [Fix] Patch the variable itself, not the .get method.
    with patch('vectorwave.monitoring.tracer.current_tracer_var') as mock_ctx_var, \
            patch('vectorwave.monitoring.tracer._background_executor.submit') as mock_submit, \
            patch('vectorwave.monitoring.tracer._perform_background_logging') as mock_perform:

        # Configure return value of current_tracer_var.get() to return mock_tracer
        mock_ctx_var.get.return_value = mock_tracer

        @trace_span()
        def sample_func(x, y):
            return x + y

        result = sample_func(10, 20)

        assert result == 30
        mock_submit.assert_called_once()
        # Verify that the first argument of submit is the actual logging function
        assert mock_submit.call_args[0][0] == mock_perform
        # Ensure direct execution did not happen (it should run inside submit)
        mock_perform.assert_not_called()


def test_sync_logging_fallback(mock_tracer):
    """
    [Case 2] When ASYNC_LOGGING=False,
    _perform_background_logging should be called directly in the main thread, instead of executor.submit.
    """
    mock_tracer.settings.ASYNC_LOGGING = False

    with patch('vectorwave.monitoring.tracer.current_tracer_var') as mock_ctx_var, \
            patch('vectorwave.monitoring.tracer._background_executor.submit') as mock_submit, \
            patch('vectorwave.monitoring.tracer._perform_background_logging') as mock_perform:

        mock_ctx_var.get.return_value = mock_tracer

        @trace_span()
        def sample_func(x, y):
            return x + y

        result = sample_func(5, 5)

        assert result == 10
        mock_submit.assert_not_called()
        mock_perform.assert_called_once()


def test_force_sync_override(mock_tracer):
    """
    [Case 3] If force_sync=True is provided,
    it should force synchronous execution even if ASYNC_LOGGING is True.
    """
    mock_tracer.settings.ASYNC_LOGGING = True

    with patch('vectorwave.monitoring.tracer.current_tracer_var') as mock_ctx_var, \
            patch('vectorwave.monitoring.tracer._background_executor.submit') as mock_submit, \
            patch('vectorwave.monitoring.tracer._perform_background_logging') as mock_perform:

        mock_ctx_var.get.return_value = mock_tracer

        @trace_span(force_sync=True)
        def critical_func():
            return "critical"

        critical_func()

        mock_submit.assert_not_called()
        mock_perform.assert_called_once()


def test_execution_source_capture_in_async(mock_tracer):
    """
    [Case 4] Verify that the exec_source context is correctly passed
    to the background task arguments even in async mode.
    """
    mock_tracer.settings.ASYNC_LOGGING = True
    test_source = "TEST_CONTROLLER"

    token = execution_source_context.set(test_source)

    try:
        with patch('vectorwave.monitoring.tracer.current_tracer_var') as mock_ctx_var, \
                patch('vectorwave.monitoring.tracer._background_executor.submit') as mock_submit:

            mock_ctx_var.get.return_value = mock_tracer

            @trace_span()
            def context_func():
                return "ok"

            context_func()

            # submit is called as submit(fn, SpanContext) — check exec_source inside SpanContext
            call_args = mock_submit.call_args[0]
            span_ctx = call_args[-1]  # SpanContext is the last positional arg

            assert span_ctx.exec_source == test_source, f"Expected '{test_source}', but got '{span_ctx.exec_source}'"

    finally:
        execution_source_context.reset(token)


@pytest.mark.asyncio
async def test_async_wrapper_logging(mock_tracer):
    """
    [Case 5] Verify that async logging configuration applies to Async functions (Coroutines) as well.
    """
    mock_tracer.settings.ASYNC_LOGGING = True

    with patch('vectorwave.monitoring.tracer.current_tracer_var') as mock_ctx_var, \
            patch('vectorwave.monitoring.tracer._background_executor.submit') as mock_submit, \
            patch('vectorwave.monitoring.tracer._perform_background_logging') as mock_perform:

        mock_ctx_var.get.return_value = mock_tracer

        @trace_span()
        async def async_sample_func():
            return "async_result"

        result = await async_sample_func()

        assert result == "async_result"
        mock_submit.assert_called_once()
        assert mock_submit.call_args[0][0] == mock_perform