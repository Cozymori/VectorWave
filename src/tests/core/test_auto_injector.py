import pytest
import sys
import types
from unittest.mock import MagicMock, ANY
from vectorwave.core.auto_injector import VectorWaveAutoInjector, create_smart_wrapper

# --- Fixtures ---

@pytest.fixture
def mock_target_module():
    """Dynamically creates a fake module for testing."""
    module_name = "fake_business_logic"
    mod = types.ModuleType(module_name)

    # Define functions for testing
    def func_a(x): return x + 1
    def func_b(y): return y * 2

    # Set __module__ attribute so the Injector recognizes them as 'functions of this module'
    func_a.__module__ = module_name
    func_b.__module__ = module_name

    mod.func_a = func_a
    mod.func_b = func_b

    # Register to sys.modules
    sys.modules[module_name] = mod
    yield mod
    # Cleanup
    del sys.modules[module_name]

@pytest.fixture
def mock_deps(monkeypatch):
    """
    Mocks vectorize, trace_span, and contextvar that auto_injector depends on.
    """
    mock_vectorize = MagicMock()
    mock_trace_span = MagicMock()
    mock_tracer_var = MagicMock()

    # Mock vectorize: @vectorize(...) -> decorator -> returns root_wrapper
    def vectorize_side_effect(**kwargs):
        def decorator(func):
            # Return a mock acting as root_wrapper (Default behavior: execute original function)
            root_wrapper = MagicMock(side_effect=func)
            return root_wrapper
        return decorator

    # [Modified] Mock trace_span: Modified to accept positional arguments (*args)
    def trace_span_side_effect(*args, **kwargs):
        # Case 1: Called as trace_span(func, capture_return_value=True)
        if args and callable(args[0]):
            func = args[0]
            def wrapper(*w_args, **w_kwargs):
                return func(*w_args, **w_kwargs)
            return wrapper

        # Case 2: Called as a decorator factory @trace_span(...)
        def decorator(func):
            def wrapper(*w_args, **w_kwargs):
                return func(*w_args, **w_kwargs)
            return wrapper
        return decorator

    mock_vectorize.side_effect = vectorize_side_effect
    mock_trace_span.side_effect = trace_span_side_effect

    # Patch according to the module path
    monkeypatch.setattr("vectorwave.core.auto_injector.vectorize", mock_vectorize)
    monkeypatch.setattr("vectorwave.core.auto_injector.trace_span", mock_trace_span)
    monkeypatch.setattr("vectorwave.core.auto_injector.current_tracer_var", mock_tracer_var)

    return {
        "vectorize": mock_vectorize,
        "trace_span": mock_trace_span,
        "tracer_var": mock_tracer_var
    }

@pytest.fixture
def mock_multiple_modules():
    """Dynamically creates two different fake modules (Payment, Auth)."""
    name_a = "service_payment"
    name_b = "service_auth"

    mod_a = types.ModuleType(name_a)
    mod_b = types.ModuleType(name_b)

    def pay(x): return x
    def login(x): return x

    pay.__module__ = name_a
    login.__module__ = name_b

    mod_a.process_payment = pay
    mod_b.login_user = login

    sys.modules[name_a] = mod_a
    sys.modules[name_b] = mod_b

    yield mod_a, mod_b

    del sys.modules[name_a]
    del sys.modules[name_b]

# --- Test Cases ---

def test_inject_calls_vectorize_immediately(mock_target_module, mock_deps):
    """
    Case 1: Verify that vectorize is called immediately at injection time to register metadata.
    """
    # Act
    VectorWaveAutoInjector.inject("fake_business_logic", team="test-team")

    # Assert
    # 1. vectorize should have been called (Immediate registration)
    mock_deps["vectorize"].assert_called()

    # 2. Verify passed configuration
    call_kwargs = mock_deps["vectorize"].call_args.kwargs
    assert call_kwargs["team"] == "test-team"

    # 3. Verify the function is wrapped
    assert getattr(mock_target_module.func_a, "_is_vectorized") is True

def test_smart_wrapper_root_execution(mock_target_module, mock_deps):
    """
    Case 2: Verify that the pre-created root_wrapper is executed when there is no tracer (Root).
    """
    # Arrange
    mock_deps["tracer_var"].get.return_value = None # No parent trace

    # [Modified] Remove fixture's side_effect so return_value setting takes effect.
    mock_deps["vectorize"].side_effect = None

    # vectorize(...) -> decorator_mock returns -> decorator_mock(func) -> root_wrapper_mock returns
    decorator_mock = MagicMock()
    root_wrapper_mock = MagicMock(return_value=11)

    mock_deps["vectorize"].return_value = decorator_mock
    decorator_mock.return_value = root_wrapper_mock

    VectorWaveAutoInjector.inject("fake_business_logic")

    # Act
    result = mock_target_module.func_a(10)

    # Assert
    assert result == 11
    # root_wrapper should have been called
    root_wrapper_mock.assert_called_once()
    # trace_span should not be called (logic is already inside root_wrapper)
    mock_deps["trace_span"].assert_not_called()

def test_smart_wrapper_child_execution(mock_target_module, mock_deps):
    """
    Case 3: Verify that trace_span is dynamically called when there is a tracer (Child).
    """
    # Arrange
    mock_deps["tracer_var"].get.return_value = MagicMock() # Parent trace exists

    # [Modified] Remove side_effect and set Mock
    mock_deps["vectorize"].side_effect = None

    decorator_mock = MagicMock()
    root_wrapper_mock = MagicMock(return_value=999) # Should not be called

    mock_deps["vectorize"].return_value = decorator_mock
    decorator_mock.return_value = root_wrapper_mock

    VectorWaveAutoInjector.inject("fake_business_logic")

    # Act
    # trace_span mock is configured to call the original function (see fixture)
    # func_b: y * 2 -> 5 * 2 = 10
    result = mock_target_module.func_b(5)

    # Assert
    assert result == 10
    # trace_span should have been called
    mock_deps["trace_span"].assert_called()
    # root_wrapper should not have been called
    root_wrapper_mock.assert_not_called()

def test_prevent_double_injection(mock_target_module, mock_deps):
    """
    Case 4: Verify that calling inject again on an already injected function does not result in double wrapping.
    """
    # 1. First Injection
    VectorWaveAutoInjector.inject("fake_business_logic")
    first_wrapped_func = mock_target_module.func_a
    call_count_first = mock_deps["vectorize"].call_count

    # 2. Second Injection
    VectorWaveAutoInjector.inject("fake_business_logic")
    second_wrapped_func = mock_target_module.func_a
    call_count_second = mock_deps["vectorize"].call_count

    # Assert
    # Object addresses must be identical
    assert first_wrapped_func is second_wrapped_func
    # vectorize call count should not increase (Prevent duplicate registration)
    assert call_count_first == call_count_second

def test_inject_using_config_dictionary_pattern(mock_multiple_modules, mock_deps):
    """
    Case 5: Verify that configurations are correctly applied when injecting multiple modules in a batch.
    """
    mod_a, mod_b = mock_multiple_modules

    # Injection
    VectorWaveAutoInjector.configure(team="default") # Global config example

    VectorWaveAutoInjector.inject("service_payment", priority=1)
    VectorWaveAutoInjector.inject("service_auth", team="security")

    # Verify: Check arguments passed to vectorize calls
    calls = mock_deps["vectorize"].call_args_list
    # Collect kwargs
    configs = [c.kwargs for c in calls]

    # service_payment: default team + priority 1
    config_a = next((c for c in configs if c.get("priority") == 1), None)
    assert config_a is not None
    assert config_a["team"] == "default"

    # service_auth: security team (override)
    config_b = next((c for c in configs if c.get("team") == "security"), None)
    assert config_b is not None