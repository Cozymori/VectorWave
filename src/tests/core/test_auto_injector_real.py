import pytest
import sys
import types
from unittest.mock import patch, MagicMock

# Import actual modules
from vectorwave import vectorize, VectorWaveAutoInjector

# --- Fixtures ---

@pytest.fixture
def mock_db_dependencies():
    """
    Mocks heavy dependencies to prevent @vectorize decorator from internally calling
    DB or AI models.
    (The wrapping logic itself runs for 'real')
    """
    with patch("vectorwave.core.decorator.get_batch_manager") as mock_batch, \
            patch("vectorwave.core.decorator.get_vectorizer") as mock_vec, \
            patch("vectorwave.core.decorator.function_cache_manager"), \
            patch("vectorwave.core.decorator.get_weaviate_settings"), \
            patch("vectorwave.core.decorator.generate_uuid5", return_value="mock-uuid"):

        # Return fake DB settings (prevent errors)
        mock_settings = MagicMock()
        mock_settings.custom_properties = {}
        mock_settings.COLLECTION_NAME = "VectorWaveFunction"
        patch("vectorwave.core.decorator.get_weaviate_settings", return_value=mock_settings).start()

        yield

# --- Real Function Integration Test ---

def test_real_function_mixed_usage(mock_db_dependencies):
    """
    [Integration Test] Verifies that double wrapping is prevented when mixing
    actual @vectorize decorator and AutoInjector using real function objects.
    """
    # 1. Create a fake module (acts like a real Python module)
    module_name = "my_real_test_module"
    mod = types.ModuleType(module_name)

    # -------------------------------------------------------
    # (A) Manually vectorized function (Using Real Decorator)
    # -------------------------------------------------------
    @vectorize(team="manual_team", auto=True)
    def manual_func(x):
        return x + 1

    # -------------------------------------------------------
    # (B) Plain function without any decorator
    # -------------------------------------------------------
    def plain_func(y):
        return y * 2

    # [Important] Set __module__ so Injector recognizes them as internal functions
    manual_func.__module__ = module_name
    plain_func.__module__ = module_name

    # Register functions to the module
    mod.manual_func = manual_func
    mod.plain_func = plain_func

    # Register to sys.modules so importlib can find it
    sys.modules[module_name] = mod

    try:
        # 2. Check state before injection
        print("\n[Before Injection]")
        print(f"manual_func vectorized? {getattr(mod.manual_func, '_is_vectorized', False)}")
        print(f"plain_func vectorized?  {getattr(mod.plain_func, '_is_vectorized', False)}")

        # manual_func should already have the decorator applied
        assert getattr(mod.manual_func, "_is_vectorized", False) is True
        # plain_func should be a pure function
        assert getattr(mod.plain_func, "_is_vectorized", False) is False

        # Save object ID of manual_func before injection (for comparison)
        original_manual_func_id = id(mod.manual_func)

        # -------------------------------------------------------
        # 3. Execute AutoInjector (Real Injection)
        # -------------------------------------------------------
        print("\n🌊 Injecting VectorWave...")
        VectorWaveAutoInjector.inject(module_name, team="auto_team")

        # 4. Verify results
        print("\n[After Injection]")

        # (A) Verify manual_func: Prevent double wrapping
        # If AutoInjector skipped it, the object ID should remain unchanged.
        current_manual_func_id = id(mod.manual_func)

        if current_manual_func_id == original_manual_func_id:
            print("✅ manual_func: SKIPPED (Identity preserved)")
        else:
            print("❌ manual_func: WRAPPED AGAIN (Identity changed!)")

        assert current_manual_func_id == original_manual_func_id, \
            "Manually configured function was double-wrapped by AutoInjector!"

        # (B) Verify plain_func: Auto-wrapping success
        # Since it's newly wrapped, _is_vectorized should become True.
        is_vectorized = getattr(mod.plain_func, "_is_vectorized", False)
        print(f"plain_func vectorized?  {is_vectorized}")

        assert is_vectorized is True, "Plain function was not auto-injected!"

    finally:
        # Cleanup module after test
        del sys.modules[module_name]