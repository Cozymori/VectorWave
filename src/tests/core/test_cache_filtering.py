import pytest
from unittest.mock import patch, MagicMock

# Import VectorWave module
from vectorwave.core.decorator import vectorize

class TestVectorWaveExtensions:

    @pytest.fixture
    def mock_dependencies(self):
        """Mocks external dependencies of VectorWave."""
        # [Important] The patch path must be where the function is 'used' (decorator.py),
        # not where it is defined.
        with patch('vectorwave.core.decorator.get_batch_manager') as mock_batch, \
                patch('vectorwave.core.decorator.get_vectorizer') as mock_vec, \
                patch('vectorwave.core.decorator.function_cache_manager') as mock_cache, \
                patch('vectorwave.core.decorator.generate_uuid5', return_value="mock-uuid"), \
                patch('vectorwave.core.decorator.trace_span') as mock_trace_span, \
                patch('vectorwave.core.decorator._check_and_return_cached_result') as mock_check_cache:

            # Default setup (Must not be None to pass internal decorator checks)
            mock_vec.return_value = MagicMock()

            yield {
                'batch': mock_batch,
                'vectorizer': mock_vec,
                'trace_span': mock_trace_span,
                'check_cache': mock_check_cache
            }

    def test_capture_inputs_auto_detection(self, mock_dependencies):
        """
        [Feature 1] Test if function parameters are automatically included
        in attributes_to_capture when capture_inputs=True is set.
        """
        mock_trace_span = mock_dependencies['trace_span']

        @vectorize(capture_inputs=True, auto=False)
        def process_user_data(user_id: int, user_name: str, is_active: bool = True):
            return "processed"

        process_user_data(101, "Alice")

        call_args = mock_trace_span.call_args
        assert call_args is not None

        kwargs = call_args.kwargs
        captured_attrs = kwargs.get('attributes_to_capture', [])

        assert 'user_id' in captured_attrs
        assert 'user_name' in captured_attrs
        assert 'is_active' in captured_attrs
        assert 'function_uuid' in captured_attrs

    def test_semantic_cache_static_filters(self, mock_dependencies):
        """
        [Feature 2] Test if semantic_cache_filters (static filters)
        are correctly passed to the caching logic (_check_and_return_cached_result).
        """
        mock_check_cache = mock_dependencies['check_cache']
        mock_check_cache.return_value = None  # Assume cache miss

        static_filter = {'environment': 'production', 'version__gte': 2}

        @vectorize(
            semantic_cache=True,
            semantic_cache_filters=static_filter
        )
        def get_server_status():
            return "OK"

        get_server_status()

        call_args = mock_check_cache.call_args
        assert call_args is not None

        passed_filters = call_args.kwargs.get('filters')
        assert passed_filters == static_filter

    def test_semantic_cache_dynamic_scope(self, mock_dependencies):
        """
        [Feature 3] Test if semantic_cache_scope (dynamic filters)
        correctly generates filters based on argument values at runtime.
        """
        mock_check_cache = mock_dependencies['check_cache']
        mock_check_cache.return_value = None

        @vectorize(
            semantic_cache=True,
            semantic_cache_scope=['region', 'user_type']
        )
        def fetch_dashboard(region, user_type, date):
            return f"Dashboard for {region}"

        fetch_dashboard(region='KR', user_type='admin', date='2023-10-25')

        call_args = mock_check_cache.call_args
        assert call_args is not None

        passed_filters = call_args.kwargs.get('filters')

        assert passed_filters['region'] == 'KR'
        assert passed_filters['user_type'] == 'admin'
        assert 'date' not in passed_filters

    def test_mixed_filters(self, mock_dependencies):
        """
        [Feature 4] Test if static filters and dynamic scope are merged correctly when used together.
        """
        mock_check_cache = mock_dependencies['check_cache']
        mock_check_cache.return_value = None

        @vectorize(
            semantic_cache=True,
            semantic_cache_filters={'status': 'active'},
            semantic_cache_scope=['project_id']
        )
        def get_project_metrics(project_id):
            return 100

        get_project_metrics(project_id=999)

        call_args = mock_check_cache.call_args
        assert call_args is not None

        passed_filters = call_args.kwargs.get('filters')

        assert passed_filters['status'] == 'active'
        assert passed_filters['project_id'] == 999