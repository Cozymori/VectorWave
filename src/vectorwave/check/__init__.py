"""VectorWave Check: pytest plugin + CLI for semantic regression testing.

Quick usage (declarative marker):

    @pytest.mark.vectorwave(target="myapp.summarize", strategy="similarity", threshold=0.85)
    def test_summarize_regression():
        pass

Quick usage (imperative fixture):

    def test_summarize_regression(vw_replay):
        result = vw_replay("myapp.summarize", strategy="similarity", threshold=0.85)
        assert result.passed_all, result.report()
"""
from .plugin import ReplayResult

__all__ = ["ReplayResult"]
