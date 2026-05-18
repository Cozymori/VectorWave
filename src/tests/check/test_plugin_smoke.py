"""Smoke tests for the vectorwave-check pytest plugin.

These verify the plugin's structural correctness without hitting Weaviate.
End-to-end golden-data tests against a real store live in nightly_live.
"""
from __future__ import annotations

pytest_plugins = ["pytester"]


def test_marker_is_registered_under_strict(pytester):
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(target="fake.target")
        def test_dummy():
            pass
        """
    )
    pytester.makeconftest(
        """
        import pytest
        from vectorwave.check import plugin
        from vectorwave.check.plugin import ReplayResult

        @pytest.fixture(autouse=True)
        def _fake_replay(monkeypatch):
            monkeypatch.setattr(
                plugin,
                "_run_replay",
                lambda **_: ReplayResult(function="fake.target", total=1, passed=1, failed=0),
            )
        """
    )
    result = pytester.runpytest("--strict-markers")
    result.assert_outcomes(passed=1)


def test_marker_requires_target(pytester):
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(strategy="exact")
        def test_no_target():
            pass
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(["*requires a `target`*"])


def test_fixture_is_injected(pytester):
    pytester.makepyfile(
        """
        def test_fixture_callable(vw_replay):
            assert callable(vw_replay)
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_marker_passes_when_replay_clean(pytester):
    pytester.makeconftest(
        """
        import pytest
        from vectorwave.check import plugin
        from vectorwave.check.plugin import ReplayResult

        @pytest.fixture(autouse=True)
        def _fake_replay(monkeypatch):
            monkeypatch.setattr(
                plugin,
                "_run_replay",
                lambda **_: ReplayResult(function="fake.target", total=5, passed=5, failed=0),
            )
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(target="fake.target", strategy="similarity", threshold=0.85)
        def test_replay():
            pass
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_marker_fails_with_report_when_replay_dirty(pytester):
    pytester.makeconftest(
        """
        import pytest
        from vectorwave.check import plugin
        from vectorwave.check.plugin import ReplayResult

        @pytest.fixture(autouse=True)
        def _fake_replay(monkeypatch):
            monkeypatch.setattr(
                plugin,
                "_run_replay",
                lambda **_: ReplayResult(
                    function="fake.target",
                    total=3,
                    passed=1,
                    failed=2,
                    failures=[
                        {"uuid": "abc", "reason": "Low Similarity (0.70 < 0.85)",
                         "expected": "hello world", "actual": "hi"},
                        {"uuid": "def", "reason": "Exact match failed",
                         "expected": 42, "actual": 41},
                    ],
                ),
            )
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(target="fake.target", strategy="similarity", threshold=0.85)
        def test_replay():
            pass
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1)
    result.stdout.fnmatch_lines(
        [
            "*2/3 regression check*failed*",
            "*UUID abc*Low Similarity*",
            "*UUID def*Exact match failed*",
        ]
    )


def test_invalid_strategy_raises_clear_error(pytester):
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(target="fake.target", strategy="nonsense")
        def test_bogus():
            pass
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(failed=1, errors=0)
    result.stdout.fnmatch_lines(["*Invalid strategy*nonsense*"])


def test_fixture_returns_replay_result(pytester):
    pytester.makeconftest(
        """
        import pytest
        from vectorwave.check import plugin
        from vectorwave.check.plugin import ReplayResult

        @pytest.fixture(autouse=True)
        def _fake_replay(monkeypatch):
            monkeypatch.setattr(
                plugin,
                "_run_replay",
                lambda **_: ReplayResult(function="fake.target", total=2, passed=2, failed=0),
            )
        """
    )
    pytester.makepyfile(
        """
        def test_fixture(vw_replay):
            result = vw_replay("fake.target", strategy="exact")
            assert result.passed_all
            assert result.total == 2
        """
    )
    result = pytester.runpytest()
    result.assert_outcomes(passed=1)


def test_pyproject_config_layers_under_marker(pytester):
    pytester.makepyfile(
        pyproject="",  # placeholder, real one below
    )
    pytester.makefile(
        ".toml",
        pyproject=(
            "[tool.vectorwave.check]\n"
            'strategy = "exact"\n'
            "threshold = 0.7\n"
            "\n"
            "[tool.vectorwave.check.\"fake.target\"]\n"
            'strategy = "similarity"\n'
            "threshold = 0.9\n"
        ),
    )
    pytester.makeconftest(
        """
        import pytest
        from vectorwave.check import plugin
        from vectorwave.check.plugin import ReplayResult

        captured = {}

        @pytest.fixture(autouse=True)
        def _spy(monkeypatch):
            def fake(**kw):
                captured.update(kw)
                return ReplayResult(function=kw["target"], total=1, passed=1, failed=0)
            monkeypatch.setattr(plugin, "_run_replay", fake)

        @pytest.fixture
        def captured_kw():
            return captured
        """
    )
    pytester.makepyfile(
        """
        import pytest

        @pytest.mark.vectorwave(target="fake.target")
        def test_uses_per_function_table():
            pass

        def test_assertions(captured_kw):
            # per-function table beats global defaults
            assert captured_kw["strategy"] == "similarity"
            assert captured_kw["threshold"] == 0.9
        """
    )
    result = pytester.runpytest("-p", "no:cacheprovider")
    result.assert_outcomes(passed=2)
