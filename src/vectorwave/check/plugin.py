"""Pytest plugin entry point for VectorWave Check.

Two surfaces:

* A ``vectorwave`` marker that fully drives a regression check on a target
  function (declarative).
* A ``vw_replay`` fixture that returns the replay callable for programmatic
  inspection (imperative).

The marker is the lead. The fixture is for power-users who want to keep
their own assertions / debugging logic around the replay result.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pytest

from .config import resolve_config
from .report import format_failure_summary


@dataclass
class ReplayResult:
    """Plugin-facing wrapper around ``VectorWaveReplayer.replay()``'s dict."""

    function: str
    total: int = 0
    passed: int = 0
    failed: int = 0
    updated: int = 0
    failures: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    @property
    def passed_all(self) -> bool:
        return self.error is None and self.failed == 0 and self.total > 0

    def report(self) -> str:
        if self.error:
            return f"vectorwave: replay failed to start — {self.error}"
        return format_failure_summary(
            self.function, self.total, self.failed, self.failures
        )

    @classmethod
    def from_raw(cls, raw: Dict[str, Any]) -> "ReplayResult":
        return cls(
            function=raw.get("function", "?"),
            total=raw.get("total", 0),
            passed=raw.get("passed", 0),
            failed=raw.get("failed", 0),
            updated=raw.get("updated", 0),
            failures=raw.get("failures", []),
            error=raw.get("error"),
        )


def _run_replay(
    *,
    target: str,
    strategy: str,
    threshold: float,
    limit: int,
    mocks: Optional[Dict[str, Any]],
) -> ReplayResult:
    """Dispatch to the right replayer based on strategy.

    Imports are local to keep plugin import-time cheap — constructing a
    replayer hits Weaviate, which we want to avoid during pytest collection.
    """
    from vectorwave.utils.replayer import VectorWaveReplayer
    from vectorwave.utils.replayer_semantic import SemanticReplayer

    if strategy == "exact":
        raw = VectorWaveReplayer().replay(
            function_full_name=target, limit=limit, mocks=mocks
        )
    elif strategy == "similarity" or strategy == "auto":
        raw = SemanticReplayer().replay(
            function_full_name=target,
            limit=limit,
            similarity_threshold=threshold,
            semantic_eval=False,
            mocks=mocks,
        )
    elif strategy == "llm":
        raw = SemanticReplayer().replay(
            function_full_name=target,
            limit=limit,
            similarity_threshold=None,
            semantic_eval=True,
            mocks=mocks,
        )
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return ReplayResult.from_raw(raw)


def pytest_configure(config: "pytest.Config") -> None:
    config.addinivalue_line(
        "markers",
        "vectorwave(target, strategy='auto', threshold=0.85, limit=10, mocks=None): "
        "run a VectorWave semantic regression check on `target` "
        "(fully-qualified function name) against its golden data.",
    )


@pytest.hookimpl(tryfirst=True)
def pytest_pyfunc_call(pyfuncitem: "pytest.Function") -> Optional[bool]:
    """Intercept tests marked with @pytest.mark.vectorwave.

    Returning True signals pytest that the call has been handled and the
    test function body should not be executed. Returning None falls through
    to the default caller for unmarked tests.
    """
    marker = pyfuncitem.get_closest_marker("vectorwave")
    if marker is None:
        return None

    target = marker.kwargs.get("target")
    if not target:
        pytest.fail(
            "@pytest.mark.vectorwave requires a `target` kwarg "
            "(fully-qualified function name, e.g. 'myapp.summarize').",
            pytrace=False,
        )

    cfg = resolve_config(target, marker.kwargs, pytestconfig=pyfuncitem.config)
    result = _run_replay(
        target=target,
        strategy=cfg["strategy"],
        threshold=cfg["threshold"],
        limit=cfg["limit"],
        mocks=cfg["mocks"],
    )

    if not result.passed_all:
        pytest.fail(result.report(), pytrace=False)
    return True


@pytest.fixture
def vw_replay(pytestconfig: "pytest.Config"):
    """Imperative replay callable. Returns a ``ReplayResult``."""

    def _call(
        target: str,
        *,
        strategy: Optional[str] = None,
        threshold: Optional[float] = None,
        limit: Optional[int] = None,
        mocks: Optional[Dict[str, Any]] = None,
    ) -> ReplayResult:
        cfg = resolve_config(
            target,
            {
                "strategy": strategy,
                "threshold": threshold,
                "limit": limit,
                "mocks": mocks,
            },
            pytestconfig=pytestconfig,
        )
        return _run_replay(
            target=target,
            strategy=cfg["strategy"],
            threshold=cfg["threshold"],
            limit=cfg["limit"],
            mocks=cfg["mocks"],
        )

    return _call
