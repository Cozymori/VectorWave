"""Format VectorWaveReplayer failures into a pytest-friendly string."""
from __future__ import annotations

import pprint
from typing import Any, Dict, List


_DEFAULT_TRUNCATE = 200


def format_failure_summary(
    function: str,
    total: int,
    failed: int,
    failures: List[Dict[str, Any]],
    *,
    truncate: int = _DEFAULT_TRUNCATE,
) -> str:
    if total == 0:
        return f"vectorwave: no golden samples found for '{function}' — nothing to compare against."

    lines = [
        f"vectorwave: {failed}/{total} regression check(s) failed on '{function}'",
        "",
    ]
    for failure in failures:
        uuid = failure.get("uuid", "?")
        reason = failure.get("reason") or "mismatch"
        golden_tag = " [GOLDEN]" if failure.get("is_golden") else ""
        lines.append(f"  UUID {uuid}  {reason}{golden_tag}")
        inputs = failure.get("inputs") or {}
        if inputs:
            lines.append(f"    inputs:   {_truncate(pprint.pformat(inputs, width=88, depth=2), truncate)}")
        expected = failure.get("expected", "")
        actual = failure.get("actual", "")
        lines.append(f"    expected: {_truncate(repr(expected), truncate)}")
        lines.append(f"    actual:   {_truncate(repr(actual), truncate)}")
        if failure.get("error"):
            lines.append(f"    error:    {failure['error']}")
        lines.append("")

    return "\n".join(lines)


def _truncate(s: str, n: int) -> str:
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."
