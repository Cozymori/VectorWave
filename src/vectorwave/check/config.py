"""Config resolution for the vectorwave-check plugin.

Priority (highest first):
    1. Marker kwargs (or fixture call kwargs)
    2. ``[tool.vectorwave.check."<target>"]`` table in pyproject.toml
    3. ``[tool.vectorwave.check]`` table in pyproject.toml
    4. Built-in defaults
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional

try:
    import tomllib
except ImportError:  # Python 3.10
    import tomli as tomllib  # type: ignore[no-redef]


DEFAULTS: Dict[str, Any] = {
    "strategy": "auto",
    "threshold": 0.85,
    "limit": 10,
    "mocks": None,
}

_VALID_STRATEGIES = {"auto", "exact", "similarity", "llm"}


def _load_check_table(rootpath: Path) -> Dict[str, Any]:
    pyproject = rootpath / "pyproject.toml"
    if not pyproject.exists():
        return {}
    with open(pyproject, "rb") as f:
        data = tomllib.load(f)
    return data.get("tool", {}).get("vectorwave", {}).get("check", {}) or {}


def resolve_config(
    target: str,
    overrides: Dict[str, Any],
    *,
    pytestconfig: Optional[Any] = None,
) -> Dict[str, Any]:
    """Resolve config for a single target by layering sources.

    ``overrides`` carries marker kwargs or fixture call kwargs. Only keys
    whose value is not ``None`` win against the lower layers — pytest markers
    populate kwargs eagerly, and a ``None`` should mean "fall back."
    """
    cfg: Dict[str, Any] = dict(DEFAULTS)

    if pytestconfig is not None:
        table = _load_check_table(Path(pytestconfig.rootpath))
        defaults_from_toml = {k: v for k, v in table.items() if not isinstance(v, dict)}
        for k, v in defaults_from_toml.items():
            if k in DEFAULTS:
                cfg[k] = v
        per_fn = table.get(target)
        if isinstance(per_fn, dict):
            for k, v in per_fn.items():
                if k in DEFAULTS:
                    cfg[k] = v

    for k in DEFAULTS:
        if k in overrides and overrides[k] is not None:
            cfg[k] = overrides[k]

    if cfg["strategy"] not in _VALID_STRATEGIES:
        raise ValueError(
            f"Invalid strategy '{cfg['strategy']}' for target '{target}'. "
            f"Expected one of: {sorted(_VALID_STRATEGIES)}"
        )

    return cfg
