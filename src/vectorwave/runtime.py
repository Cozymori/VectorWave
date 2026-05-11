"""Runtime state + visible indicator that VectorWave is active in this process.

When a process imports VectorWave we:

1. Print a single line to stderr summarising the active mode (Pro / Lite),
   the OTel toggle, and any tracking hint. Silence via ``VECTORWAVE_QUIET=1``.
2. Drop a small JSON file at ``~/.vectorwave/run/{pid}.json`` so external
   tools (or the user, via ``vectorwave info``) can list every Python
   process currently instrumented by VectorWave.

The PID file is cleaned up at interpreter shutdown via ``atexit``.

This module is import-side-effect-free until ``activate()`` is called
explicitly by ``vectorwave/__init__.py``.
"""
from __future__ import annotations

import atexit
import json
import logging
import os
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)


def _run_dir() -> Path:
    return Path(os.environ.get("VECTORWAVE_RUN_DIR", Path.home() / ".vectorwave" / "run"))


@dataclass
class RuntimeInfo:
    """Process-level VectorWave state. Serialised into the PID file."""

    pid: int = field(default_factory=os.getpid)
    started_at: float = field(default_factory=time.time)
    mode: str = "pro"  # 'pro' | 'lite'
    lite_path: Optional[str] = None
    otel_enabled: bool = False
    otel_service_name: Optional[str] = None
    instrumented_modules: List[str] = field(default_factory=list)
    rust_core: bool = False


# Singleton — there's one runtime state per Python process.
_info: Optional[RuntimeInfo] = None
_pid_file: Optional[Path] = None
_lock = threading.Lock()


def _detect_mode() -> str:
    return os.environ.get("VECTORWAVE_MODE", "pro").lower()


def _detect_lite_path() -> Optional[str]:
    return os.environ.get("VECTORWAVE_LITE_PATH") or (
        ".vectorwave/lance" if _detect_mode() == "lite" else None
    )


def _detect_otel() -> bool:
    return os.environ.get("OTEL_ENABLED", "").lower() in ("1", "true", "yes")


def _detect_rust_core() -> bool:
    try:
        import vectorwave.vectorwave_core  # noqa: F401
        return True
    except ImportError:
        return False


def get_info() -> RuntimeInfo:
    """Return the current process's runtime info, initialising on first call."""
    global _info
    if _info is None:
        with _lock:
            if _info is None:
                _info = RuntimeInfo(
                    mode=_detect_mode(),
                    lite_path=_detect_lite_path(),
                    otel_enabled=_detect_otel(),
                    otel_service_name=os.environ.get("OTEL_SERVICE_NAME"),
                    rust_core=_detect_rust_core(),
                )
    return _info


def register_instrumented_module(module_name: str) -> None:
    """Called by VectorWaveAutoInjector each time a module is auto-wired so
    `vectorwave info` can list what's currently being observed."""
    info = get_info()
    if module_name not in info.instrumented_modules:
        info.instrumented_modules.append(module_name)
        _refresh_pid_file()


def _format_banner(info: RuntimeInfo) -> str:
    bits = [f"mode={info.mode}"]
    if info.lite_path:
        bits.append(f"path={info.lite_path}")
    if info.otel_enabled:
        bits.append(f"otel={info.otel_service_name or 'on'}")
    if info.rust_core:
        bits.append("rust-core")
    return f"[vectorwave] active — {', '.join(bits)} (pid={info.pid})"


def _refresh_pid_file() -> None:
    if _pid_file is None:
        return
    try:
        _pid_file.parent.mkdir(parents=True, exist_ok=True)
        with open(_pid_file, "w", encoding="utf-8") as f:
            json.dump(asdict(get_info()), f, indent=2, sort_keys=True)
    except OSError as e:
        logger.debug(f"vectorwave: could not refresh PID file: {e}")


def _cleanup_pid_file() -> None:
    if _pid_file and _pid_file.exists():
        try:
            _pid_file.unlink()
        except OSError:
            pass


def activate() -> None:
    """Light up the indicator: print banner, write PID file, register atexit.

    Idempotent. Called once by ``vectorwave/__init__.py``; tests can call
    again after monkeypatching env vars to refresh state.
    """
    global _pid_file
    # Recompute so test fixtures that flip env vars get a fresh snapshot.
    global _info
    _info = None
    info = get_info()

    # Banner (stderr, silenceable). Skipped when stderr isn't a real stream
    # (e.g., inside some test runners that redirect to closed pipes).
    if os.environ.get("VECTORWAVE_QUIET", "").lower() not in ("1", "true", "yes"):
        try:
            print(_format_banner(info), file=sys.stderr)
        except (ValueError, OSError):
            pass

    # PID file
    _pid_file = _run_dir() / f"{info.pid}.json"
    _refresh_pid_file()
    atexit.register(_cleanup_pid_file)


def deactivate() -> None:
    """Mostly useful in tests — wipes the PID file and resets state."""
    global _info, _pid_file
    _cleanup_pid_file()
    _pid_file = None
    _info = None


def list_active_processes() -> List[RuntimeInfo]:
    """Read every PID file in the run dir and return parsed RuntimeInfo.

    Stale entries (PIDs that aren't alive any more) are silently dropped
    AND removed from disk — `vectorwave info` should never show a process
    that has already exited.
    """
    out: List[RuntimeInfo] = []
    run_dir = _run_dir()
    if not run_dir.exists():
        return out
    for path in sorted(run_dir.glob("*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            pid = int(data.get("pid", -1))
            if pid <= 0 or not _pid_alive(pid):
                try:
                    path.unlink()
                except OSError:
                    pass
                continue
            out.append(RuntimeInfo(**data))
        except (OSError, json.JSONDecodeError, TypeError) as e:
            logger.debug(f"vectorwave: skipping unreadable PID file {path}: {e}")
    return out


def _pid_alive(pid: int) -> bool:
    """POSIX-style liveness check. Returns False on permission errors as a
    conservative default so stale files get cleaned up rather than retained
    forever."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Another user's process — assume alive (don't unlink someone else's file)
        return True
    except OSError:
        return False
