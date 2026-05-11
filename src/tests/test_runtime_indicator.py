"""Tests for the runtime indicator (banner + PID file)."""
from __future__ import annotations

import json
import os

import pytest


def _reload_runtime():
    """Force-reload vectorwave.runtime so each test gets a fresh module-state."""
    import vectorwave.runtime as rt
    rt.deactivate()
    return rt


def test_activate_writes_pid_file(tmp_path, monkeypatch):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.setenv("VECTORWAVE_LITE_PATH", "/tmp/somepath")
    monkeypatch.setenv("VECTORWAVE_QUIET", "1")
    monkeypatch.delenv("OTEL_ENABLED", raising=False)

    rt = _reload_runtime()
    rt.activate()

    pid_file = tmp_path / f"{os.getpid()}.json"
    assert pid_file.exists()
    data = json.loads(pid_file.read_text())
    assert data["mode"] == "lite"
    assert data["lite_path"] == "/tmp/somepath"
    assert data["otel_enabled"] is False
    assert data["pid"] == os.getpid()

    rt.deactivate()
    assert not pid_file.exists()


def test_activate_captures_otel_state(tmp_path, monkeypatch):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "my-svc")
    monkeypatch.setenv("VECTORWAVE_QUIET", "1")

    rt = _reload_runtime()
    rt.activate()

    info = rt.get_info()
    assert info.otel_enabled is True
    assert info.otel_service_name == "my-svc"
    rt.deactivate()


def test_register_instrumented_module_persists(tmp_path, monkeypatch):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("VECTORWAVE_QUIET", "1")

    rt = _reload_runtime()
    rt.activate()
    rt.register_instrumented_module("demo.api")
    rt.register_instrumented_module("demo.workers")
    rt.register_instrumented_module("demo.api")  # dedupes

    data = json.loads((tmp_path / f"{os.getpid()}.json").read_text())
    assert data["instrumented_modules"] == ["demo.api", "demo.workers"]
    rt.deactivate()


def test_list_active_processes_filters_dead_pids(tmp_path, monkeypatch):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("VECTORWAVE_QUIET", "1")

    rt = _reload_runtime()
    rt.activate()

    # Plant a fake PID file for a process that definitely isn't alive.
    fake_pid = 999999
    fake_file = tmp_path / f"{fake_pid}.json"
    fake_file.write_text(json.dumps({
        "pid": fake_pid, "started_at": 0, "mode": "pro",
        "lite_path": None, "otel_enabled": False,
        "otel_service_name": None, "instrumented_modules": [],
        "rust_core": False,
    }))

    procs = rt.list_active_processes()
    pids = {p.pid for p in procs}
    assert os.getpid() in pids
    assert fake_pid not in pids
    # The stale file should have been pruned.
    assert not fake_file.exists()

    rt.deactivate()


def test_banner_emits_to_stderr(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("VECTORWAVE_MODE", "lite")
    monkeypatch.delenv("VECTORWAVE_QUIET", raising=False)

    rt = _reload_runtime()
    rt.activate()
    captured = capsys.readouterr()
    assert "[vectorwave] active" in captured.err
    assert "mode=lite" in captured.err
    rt.deactivate()


def test_quiet_env_silences_banner(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("VECTORWAVE_RUN_DIR", str(tmp_path))
    monkeypatch.setenv("VECTORWAVE_QUIET", "1")

    rt = _reload_runtime()
    rt.activate()
    captured = capsys.readouterr()
    assert captured.err == ""
    rt.deactivate()
