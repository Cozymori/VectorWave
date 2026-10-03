"""@vectorize must record a file path even when the source is outside a Git repo."""
import logging
import os

from vectorwave.core import decorator as decorator_module
from vectorwave.core.decorator import vectorize


def _registered_file_path(monkeypatch, repo_info):
    monkeypatch.setattr(decorator_module, "get_repo_root_and_relative_path", lambda _path: repo_info)
    monkeypatch.setattr(decorator_module, "PENDING_FUNCTIONS", [])

    # auto=True parks the static properties in PENDING_FUNCTIONS without a DB write.
    @vectorize(auto=True)
    def sample_fn(x):
        return x

    return decorator_module.PENDING_FUNCTIONS[-1]["static_properties"]["file_path"]


def test_file_path_falls_back_to_absolute_outside_git(monkeypatch, caplog):
    with caplog.at_level(logging.WARNING):
        file_path = _registered_file_path(monkeypatch, None)

    assert file_path == os.path.abspath(__file__)
    assert "Failed to determine file path" not in caplog.text


def test_file_path_is_repo_relative_inside_git(monkeypatch):
    file_path = _registered_file_path(monkeypatch, ("/repo", "pkg/module.py"))

    assert file_path == "pkg/module.py"
