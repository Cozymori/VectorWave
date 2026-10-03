"""The `vectorwave` console script must import user modules from the cwd."""
import sys

from vectorwave import cli


def test_main_adds_cwd_to_sys_path(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p not in ("", str(tmp_path))])

    cli._ensure_cwd_on_path()

    assert sys.path[0] == str(tmp_path)


def test_main_does_not_duplicate_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])

    cli._ensure_cwd_on_path()

    assert sys.path.count(str(tmp_path)) == 1


def test_cwd_module_importable_after_main(tmp_path, monkeypatch, capsys):
    (tmp_path / "vw_cli_target_mod.py").write_text("VALUE = 42\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(sys, "path", [p for p in sys.path if p not in ("", str(tmp_path))])
    monkeypatch.delitem(sys.modules, "vw_cli_target_mod", raising=False)

    cli.main(["info"])

    import importlib
    assert importlib.import_module("vw_cli_target_mod").VALUE == 42
