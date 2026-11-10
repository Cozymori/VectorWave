"""mask_and_serialize must never crash the host process, whatever data a traced function gets."""
import subprocess
import sys

import pytest

import vectorwave.vectorwave_core as core


def _nested_list(depth):
    root = cur = []
    for _ in range(depth):
        nxt = []
        cur.append(nxt)
        cur = nxt
    return root


def test_masks_sensitive_keys_at_any_level():
    out = core.mask_and_serialize(
        {"user": "bob", "password": "pw", "nested": [{"API_KEY": "k", "v": 3}]}, ["password", "api_key"])
    assert out == {"user": "bob", "password": "[MASKED]", "nested": [{"API_KEY": "[MASKED]", "v": 3}]}


def test_self_references_become_a_marker():
    d = {"x": 1}
    d["self"] = d
    lst = [1]
    lst.append(lst)
    assert core.mask_and_serialize(d, []) == {"x": 1, "self": "[CIRCULAR]"}
    assert core.mask_and_serialize(lst, []) == [1, "[CIRCULAR]"]


def test_shared_but_acyclic_values_are_kept_in_full():
    shared = [1, 2]
    out = core.mask_and_serialize({"a": shared, "b": shared, "c": [shared, shared]}, [])
    assert out == {"a": [1, 2], "b": [1, 2], "c": [[1, 2], [1, 2]]}


def test_deep_nesting_is_cut_with_a_marker():
    out = core.mask_and_serialize(_nested_list(200_000), [])
    depth = 0
    while isinstance(out, list):
        out = out[0]
        depth += 1
    assert depth == 64 and out == "[MAX_DEPTH]"


@pytest.mark.parametrize("snippet", [
    "d = {}; d['self'] = d",
    "d = []\ncur = d\nfor _ in range(200000):\n    nxt = []; cur.append(nxt); cur = nxt",
])
def test_bad_input_does_not_kill_the_process(snippet):
    # These inputs used to overflow the native stack (exit code 139), so run them in a child.
    code = f"import vectorwave.vectorwave_core as c\n{snippet}\nc.mask_and_serialize(d, [])\nprint('ok')"
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr[-500:]
    assert proc.stdout.strip().endswith("ok")
