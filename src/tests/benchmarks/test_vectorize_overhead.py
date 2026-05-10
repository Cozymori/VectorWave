"""Benchmarks for the VectorWave hot path.

Run with:
    pytest src/tests/benchmarks/ --benchmark-only

Compare two runs (e.g., before/after a Rust port):
    pytest src/tests/benchmarks/ --benchmark-only --benchmark-save=baseline
    # ... apply changes ...
    pytest src/tests/benchmarks/ --benchmark-only --benchmark-compare=baseline

The macro benchmarks stub out the batch manager + alerter so we measure
the Python overhead `@vectorize` adds to a function call, not Weaviate I/O.
The micro benchmarks call the hot helpers directly to isolate which piece
contributes the most time.
"""
from __future__ import annotations

import pytest

from vectorwave.core.decorator import vectorize
from vectorwave.monitoring.tracer import (
    _capture_span_attributes,
    _create_input_vector_data,
)


# ---------------------------------------------------------------------------
# Baseline: bare Python function with no decoration. Anchor for "what does
# VectorWave cost on top of vanilla Python?"
# ---------------------------------------------------------------------------

def test_baseline_plain_function(benchmark):
    """Undecorated control. The macro benchmarks below should be compared
    against this number to read the @vectorize overhead."""
    def add(a, b):
        return a + b

    benchmark(add, 3, 4)


# ---------------------------------------------------------------------------
# Macro: @vectorize wrapping a no-op sync function.
# ---------------------------------------------------------------------------

def test_vectorize_wraps_simple_sync_call(benchmark, stubbed_vectorize_env):
    """Per-call cost of @vectorize on a tiny sync function with no captured
    inputs. This is the closest thing to "the wrapper tax everyone pays"."""

    @vectorize(
        search_description="benchmark add",
        sequence_narrative="benchmark add",
    )
    def add(a, b):
        return a + b

    # Warm up: first call pays the function-cache check + signature inspect cost.
    for _ in range(3):
        add(1, 2)

    benchmark(add, 3, 4)


def test_vectorize_with_capture_inputs(benchmark, stubbed_vectorize_env):
    """Per-call cost when capture_inputs=True forces full attribute capture
    + masking of every parameter on every call."""

    @vectorize(
        search_description="benchmark capture",
        sequence_narrative="benchmark capture",
        capture_inputs=True,
    )
    def process(user_id, amount, is_active=True, tags=("a", "b")):
        return amount * 2

    for _ in range(3):
        process("u1", 100)

    benchmark(process, "u-bench", 500)


def test_vectorize_with_capture_return_value(benchmark, stubbed_vectorize_env):
    """capture_return_value forces JSON serialisation of the return value;
    measures the cost of the return-side path."""

    @vectorize(
        search_description="benchmark cap-ret",
        sequence_narrative="benchmark cap-ret",
        capture_return_value=True,
    )
    def lookup(key):
        return {"key": key, "rows": [{"id": 1}, {"id": 2}, {"id": 3}]}

    for _ in range(3):
        lookup("k1")

    benchmark(lookup, "k-bench")


# ---------------------------------------------------------------------------
# Micro: the two functions most likely to dominate the per-call cost.
# These are isolated so a Rust port can target them surgically.
# ---------------------------------------------------------------------------

_SENSITIVE = {"password", "token", "api_key"}


def _example_func(user_id, amount, is_active=True, tags=("a", "b"), password=None):
    return amount


def test_micro_capture_span_attributes_simple(benchmark):
    """`_capture_span_attributes` with 5 params, no sensitive masking triggered."""
    args = ("u1", 100)
    kwargs = {"is_active": True, "tags": ("a", "b")}
    attrs = ["user_id", "amount", "is_active", "tags"]

    benchmark(_capture_span_attributes, attrs, args, kwargs, _example_func, _SENSITIVE)


def test_micro_capture_span_attributes_with_masking(benchmark):
    """Same shape but `password` triggers the sensitive-key path on every call."""
    args = ("u1", 100)
    kwargs = {"password": "topsecret", "tags": ("a", "b")}
    attrs = ["user_id", "amount", "password", "tags"]

    benchmark(_capture_span_attributes, attrs, args, kwargs, _example_func, _SENSITIVE)


def test_micro_create_input_vector_data_simple(benchmark):
    """Input-vector text construction for a small kwargs payload."""
    benchmark(
        _create_input_vector_data,
        func_name="lookup_user",
        args=("u1", 100),
        kwargs={"region": "us-west", "active": True},
        sensitive_keys=_SENSITIVE,
    )


def test_micro_create_input_vector_data_nested(benchmark):
    """Same with a moderately nested dict to stretch the masking walker."""
    payload = {
        "filters": {"status": "ok", "tier": "gold", "tags": ["billing", "auth"]},
        "page": {"limit": 50, "cursor": "abc123"},
        "user": {"id": "u1", "password": "should-be-masked"},
    }
    benchmark(
        _create_input_vector_data,
        func_name="search",
        args=(),
        kwargs={"query": "find me", "payload": payload},
        sensitive_keys=_SENSITIVE,
    )
