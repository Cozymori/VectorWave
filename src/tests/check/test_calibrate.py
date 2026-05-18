"""Unit tests for the calibration math + summary logic.

These tests cover the pure functions only (cosine, percentile, summarize,
formatters). End-to-end calibration against a real vector store + golden
data lives in nightly_live.
"""
from __future__ import annotations

import pytest

from vectorwave.check.calibrate import (
    CalibrationResult,
    PERCENTILES,
    _cosine,
    _pairwise_similarities,
    _percentile,
    _summarize,
    format_pyproject_snippet,
    format_report,
)


def test_cosine_identical_vectors_returns_one():
    v = [1.0, 2.0, 3.0]
    assert _cosine(v, v) == pytest.approx(1.0)


def test_cosine_orthogonal_vectors_returns_zero():
    assert _cosine([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)


def test_cosine_opposite_vectors_returns_negative_one():
    assert _cosine([1.0, 0.0], [-1.0, 0.0]) == pytest.approx(-1.0)


def test_cosine_zero_vector_returns_zero():
    assert _cosine([0.0, 0.0], [1.0, 1.0]) == 0.0


def test_percentile_empty_returns_zero():
    assert _percentile([], 50) == 0.0


def test_percentile_single_value_returns_that_value():
    assert _percentile([0.7], 5) == 0.7
    assert _percentile([0.7], 95) == 0.7


def test_percentile_interpolates_between_ranks():
    # 0, 1, 2, 3, 4 → p50 should be 2.0 (middle)
    values = [0.0, 1.0, 2.0, 3.0, 4.0]
    assert _percentile(values, 50) == pytest.approx(2.0)
    # p25 should land at index 1.0 → exactly 1.0
    assert _percentile(values, 25) == pytest.approx(1.0)
    # p75 should land at index 3.0 → exactly 3.0
    assert _percentile(values, 75) == pytest.approx(3.0)


def test_pairwise_similarities_count():
    embeddings = [[1.0, 0.0], [0.0, 1.0], [1.0, 1.0]]
    pairs = _pairwise_similarities(embeddings)
    assert len(pairs) == 3  # N choose 2 for N=3


def test_pairwise_similarities_empty_for_single_vector():
    assert _pairwise_similarities([[1.0, 2.0]]) == []


def test_summarize_recommends_p5_threshold_for_normal_function():
    sims = [0.7, 0.75, 0.8, 0.85, 0.9, 0.92, 0.95, 0.97, 0.98, 0.99]
    result = _summarize("myapp.fn", "diversity", sample_count=10, similarities=sims, vectorizer_name="HF")
    assert result.recommended_strategy == "similarity"
    assert result.recommended_threshold is not None
    assert result.recommended_threshold == pytest.approx(result.percentiles[5])
    assert "exact" not in " ".join(result.notes).lower()


def test_summarize_flags_deterministic_function_with_exact_recommendation():
    sims = [1.0] * 20
    result = _summarize("myapp.detfn", "diversity", sample_count=20, similarities=sims, vectorizer_name="HF")
    assert result.recommended_strategy == "exact"
    assert result.recommended_threshold is None
    assert any("deterministic" in n.lower() for n in result.notes)


def test_summarize_flags_highly_noisy_function_with_llm_recommendation():
    sims = [0.2, 0.3, 0.35, 0.4, 0.45, 0.5, 0.55]
    result = _summarize("myapp.chaos", "diversity", sample_count=7, similarities=sims, vectorizer_name="HF")
    assert result.recommended_strategy == "llm"
    assert any("llm" in n.lower() for n in result.notes)


def test_summarize_empty_similarities_emits_note():
    result = _summarize("myapp.empty", "diversity", sample_count=1, similarities=[], vectorizer_name="HF")
    assert result.recommended_threshold is None
    assert any("no similarity pairs" in n.lower() for n in result.notes)


def test_summarize_returns_all_requested_percentiles():
    sims = [0.5, 0.6, 0.7, 0.8, 0.9]
    result = _summarize("myapp.fn", "diversity", sample_count=5, similarities=sims, vectorizer_name="HF")
    for p in PERCENTILES:
        assert p in result.percentiles


def test_format_pyproject_snippet_similarity_strategy():
    result = CalibrationResult(
        function="myapp.summarize",
        mode="diversity",
        sample_count=10,
        pair_count=45,
        percentiles={5: 0.812, 50: 0.91},
        recommended_threshold=0.812,
        recommended_strategy="similarity",
    )
    snippet = format_pyproject_snippet(result)
    assert '[tool.vectorwave.check."myapp.summarize"]' in snippet
    assert 'strategy = "similarity"' in snippet
    assert "threshold = 0.812" in snippet


def test_format_pyproject_snippet_exact_strategy_omits_threshold():
    result = CalibrationResult(
        function="myapp.id",
        mode="diversity",
        sample_count=10,
        pair_count=45,
        recommended_threshold=None,
        recommended_strategy="exact",
    )
    snippet = format_pyproject_snippet(result)
    assert 'strategy = "exact"' in snippet
    assert "threshold" not in snippet


def test_format_report_contains_percentiles_and_recommendation():
    result = CalibrationResult(
        function="myapp.fn",
        mode="rerun",
        sample_count=3,
        pair_count=135,
        percentiles={p: 0.5 + p / 200 for p in PERCENTILES},
        recommended_threshold=0.525,
        recommended_strategy="similarity",
        vectorizer_name="HuggingFaceVectorizer",
    )
    text = format_report(result)
    assert "myapp.fn" in text
    assert "mode=rerun" in text
    assert "HuggingFaceVectorizer" in text
    assert "p5" in text and "p95" in text
    assert "Recommended" in text
    assert "pyproject.toml" in text
