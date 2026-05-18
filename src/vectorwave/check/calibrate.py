"""Threshold calibration for the vectorwave-check pytest plugin.

Two measurement modes:

* **diversity** (default) — pull existing golden outputs for the target,
  compute pairwise cosine similarity. Reports how diverse the function's
  typical outputs are. No function calls, no side effects.
* **rerun** — sample a few golden inputs, re-execute the function N times
  per input, compute pairwise similarity within each input group. Reports
  the function's intrinsic noise floor. Hits the function (and any APIs it
  calls); skip for functions with side effects.

Both modes emit the same shape so the downstream CLI / pyproject snippet
generator can stay one path.
"""
from __future__ import annotations

import asyncio
import concurrent.futures
import importlib
import inspect
import logging
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)


PERCENTILES = (5, 10, 25, 50, 75, 95)


@dataclass
class CalibrationResult:
    function: str
    mode: str
    sample_count: int  # how many goldens (diversity) / inputs sampled (rerun)
    pair_count: int    # how many similarity pairs computed
    percentiles: Dict[int, float] = field(default_factory=dict)
    recommended_threshold: Optional[float] = None
    recommended_strategy: str = "similarity"
    notes: List[str] = field(default_factory=list)
    vectorizer_name: Optional[str] = None


def _cosine(v1: Sequence[float], v2: Sequence[float]) -> float:
    dot = 0.0
    n1 = 0.0
    n2 = 0.0
    for a, b in zip(v1, v2):
        dot += a * b
        n1 += a * a
        n2 += b * b
    if n1 == 0 or n2 == 0:
        return 0.0
    return dot / (math.sqrt(n1) * math.sqrt(n2))


def _percentile(sorted_values: List[float], p: int) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    # linear interpolation between closest ranks
    k = (len(sorted_values) - 1) * p / 100.0
    lo = int(math.floor(k))
    hi = int(math.ceil(k))
    if lo == hi:
        return sorted_values[lo]
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


def _pairwise_similarities(embeddings: List[Sequence[float]]) -> List[float]:
    out: List[float] = []
    for i in range(len(embeddings)):
        for j in range(i + 1, len(embeddings)):
            out.append(_cosine(embeddings[i], embeddings[j]))
    return out


def _embed_all(vectorizer, texts: List[str]) -> List[Sequence[float]]:
    if hasattr(vectorizer, "embed_batch"):
        try:
            return vectorizer.embed_batch(texts)
        except Exception as e:  # noqa: BLE001
            logger.warning("embed_batch failed (%s); falling back to per-text embed", e)
    return [vectorizer.embed(t) for t in texts]


def _summarize(
    function: str,
    mode: str,
    sample_count: int,
    similarities: List[float],
    vectorizer_name: Optional[str],
) -> CalibrationResult:
    sims_sorted = sorted(similarities)
    percentiles = {p: _percentile(sims_sorted, p) for p in PERCENTILES}

    notes: List[str] = []
    rec_threshold = percentiles.get(5)
    rec_strategy = "similarity"

    if not sims_sorted:
        notes.append("No similarity pairs were computed — recommendation not available.")
        rec_threshold = None
    else:
        p5 = percentiles[5]
        p95 = percentiles[95]
        if p5 > 0.99 and p95 > 0.99:
            rec_strategy = "exact"
            rec_threshold = None
            notes.append(
                "Function appears deterministic (p5 and p95 both >0.99). "
                "Use `strategy=\"exact\"` instead of similarity."
            )
        elif p5 < 0.6:
            rec_strategy = "llm"
            notes.append(
                f"Function is highly variable (p5={p5:.3f} < 0.6). "
                "Similarity threshold will be noisy; consider `strategy=\"llm\"` "
                "(LLM-as-a-judge) for more robust regression detection."
            )

    return CalibrationResult(
        function=function,
        mode=mode,
        sample_count=sample_count,
        pair_count=len(similarities),
        percentiles=percentiles,
        recommended_threshold=rec_threshold,
        recommended_strategy=rec_strategy,
        notes=notes,
        vectorizer_name=vectorizer_name,
    )


def _stringify(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        import json
        return json.dumps(value, sort_keys=True, default=str)
    except (TypeError, ValueError):
        return str(value)


def _run_coroutine_safely(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(asyncio.run, coro).result()


def _vectorizer_name(vectorizer) -> Optional[str]:
    if vectorizer is None:
        return None
    return type(vectorizer).__name__


# ---------------------------------------------------------------------------
# Mode 1: diversity (cheap, default)
# ---------------------------------------------------------------------------

def _calibrate_diversity(function_full_name: str, samples: int) -> CalibrationResult:
    from ..models.db_config import get_weaviate_settings
    from ..store import get_vector_store
    from ..utils.serialization import deserialize_return_value
    from ..vectorizer.factory import get_vectorizer

    func_short_name = function_full_name.rsplit(".", 1)[-1]
    settings = get_weaviate_settings()
    store = get_vector_store()
    vectorizer = get_vectorizer()
    if vectorizer is None:
        raise RuntimeError(
            "Calibration requires a local vectorizer (VECTORIZER=huggingface or openai_client). "
            "Server-side vectorization (weaviate_module) cannot embed offline."
        )

    records = store.query(
        collection=settings.GOLDEN_COLLECTION_NAME,
        filters={"function_name": func_short_name},
        limit=samples,
    )

    outputs: List[str] = []
    for rec in records:
        raw = rec.properties.get("return_value")
        deserialized = deserialize_return_value(raw)
        outputs.append(_stringify(deserialized))

    if len(outputs) < 2:
        raise RuntimeError(
            f"Need at least 2 golden samples for '{function_full_name}', found {len(outputs)}. "
            "Run the function a few times in production / replay-capture mode first, "
            "or mark known-good executions as Golden."
        )

    embeddings = _embed_all(vectorizer, outputs)
    similarities = _pairwise_similarities(embeddings)

    return _summarize(
        function=function_full_name,
        mode="diversity",
        sample_count=len(outputs),
        similarities=similarities,
        vectorizer_name=_vectorizer_name(vectorizer),
    )


# ---------------------------------------------------------------------------
# Mode 2: rerun (honest noise floor, opt-in)
# ---------------------------------------------------------------------------

def _calibrate_rerun(
    function_full_name: str,
    samples: int,
    runs: int,
) -> CalibrationResult:
    from ..models.db_config import get_weaviate_settings
    from ..utils.replayer import VectorWaveReplayer
    from ..vectorizer.factory import get_vectorizer

    module_name, func_short_name = function_full_name.rsplit(".", 1)
    module = importlib.import_module(module_name)
    target_func = getattr(module, func_short_name)
    is_async = inspect.iscoroutinefunction(target_func)

    get_weaviate_settings()  # surface config errors early
    vectorizer = get_vectorizer()
    if vectorizer is None:
        raise RuntimeError(
            "Calibration requires a local vectorizer (VECTORIZER=huggingface or openai_client)."
        )

    helper = VectorWaveReplayer()
    candidates = helper._fetch_test_candidates(func_short_name, limit=samples)
    if not candidates:
        raise RuntimeError(
            f"No golden samples or execution logs found for '{function_full_name}'. "
            "Rerun calibration needs at least one captured input."
        )

    all_similarities: List[float] = []
    sampled = candidates[:samples]
    for cand in sampled:
        raw_inputs = cand["inputs"]
        inputs = helper._extract_inputs(raw_inputs, target_func)
        outputs: List[str] = []
        for _ in range(runs):
            try:
                if is_async:
                    out = _run_coroutine_safely(target_func(**inputs))
                else:
                    out = target_func(**inputs)
            except Exception as e:  # noqa: BLE001
                logger.warning("Calibration run raised %s; skipping this run", e)
                continue
            outputs.append(_stringify(out))
        if len(outputs) >= 2:
            embeddings = _embed_all(vectorizer, outputs)
            all_similarities.extend(_pairwise_similarities(embeddings))

    if not all_similarities:
        raise RuntimeError(
            "Rerun calibration produced no usable comparisons "
            "(function raised every time, or only one successful run per input). "
            "Inspect the function's behavior manually."
        )

    return _summarize(
        function=function_full_name,
        mode="rerun",
        sample_count=len(sampled),
        similarities=all_similarities,
        vectorizer_name=_vectorizer_name(vectorizer),
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calibrate(
    function_full_name: str,
    *,
    rerun: bool = False,
    samples: Optional[int] = None,
    runs: int = 10,
) -> CalibrationResult:
    """Compute a threshold recommendation for `function_full_name`.

    Args:
        function_full_name: Fully-qualified target, e.g. `myapp.summarize`.
        rerun: If True, sample inputs and re-execute the function. Hits APIs
            and triggers any side effects. If False (default), only pull
            existing golden outputs and measure diversity — no side effects.
        samples: Number of goldens to pull (diversity) or inputs to sample
            (rerun). Defaults to 30 for diversity, 3 for rerun.
        runs: Re-executions per sampled input. Only used in rerun mode.
    """
    if rerun:
        effective_samples = samples if samples is not None else 3
        return _calibrate_rerun(function_full_name, samples=effective_samples, runs=runs)
    effective_samples = samples if samples is not None else 30
    return _calibrate_diversity(function_full_name, samples=effective_samples)


# ---------------------------------------------------------------------------
# Rendering for CLI
# ---------------------------------------------------------------------------

def format_pyproject_snippet(result: CalibrationResult) -> str:
    if result.recommended_strategy == "exact":
        return (
            f'[tool.vectorwave.check."{result.function}"]\n'
            f'strategy = "exact"\n'
        )
    threshold = result.recommended_threshold or 0.85
    return (
        f'[tool.vectorwave.check."{result.function}"]\n'
        f'strategy = "{result.recommended_strategy}"\n'
        f"threshold = {threshold:.3f}\n"
    )


def format_report(result: CalibrationResult) -> str:
    lines = [
        f"Calibration for '{result.function}' "
        f"(mode={result.mode}, vectorizer={result.vectorizer_name or 'n/a'})",
        f"  samples={result.sample_count}, pairs={result.pair_count}",
        "",
    ]
    for p in PERCENTILES:
        v = result.percentiles.get(p)
        if v is not None:
            lines.append(f"  p{p:<3} {v:.4f}")
    lines.append("")
    if result.recommended_threshold is None and result.recommended_strategy == "exact":
        lines.append("Recommended strategy: exact (no threshold needed)")
    else:
        lines.append(
            f"Recommended: strategy={result.recommended_strategy}, "
            f"threshold={result.recommended_threshold:.4f}"
        )
    if result.notes:
        lines.append("")
        for note in result.notes:
            lines.append(f"  note: {note}")
    lines.append("")
    lines.append("Add to pyproject.toml:")
    lines.append("")
    for ln in format_pyproject_snippet(result).splitlines():
        lines.append(f"  {ln}")
    return "\n".join(lines)
