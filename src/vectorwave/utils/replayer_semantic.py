import json
import logging
import math
from typing import Any, Dict, Optional

from ..core.llm.factory import get_llm_client
from .replayer import VectorWaveReplayer
from ..vectorizer.factory import get_vectorizer

logger = logging.getLogger(__name__)


class SemanticReplayer(VectorWaveReplayer):
    """
    A subclass of VectorWaveReplayer that adds AI-powered semantic comparison capabilities.
    Updated to prioritize 'Golden Data' using the parent's data fetching logic.
    """

    def __init__(self):
        super().__init__()
        self.openai_client = get_llm_client()

    def replay(self,
               function_full_name: str,
               limit: int = 10,
               update_baseline: bool = False,
               similarity_threshold: Optional[float] = None,
               semantic_eval: bool = False
               ) -> Dict[str, Any]:
        """
        Retrieves past execution history (Golden > Standard), re-executes it,
        and validates the result using semantic comparison.
        """
        target_func, test_objects, results = self._load_and_fetch(function_full_name, limit)
        if target_func is None:
            return results

        logger.info(f"Starting Semantic Replay: {len(test_objects)} logs for '{function_full_name}'")
        if similarity_threshold is not None:
            logger.info(f"   - Mode: Vector Similarity >= {similarity_threshold}")
        if semantic_eval:
            logger.info(f"   - Mode: Semantic Evaluation (LLM-as-a-Judge)")

        def compare_fn(expected, actual):
            is_match, reason = self._compare_results_semantic(
                expected, actual, similarity_threshold, semantic_eval
            )
            return is_match, reason, ({"reason": reason} if not is_match else {})

        return self._run_replay_loop(target_func, test_objects, results, update_baseline, compare_fn)

    def _compare_results_semantic(self, expected: Any, actual: Any,
                                  similarity_threshold: Optional[float],
                                  semantic_eval: bool) -> tuple[bool, str]:
        """
        Pipeline: Exact Match -> Vector Similarity -> LLM Eval
        """
        # 1. Base Exact Match (Fastest)
        if expected == actual:
            return True, "Exact Match"

        str_expected = str(expected)
        str_actual = str(actual)

        if str_expected == str_actual:
            return True, "String Match"

        # 2. Vector Similarity
        if similarity_threshold is not None:
            score = self._calculate_cosine_similarity(str_expected, str_actual)
            if score >= similarity_threshold:
                return True, f"Vector Similarity ({score:.4f})"

            if not semantic_eval:
                return False, f"Low Similarity ({score:.4f} < {similarity_threshold})"

        # 3. Semantic Eval (LLM)
        if semantic_eval:
            if self._evaluate_with_llm(str_expected, str_actual):
                return True, "Semantic Match (LLM)"
            return False, "Semantic Mismatch (LLM)"

        return False, "Exact match failed"

    def _calculate_cosine_similarity(self, text1: str, text2: str) -> float:
        vectorizer = get_vectorizer()
        if vectorizer is None:
            return 0.0
        try:
            v1 = vectorizer.embed(text1)
            v2 = vectorizer.embed(text2)
            dot = sum(a * b for a, b in zip(v1, v2))
            norm1 = math.sqrt(sum(a * a for a in v1))
            norm2 = math.sqrt(sum(b * b for b in v2))
            return dot / (norm1 * norm2) if norm1 and norm2 else 0.0

        except Exception:
            return 0.0

    def _evaluate_with_llm(self, expected: str, actual: str) -> bool:
        if self.openai_client is None:
            return False

        prompt = f"""
        Compare two outputs. Are they semantically equivalent?
        Ignore minor formatting differences.

        Expected: {expected}
        Actual: {actual}

        Respond JSON: {{"equivalent": true}} or {{"equivalent": false}}
        """
        try:
            # Refactored to use create_chat_completion
            response_text = self.openai_client.create_chat_completion(
                model="gpt-4-turbo",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0,
                category="semantic_replay"
            )

            if response_text:
                return json.loads(response_text).get("equivalent", False)
            return False

        except Exception as e:
            logger.error(f"LLM Eval failed: {e}")
            return False
