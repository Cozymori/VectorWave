import importlib
import json
import logging
import traceback
import math
import asyncio
import inspect
from typing import Any, Dict, List, Optional

from .context import execution_source_context
from ..core.llm.factory import get_llm_client

import weaviate.classes.query as wvc_query

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

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
        # 1. Dynamic Function Loading
        try:
            module_name, func_short_name = function_full_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            target_func = getattr(module, func_short_name)
        except (ValueError, ImportError, AttributeError) as e:
            logger.error(f"Could not load function: {function_full_name}. Error: {e}")
            return {"error": f"Function loading failed: {e}"}

        is_async_func = inspect.iscoroutinefunction(target_func)

        # 2. Retrieve Test Data using Parent's Logic (Golden Priority)
        test_objects = self._fetch_test_candidates(func_short_name, limit)

        results = {
            "function": function_full_name,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "updated": 0,
            "failures": []
        }

        if not test_objects:
            logger.warning(f"No data found to test: {function_full_name}")
            return results

        logger.info(f"Starting Semantic Replay: {len(test_objects)} logs for '{function_full_name}'")
        if similarity_threshold:
            logger.info(f"   - Mode: Vector Similarity >= {similarity_threshold}")
        if semantic_eval:
            logger.info(f"   - Mode: Semantic Evaluation (LLM-as-a-Judge)")

        for obj_data in test_objects:
            results["total"] += 1

            # Unpack data
            uuid_str = obj_data['uuid']
            raw_inputs = obj_data['inputs']
            expected_output = obj_data['expected_output']
            is_golden = obj_data.get('is_golden', False)

            # [FIX] Extract inputs using parent method
            inputs = self._extract_inputs(raw_inputs, target_func)

            try:
                token = execution_source_context.set("REPLAY")
                # 3. Function Re-execution
                if is_async_func:
                    actual_output = asyncio.run(target_func(**inputs))
                else:
                    actual_output = target_func(**inputs)

                if token:
                    execution_source_context.reset(token)

                # 4. Result Validation (Semantic Comparison)
                is_match, match_reason = self._compare_results_semantic(
                    expected_output,
                    actual_output,
                    similarity_threshold,
                    semantic_eval
                )

                if is_match:
                    results["passed"] += 1
                    logger.debug(f"UUID {uuid_str}: PASSED ({match_reason}) {'[GOLDEN]' if is_golden else ''}")
                else:
                    # 5. Handle Mismatch
                    if update_baseline:
                        self._update_baseline_value(uuid_str, actual_output, is_golden)
                        results["updated"] += 1
                        results["passed"] += 1
                        logger.info(f"UUID {uuid_str}: Baseline UPDATED")
                    else:
                        results["failed"] += 1

                        # Generate Visual Diff
                        diff_html = self._generate_diff_html(expected_output, actual_output)

                        results["failures"].append({
                            "uuid": uuid_str,
                            "inputs": inputs,
                            "expected": expected_output,
                            "actual": actual_output,
                            "diff_html": diff_html,
                            "reason": match_reason,
                            "is_golden": is_golden
                        })
                        logger.warning(f"UUID {uuid_str}: FAILED ({match_reason}) {'[GOLDEN]' if is_golden else ''}")

            except Exception as e:
                results["failed"] += 1
                error_msg = f"Exception: {str(e)}"
                logger.error(f"UUID {uuid_str}: EXECUTION ERROR - {e}")

                results["failures"].append({
                    "uuid": uuid_str,
                    "inputs": inputs,
                    "expected": expected_output,
                    "actual": "EXCEPTION_RAISED",
                    "error": error_msg,
                    "diff_html": f"<div class='error'>{traceback.format_exc()}</div>",
                    "traceback": traceback.format_exc()
                })

        logger.info(
            f"Replay Finished. Passed: {results['passed']}, Failed: {results['failed']}, Updated: {results['updated']}")
        return results

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
        if not vectorizer:
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
        if not self.openai_client:
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
