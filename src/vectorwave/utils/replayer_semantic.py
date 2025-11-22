import json
import logging
import traceback
import math
import asyncio
import inspect
from typing import Any, Dict, Optional

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
    It supports Vector Cosine Similarity and LLM-as-a-Judge evaluation.
    """

    def __init__(self):
        super().__init__()
        self.openai_client = self._init_openai()

    def _init_openai(self) -> Optional[Any]:
        """Initialize OpenAI client if available and configured."""
        if OpenAI and self.settings.OPENAI_API_KEY:
            return OpenAI(api_key=self.settings.OPENAI_API_KEY)
        return None

    def replay(self,
               function_full_name: str,
               limit: int = 10,
               update_baseline: bool = False,
               similarity_threshold: Optional[float] = None,  # [New]
               semantic_eval: bool = False  # [New]
               ) -> Dict[str, Any]:
        """
        Overridden replay method to support semantic comparison options.
        """
        # 1. Load Function dynamically (Reuse parent method logic if possible, but here we reimplement for clarity due to local vars)
        target_func = self._load_target_function(function_full_name)
        if isinstance(target_func, dict) and "error" in target_func:
            return target_func

        is_async_func = inspect.iscoroutinefunction(target_func)
        func_short_name = function_full_name.rsplit('.', 1)[-1]

        # 2. Retrieve Data
        collection = self.client.collections.get(self.collection_name)
        filters = (
                wvc_query.Filter.by_property("function_name").equal(func_short_name) &
                wvc_query.Filter.by_property("status").equal("SUCCESS")
        )

        response = collection.query.fetch_objects(
            filters=filters,
            limit=limit,
            sort=wvc_query.Sort.by_property("timestamp_utc", ascending=False)
        )

        results = {
            "function": function_full_name,
            "total": 0,
            "passed": 0,
            "failed": 0,
            "updated": 0,
            "failures": []
        }

        if not response.objects:
            logger.warning(f"No data found to test: {function_full_name}")
            return results

        print(f"🤖 [Semantic Replay] Testing {len(response.objects)} logs for '{function_full_name}'...")
        if similarity_threshold:
            print(f"   - Mode: Vector Similarity >= {similarity_threshold}")
        if semantic_eval:
            print(f"   - Mode: Semantic Evaluation (LLM-as-a-Judge)")

        for obj in response.objects:
            results["total"] += 1
            inputs = self._extract_inputs(obj.properties, target_func)
            expected_output = self._deserialize_value(obj.properties.get("return_value"))

            try:
                # 3. Re-execution
                if is_async_func:
                    actual_output = asyncio.run(target_func(**inputs))
                else:
                    actual_output = target_func(**inputs)

                # 4. Enhanced Comparison
                is_match, match_reason = self._compare_results_semantic(
                    expected_output,
                    actual_output,
                    similarity_threshold,
                    semantic_eval
                )

                if is_match:
                    results["passed"] += 1
                else:
                    # 5. Handle Mismatch
                    if update_baseline:
                        self._update_baseline_value(collection, obj.uuid, actual_output)
                        results["updated"] += 1
                        results["passed"] += 1
                        print(f"  ⚠️ [Updated] UUID {obj.uuid} baseline updated.")
                    else:
                        results["failed"] += 1
                        results["failures"].append({
                            "uuid": str(obj.uuid),
                            "inputs": inputs,
                            "expected": expected_output,
                            "actual": actual_output,
                            "reason": match_reason
                        })
                        print(f"  ❌ [Failed] UUID {obj.uuid} | {match_reason}")
                        print(f"     Expected: {str(expected_output)[:60]}...")
                        print(f"     Actual:   {str(actual_output)[:60]}...")

            except Exception as e:
                results["failed"] += 1
                results["failures"].append({
                    "uuid": str(obj.uuid),
                    "inputs": inputs,
                    "expected": expected_output,
                    "actual": "EXCEPTION_RAISED",
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
                print(f"  ❌ [Error] UUID {obj.uuid}: {e}")

        return results

    def _load_target_function(self, full_name: str):
        """Helper to load function (Refactored from original code for reuse)"""
        try:
            import importlib
            module_name, func_short_name = full_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            return getattr(module, func_short_name)
        except Exception as e:
            return {"error": str(e)}

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
            res = self.openai_client.chat.completions.create(
                model="gpt-4-turbo",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"},
                temperature=0.0
            )
            return json.loads(res.choices[0].message.content).get("equivalent", False)
        except Exception as e:
            logger.error(f"LLM Eval failed: {e}")
            return False
