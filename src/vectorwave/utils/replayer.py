import importlib
import json
import logging
import traceback
import inspect
import asyncio
import difflib
import pprint
from typing import Any, Dict, List, Optional

import weaviate.classes.query as wvc_query

from ..database.db import get_cached_client
from ..models.db_config import get_weaviate_settings
import vectorwave.vectorwave_core as vectorwave_core
from .context import execution_source_context
from .serialization import deserialize_return_value

logger = logging.getLogger(__name__)


class VectorWaveReplayer:
    """
    A class that performs automated regression testing (Replay) based on VectorWave execution logs.
    It prioritizes 'Golden Data' as high-quality test cases.
    """

    def __init__(self):
        self.client = get_cached_client()
        self.settings = get_weaviate_settings()
        self.collection_name = self.settings.EXECUTION_COLLECTION_NAME
        self.golden_collection_name = self.settings.GOLDEN_COLLECTION_NAME

    def replay(self,
               function_full_name: str,
               limit: int = 10,
               update_baseline: bool = False) -> Dict[str, Any]:
        """
        Retrieves past execution history (Golden Data First -> Standard Logs),
        re-executes the function, and validates the result.
        """
        target_func, test_objects, results = self._load_and_fetch(function_full_name, limit)
        if target_func is None:
            return results

        logger.info(f"Starting Replay: {len(test_objects)} logs for '{function_full_name}'")
        return self._run_replay_loop(
            target_func, test_objects, results, update_baseline,
            compare_fn=lambda exp, act: (self._compare_results(exp, act), None, {})
        )

    def _load_and_fetch(self, function_full_name: str, limit: int):
        """Load function and fetch test candidates. Returns (target_func, test_objects, results_stub)."""
        results = {
            "function": function_full_name,
            "total": 0, "passed": 0, "failed": 0, "updated": 0, "failures": []
        }
        try:
            module_name, func_short_name = function_full_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            target_func = getattr(module, func_short_name)
        except (ValueError, ImportError, AttributeError) as e:
            logger.error(f"Could not load function: {function_full_name}. Error: {e}")
            results["error"] = f"Function loading failed: {e}"
            return None, [], results

        test_objects = self._fetch_test_candidates(func_short_name, limit)
        if not test_objects:
            logger.warning(f"No data found to test: {function_full_name}")
        return target_func, test_objects, results

    def _run_replay_loop(
            self,
            target_func,
            test_objects: List[Dict[str, Any]],
            results: Dict[str, Any],
            update_baseline: bool,
            compare_fn
    ) -> Dict[str, Any]:
        """
        Core replay loop. compare_fn(expected, actual) -> (is_match, reason, extra_failure_fields).
        'reason' may be None; 'extra_failure_fields' is merged into the failure entry.
        """
        is_async_func = inspect.iscoroutinefunction(target_func)

        for obj_data in test_objects:
            results["total"] += 1
            uuid_str = obj_data['uuid']
            raw_inputs = obj_data['inputs']
            expected_output = obj_data['expected_output']
            is_golden = obj_data.get('is_golden', False)
            inputs = self._extract_inputs(raw_inputs, target_func)

            token = None
            try:
                token = execution_source_context.set("REPLAY")
                if is_async_func:
                    actual_output = asyncio.run(target_func(**inputs))
                else:
                    actual_output = target_func(**inputs)

                is_match, reason, extra_fields = compare_fn(expected_output, actual_output)

                tag = f" ({reason})" if reason else ""
                golden_tag = " [GOLDEN]" if is_golden else ""

                if is_match:
                    results["passed"] += 1
                    logger.debug(f"UUID {uuid_str}: PASSED{tag}{golden_tag}")
                else:
                    if update_baseline:
                        self._update_baseline_value(uuid_str, actual_output, is_golden)
                        results["updated"] += 1
                        results["passed"] += 1
                        logger.info(f"UUID {uuid_str}: Baseline UPDATED")
                    else:
                        results["failed"] += 1
                        failure_entry = {
                            "uuid": uuid_str,
                            "inputs": inputs,
                            "expected": expected_output,
                            "actual": actual_output,
                            "diff_html": self._generate_diff_html(expected_output, actual_output),
                            "is_golden": is_golden,
                        }
                        failure_entry.update(extra_fields)
                        results["failures"].append(failure_entry)
                        logger.warning(f"UUID {uuid_str}: FAILED{tag or ' (Mismatch)'}{golden_tag}")

            except Exception as e:
                results["failed"] += 1
                logger.error(f"UUID {uuid_str}: EXECUTION ERROR - {e}")
                results["failures"].append({
                    "uuid": uuid_str,
                    "inputs": inputs,
                    "expected": expected_output,
                    "actual": "EXCEPTION_RAISED",
                    "error": f"Exception: {str(e)}",
                    "diff_html": f"<div class='error'>{traceback.format_exc()}</div>",
                    "traceback": traceback.format_exc()
                })
            finally:
                if token is not None:
                    execution_source_context.reset(token)

        logger.info(f"Replay Finished. Passed: {results['passed']}, Failed: {results['failed']}")
        return results

    def _fetch_test_candidates(self, func_short_name: str, limit: int) -> List[Dict[str, Any]]:
        """
        Helper to fetch Golden Data first, then fill remainder with Standard Executions.
        Resolves inputs for Golden Data by querying the original log.
        """
        candidates = []

        # 2-1. Fetch from Golden Dataset
        golden_col = self.client.collections.get(self.golden_collection_name)
        exec_col = self.client.collections.get(self.collection_name)

        try:
            golden_res = golden_col.query.fetch_objects(
                filters=wvc_query.Filter.by_property("function_name").equal(func_short_name),
                limit=limit
            )

            for obj in golden_res.objects:
                original_uuid = obj.properties.get("original_uuid")
                if not original_uuid:
                    continue

                original_log = exec_col.query.fetch_object_by_id(original_uuid)
                if original_log is None:
                    logger.warning(f"Golden Data {obj.uuid} refers to missing log {original_uuid}. Skipping.")
                    continue

                candidates.append({
                    "uuid": str(obj.uuid),
                    "inputs": original_log.properties,
                    "expected_output": self._deserialize_value(obj.properties.get("return_value")),
                    "is_golden": True
                })

            if candidates:
                logger.info(f"Loaded {len(candidates)} Golden Data test cases.")

        except Exception as e:
            logger.error(f"Failed to fetch Golden Data: {e}")

        # 2-2. Fetch from Standard Executions (if limit not reached)
        remaining = limit - len(candidates)
        if remaining > 0:
            try:
                filters = (
                        wvc_query.Filter.by_property("function_name").equal(func_short_name) &
                        wvc_query.Filter.by_property("status").equal("SUCCESS")
                )
                exec_res = exec_col.query.fetch_objects(
                    filters=filters,
                    limit=remaining,
                    sort=wvc_query.Sort.by_property("timestamp_utc", ascending=False)
                )

                for obj in exec_res.objects:
                    candidates.append({
                        "uuid": str(obj.uuid),
                        "inputs": obj.properties,
                        "expected_output": self._deserialize_value(obj.properties.get("return_value")),
                        "is_golden": False
                    })
            except Exception as e:
                logger.error(f"Failed to fetch Standard Executions: {e}")

        return candidates

    def _extract_inputs(self, props: Dict[str, Any], target_func: callable) -> Dict[str, Any]:
        """Extracts only the arguments defined in the target function's signature."""
        try:
            sig = inspect.signature(target_func)
            valid_params = sig.parameters.keys()
            inputs = {}
            for k, v in props.items():
                if k in valid_params and v != "[MASKED]":
                    inputs[k] = v
            return inputs
        except Exception as e:
            logger.warning(f"Failed to extract inputs: {e}")
            return props

    def _deserialize_value(self, value: Any) -> Any:
        return deserialize_return_value(value)

    def _compare_results(self, expected: Any, actual: Any) -> bool:
        if expected == actual: return True
        if str(expected) == str(actual): return True
        try:
            return json.dumps(expected, sort_keys=True) == json.dumps(actual, sort_keys=True)
        except:
            return False

    def _update_baseline_value(self, uuid_str: str, new_value: Any, is_golden: bool):
        collection_name = self.golden_collection_name if is_golden else self.collection_name
        collection = self.client.collections.get(collection_name)

        processed_val = vectorwave_core.mask_and_serialize(new_value, [])
        try:
            val_str = json.dumps(processed_val)
        except (TypeError, ValueError):
            val_str = str(processed_val)

        try:
            collection.data.update(
                uuid=uuid_str,
                properties={"return_value": str(val_str)}
            )
        except Exception as e:
            logger.error(f"Failed to update baseline for {uuid_str}: {e}")

    def _generate_diff_html(self, expected: Any, actual: Any) -> str:
        exp_str = pprint.pformat(expected, width=80)
        act_str = pprint.pformat(actual, width=80)
        return difflib.HtmlDiff(wrapcolumn=80).make_table(
            fromlines=exp_str.splitlines(),
            tolines=act_str.splitlines(),
            fromdesc='Expected (Baseline)',
            todesc='Actual (Current)',
            context=True,
            numlines=3
        )