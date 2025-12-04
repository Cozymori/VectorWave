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
from ..monitoring.tracer import _mask_and_serialize
from .context import execution_source_context

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
        # 1. Dynamic Function Loading
        try:
            module_name, func_short_name = function_full_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            target_func = getattr(module, func_short_name)
        except (ValueError, ImportError, AttributeError) as e:
            logger.error(f"Could not load function: {function_full_name}. Error: {e}")
            return {"error": f"Function loading failed: {e}"}

        is_async_func = inspect.iscoroutinefunction(target_func)

        # 2. Retrieve Test Data (Priority: Golden > Standard)
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

        logger.info(f"Starting Replay: {len(test_objects)} logs for '{function_full_name}'")

        for obj_data in test_objects:
            results["total"] += 1

            # Unpack data
            uuid_str = obj_data['uuid']
            raw_inputs = obj_data['inputs']
            expected_output = obj_data['expected_output']
            is_golden = obj_data.get('is_golden', False)

            # [FIX] Extract only valid arguments for the target function
            inputs = self._extract_inputs(raw_inputs, target_func)

            token = None
            try:
                token = execution_source_context.set("REPLAY")

                # 3. Function Re-execution
                if is_async_func:
                    actual_output = asyncio.run(target_func(**inputs))
                else:
                    actual_output = target_func(**inputs)

                # 4. Result Validation
                is_match = self._compare_results(expected_output, actual_output)

                if is_match:
                    results["passed"] += 1
                    logger.debug(f"UUID {uuid_str}: PASSED {'(Golden)' if is_golden else ''}")
                else:
                    # 5. Handle Mismatch
                    if update_baseline:
                        self._update_baseline_value(uuid_str, actual_output, is_golden)
                        results["updated"] += 1
                        results["passed"] += 1
                        logger.info(f"UUID {uuid_str}: Baseline UPDATED")
                    else:
                        results["failed"] += 1
                        diff_html = self._generate_diff_html(expected_output, actual_output)

                        results["failures"].append({
                            "uuid": uuid_str,
                            "inputs": inputs,
                            "expected": expected_output,
                            "actual": actual_output,
                            "diff_html": diff_html,
                            "is_golden": is_golden
                        })
                        logger.warning(f"UUID {uuid_str}: FAILED (Mismatch) {'[GOLDEN]' if is_golden else ''}")

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

            finally:
                if token:
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
                if not original_log:
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
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return value

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

        processed_val = _mask_and_serialize(new_value, set())
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