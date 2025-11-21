import importlib
import json
import logging
import traceback
import inspect
from typing import Any, Dict, List, Optional

import weaviate.classes.query as wvc_query

from ..database.db import get_cached_client
from ..models.db_config import get_weaviate_settings
from ..monitoring.tracer import _mask_and_serialize

# Logger setup
logger = logging.getLogger(__name__)

class VectorWaveReplayer:
    """
    A class that performs automated regression testing (Replay) based on VectorWave execution logs.
    Location: src/vectorwave/utils/replayer.py
    """

    def __init__(self):
        self.client = get_cached_client()
        self.settings = get_weaviate_settings()
        self.collection_name = self.settings.EXECUTION_COLLECTION_NAME

    def replay(self,
               function_full_name: str,
               limit: int = 10,
               update_baseline: bool = False) -> Dict[str, Any]:
        """
        Retrieves past execution history of a specific function, re-executes it (Replay), and validates the result.
        """
        # 1. Dynamic Function Loading (Dynamic Import)
        try:
            module_name, func_short_name = function_full_name.rsplit('.', 1)
            module = importlib.import_module(module_name)
            target_func = getattr(module, func_short_name)
        except (ValueError, ImportError, AttributeError) as e:
            logger.error(f"Could not load function: {function_full_name}. Error: {e}")
            return {"error": f"Function loading failed: {e}"}

        # 2. Retrieve Test Data (Past Logs) from DB
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

        print(f"🔄 Replaying {len(response.objects)} logs for '{function_full_name}'...")

        for obj in response.objects:
            results["total"] += 1

            inputs = self._extract_inputs(obj.properties, target_func)
            expected_output = self._deserialize_value(obj.properties.get("return_value"))

            try:
                # 3. Function Re-execution
                actual_output = target_func(**inputs)

                # 4. Result Validation
                is_match = self._compare_results(expected_output, actual_output)

                if is_match:
                    results["passed"] += 1
                else:
                    # 5. Handle Mismatch
                    if update_baseline:
                        self._update_baseline_value(collection, obj.uuid, actual_output)
                        results["updated"] += 1
                        results["passed"] += 1
                        print(f"  ⚠️ [Updated] UUID {obj.uuid} result updated.")
                    else:
                        results["failed"] += 1
                        results["failures"].append({
                            "uuid": str(obj.uuid),
                            "inputs": inputs,
                            "expected": expected_output,
                            "actual": actual_output
                        })
                        print(f"  ❌ [Failed] UUID {obj.uuid} | Expected: {expected_output} != Actual: {actual_output}")

            except Exception as e:

                results["failed"] += 1

                results["failures"].append({
                    "uuid": str(obj.uuid),
                    "inputs": inputs,
                    "expected": expected_output,  # <-- Added
                    "actual": "EXCEPTION_RAISED", # <-- Added
                    "error": str(e),
                    "traceback": traceback.format_exc()
                })
                print(f"  ❌ [Error] UUID {obj.uuid} exception occurred during execution: {e}")

        return results

    def _extract_inputs(self, props: Dict[str, Any], target_func: callable) -> Dict[str, Any]:
        """
        Extracts only the arguments defined in the target function's signature from the DB properties.
        This prevents extraneous metadata like 'user_id' from being passed to the function.
        """
        # 1. Check the list of arguments the function can accept
        sig = inspect.signature(target_func)
        valid_params = sig.parameters.keys()

        inputs = {}
        for k, v in props.items():
            # 2. Select only keys matching function arguments and not masked
            if k in valid_params and v != "[MASKED]":
                inputs[k] = v

        return inputs

    def _deserialize_value(self, value: Any) -> Any:
        """Restores a Python object from a string value stored in the DB."""
        if isinstance(value, str):
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return value
        return value

    def _compare_results(self, expected: Any, actual: Any) -> bool:
        """Result comparison logic."""
        if expected == actual:
            return True
        if str(expected) == str(actual):
            return True
        try:
            return json.dumps(expected, sort_keys=True) == json.dumps(actual, sort_keys=True)
        except:
            return False

    def _update_baseline_value(self, collection, uuid, new_value):
        """Updates the DB with the new result value as the new baseline."""
        # Use the same serialization logic as Tracer.py for consistency
        processed_val = _mask_and_serialize(new_value, set())

        try:
            # Serialize to JSON string (e.g., "value" -> "\"value\"")
            val_str = json.dumps(processed_val)
        except (TypeError, ValueError):
            val_str = str(processed_val) # Fallback to standard string conversion

        try:
            collection.data.update(
                uuid=uuid,
                properties={"return_value": str(val_str)}
            )
        except Exception as e:
            logger.error(f"Failed to update baseline for {uuid}: {e}")