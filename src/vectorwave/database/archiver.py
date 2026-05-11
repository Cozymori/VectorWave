import json
import os
from typing import Dict, Any
from ..models.db_config import get_weaviate_settings
from ..store import get_vector_store


class VectorWaveArchiver:
    def __init__(self):
        self.settings = get_weaviate_settings()
        self.store = get_vector_store()
        self.collection_name = self.settings.EXECUTION_COLLECTION_NAME

    def export_and_clear(self,
                         function_name: str,
                         output_file: str,
                         clear_after_export: bool = False,
                         delete_only: bool = False) -> Dict[str, int]:
        """
        Exports execution logs or cleans them up from the backend store.
        Routes through the VectorStore interface so the same flow works in
        Pro (Weaviate) and Lite (LanceDB) modes.
        """
        # 1. Configure the filter. Non-delete-only mode also requires SUCCESS.
        filters: Dict[str, Any] = {"function_name": function_name}
        if not delete_only:
            filters["status"] = "SUCCESS"

        # 2. Retrieve data
        records = self.store.query(
            collection=self.collection_name,
            filters=filters,
            limit=10000,
            return_properties=["return_value", "timestamp_utc"],
        )

        if not records:
            return {"exported": 0, "deleted": 0}

        exported_count = 0
        deleted_count = 0
        uuids_to_delete = []

        # 3. Save to file (Export mode)
        if not delete_only:
            try:
                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(output_file) or '.', exist_ok=True)

                with open(output_file, 'a', encoding='utf-8') as f:
                    for rec in records:
                        data_entry = self._convert_to_training_format(rec)
                        f.write(json.dumps(data_entry, ensure_ascii=False) + "\n")
                        uuids_to_delete.append(rec.uuid)
                        exported_count += 1
                print(f"✅ [Export] {exported_count} records saved: {output_file}")
            except Exception as e:
                print(f"❌ [Error] Failed to save file: {e}")
                return {"exported": 0, "deleted": 0}  # Stop deletion upon save failure
        else:
            # If in delete-only mode, every retrieved row goes to delete list
            uuids_to_delete = [rec.uuid for rec in records]

        # 4. Delete from store (if option is enabled)
        if (clear_after_export or delete_only) and uuids_to_delete:
            try:
                deleted_count = self.store.delete_by_filter(
                    collection=self.collection_name,
                    filters=filters,
                )
                print(f"🗑️ [Clear] {deleted_count} records deleted.")
            except Exception as e:
                print(f"❌ [Error] Delete failed: {e}")

        return {"exported": exported_count, "deleted": deleted_count}

    def _convert_to_training_format(self, rec) -> Dict[str, Any]:
        """
        Converts a StoreRecord (or any object with `.properties` + `.uuid`) into
        an LLM fine-tuning JSONL row.
        """
        # Accepts either a StoreRecord or a dict (for backwards compatibility).
        if hasattr(rec, "properties"):
            props = rec.properties
        else:
            props = rec
        exclude_keys = {
            'status', 'duration_ms', 'timestamp_utc', 'error_message', 'error_code',
            'return_value', 'function_name', 'trace_id', 'span_id', 'parent_span_id',
            'function_uuid', 'run_id', 'uuid'
        }

        inputs = {k: v for k, v in props.items() if k not in exclude_keys}
        output = props.get('return_value')

        return {
            "messages": [
                {"role": "user", "content": json.dumps(inputs, ensure_ascii=False)},
                {"role": "assistant", "content": str(output)}
            ]
        }
