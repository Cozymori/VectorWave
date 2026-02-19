import json
from typing import Any, Optional


def deserialize_return_value(value: Optional[Any]) -> Any:
    """
    Deserializes a return value back to a Python object if possible.
    Handles both string (JSON-encoded) and non-string values.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value
