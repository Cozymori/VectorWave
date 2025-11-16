import requests
from .base import BaseAlerter
from typing import Dict, Any
import logging

logger = logging.getLogger(__name__)


class WebhookAlerter(BaseAlerter):
    def __init__(self, url: str):
        self.url = url

        self.STANDARD_KEYS = {
            'trace_id', 'span_id', 'function_name', 'timestamp_utc',
            'duration_ms', 'status', 'error_message', 'error_code'
        }

    def notify(self, error_log: Dict[str, Any]):
        try:
            func_name = error_log.get('function_name', 'N/A')
            error_code = error_log.get('error_code', 'UNKNOWN')
            trace_id = error_log.get('trace_id', 'N/A')
            duration = error_log.get('duration_ms', 0)
            timestamp = error_log.get('timestamp_utc')

            error_msg_full = error_log.get('error_message', 'No error message provided.')
            error_msg_snippet = (error_msg_full[:1000] + '...') if len(error_msg_full) > 1000 else error_msg_full


            fields = [
                {"name": "Function Name", "value": f"`{func_name}`", "inline": True},
                {"name": "Error Code", "value": f"`{error_code}`", "inline": True},
                {"name": "Duration", "value": f"{duration:.2f} ms", "inline": True},
                {"name": "Trace ID", "value": f"`{trace_id}`", "inline": False},
            ]

            for key, value in error_log.items():
                if key not in self.STANDARD_KEYS and value is not None:
                    value_str = str(value)
                    if value_str:
                        fields.append(
                            {"name": f"Attribute: {key}", "value": f"`{value_str}`", "inline": True}
                        )

            formatted_payload = {
                "username": "VectorWave Alerter",
                "embeds": [
                    {
                        "title": f"🚨 VectorWave ERROR: {func_name}",
                        "description": f"```python\n{error_msg_snippet}\n```",
                        "color": 15158332,  # Discord Red
                        "fields": fields,
                        "timestamp": timestamp if timestamp else None
                    }
                ]
            }

            requests.post(self.url, json=formatted_payload, timeout=5)

        except Exception as e:
            logger.error(f"WebhookAlerter failed to format or send notification: {e}")

            try:
                fallback_payload = {
                    "content": f"🚨 ERROR (Fallback): {error_log.get('function_name')}\nCode: {error_log.get('error_code')}"
                }
                requests.post(self.url, json=fallback_payload, timeout=3)
            except Exception as fe:
                logger.error(f"WebhookAlerter fallback also failed: {fe}")