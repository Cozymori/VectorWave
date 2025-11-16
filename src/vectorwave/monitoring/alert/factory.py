from functools import lru_cache
from ...models.db_config import get_weaviate_settings
from .base import BaseAlerter
from .webhook_alerter import WebhookAlerter
from .null_alerter import NullAlerter

@lru_cache()
def get_alerter() -> BaseAlerter:
    settings = get_weaviate_settings()
    strategy = settings.ALERTER_STRATEGY.lower()

    if strategy == "webhook":
        if not settings.ALERTER_WEBHOOK_URL:
            print("Warning: ALERTER_STRATEGY='webhook' but ALERTER_WEBHOOK_URL is not set. Using 'none'.")
            return NullAlerter()
        return WebhookAlerter(url=settings.ALERTER_WEBHOOK_URL)

    # 기본값
    return NullAlerter()