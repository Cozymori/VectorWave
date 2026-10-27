# src/vectorwave/core/llm/openai_client.py
import logging
from typing import List, Dict, Optional
from datetime import datetime, timezone
from ...models.db_config import get_weaviate_settings
from ...batch.batch import get_batch_manager  # [추가]
from .base import BaseLLMClient

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


class VectorWaveOpenAIClient(BaseLLMClient):
    def __init__(self):
        self.settings = get_weaviate_settings()
        self.batch_manager = get_batch_manager()

        if OpenAI is None or not self.settings.OPENAI_API_KEY:
            self.client = None
        else:
            self.client = OpenAI(api_key=self.settings.OPENAI_API_KEY)

    def _log_usage(self, tokens: int, model: str, usage_type: str, category: str):
        if tokens > 0:
            try:
                self.batch_manager.add_object(
                    collection="VectorWaveTokenUsage",
                    properties={
                        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
                        "model": model,
                        "usage_type": usage_type,
                        "category": category,
                        "tokens": tokens
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to log token usage: {e}")

    def create_embedding(self, text: str, model: str = "text-embedding-3-small", category: str = "default") -> Optional[
        List[float]]:
        """
        Returns: Vector list only (Tokens are logged internally)
        """
        if not self.client: return None
        try:
            text = text.replace("\n", " ")
            res = self.client.embeddings.create(input=[text], model=model)

            tokens = res.usage.total_tokens if res.usage else 0
            self._log_usage(tokens, model, "embedding", category)

            return res.data[0].embedding
        except Exception as e:
            logger.error(f"Embedding error: {e}")
            return None

    def create_chat_completion(self, messages: List[Dict], model: str = "gpt-4o", temperature: float = 0.1,
                               response_format=None, category: str = "default") -> Optional[str]:
        """
        Returns: Content string only (Tokens are logged internally)
        """
        if not self.client: return None
        try:
            kwargs = {"model": model, "messages": messages, "temperature": temperature}
            if response_format: kwargs["response_format"] = response_format

            res = self.client.chat.completions.create(**kwargs)

            tokens = res.usage.total_tokens if res.usage else 0
            self._log_usage(tokens, model, "generation", category)

            return res.choices[0].message.content
        except Exception as e:
            logger.error(f"Completion error: {e}")
            return None
