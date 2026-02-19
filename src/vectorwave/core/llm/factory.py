from functools import lru_cache

from .base import BaseLLMClient
from .openai_client import VectorWaveOpenAIClient


@lru_cache()
def get_llm_client() -> BaseLLMClient:
    """Returns the singleton LLM client instance."""
    return VectorWaveOpenAIClient()
