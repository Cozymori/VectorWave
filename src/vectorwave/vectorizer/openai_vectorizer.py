from .base import BaseVectorizer
from typing import List
from ..core.llm.factory import get_llm_client
import logging

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
except ImportError:
    logger.warning("The 'openai' library is not installed.")
    logger.warning("To use OpenAIVectorizer, run 'pip install openai'.")
    OpenAI = None


class OpenAIVectorizer(BaseVectorizer):

    def __init__(self, api_key: str, model: str = "text-embedding-3-small"):
        if OpenAI is None:
            # Could not find the 'openai' library.
            raise ImportError("Could not find the 'openai' library.")
        if not api_key:
            raise ValueError("OpenAI API key is required for OpenAIVectorizer.")

        self.client = get_llm_client()
        self.model = model
        logger.info("OpenAIVectorizer initialized with model '%s'.", self.model)

    def embed(self, text: str) -> List[float]:
        text = text.replace("\n", " ")
        vector = self.client.create_embedding(
            text=text,
            model=self.model,
            category="embedding"
        )
        return vector if vector else []

    def embed_batch(self, texts: List[str]) -> List[List[float]]:
        embeddings = []
        for text in texts:
            emb = self.embed(text)
            if emb:
                embeddings.append(emb)
            else:
                embeddings.append([])
        return embeddings
