"""Backend-agnostic vector store interface.

VectorWave historically wired Weaviate's v4 client directly into batch /
search / replayer / archiver code. To support a Docker-less "Lite" mode
(see issue #95), the store layer abstracts the operations everyone
shares — insert, query, near_vector, update, delete — so each backend
can satisfy them independently.

Backends today:

- ``WeaviateVectorStore`` (Pro / default)  — wraps the existing client
- ``LanceVectorStore``     (Lite)          — local LanceDB, no Docker

Pick one via ``get_vector_store()`` (env: ``VECTORWAVE_MODE``).
"""
from .base import StoreRecord, VectorStore
from .factory import get_vector_store

__all__ = ["StoreRecord", "VectorStore", "get_vector_store"]
