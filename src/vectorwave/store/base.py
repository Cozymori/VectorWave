"""Abstract vector-store interface.

Designed against the operations VectorWave actually performs today
(`batch.add_object`, `db_search.search_executions`, near_vector cache
lookup, replay fetches, archiver delete). Each backend implements the
methods below; callers should never reach past the interface to a
backend-specific client.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional


@dataclass
class StoreRecord:
    """Backend-agnostic shape for a single object returned from the store.

    ``properties`` is the user-facing dict (function_name, status, etc.).
    ``vector`` is the stored embedding when ``include_vector=True`` was
    requested, otherwise None. ``distance`` is the cosine/L2 distance from
    the query vector when the record came out of a vector search.
    """
    uuid: str
    properties: Dict[str, Any]
    vector: Optional[List[float]] = None
    distance: Optional[float] = None
    certainty: Optional[float] = None
    extras: Dict[str, Any] = field(default_factory=dict)


class VectorStore(ABC):
    """Common contract for VectorWave's storage backends."""

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    @abstractmethod
    def is_ready(self) -> bool: ...

    @abstractmethod
    def close(self) -> None: ...

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    @abstractmethod
    def collection_exists(self, collection: str) -> bool: ...

    @abstractmethod
    def ensure_collection(
        self,
        collection: str,
        properties: List[Dict[str, Any]],
        vector_dim: Optional[int] = None,
    ) -> None:
        """Create the collection if missing. ``properties`` is a list of
        ``{"name": str, "data_type": str, "tokenization": Optional[str]}``
        dicts. ``vector_dim`` is required by some backends (LanceDB) and
        ignored by others (Weaviate self-provided)."""

    @abstractmethod
    def delete_collection(self, collection: str) -> None: ...

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    @abstractmethod
    def insert(
        self,
        collection: str,
        properties: Dict[str, Any],
        uuid: Optional[str] = None,
        vector: Optional[List[float]] = None,
    ) -> str:
        """Insert one object. Returns the assigned UUID."""

    @abstractmethod
    def insert_many(self, collection: str, items: List[Dict[str, Any]]) -> int:
        """Insert N objects. Each item:
        ``{"properties": ..., "uuid": ..., "vector": ...}``
        Returns the count successfully written."""

    @abstractmethod
    def update(self, collection: str, uuid: str, properties: Dict[str, Any]) -> None: ...

    @abstractmethod
    def delete_by_filter(self, collection: str, filters: Dict[str, Any]) -> int:
        """Delete every record matching ``filters``. Returns the deleted count."""

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_by_id(
        self,
        collection: str,
        uuid: str,
        include_vector: bool = False,
    ) -> Optional[StoreRecord]: ...

    @abstractmethod
    def query(
        self,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_ascending: bool = False,
        limit: int = 10,
        return_properties: Optional[List[str]] = None,
    ) -> List[StoreRecord]:
        """Filter + sort fetch. Used by archiver / replayer / search_executions."""

    @abstractmethod
    def near_vector(
        self,
        collection: str,
        vector: List[float],
        filters: Optional[Dict[str, Any]] = None,
        certainty: Optional[float] = None,
        limit: int = 1,
        include_vector: bool = False,
        return_properties: Optional[List[str]] = None,
    ) -> List[StoreRecord]:
        """Vector similarity search. ``certainty`` is interpreted as
        ``(1 + cos_sim) / 2`` — backends that don't provide it natively
        compute it from the distance."""

    @abstractmethod
    def iterate(self, collection: str, batch_size: int = 100) -> Iterable[StoreRecord]:
        """Stream every record in a collection (used by token-usage aggregation)."""

    # ------------------------------------------------------------------
    # Identification (helps tests / docs / logs)
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def backend_name(self) -> str: ...
