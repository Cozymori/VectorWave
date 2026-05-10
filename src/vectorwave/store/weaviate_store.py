"""Weaviate-backed VectorStore implementation.

Wraps the existing client/get_cached_client logic so the rest of the
codebase can talk to a backend-neutral interface while production
deployments keep using Weaviate.
"""
from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterable, List, Optional

import weaviate
import weaviate.classes.config as wvc
import weaviate.classes.query as wvc_query
from weaviate.classes.config import Tokenization
from weaviate.classes.query import Filter

from .base import StoreRecord, VectorStore

logger = logging.getLogger(__name__)

_PROP_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _build_weaviate_filter(filters: Optional[Dict[str, Any]]):
    """Translate the dict-style filter we use across the codebase into a
    Weaviate ``Filter``. Mirrors the behaviour previously implemented
    inline in ``db_search._build_weaviate_filters`` so existing callers
    keep working unchanged."""
    if not filters:
        return None

    parts = []
    for key, value in filters.items():
        segments = key.split("__")
        prop_name = segments[0]
        op = segments[1] if len(segments) > 1 else "equal"

        if not _PROP_NAME_PATTERN.match(prop_name):
            logger.warning("Rejecting unsafe filter prop name '%s'", prop_name)
            continue

        prop = Filter.by_property(prop_name)
        try:
            if op == "equal":
                if isinstance(value, list) and value:
                    parts.append(prop.contains_any(value))
                else:
                    parts.append(prop.equal(value))
            elif op == "not_equal":
                parts.append(prop.not_equal(value))
            elif op == "gte":
                parts.append(prop.greater_or_equal(value))
            elif op == "gt":
                parts.append(prop.greater_than(value))
            elif op == "lte":
                parts.append(prop.less_or_equal(value))
            elif op == "lt":
                parts.append(prop.less_than(value))
            elif op == "like":
                parts.append(prop.like(f"*{value}*"))
            else:
                logger.warning("Unknown filter operator '%s' on '%s' — using equal.", op, prop_name)
                parts.append(prop.equal(value))
        except Exception as e:
            logger.error("Failed to build filter for '%s': %s", key, e)

    if not parts:
        return None
    return Filter.all_of(parts)


def _to_record(obj, include_vector: bool = False) -> StoreRecord:
    metadata = getattr(obj, "metadata", None)
    distance = getattr(metadata, "distance", None) if metadata is not None else None
    certainty = getattr(metadata, "certainty", None) if metadata is not None else None
    vector = None
    if include_vector and obj.vector:
        vector = obj.vector.get("default") if isinstance(obj.vector, dict) else obj.vector
    return StoreRecord(
        uuid=str(obj.uuid),
        properties=dict(obj.properties or {}),
        vector=vector,
        distance=distance,
        certainty=certainty,
    )


_DATA_TYPE_MAP = {
    "TEXT": wvc.DataType.TEXT,
    "INT": wvc.DataType.INT,
    "NUMBER": wvc.DataType.NUMBER,
    "BOOL": wvc.DataType.BOOL,
    "DATE": wvc.DataType.DATE,
    "UUID": wvc.DataType.UUID,
    "TEXT_ARRAY": wvc.DataType.TEXT_ARRAY,
}


_TOKENIZATION_MAP = {
    "word": Tokenization.WORD,
    "whitespace": Tokenization.WHITESPACE,
    "field": Tokenization.FIELD,
    "lowercase": Tokenization.LOWERCASE,
}


class WeaviateVectorStore(VectorStore):
    """Adapter around an existing weaviate.WeaviateClient instance.

    Construction is lazy via ``WeaviateVectorStore(client=...)`` from the
    factory; passing the cached client preserves the singleton semantics
    the rest of the package relies on.
    """

    backend_name = "weaviate"

    def __init__(self, client: weaviate.WeaviateClient):
        self._client = client

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def is_ready(self) -> bool:
        try:
            return bool(self._client.is_ready())
        except Exception:
            return False

    def close(self) -> None:
        try:
            self._client.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def collection_exists(self, collection: str) -> bool:
        return self._client.collections.exists(collection)

    def ensure_collection(
        self,
        collection: str,
        properties: List[Dict[str, Any]],
        vector_dim: Optional[int] = None,
    ) -> None:
        if self._client.collections.exists(collection):
            return
        wvc_props = []
        for p in properties:
            dtype = _DATA_TYPE_MAP.get(p["data_type"].upper())
            if dtype is None:
                raise ValueError(f"Unsupported data_type '{p['data_type']}' on '{p['name']}'")
            tok_str = p.get("tokenization")
            tok = _TOKENIZATION_MAP.get(tok_str.lower()) if tok_str else None
            wvc_props.append(
                wvc.Property(
                    name=p["name"],
                    data_type=dtype,
                    description=p.get("description"),
                    tokenization=tok,
                )
            )
        self._client.collections.create(
            name=collection,
            properties=wvc_props,
            vector_config=wvc.Configure.Vectors.self_provided(),
        )

    def delete_collection(self, collection: str) -> None:
        if self._client.collections.exists(collection):
            self._client.collections.delete(collection)

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def insert(
        self,
        collection: str,
        properties: Dict[str, Any],
        uuid: Optional[str] = None,
        vector: Optional[List[float]] = None,
    ) -> str:
        col = self._client.collections.get(collection)
        kwargs: Dict[str, Any] = {"properties": properties}
        if uuid:
            kwargs["uuid"] = uuid
        if vector is not None:
            kwargs["vector"] = vector
        return str(col.data.insert(**kwargs))

    def insert_many(self, collection: str, items: List[Dict[str, Any]]) -> int:
        if not items:
            return 0
        # Use the dynamic batch context for efficient bulk writes.
        try:
            with self._client.batch.dynamic() as batch:
                for item in items:
                    batch.add_object(
                        collection=item.get("collection", collection),
                        properties=item["properties"],
                        uuid=item.get("uuid"),
                        vector=item.get("vector"),
                    )
            failed = getattr(self._client.batch, "failed_objects", []) or []
            for f in failed:
                logger.error("Batch insert failed for one item: %s", getattr(f, "message", f))
            return len(items) - len(failed)
        except RuntimeError:
            return 0
        except Exception as e:
            msg = str(e).lower()
            if "shutdown" in msg or "closed" in msg:
                return 0
            logger.error("insert_many error: %s", e)
            return 0

    def update(self, collection: str, uuid: str, properties: Dict[str, Any]) -> None:
        col = self._client.collections.get(collection)
        col.data.update(uuid=uuid, properties=properties)

    def delete_by_filter(self, collection: str, filters: Dict[str, Any]) -> int:
        if not filters:
            return 0
        col = self._client.collections.get(collection)
        wf = _build_weaviate_filter(filters)
        if wf is None:
            return 0
        result = col.data.delete_many(where=wf)
        return int(getattr(result, "successful", 0) or 0)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def fetch_by_id(
        self,
        collection: str,
        uuid: str,
        include_vector: bool = False,
    ) -> Optional[StoreRecord]:
        col = self._client.collections.get(collection)
        try:
            obj = col.query.fetch_object_by_id(uuid=uuid, include_vector=include_vector)
        except Exception as e:
            logger.warning("fetch_by_id failed for %s/%s: %s", collection, uuid, e)
            return None
        if obj is None:
            return None
        return _to_record(obj, include_vector=include_vector)

    def query(
        self,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_ascending: bool = False,
        limit: int = 10,
        return_properties: Optional[List[str]] = None,
    ) -> List[StoreRecord]:
        col = self._client.collections.get(collection)
        wf = _build_weaviate_filter(filters)
        sort = None
        if sort_by:
            sort = wvc_query.Sort.by_property(name=sort_by, ascending=sort_ascending)
        kwargs: Dict[str, Any] = {"limit": limit, "filters": wf, "sort": sort}
        if return_properties:
            kwargs["return_properties"] = return_properties
        response = col.query.fetch_objects(**kwargs)
        return [_to_record(o) for o in response.objects]

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
        col = self._client.collections.get(collection)
        wf = _build_weaviate_filter(filters)
        kwargs: Dict[str, Any] = {
            "near_vector": vector,
            "limit": limit,
            "filters": wf,
            "return_metadata": wvc_query.MetadataQuery(distance=True, certainty=True),
            "include_vector": include_vector,
        }
        if certainty is not None:
            kwargs["certainty"] = certainty
        if return_properties:
            kwargs["return_properties"] = return_properties
        response = col.query.near_vector(**kwargs)
        return [_to_record(o, include_vector=include_vector) for o in response.objects]

    def iterate(self, collection: str, batch_size: int = 100) -> Iterable[StoreRecord]:
        col = self._client.collections.get(collection)
        for obj in col.iterator():
            yield _to_record(obj)
