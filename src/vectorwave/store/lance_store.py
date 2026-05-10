"""LanceDB-backed VectorStore implementation (VectorWave Lite mode).

Provides a Docker-less, file-based vector store for hackathon / Colab /
local-dev use. Tradeoffs vs the Weaviate backend:

- ✅ Zero infrastructure: a directory under ``.vectorwave/lance/`` holds everything
- ✅ Vector + filter + sort search are supported
- ⚠️  Hybrid search (BM25 + vector) and Weaviate's modular vectorizers are
   not available; Lite mode requires a Python-side vectorizer (HF / OpenAI client)
- ⚠️  Filtering happens in Python after fetch (good enough for ≲100k rows; if
   you outgrow Lite, switch to Weaviate)

Each VectorWave collection becomes a LanceDB table with a fixed schema
``(uuid: string, vector: list<float32, dim>, payload: string)`` — every
property is stuffed into the JSON ``payload`` so the same code can host
arbitrary VectorWave property shapes without per-collection schemas. We
trade a bit of query speed for schema simplicity. For Lite use this is the
right call.
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

from .base import StoreRecord, VectorStore

logger = logging.getLogger(__name__)

# Default embedding dim (HF all-MiniLM-L6-v2). Lite mode users typically run with
# `VECTORIZER=huggingface`; this matches that.
DEFAULT_VECTOR_DIM = 384


def _serialize_properties(props: Dict[str, Any]) -> str:
    return json.dumps(props, default=str, ensure_ascii=False)


def _deserialize_properties(blob: Optional[str]) -> Dict[str, Any]:
    if not blob:
        return {}
    try:
        return json.loads(blob)
    except (TypeError, ValueError):
        return {}


def _matches_filter(props: Dict[str, Any], filters: Optional[Dict[str, Any]]) -> bool:
    """Mirror ``_build_weaviate_filter`` semantics in Python.

    Supports the same operator suffixes (``__equal``, ``__not_equal``,
    ``__gte``, ``__gt``, ``__lte``, ``__lt``, ``__like``) so the rest of the
    codebase can use the same filter shape with either backend.
    """
    if not filters:
        return True
    for key, value in filters.items():
        segments = key.split("__")
        prop = segments[0]
        op = segments[1] if len(segments) > 1 else "equal"
        actual = props.get(prop)
        if op == "equal":
            if isinstance(value, list):
                if actual not in value:
                    return False
            else:
                if actual != value:
                    return False
        elif op == "not_equal":
            if actual == value:
                return False
        elif op == "gte":
            if actual is None or actual < value:
                return False
        elif op == "gt":
            if actual is None or actual <= value:
                return False
        elif op == "lte":
            if actual is None or actual > value:
                return False
        elif op == "lt":
            if actual is None or actual >= value:
                return False
        elif op == "like":
            if not isinstance(actual, str) or value not in actual:
                return False
        else:
            logger.warning("Unknown filter op '%s' on '%s'; treating as equal.", op, prop)
            if actual != value:
                return False
    return True


def _row_to_record(row: Dict[str, Any], include_vector: bool = False, distance: Optional[float] = None) -> StoreRecord:
    props = _deserialize_properties(row.get("payload"))
    vector = None
    if include_vector and row.get("vector") is not None:
        vector = list(row["vector"])
    cert = (1.0 - distance / 2) if distance is not None else None
    return StoreRecord(
        uuid=row["uuid"],
        properties=props,
        vector=vector,
        distance=distance,
        certainty=cert,
    )


class LanceVectorStore(VectorStore):
    backend_name = "lance"

    def __init__(self, db_path: str = ".vectorwave/lance", vector_dim: int = DEFAULT_VECTOR_DIM):
        try:
            import lancedb
        except ImportError as e:
            raise ImportError(
                "LanceDB is required for VectorWave Lite mode. "
                "Install with `pip install lancedb`."
            ) from e
        os.makedirs(db_path, exist_ok=True)
        self._lancedb = lancedb
        self._db = lancedb.connect(db_path)
        self._db_path = db_path
        self._vector_dim = vector_dim
        self._open_tables: Dict[str, Any] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def is_ready(self) -> bool:
        return True

    def close(self) -> None:
        self._open_tables.clear()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def collection_exists(self, collection: str) -> bool:
        return collection in self._db.list_tables()

    def ensure_collection(
        self,
        collection: str,
        properties: List[Dict[str, Any]],
        vector_dim: Optional[int] = None,
    ) -> None:
        if collection in self._db.list_tables():
            return
        import pyarrow as pa
        dim = vector_dim or self._vector_dim
        schema = pa.schema(
            [
                pa.field("uuid", pa.string()),
                pa.field("vector", pa.list_(pa.float32(), dim)),
                pa.field("payload", pa.string()),
            ]
        )
        # `exist_ok=True` makes ensure_collection truly idempotent across the
        # in-memory list_tables cache and any concurrent worker — if the table
        # is already on disk we just no-op and move on.
        try:
            self._db.create_table(collection, schema=schema, exist_ok=True)
        except TypeError:
            # Older lancedb versions don't accept exist_ok; fall back to
            # catching the existence error.
            try:
                self._db.create_table(collection, schema=schema, mode="create")
            except ValueError as e:
                if "already exists" not in str(e):
                    raise

    def delete_collection(self, collection: str) -> None:
        if collection in self._db.list_tables():
            self._db.drop_table(collection)
            self._open_tables.pop(collection, None)

    def _open(self, collection: str):
        cached = self._open_tables.get(collection)
        if cached is not None:
            return cached
        if collection not in self._db.list_tables():
            # Collections appear lazily — create with the default schema if a
            # caller writes before calling ensure_collection.
            self.ensure_collection(collection, properties=[])
        tbl = self._db.open_table(collection)
        self._open_tables[collection] = tbl
        return tbl

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
        uuid = uuid or str(uuid4())
        vec = list(vector) if vector is not None else [0.0] * self._vector_dim
        if len(vec) != self._vector_dim:
            # Pad / truncate so the schema's fixed-size list is satisfied. Logged
            # so callers notice, but we keep going so a Lite user with a stray
            # different-dim vector doesn't get a hard crash.
            logger.warning(
                "Vector dim %d != table dim %d for collection '%s'; padding/truncating.",
                len(vec), self._vector_dim, collection,
            )
            if len(vec) < self._vector_dim:
                vec = vec + [0.0] * (self._vector_dim - len(vec))
            else:
                vec = vec[: self._vector_dim]
        tbl = self._open(collection)
        tbl.add([{"uuid": uuid, "vector": vec, "payload": _serialize_properties(properties)}])
        return uuid

    def insert_many(self, collection: str, items: List[Dict[str, Any]]) -> int:
        if not items:
            return 0
        tbl = self._open(collection)
        rows = []
        for it in items:
            uuid = it.get("uuid") or str(uuid4())
            vec = it.get("vector")
            vec = list(vec) if vec is not None else [0.0] * self._vector_dim
            if len(vec) != self._vector_dim:
                if len(vec) < self._vector_dim:
                    vec = vec + [0.0] * (self._vector_dim - len(vec))
                else:
                    vec = vec[: self._vector_dim]
            rows.append({
                "uuid": uuid,
                "vector": vec,
                "payload": _serialize_properties(it["properties"]),
            })
        tbl.add(rows)
        return len(rows)

    def update(self, collection: str, uuid: str, properties: Dict[str, Any]) -> None:
        tbl = self._open(collection)
        # Read current row, merge properties, rewrite. LanceDB's `update` API
        # works on column expressions; for our JSON-blob schema that's awkward,
        # so we delete + re-insert with the same UUID. The vector is preserved.
        rows = tbl.search().where(f"uuid = '{uuid}'").limit(1).to_list()
        if not rows:
            logger.warning("update: uuid '%s' not found in '%s'", uuid, collection)
            return
        existing = rows[0]
        merged = _deserialize_properties(existing.get("payload"))
        merged.update(properties)
        tbl.delete(f"uuid = '{uuid}'")
        tbl.add(
            [{
                "uuid": uuid,
                "vector": list(existing.get("vector") or [0.0] * self._vector_dim),
                "payload": _serialize_properties(merged),
            }]
        )

    def delete_by_filter(self, collection: str, filters: Dict[str, Any]) -> int:
        if not filters:
            return 0
        tbl = self._open(collection)
        # LanceDB's `delete` takes a SQL expression. We can't easily push our
        # dict-style filters down without translating each operator, so do a
        # Python-side fetch + bulk delete by uuid list.
        all_rows = tbl.to_pandas().to_dict("records")
        targets = [r["uuid"] for r in all_rows
                   if _matches_filter(_deserialize_properties(r.get("payload")), filters)]
        if not targets:
            return 0
        # SQL-escape uuids (they are generated UUIDs / safe strings) and delete.
        uuid_list = ",".join(f"'{u}'" for u in targets)
        tbl.delete(f"uuid IN ({uuid_list})")
        return len(targets)

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def fetch_by_id(
        self,
        collection: str,
        uuid: str,
        include_vector: bool = False,
    ) -> Optional[StoreRecord]:
        tbl = self._open(collection)
        rows = tbl.search().where(f"uuid = '{uuid}'").limit(1).to_list()
        if not rows:
            return None
        return _row_to_record(rows[0], include_vector=include_vector)

    def query(
        self,
        collection: str,
        filters: Optional[Dict[str, Any]] = None,
        sort_by: Optional[str] = None,
        sort_ascending: bool = False,
        limit: int = 10,
        return_properties: Optional[List[str]] = None,
    ) -> List[StoreRecord]:
        tbl = self._open(collection)
        # Fetch-all-then-filter — fine for Lite mode dataset sizes.
        df = tbl.to_pandas()
        records: List[StoreRecord] = []
        rows = df.to_dict("records")
        filtered = [r for r in rows if _matches_filter(_deserialize_properties(r.get("payload")), filters)]
        if sort_by:
            def _sort_key(row):
                key = _deserialize_properties(row.get("payload")).get(sort_by)
                # None values sort last regardless of order
                return (key is None, key)
            filtered.sort(key=_sort_key, reverse=not sort_ascending)
        filtered = filtered[: limit]
        for r in filtered:
            records.append(_row_to_record(r))
        return records

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
        tbl = self._open(collection)
        vec = list(vector)
        if len(vec) != self._vector_dim:
            if len(vec) < self._vector_dim:
                vec = vec + [0.0] * (self._vector_dim - len(vec))
            else:
                vec = vec[: self._vector_dim]
        # Fetch a generous superset; we filter + truncate in Python.
        fetch_limit = max(limit * 5, 50)
        rows = tbl.search(vec).limit(fetch_limit).to_list()
        out: List[StoreRecord] = []
        for r in rows:
            props = _deserialize_properties(r.get("payload"))
            if not _matches_filter(props, filters):
                continue
            distance = float(r.get("_distance", 0.0))
            cert = max(0.0, 1.0 - distance / 2.0)
            if certainty is not None and cert < certainty:
                continue
            rec = _row_to_record(r, include_vector=include_vector, distance=distance)
            out.append(rec)
            if len(out) >= limit:
                break
        return out

    def iterate(self, collection: str, batch_size: int = 100) -> Iterable[StoreRecord]:
        tbl = self._open(collection)
        for r in tbl.to_pandas().to_dict("records"):
            yield _row_to_record(r)
