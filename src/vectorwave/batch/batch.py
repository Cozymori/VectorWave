import os
import weaviate
import atexit
import logging
import threading
import queue
import time
from functools import lru_cache
from typing import Optional, List, Dict, Any

from ..models.db_config import get_weaviate_settings, WeaviateSettings
from ..database.db import get_weaviate_client

# Rust Core 모듈 Import 시도


logger = logging.getLogger(__name__)

try:
    from vectorwave.vectorwave_core import RustBatchManager
    USE_RUST_CORE = True
except ImportError:
    USE_RUST_CORE = False

class WeaviateBatchManager:
    """
    Manages Weaviate batch imports.
    Uses High-Performance Rust Core if available, otherwise falls back to Python.
    """

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 grpc_port: Optional[int] = None, api_key: Optional[str] = None):
        self._initialized = False
        self._shutdown_done = False
        self.settings: WeaviateSettings = get_weaviate_settings()
        self.client: Optional[weaviate.WeaviateClient] = None

        # Lite mode (LanceDB local file store) skips the Weaviate client entirely.
        # When VECTORWAVE_MODE=lite, _flush_batch_core delegates to the
        # configured VectorStore so no Docker / Weaviate connection is needed.
        self._lite_mode = os.environ.get("VECTORWAVE_MODE", "pro").lower() == "lite"

        # Store dynamic connection params
        self._host = host
        self._port = port
        self._grpc_port = grpc_port
        self._api_key = api_key

        # Batch Configuration
        self.batch_threshold = self.settings.BATCH_THRESHOLD
        self.flush_interval = self.settings.FLUSH_INTERVAL_SECONDS

        # Connect to DB
        self._connect_client()

        if USE_RUST_CORE:
            logger.info(f"🚀 [VectorWave] Rust Core Activated! (Threshold: {self.batch_threshold}, Interval: {self.flush_interval}s)")
            self._rust_manager = RustBatchManager(
                self._flush_batch_core,
                self.batch_threshold,
                int(self.flush_interval * 1000) # ms 단위 변환
            )
            self._worker_thread = None
        else:
            logger.warning("⚠️ [VectorWave] Rust Core not found. Using slower Python implementation.")
            # --- Legacy Python Implementation ---
            self.queue = queue.Queue(maxsize=10000)
            self._stop_event = threading.Event()
            self._start_python_worker()

        # Register shutdown handler
        atexit.register(self.shutdown)

    def _connect_client(self):
        """Attempts to connect to the configured backend (Weaviate or Lite store)."""
        if self._lite_mode:
            try:
                from ..store import get_vector_store
                store = get_vector_store()
                if store.is_ready():
                    self._initialized = True
                    self.client = None  # not used in Lite mode
            except Exception as e:
                logger.warning(f"Lite store init failed: {e}")
                self._initialized = False
            return

        try:
            if self._host is not None:
                self.client = get_weaviate_client(
                    host=self._host, port=self._port,
                    grpc_port=self._grpc_port, api_key=self._api_key
                )
            else:
                self.client = get_weaviate_client(self.settings)
            if self.client:
                self._initialized = True
        except Exception as e:
            logger.warning(f"Initial DB connection failed: {e}")
            self._initialized = False

    def _start_python_worker(self):
        """Starts the legacy Python background thread."""
        self._worker_thread = threading.Thread(target=self._python_worker_loop, daemon=True)
        self._worker_thread.start()

    def add_object(self, collection: str, properties: dict, uuid: str = None, vector: Optional[List[float]] = None):
        """
        [Public API] Adds an object to the batch queue.
        """
        if USE_RUST_CORE:

            self._rust_manager.add_object(collection, properties, uuid, vector)
        else:
            # Python Legacy Queue
            item = {
                "collection": collection,
                "properties": properties,
                "uuid": uuid,
                "vector": vector
            }
            try:
                self.queue.put_nowait(item)
            except queue.Full:
                logger.warning("🚨 VectorWave Log Queue is FULL. Dropping log.")

    def _flush_batch_core(self, items: List[Dict[str, Any]]):
        """
        The actual flush logic called by either Rust or Python worker.

        In Pro mode (default) this uses Weaviate's bulk batch.dynamic() context.
        In Lite mode it groups items by collection and calls
        VectorStore.insert_many — LanceDB has no equivalent of Weaviate's
        single-context bulk write, but per-collection batching is fine for the
        Lite use case.
        """
        if not items:
            return

        # 1. Check/Retry Connection
        if not self._initialized or (not self._lite_mode and not self.client):
            self._connect_client()
            if not self._initialized:
                return

        if self._lite_mode:
            self._flush_via_store(items)
            return

        # 2. Send Batch via Weaviate Client (Pro mode)
        try:
            # Weaviate v4 batch context
            with self.client.batch.dynamic() as batch:
                for item in items:
                    batch.add_object(
                        collection=item['collection'],
                        properties=item['properties'],
                        uuid=item.get('uuid'),
                        vector=item.get('vector')
                    )

            if len(self.client.batch.failed_objects) > 0:
                for failed in self.client.batch.failed_objects:
                    logger.error(f"⚠️ Batch Item Failed: {failed.message}")

        except RuntimeError:
            return
        except Exception as e:
            msg = str(e).lower()
            if "shutdown" in msg or "closed" in msg:
                return
            logger.error(f"❌ Batch Flush Error: {e}")

    def _flush_via_store(self, items: List[Dict[str, Any]]):
        """Lite-mode flush: route items through the VectorStore abstraction."""
        from ..store import get_vector_store
        try:
            store = get_vector_store()
        except Exception as e:
            logger.error(f"❌ Lite store unavailable: {e}")
            return
        by_collection: Dict[str, List[Dict[str, Any]]] = {}
        for item in items:
            by_collection.setdefault(item["collection"], []).append({
                "properties": item["properties"],
                "uuid": item.get("uuid"),
                "vector": item.get("vector"),
            })
        for collection, batch in by_collection.items():
            try:
                # Lite stores create tables lazily, but ensure the schema exists
                # so writes don't fail with "table not found".
                if not store.collection_exists(collection):
                    store.ensure_collection(collection, properties=[])
                store.insert_many(collection, batch)
            except Exception as e:
                logger.error(f"❌ Lite batch flush failed for '{collection}': {e}")

    # --- Legacy Python Worker Methods (Only used if Rust is missing) ---
    def _python_worker_loop(self):
        pending_items = []
        last_flush_time = time.time()

        while not self._stop_event.is_set():
            try:
                item = self.queue.get(timeout=0.5)
                pending_items.append(item)
            except queue.Empty:
                pass

            current_time = time.time()
            if len(pending_items) >= self.batch_threshold or (pending_items and current_time - last_flush_time >= self.flush_interval):
                self._flush_batch_core(pending_items)
                pending_items = []
                last_flush_time = current_time

    def shutdown(self):
        """Gracefully shuts down. Idempotent — repeated calls are no-ops, so the
        atexit handler firing after a test-time cache_clear cannot trigger a
        second shutdown on an already-closed Rust worker or Weaviate client."""
        if self._shutdown_done:
            return
        self._shutdown_done = True

        if USE_RUST_CORE:
            try:
                self._rust_manager.shutdown()
            except Exception as e:
                logger.debug(f"Rust manager shutdown raised: {e}")
        else:
            if not self._stop_event.is_set():
                self._stop_event.set()
                if self._worker_thread and self._worker_thread.is_alive():
                    self._worker_thread.join(timeout=1.0)

                # Flush remaining items
                remaining = []
                while not self.queue.empty():
                    remaining.append(self.queue.get_nowait())
                if remaining:
                    self._flush_batch_core(remaining)

        # Close client
        if self.client:
            try:
                self.client.close()
            except Exception:
                pass

@lru_cache()
def get_batch_manager(
        host: Optional[str] = None,
        port: Optional[int] = None,
        grpc_port: Optional[int] = None,
        api_key: Optional[str] = None
) -> WeaviateBatchManager:
    return WeaviateBatchManager(host=host, port=port, grpc_port=grpc_port, api_key=api_key)