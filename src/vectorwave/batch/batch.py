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

# Create module-level logger
logger = logging.getLogger(__name__)

class WeaviateBatchManager:
    """
    A singleton class that manages Weaviate batch imports using a local queue and a background worker.
    """

    def __init__(self):
        self._initialized = False
        logger.debug("Initializing WeaviateBatchManager with Background Worker")

        self.settings: WeaviateSettings = get_weaviate_settings()
        self.client: Optional[weaviate.WeaviateClient] = None

        # --- 1. Local Buffer Queue ---
        self.queue = queue.Queue(maxsize=10000)

        # --- 2. Batch Configuration ---
        self.batch_threshold = self.settings.BATCH_THRESHOLD
        self.flush_interval = self.settings.FLUSH_INTERVAL_SECONDS

        # --- 3. Control Flags ---
        self._stop_event = threading.Event()
        self._worker_thread = None

        # Connect to DB
        self._connect_client()

        # Start Background Worker
        self._start_worker()

        # Register shutdown handler
        atexit.register(self.shutdown)

    def _connect_client(self):
        """Attempts to connect to Weaviate."""
        try:
            self.client = get_weaviate_client(self.settings)
            if self.client:
                self._initialized = True
                logger.info("WeaviateBatchManager connected and worker started.")
        except Exception as e:
            logger.warning(f"Initial DB connection failed (will retry in worker): {e}")
            self._initialized = False

    def _start_worker(self):
        """Starts the background thread that consumes the queue."""
        self._worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        self._worker_thread.start()

    def _worker_loop(self):
        """
        Main loop for the background worker.
        Aggregates items from queue and flushes them periodically.
        """
        pending_items: List[Dict[str, Any]] = []
        last_flush_time = time.time()

        while not self._stop_event.is_set():
            try:
                # Non-blocking get or wait for a short time
                item = self.queue.get(timeout=0.5)
                pending_items.append(item)
            except queue.Empty:
                pass

            current_time = time.time()
            time_since_flush = current_time - last_flush_time

            # Flush conditions: Batch full OR Time interval passed
            if len(pending_items) >= self.batch_threshold or (pending_items and time_since_flush >= self.flush_interval):
                self._flush_batch(pending_items)
                pending_items = [] # Clear buffer
                last_flush_time = current_time

    def _flush_batch(self, items: List[Dict[str, Any]]):
        """
        Sends a list of items to Weaviate. Handles re-connection if needed.
        """
        if not items:
            return

        # 1. Check/Retry Connection
        if not self._initialized or not self.client:
            self._connect_client()
            if not self._initialized:
                # If still failing, just drop to avoid log spam
                return

        # 2. Send Batch
        try:
            # Weaviate v4 batch context
            with self.client.batch.dynamic() as batch:
                for item in items:
                    batch.add_object(
                        collection=item['collection'],
                        properties=item['properties'],
                        uuid=item['uuid'],
                        vector=item['vector']
                    )

            if len(self.client.batch.failed_objects) > 0:
                for failed in self.client.batch.failed_objects:
                    logger.error(f"⚠️ Batch Item Failed: {failed.message}")

        except RuntimeError:
            # [Silent Catch] Ignore "cannot schedule new futures after shutdown"
            return
        except Exception as e:
            # Suppress shutdown-related errors
            msg = str(e).lower()
            if "shutdown" in msg or "closed" in msg:
                return
            logger.error(f"❌ Batch Flush Error: {e}")

    def add_object(self, collection: str, properties: dict, uuid: str = None, vector: Optional[List[float]] = None):
        """
        [Non-blocking] Adds an object to the local in-memory queue.
        """
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

    def shutdown(self):
        """Gracefully shuts down. Called by atexit."""
        if not self._stop_event.is_set():
            # 1. Stop worker
            self._stop_event.set()

            if self._worker_thread and self._worker_thread.is_alive():
                self._worker_thread.join(timeout=1.0)

            # 2. Try to flush remaining items ONE LAST TIME, but ignore errors aggressively
            remaining_items = []
            try:
                while not self.queue.empty():
                    remaining_items.append(self.queue.get_nowait())
            except queue.Empty:
                pass

            if remaining_items:
                # If flushing fails due to shutdown, we catch and ignore it inside _flush_batch
                self._flush_batch(remaining_items)

            # 3. Close client safely - The most critical part for avoiding errors
            if self.client:
                try:
                    self.client.close()
                except (Exception, RuntimeError):
                    # Absorb ANY error during close (including 'schedule new futures')
                    pass


@lru_cache(None)
def get_batch_manager() -> WeaviateBatchManager:
    return WeaviateBatchManager()