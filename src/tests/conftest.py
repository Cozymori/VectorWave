import pytest
import os
import logging
from vectorwave.batch.batch import get_batch_manager
import time

CACHE_FILE_PATH = ".vectorwave_functions_cache.json"
WEAVIATE_IMAGE = "semitechnologies/weaviate:1.28.4"

# Our session fixture explicitly stops the container in its finally, so the
# testcontainers Reaper (Ryuk) is redundant — and it's been racy on macOS docker
# desktop. Disable it before any testcontainers imports happen.
os.environ.setdefault("TESTCONTAINERS_RYUK_DISABLED", "true")
WEAVIATE_READY_TIMEOUT = 60
VECTORWAVE_COLLECTIONS = (
    "VectorWaveFunctions",
    "VectorWaveExecutions",
    "VectorWaveGoldenDataset",
    "VectorWaveTokenUsage",
)
logger = logging.getLogger(__name__)


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "e2e: requires a real Weaviate instance (provided by the weaviate_container fixture)",
    )


def _delete_cache():
    """Helper function to remove the cache file if it exists."""
    if os.path.exists(CACHE_FILE_PATH):
        try:
            os.remove(CACHE_FILE_PATH)
        except OSError as e:
            print(f"\n[CacheFixture Error] Failed to remove {CACHE_FILE_PATH}: {e}")


@pytest.fixture(autouse=True, scope="function")
def atomic_function_cache():
    # --- SETUP (Before Test) ---
    _delete_cache()

    # --- Run the test ---
    yield

    # --- TEARDOWN (After Test) ---
    _delete_cache()


@pytest.fixture(scope="session", autouse=True)
def shutdown_batch_manager():
    yield
    try:
        manager = get_batch_manager()
        if hasattr(manager, "shutdown"):
            manager.shutdown()
            time.sleep(0.1)
    except Exception as e:
        print(f"Cleanup error: {e}")


def _clear_vectorwave_singletons():
    """Reset @lru_cache singletons so they pick up newly-set env vars."""
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    from vectorwave.batch.batch import get_batch_manager as _gbm
    for fn in (get_weaviate_settings, get_cached_client, get_vectorizer, _gbm):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture(scope="session")
def weaviate_container():
    """
    Session-scoped Weaviate container started via testcontainers.

    Tests that need a real Weaviate instance request this fixture by name.
    Boots once per pytest session, sets WEAVIATE_* env vars, and clears
    VectorWave's @lru_cache singletons so they pick up the new connection.
    """
    try:
        from testcontainers.core.container import DockerContainer
        from testcontainers.core.waiting_utils import wait_for_logs
    except ImportError:
        pytest.skip("testcontainers not installed (pip install 'vectorwave[dev]')")

    container = (
        DockerContainer(WEAVIATE_IMAGE)
        .with_env("AUTHENTICATION_ANONYMOUS_ENABLED", "true")
        .with_env("PERSISTENCE_DATA_PATH", "/var/lib/weaviate")
        .with_env("CLUSTER_HOSTNAME", "node1")
        .with_env("DEFAULT_VECTORIZER_MODULE", "none")
        .with_env("ENABLE_MODULES", "text2vec-openai,generative-openai")
        .with_env("QUERY_DEFAULTS_LIMIT", "25")
        .with_exposed_ports(8080, 50051)
    )
    container.start()
    wait_for_logs(container, "Serving weaviate", timeout=WEAVIATE_READY_TIMEOUT)

    host = container.get_container_host_ip()
    http_port = int(container.get_exposed_port(8080))
    grpc_port = int(container.get_exposed_port(50051))

    # The "Serving weaviate" log line fires before the gRPC server is fully
    # accepting traffic; poll the readiness endpoint to avoid a race on the
    # first connection attempt.
    import urllib.error
    import urllib.request
    ready_url = f"http://{host}:{http_port}/v1/.well-known/ready"
    deadline = time.time() + 30
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(ready_url, timeout=1) as resp:
                if resp.status == 200:
                    break
        except (urllib.error.URLError, ConnectionError, TimeoutError):
            pass
        time.sleep(0.5)

    saved_env = {
        k: os.environ.get(k)
        for k in (
            "WEAVIATE_HOST",
            "WEAVIATE_PORT",
            "WEAVIATE_GRPC_PORT",
            "WEAVIATE_API_KEY",
            "VECTORIZER",
            "IS_VECTORIZE_COLLECTION_NAME",
        )
    }
    os.environ["WEAVIATE_HOST"] = host
    os.environ["WEAVIATE_PORT"] = str(http_port)
    os.environ["WEAVIATE_GRPC_PORT"] = str(grpc_port)
    os.environ.pop("WEAVIATE_API_KEY", None)
    os.environ["VECTORIZER"] = "none"
    os.environ["IS_VECTORIZE_COLLECTION_NAME"] = "False"
    _clear_vectorwave_singletons()

    try:
        yield {"host": host, "port": http_port, "grpc_port": grpc_port}
    finally:
        for k, v in saved_env.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        _clear_vectorwave_singletons()
        container.stop()


@pytest.fixture
def clean_weaviate(weaviate_container):
    """Function-scoped fixture: deletes VectorWave collections before and after each test."""
    from vectorwave.database.db import get_weaviate_client

    def _wipe():
        client = get_weaviate_client()
        try:
            for name in VECTORWAVE_COLLECTIONS:
                if client.collections.exists(name):
                    client.collections.delete(name)
        finally:
            client.close()

    _wipe()
    try:
        yield weaviate_container
    finally:
        _wipe()
