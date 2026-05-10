"""End-to-end test for auto-metadata generation (`@vectorize(auto=True)`).

Verifies the full flow: a function decorated with `auto=True` is queued in
PENDING_FUNCTIONS without an immediate DB write; calling
`generate_and_register_metadata()` then asks the LLM to produce a description
and narrative (replayed from a VCR cassette), and the resulting row lands in
VectorWaveFunctions.
"""
import time

import pytest

from vectorwave.core.decorator import vectorize, PENDING_FUNCTIONS
from vectorwave.core.generator import generate_and_register_metadata
from vectorwave.database.db import (
    create_vectorwave_schema,
    get_weaviate_client,
)


def _wait_for_count(coll, expected: int, timeout: float = 8.0) -> int:
    deadline = time.time() + timeout
    last = -1
    while time.time() < deadline:
        last = len(coll.query.fetch_objects(limit=200).objects)
        if last >= expected:
            return last
        time.sleep(0.1)
    raise AssertionError(f"expected at least {expected} rows, got {last}")


def _clear_caches():
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.batch.batch import get_batch_manager
    from vectorwave.database.db import get_cached_client
    from vectorwave.vectorizer.factory import get_vectorizer
    from vectorwave.core.llm.factory import get_llm_client
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client, get_vectorizer, get_llm_client):
        if hasattr(fn, "cache_clear"):
            fn.cache_clear()


@pytest.fixture
def auto_metadata_env(weaviate_container, monkeypatch):
    """E2E setup: schema + fast batch flush + OpenAI key for the LLM client."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-cassette-replay")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    PENDING_FUNCTIONS.clear()
    _clear_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    try:
        if client.collections.exists(settings.COLLECTION_NAME):
            client.collections.delete(settings.COLLECTION_NAME)
        create_vectorwave_schema(client, settings)
    finally:
        client.close()

    yield settings
    PENDING_FUNCTIONS.clear()
    _clear_caches()


@pytest.mark.e2e
@pytest.mark.vcr
def test_auto_metadata_generates_description_and_registers(auto_metadata_env):
    settings = auto_metadata_env

    @vectorize(auto=True)
    def add_two_numbers(a, b):
        return a + b

    # auto=True defers DB write — PENDING_FUNCTIONS holds the entry until generate_* runs.
    assert any(item["func_name"] == "add_two_numbers" for item in PENDING_FUNCTIONS)

    generate_and_register_metadata()

    assert PENDING_FUNCTIONS == []

    client = get_weaviate_client(settings)
    try:
        coll = client.collections.get(settings.COLLECTION_NAME)
        _wait_for_count(coll, 1)
        objs = list(coll.iterator())
        assert len(objs) == 1
        props = objs[0].properties
        assert props["function_name"] == "add_two_numbers"
        assert props["search_description"] == "Adds two numbers and returns the sum."
        assert props["sequence_narrative"] == "Receives two integers a and b, returns a + b."
    finally:
        client.close()
