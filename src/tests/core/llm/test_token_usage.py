"""End-to-end tests for VectorWaveOpenAIClient using VCR cassettes.

Real OpenAI HTTP traffic is replayed from YAML cassettes committed alongside
the test, so the suite runs without an API key. Re-record by setting
OPENAI_API_KEY in the env and running pytest with `--record-mode=once`.
The session-level vcr_config in conftest strips Authorization-style headers
before write, so cassettes never contain secrets.
"""
import time

import pytest

from vectorwave.core.llm.openai_client import VectorWaveOpenAIClient
from vectorwave.database.db import (
    create_usage_schema,
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
    for fn in (get_weaviate_settings, get_batch_manager, get_cached_client):
        fn.cache_clear()


@pytest.fixture
def openai_client_e2e(weaviate_container, monkeypatch):
    """Configure a fast-flushing batch manager and ensure VectorWaveTokenUsage exists."""
    monkeypatch.setenv("OPENAI_API_KEY", "sk-test-cassette-replay")
    monkeypatch.setenv("BATCH_THRESHOLD", "1")
    monkeypatch.setenv("FLUSH_INTERVAL_SECONDS", "0.1")
    _clear_caches()

    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()

    client = get_weaviate_client(settings)
    try:
        if client.collections.exists("VectorWaveTokenUsage"):
            client.collections.delete("VectorWaveTokenUsage")
        create_usage_schema(client, settings)
    finally:
        client.close()

    yield settings
    _clear_caches()


@pytest.mark.e2e
@pytest.mark.vcr
def test_chat_completion_returns_content_and_logs_tokens(openai_client_e2e):
    """A real OpenAI chat-completion request (replayed from a cassette) returns
    the assistant message and writes a token-usage row to Weaviate."""
    settings = openai_client_e2e
    llm = VectorWaveOpenAIClient()

    response = llm.create_chat_completion(
        messages=[{"role": "user", "content": "Say hello in one word."}],
        model="gpt-4o-mini",
        temperature=0.0,
        category="cassette_chat",
    )

    assert response == "Hello"

    client = get_weaviate_client(settings)
    try:
        usage = client.collections.get("VectorWaveTokenUsage")
        _wait_for_count(usage, 1)
        objs = list(usage.iterator())
        assert len(objs) == 1
        props = objs[0].properties
        assert props["tokens"] == 16
        assert props["model"] == "gpt-4o-mini"
        assert props["category"] == "cassette_chat"
        assert props["usage_type"] == "generation"
    finally:
        client.close()


@pytest.mark.e2e
@pytest.mark.vcr
def test_create_embedding_returns_vector_and_logs_tokens(openai_client_e2e):
    """An embedding request returns the vector and writes its token usage."""
    settings = openai_client_e2e
    llm = VectorWaveOpenAIClient()

    vector = llm.create_embedding(
        text="Test text for embedding",
        model="text-embedding-3-small",
        category="cassette_embed",
    )

    assert vector == [0.1, 0.2, 0.3, 0.4, 0.5]

    client = get_weaviate_client(settings)
    try:
        usage = client.collections.get("VectorWaveTokenUsage")
        _wait_for_count(usage, 1)
        props = list(usage.iterator())[0].properties
        assert props["tokens"] == 5
        assert props["category"] == "cassette_embed"
        assert props["usage_type"] == "embedding"
    finally:
        client.close()
