"""Smoke test for the testcontainers-backed weaviate_container fixture.

Verifies that the session-scoped fixture spins up a real Weaviate instance,
the connection helpers pick up the dynamic host/port via env vars, and the
clean_weaviate fixture wipes collections between tests.
"""
import pytest


@pytest.mark.e2e
def test_weaviate_container_is_reachable(weaviate_container):
    from vectorwave.database.db import get_weaviate_client

    client = get_weaviate_client()
    try:
        assert client.is_ready()
    finally:
        client.close()


@pytest.mark.e2e
def test_clean_weaviate_creates_and_wipes(clean_weaviate):
    from vectorwave.database.db import get_weaviate_client
    import weaviate.classes.config as wvc

    client = get_weaviate_client()
    try:
        client.collections.create(
            name="VectorWaveFunctions",
            properties=[wvc.Property(name="dummy", data_type=wvc.DataType.TEXT)],
            vectorizer_config=wvc.Configure.Vectorizer.none(),
        )
        assert client.collections.exists("VectorWaveFunctions")
    finally:
        client.close()


@pytest.mark.e2e
def test_clean_weaviate_isolates_between_tests(clean_weaviate):
    from vectorwave.database.db import get_weaviate_client

    client = get_weaviate_client()
    try:
        assert not client.collections.exists("VectorWaveFunctions"), (
            "previous test's collection leaked — clean_weaviate teardown failed"
        )
    finally:
        client.close()
