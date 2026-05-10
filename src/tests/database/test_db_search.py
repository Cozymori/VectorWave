import pytest
from datetime import datetime, timezone
from uuid import uuid4

from vectorwave.database.db_search import (
    search_functions,
    search_executions,
    _build_weaviate_filters,
)
from vectorwave.database.db import (
    get_weaviate_client,
    create_vectorwave_schema,
    create_execution_schema,
)
from vectorwave.models.db_config import WeaviateSettings


# ---------------------------------------------------------------------------
# Unit tests — pure filter-builder logic (no Weaviate involved)
# ---------------------------------------------------------------------------

def test_build_filters_none_or_empty():
    assert _build_weaviate_filters(None) is None
    assert _build_weaviate_filters({}) is None


def test_build_filters_single_property():
    assert _build_weaviate_filters({"team": "billing"}) is not None


def test_build_filters_multiple_properties():
    assert _build_weaviate_filters({"team": "billing", "priority": 1}) is not None


def test_build_filters_supports_operators():
    """Operators encoded in the key (e.g. duration_ms__gte) build without error."""
    assert _build_weaviate_filters({"duration_ms__gte": 100}) is not None
    assert _build_weaviate_filters({"status__not_equal": "FAILURE"}) is not None
    assert _build_weaviate_filters({"function_name__like": "foo"}) is not None


def test_build_filters_rejects_unsafe_property_names():
    """Filter keys outside [A-Za-z_][A-Za-z0-9_]* must be skipped, not forwarded
    to Filter.by_property where they could target arbitrary identifiers."""
    # Hyphens, dots, spaces, leading digits — all outside the safe pattern.
    # Each filter has only the unsafe key, so the whole result should be None.
    assert _build_weaviate_filters({"bad-key": "x"}) is None
    assert _build_weaviate_filters({"some.field": "x"}) is None
    assert _build_weaviate_filters({"with space": "x"}) is None
    assert _build_weaviate_filters({"1leadingdigit": "x"}) is None
    # Mixed: the safe key still produces a filter, the unsafe one is dropped.
    assert _build_weaviate_filters({"status": "ok", "bad-key": "x"}) is not None


# ---------------------------------------------------------------------------
# E2E fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def e2e_settings() -> WeaviateSettings:
    s = WeaviateSettings()
    s.IS_VECTORIZE_COLLECTION_NAME = False
    s.VECTORIZER = "none"
    return s


@pytest.fixture
def hf_vectorizer_env(monkeypatch):
    """Switch the process to the local HuggingFace vectorizer for search-by-text tests.

    This avoids any dependency on OpenAI keys: embeddings are computed locally with
    sentence-transformers, then passed to Weaviate's near_vector path.
    """
    from vectorwave.models.db_config import get_weaviate_settings
    from vectorwave.vectorizer.factory import get_vectorizer

    monkeypatch.setenv("VECTORIZER", "huggingface")
    get_weaviate_settings.cache_clear()
    get_vectorizer.cache_clear()
    try:
        yield
    finally:
        monkeypatch.setenv("VECTORIZER", "none")
        get_weaviate_settings.cache_clear()
        get_vectorizer.cache_clear()


def _seed_executions(client, settings, rows):
    """Insert (function_name, status, duration_ms) rows into the executions collection."""
    coll = client.collections.get(settings.EXECUTION_COLLECTION_NAME)
    for r in rows:
        coll.data.insert(properties={
            "function_uuid": str(uuid4()),
            "function_name": r["function_name"],
            "status": r["status"],
            "duration_ms": r["duration_ms"],
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        })


# ---------------------------------------------------------------------------
# E2E — search_executions (no vectorizer needed; uses fetch_objects)
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_search_executions_filter_by_status(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        _seed_executions(client, e2e_settings, [
            {"function_name": "a", "status": "SUCCESS", "duration_ms": 100},
            {"function_name": "b", "status": "FAILURE", "duration_ms": 50},
            {"function_name": "c", "status": "SUCCESS", "duration_ms": 200},
        ])

        all_rows = search_executions(limit=10)
        assert len(all_rows) == 3

        success_only = search_executions(limit=10, filters={"status": "SUCCESS"})
        assert len(success_only) == 2
        assert all(r["status"] == "SUCCESS" for r in success_only)
    finally:
        client.close()


@pytest.mark.e2e
def test_search_executions_sort_by_duration_descending(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        _seed_executions(client, e2e_settings, [
            {"function_name": "a", "status": "SUCCESS", "duration_ms": 50},
            {"function_name": "b", "status": "SUCCESS", "duration_ms": 200},
            {"function_name": "c", "status": "SUCCESS", "duration_ms": 100},
        ])

        results = search_executions(limit=10, sort_by="duration_ms", sort_ascending=False)
        durations = [r["duration_ms"] for r in results]
        assert durations == sorted(durations, reverse=True)
    finally:
        client.close()


@pytest.mark.e2e
def test_search_executions_limit_is_respected(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        _seed_executions(client, e2e_settings, [
            {"function_name": f"f{i}", "status": "SUCCESS", "duration_ms": i}
            for i in range(5)
        ])

        results = search_executions(limit=2)
        assert len(results) == 2
    finally:
        client.close()


# ---------------------------------------------------------------------------
# E2E — search_functions (uses HuggingFace vectorizer for near_vector path)
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_search_functions_returns_semantically_closest_match(
    clean_weaviate, e2e_settings, hf_vectorizer_env
):
    """Seed two semantically distinct functions, verify the closer one ranks first."""
    from vectorwave.vectorizer.factory import get_vectorizer

    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.COLLECTION_NAME)
        vectorizer = get_vectorizer()
        assert vectorizer is not None

        text_a = "calculate the total price including sales tax for an order"
        text_b = "render a react component into the dom tree"
        coll.data.insert(
            properties={
                "function_name": "calc_tax",
                "source_code": text_a,
                "search_description": text_a,
            },
            vector=vectorizer.embed(text_a),
        )
        coll.data.insert(
            properties={
                "function_name": "render_dom",
                "source_code": text_b,
                "search_description": text_b,
            },
            vector=vectorizer.embed(text_b),
        )

        results = search_functions(query="how do I compute sales tax?", limit=2)
        assert len(results) >= 1
        assert results[0]["properties"]["function_name"] == "calc_tax"
    finally:
        client.close()


@pytest.mark.e2e
def test_search_functions_respects_filter(clean_weaviate, e2e_settings, hf_vectorizer_env):
    """Filter narrows the result set to entries matching the property."""
    from vectorwave.vectorizer.factory import get_vectorizer

    e2e_settings.custom_properties = {
        "team": {"data_type": "TEXT", "description": "team owner"},
    }

    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.COLLECTION_NAME)
        vectorizer = get_vectorizer()

        for name, team in [("billing_calc", "billing"), ("auth_check", "auth")]:
            text = f"function for {team}"
            coll.data.insert(
                properties={
                    "function_name": name,
                    "source_code": text,
                    "search_description": text,
                    "team": team,
                },
                vector=vectorizer.embed(text),
            )

        results = search_functions(query="anything", limit=10, filters={"team": "billing"})
        assert len(results) == 1
        assert results[0]["properties"]["function_name"] == "billing_calc"
    finally:
        client.close()
