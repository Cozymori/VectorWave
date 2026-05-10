import pytest
from unittest.mock import MagicMock, patch
import weaviate
from weaviate.exceptions import WeaviateConnectionError as WeaviateClientConnectionError

from vectorwave.database.db import (
    get_weaviate_client,
    create_vectorwave_schema,
    create_execution_schema,
)
from vectorwave.models.db_config import WeaviateSettings
from vectorwave.exception.exceptions import (
    WeaviateConnectionError,
    WeaviateNotReadyError,
    SchemaCreationError,
)


# ---------------------------------------------------------------------------
# Unit tests — failure paths and pure config validation (no real Weaviate)
# ---------------------------------------------------------------------------

@pytest.fixture
def unit_settings() -> WeaviateSettings:
    return WeaviateSettings(
        WEAVIATE_HOST="test.host.local",
        WEAVIATE_PORT=1234,
        WEAVIATE_GRPC_PORT=5678,
        COLLECTION_NAME="TestCollection",
        IS_VECTORIZE_COLLECTION_NAME=False,
    )


@patch('vectorwave.database.db.weaviate.connect_to_local')
def test_get_weaviate_client_connection_refused(mock_connect_to_local, unit_settings):
    """Connection refused at the driver level is wrapped as WeaviateConnectionError."""
    mock_connect_to_local.side_effect = WeaviateClientConnectionError("Connection refused")
    with pytest.raises(WeaviateConnectionError) as exc_info:
        get_weaviate_client(settings=unit_settings)
    assert "Connection refused" in str(exc_info.value)
    assert "Failed to connect to Weaviate" in str(exc_info.value)


@patch('vectorwave.database.db.weaviate.connect_to_local')
def test_get_weaviate_client_not_ready(mock_connect_to_local, unit_settings):
    """A connected-but-not-ready client raises WeaviateNotReadyError."""
    mock_client = MagicMock()
    mock_client.is_ready.return_value = False
    mock_connect_to_local.return_value = mock_client
    with pytest.raises(WeaviateNotReadyError) as exc_info:
        get_weaviate_client(settings=unit_settings)
    assert "server is not ready" in str(exc_info.value)


def test_create_schema_creation_error_is_wrapped(unit_settings):
    """An exception from client.collections.create surfaces as SchemaCreationError."""
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    mock_client.collections.create.side_effect = Exception("Invalid OpenAI API Key")
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, unit_settings)
    assert "Error creating collection" in str(exc_info.value)
    assert "Invalid OpenAI API Key" in str(exc_info.value)


def test_create_schema_custom_prop_invalid_data_type():
    """Invalid `data_type` in custom_properties raises SchemaCreationError before any DB call."""
    settings = WeaviateSettings(COLLECTION_NAME="TestCollection")
    settings.custom_properties = {
        "bad_prop": {"data_type": "INVALID_WEAVIATE_TYPE", "description": "x"}
    }
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, settings)
    assert "Invalid data_type 'INVALID_WEAVIATE_TYPE'" in str(exc_info.value)
    assert "bad_prop" in str(exc_info.value)


def test_create_schema_custom_prop_missing_data_type():
    """A custom_properties entry without `data_type` raises SchemaCreationError."""
    settings = WeaviateSettings(COLLECTION_NAME="TestCollection")
    settings.custom_properties = {
        "another_bad_prop": {"description": "data_type key is missing"}
    }
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, settings)
    assert "missing 'data_type'" in str(exc_info.value)
    assert "another_bad_prop" in str(exc_info.value)


def test_create_schema_invalid_vectorizer_setting(unit_settings):
    """An unsupported VECTORIZER value raises SchemaCreationError."""
    mock_client = MagicMock()
    mock_client.collections.exists.return_value = False
    unit_settings.VECTORIZER = "unsupported-module"
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, unit_settings)
    assert "Invalid VECTORIZER setting" in str(exc_info.value)
    assert "unsupported-module" in str(exc_info.value)


# ---------------------------------------------------------------------------
# E2E tests — exercise real Weaviate via the testcontainer fixture
# ---------------------------------------------------------------------------

@pytest.fixture
def e2e_settings() -> WeaviateSettings:
    """Settings pointing at the testcontainer (host/port from env)."""
    s = WeaviateSettings()
    s.IS_VECTORIZE_COLLECTION_NAME = False
    s.VECTORIZER = "none"
    return s


@pytest.mark.e2e
def test_get_weaviate_client_success(weaviate_container):
    client = get_weaviate_client()
    try:
        assert client.is_ready()
    finally:
        client.close()


@pytest.mark.e2e
def test_create_vectorwave_schema_creates_expected_properties(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.COLLECTION_NAME)
        prop_names = {p.name for p in coll.config.get().properties}
        assert {"function_name", "source_code", "search_description", "sequence_narrative"} <= prop_names
    finally:
        client.close()


@pytest.mark.e2e
def test_create_vectorwave_schema_is_idempotent(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        create_vectorwave_schema(client, e2e_settings)
        assert client.collections.exists(e2e_settings.COLLECTION_NAME)
    finally:
        client.close()


@pytest.mark.e2e
def test_create_vectorwave_schema_with_custom_properties(clean_weaviate, e2e_settings):
    e2e_settings.custom_properties = {
        "run_id": {"data_type": "TEXT", "description": "The ID of the specific test run"},
        "experiment_id": {"data_type": "INT", "description": "Identifier for the experiment"},
    }
    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.COLLECTION_NAME)
        props = {p.name for p in coll.config.get().properties}
        assert "run_id" in props
        assert "experiment_id" in props
    finally:
        client.close()


@pytest.mark.e2e
def test_create_execution_schema_creates_expected_properties(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        prop_names = {p.name for p in coll.config.get().properties}
        assert {"function_uuid", "timestamp_utc", "status", "duration_ms", "return_value"} <= prop_names
    finally:
        client.close()


@pytest.mark.e2e
def test_create_execution_schema_is_idempotent(clean_weaviate, e2e_settings):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        create_execution_schema(client, e2e_settings)
        assert client.collections.exists(e2e_settings.EXECUTION_COLLECTION_NAME)
    finally:
        client.close()


@pytest.mark.e2e
def test_schema_accepts_text2vec_openai_config(clean_weaviate, e2e_settings):
    """Weaviate accepts the text2vec-openai vectorizer + generative-openai config we build."""
    e2e_settings.VECTORIZER = "weaviate_module"
    e2e_settings.WEAVIATE_VECTORIZER_MODULE = "text2vec-openai"
    e2e_settings.WEAVIATE_GENERATIVE_MODULE = "generative-openai"
    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        assert client.collections.exists(e2e_settings.COLLECTION_NAME)
    finally:
        client.close()


@pytest.mark.e2e
def test_schema_accepts_vectorizer_none(clean_weaviate, e2e_settings):
    e2e_settings.VECTORIZER = "none"
    client = get_weaviate_client()
    try:
        create_vectorwave_schema(client, e2e_settings)
        assert client.collections.exists(e2e_settings.COLLECTION_NAME)
    finally:
        client.close()
