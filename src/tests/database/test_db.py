import pytest
from unittest.mock import MagicMock, patch, ANY
import weaviate
import weaviate.classes.config as wvc
# Import the specific driver exception to mock it
from weaviate.exceptions import WeaviateConnectionError as WeaviateClientConnectionError

# Import functions to be tested
# (Assuming pytest is run from the project root and pytest.ini is set)
from vectorwave.database.db import get_weaviate_client, create_vectorwave_schema
from vectorwave.models.db_config import WeaviateSettings, get_weaviate_settings
from vectorwave.exception.exceptions import (
    WeaviateConnectionError,
    WeaviateNotReadyError,
    SchemaCreationError
)

from vectorwave.database.db import create_execution_schema


# --- Test Fixtures ---

@pytest.fixture
def test_settings() -> WeaviateSettings:
    """Returns a test Weaviate settings object."""
    return WeaviateSettings(
        WEAVIATE_HOST="test.host.local",
        WEAVIATE_PORT=1234,
        WEAVIATE_GRPC_PORT=5678,
        COLLECTION_NAME="TestCollection",
        IS_VECTORIZE_COLLECTION_NAME=False
    )

# --- Tests for get_weaviate_client ---

@patch('vectorwave.database.db.weaviate.connect_to_local')
def test_get_weaviate_client_success(mock_connect_to_local, test_settings):
    """
    Case 1: Weaviate connection is successful.
    - .connect_to_local() should be called.
    - .is_ready() should return True.
    - The created client object should be returned.
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_client.is_ready.return_value = True
    mock_connect_to_local.return_value = mock_client

    # 2. Act
    client = get_weaviate_client(settings=test_settings)

    # 3. Assert
    # Check if 'connect_to_local' was called once with the correct args
    mock_connect_to_local.assert_called_once_with(
        host=test_settings.WEAVIATE_HOST,
        port=test_settings.WEAVIATE_PORT,
        grpc_port=test_settings.WEAVIATE_GRPC_PORT,
        additional_config=ANY
    )
    mock_client.is_ready.assert_called_once()
    assert client == mock_client


@patch('vectorwave.database.db.weaviate.connect_to_local')
def test_get_weaviate_client_connection_refused(mock_connect_to_local, test_settings):
    """
    Case 2: Connection is refused because Weaviate server is down.
    - Should raise WeaviateConnectionError.
    """
    # 1. Arrange
    # Mock the original Weaviate driver exception
    mock_connect_to_local.side_effect = WeaviateClientConnectionError("Connection refused")

    # 2. Act & 3. Assert
    with pytest.raises(WeaviateConnectionError) as exc_info:
        get_weaviate_client(settings=test_settings)

    # Check if the error message from the original exception is included
    assert "Connection refused" in str(exc_info.value)
    assert "Failed to connect to Weaviate" in str(exc_info.value)


@patch('vectorwave.database.db.weaviate.connect_to_local')
def test_get_weaviate_client_not_ready(mock_connect_to_local, test_settings):
    """
    Case 3: Connected, but Weaviate is not ready (.is_ready() returns False).
    - Should raise WeaviateNotReadyError.
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_client.is_ready.return_value = False # This is the trigger
    mock_connect_to_local.return_value = mock_client

    # 2. Act & 3. Assert
    with pytest.raises(WeaviateNotReadyError) as exc_info:
        get_weaviate_client(settings=test_settings)

    assert "server is not ready" in str(exc_info.value)


# --- Tests for create_vectorwave_schema ---

def test_create_schema_new(test_settings):
    """
    Case 4: Schema doesn't exist and is created successfully.
    - .collections.exists() returns False.
    - .collections.create() should be called.
    - .collections.get() should not be called.
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False # Trigger for creation
    mock_new_collection = MagicMock()
    mock_collections.create.return_value = mock_new_collection
    mock_client.collections = mock_collections

    # 2. Act
    collection = create_vectorwave_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.exists.assert_called_once_with(test_settings.COLLECTION_NAME)
    mock_collections.create.assert_called_once()

    # Check if 'create' was called with the correct 'name'
    call_args = mock_collections.create.call_args
    assert call_args.kwargs.get('name') == test_settings.COLLECTION_NAME

    # Check if key properties were passed
    passed_props = [prop.name for prop in call_args.kwargs.get('properties', [])]
    assert "function_name" in passed_props
    assert "source_code" in passed_props

    mock_collections.get.assert_not_called()
    assert collection == mock_new_collection


def test_create_schema_existing(test_settings):
    """
    Case 5: Schema already exists.
    - .collections.exists() returns True.
    - .collections.create() should not be called.
    - .collections.get() should be called.
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = True # Trigger for skipping
    mock_existing_collection = MagicMock()
    mock_collections.get.return_value = mock_existing_collection
    mock_client.collections = mock_collections

    # 2. Act
    collection = create_vectorwave_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.exists.assert_called_once_with(test_settings.COLLECTION_NAME)
    mock_collections.create.assert_not_called()
    mock_collections.get.assert_called_once_with(test_settings.COLLECTION_NAME)
    assert collection == mock_existing_collection


def test_create_schema_creation_error(test_settings):
    """
    Case 6: An error occurs during schema creation (e.g., bad API key).
    - Should raise SchemaCreationError.
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    # Set .create() to raise an exception
    mock_collections.create.side_effect = Exception("Invalid OpenAI API Key")
    mock_client.collections = mock_collections

    # 2. Act & 3. Assert
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, test_settings)

    assert "Error during schema creation" in str(exc_info.value)
    assert "Invalid OpenAI API Key" in str(exc_info.value)


@pytest.fixture
def settings_with_custom_props() -> WeaviateSettings:
    """
    Returns a WeaviateSettings object assuming 'get_weaviate_settings'
    successfully loaded the JSON.
    """
    settings = WeaviateSettings(
        COLLECTION_NAME="TestCollection",
        IS_VECTORIZE_COLLECTION_NAME=False
    )
    # Manually inject the loaded data into the custom_properties field
    settings.custom_properties = {
        "run_id": {
            "data_type": "TEXT",
            "description": "The ID of the specific test run"
        },
        "experiment_id": {
            "data_type": "INT",
            "description": "Identifier for the experiment"
        }
    }
    return settings

@pytest.fixture
def settings_with_invalid_type_prop() -> WeaviateSettings:
    """Returns settings with an invalid string in 'data_type'."""
    settings = WeaviateSettings(COLLECTION_NAME="TestCollection")
    settings.custom_properties = {
        "bad_prop": {
            "data_type": "INVALID_WEAVIATE_TYPE", # <-- Invalid type
            "description": "This should fail"
        }
    }
    return settings

@pytest.fixture
def settings_with_missing_type_prop() -> WeaviateSettings:
    """Returns settings where the 'data_type' key itself is missing."""
    settings = WeaviateSettings(COLLECTION_NAME="TestCollection")
    settings.custom_properties = {
        "another_bad_prop": {
            "description": "data_type key is missing" # <-- data_type missing
        }
    }
    return settings



# [New] Test Cases for Issue #11: Custom Property *Parsing*

def test_create_schema_with_custom_properties(settings_with_custom_props):
    """
    Case 7: Test if custom properties (run_id, experiment_id) are correctly added to the schema
    - .collections.create() should be called with the correct 'properties' argument
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False # Trigger creation
    mock_new_collection = MagicMock()
    mock_collections.create.return_value = mock_new_collection
    mock_client.collections = mock_collections

    # 2. Act
    # Use the 'settings_with_custom_props' fixture
    create_vectorwave_schema(mock_client, settings_with_custom_props)

    # 3. Assert
    # Check if create was called
    mock_collections.create.assert_called_once()

    # Check the arguments (kwargs) passed to create
    call_args = mock_collections.create.call_args
    passed_props_list = call_args.kwargs.get('properties', [])

    # For convenience, convert the list to a map by name
    passed_props_map = {prop.name: prop for prop in passed_props_list}

    # Check if base properties still exist
    assert "function_name" in passed_props_map
    assert "source_code" in passed_props_map
    assert passed_props_map["function_name"].dataType == wvc.DataType.TEXT

    assert "search_description" in passed_props_map
    assert "sequence_narrative" in passed_props_map

    # --- Custom Property Validation ---
    assert "run_id" in passed_props_map
    assert "experiment_id" in passed_props_map


    # Validate 'run_id' type and description
    run_id_prop = passed_props_map["run_id"]
    assert run_id_prop.dataType == wvc.DataType.TEXT
    assert run_id_prop.description == "The ID of the specific test run"

    # Validate 'experiment_id' type and description
    exp_id_prop = passed_props_map["experiment_id"]
    assert exp_id_prop.dataType == wvc.DataType.INT
    assert exp_id_prop.description == "Identifier for the experiment"


    # Check total property count (5 base + 2 custom)
    assert len(passed_props_list) == 6 + 2


def test_create_schema_custom_prop_invalid_type(settings_with_invalid_type_prop):
    """
    Case 8: Test if SchemaCreationError is raised when 'data_type' has an invalid value
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    mock_client.collections = mock_collections

    # 2. Act & 3. Assert
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, settings_with_invalid_type_prop)

    # Check if the error message includes the invalid type name
    assert "Invalid data_type 'INVALID_WEAVIATE_TYPE'" in str(exc_info.value)
    assert "bad_prop" in str(exc_info.value)


def test_create_schema_custom_prop_missing_type(settings_with_missing_type_prop):
    """
    Case 9: Test if SchemaCreationError is raised when the 'data_type' key is missing
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    mock_client.collections = mock_collections

    # 2. Act & 3. Assert
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, settings_with_missing_type_prop)

    # Check if the error message indicates 'data_type' is missing
    assert "missing 'data_type'" in str(exc_info.value)
    assert "another_bad_prop" in str(exc_info.value)


@patch('vectorwave.database.db.wvc.Configure.Vectorizer.none')
def test_create_execution_schema_new(mock_vectorizer_none, test_settings):
    """
    Case 10: Test if 'VectorWaveExecutions' schema is created successfully when it does not exist
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False # 생성 트리거
    mock_new_collection = MagicMock()
    mock_collections.create.return_value = mock_new_collection
    mock_client.collections = mock_collections

    # 2. Act
    collection = create_execution_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.exists.assert_called_once_with(test_settings.EXECUTION_COLLECTION_NAME)
    mock_collections.create.assert_called_once()

    call_args = mock_collections.create.call_args
    assert call_args.kwargs.get('name') == test_settings.EXECUTION_COLLECTION_NAME

    # Check if base properties are included
    passed_props_map = {prop.name: prop for prop in call_args.kwargs.get('properties', [])}
    assert "function_uuid" in passed_props_map
    assert "timestamp_utc" in passed_props_map
    assert "status" in passed_props_map
    assert "duration_ms" in passed_props_map

    assert "return_value" in passed_props_map
    assert passed_props_map["return_value"].dataType == wvc.DataType.TEXT

    assert collection == mock_new_collection


def test_create_execution_schema_existing(test_settings):
    """
    Case 11: Test if creation is skipped when 'VectorWaveExecutions' schema already exists
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = True
    mock_existing_collection = MagicMock()
    mock_collections.get.return_value = mock_existing_collection
    mock_client.collections = mock_collections

    # 2. Act
    collection = create_execution_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.exists.assert_called_once_with(test_settings.EXECUTION_COLLECTION_NAME)
    mock_collections.create.assert_not_called() # create should not be called
    mock_collections.get.assert_called_once_with(test_settings.EXECUTION_COLLECTION_NAME) # get should be called
    assert collection == mock_existing_collection




def test_create_schema_vectorizer_openai(test_settings):
    """
    Case 12: Test if the correct dict config is passed when VECTORIZER_CONFIG='text2vec-openai'
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    mock_client.collections = mock_collections

    test_settings.VECTORIZER = "weaviate_module"
    test_settings.WEAVIATE_VECTORIZER_MODULE = "text2vec-openai"

    # [수정] test_settings.GENERATIVE_CONFIG = "generative-openai" ->
    test_settings.WEAVIATE_GENERATIVE_MODULE = "generative-openai"


    # 2. Act
    create_vectorwave_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.create.assert_called_once()

    call_args = mock_collections.create.call_args

    vector_config_arg = call_args.kwargs.get('vectorizer_config')
    # assert isinstance(vector_config_arg, wvc.Configure.Vectorizer)
    # assert vector_config_arg.name == "text2vec-openai"

    assert vector_config_arg.vectorizer == "text2vec-openai"

    # 3b. Generative Config 검증
    generative_config_arg = call_args.kwargs.get('generative_config')
    assert generative_config_arg.generative == "generative-openai"




def test_create_schema_vectorizer_none(test_settings):
    """
    Case 13: Test if the correct dict config is passed when VECTORIZER_CONFIG='none'
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    mock_client.collections = mock_collections

    test_settings.VECTORIZER = "none"
    test_settings.WEAVIATE_VECTORIZER_MODULE = ""

    # 2. Act
    create_vectorwave_schema(mock_client, test_settings)

    # 3. Assert
    mock_collections.create.assert_called_once()

    call_args = mock_collections.create.call_args
    # mock_none.assert_called_once() -> 삭제

    vector_config_arg = call_args.kwargs.get('vectorizer_config')


    assert vector_config_arg.vectorizer == "none"


def test_create_schema_vectorizer_invalid(test_settings):
    """
    Case 14: Test if SchemaCreationError is raised for an unsupported VECTORIZER_CONFIG value
    """
    # 1. Arrange
    mock_client = MagicMock(spec=weaviate.WeaviateClient)
    mock_collections = MagicMock()
    mock_collections.exists.return_value = False
    mock_client.collections = mock_collections

    test_settings.VECTORIZER = "unsupported-module"

    # 2. Act & 3. Assert
    with pytest.raises(SchemaCreationError) as exc_info:
        create_vectorwave_schema(mock_client, test_settings)

    assert "Invalid VECTORIZER setting" in str(exc_info.value)
    assert "unsupported-module" in str(exc_info.value)