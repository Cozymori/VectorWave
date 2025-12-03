import logging
from functools import lru_cache

import weaviate
import weaviate.classes.config as wvc  # (wvc = Weaviate Classes Config)
from vectorwave.exception.exceptions import (
    WeaviateConnectionError,
    WeaviateNotReadyError,
    SchemaCreationError
)
from vectorwave.models.db_config import WeaviateSettings
from vectorwave.models.db_config import get_weaviate_settings
from weaviate.config import AdditionalConfig
from weaviate.exceptions import WeaviateConnectionError as WeaviateClientConnectionError

# Create module-level logger
logger = logging.getLogger(__name__)


# Code based on Weaviate v4 (latest) client.

def get_weaviate_client(settings: WeaviateSettings) -> weaviate.WeaviateClient:
    """
    Creates and returns a Weaviate client.

    [Raises]
    - WeaviateConnectionError: If connection fails.
    - WeaviateNotReadyError: If connected, but the server is not ready.
    """

    client: weaviate.WeaviateClient

    try:
        client = weaviate.connect_to_local(
            host=settings.WEAVIATE_HOST,
            port=settings.WEAVIATE_PORT,
            grpc_port=settings.WEAVIATE_GRPC_PORT,
            additional_config=AdditionalConfig(
                dynamic=True,
                batch_size=20,
                timeout_retries=3
            )
        )
    except WeaviateClientConnectionError as e:
        raise WeaviateConnectionError(f"Failed to connect to Weaviate: {e}")
    except Exception as e:
        raise WeaviateConnectionError(f"An unknown error occurred while connecting to Weaviate: {e}")

    if not client.is_ready():
        raise WeaviateNotReadyError("Connected to Weaviate, but the server is not ready.")

    logger.info("Weaviate client connected successfully")
    return client


@lru_cache()
def get_cached_client() -> weaviate.WeaviateClient:
    """
    Singleton factory: Gets settings and returns a single client instance.
    This function IS cached.
    """
    logger.debug("Creating and caching new Weaviate client instance")
    settings = get_weaviate_settings()
    client = get_weaviate_client(settings)
    return client


def create_vectorwave_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    """
    Defines and creates the VectorWaveFunctions collection schema.
    Now includes custom properties loaded from the settings (via .weaviate_properties file).

    [Raises]
    - SchemaCreationError: If an error occurs during schema creation.
    """
    collection_name = settings.COLLECTION_NAME

    # 1. Check if the collection already exists
    if client.collections.exists(collection_name):
        logger.info("Collection '%s' already exists, skipping creation", collection_name)
        return client.collections.get(collection_name)

    # 2. If it doesn't exist, define and create the collection
    logger.info("Creating collection '%s'", collection_name)

    # 3. Define Base Properties
    base_properties = [
        wvc.Property(
            name="function_name",
            data_type=wvc.DataType.TEXT,
            description="The name of the vectorized function"
        ),
        wvc.Property(
            name="module_name",
            data_type=wvc.DataType.TEXT,
            description="The Python module path where the function is defined"
        ),
        wvc.Property(
            name="docstring",
            data_type=wvc.DataType.TEXT,
            description="The function's Docstring (description)"
        ),
        wvc.Property(
            name="source_code",
            data_type=wvc.DataType.TEXT,
            description="The actual source code of the function"
        ),
        wvc.Property(
            name="search_description",
            data_type=wvc.DataType.TEXT,
            description="User-provided description for similarity search (from @vectorize)"
        ),
        wvc.Property(
            name="sequence_narrative",
            data_type=wvc.DataType.TEXT,
            description="User-provided context about what happens next (from @vectorize)"
        ),
    ]

    # 4. Parse Custom Properties (loaded from JSON file via settings object)
    custom_properties = []
    if settings.custom_properties:
        logger.info(
            "Adding %d custom properties to '%s': %s",
            len(settings.custom_properties),
            collection_name,
            list(settings.custom_properties.keys())
        )

        for name, prop_details in settings.custom_properties.items():
            if not isinstance(prop_details, dict):
                raise SchemaCreationError(f"Custom property '{name}' in config file must be a dictionary.")

            # Get data_type (Required)
            dtype_str = prop_details.get("data_type")
            if not dtype_str:
                raise SchemaCreationError(f"Custom property '{name}' in config file is missing 'data_type'.")

            # Get description (Optional)
            description = prop_details.get("description")

            try:
                # Convert string (e.g., "TEXT") to Weaviate Enum (wvc.DataType.TEXT)
                data_type = getattr(wvc.DataType, dtype_str.upper())

                custom_properties.append(
                    wvc.Property(
                        name=name,
                        data_type=data_type,
                        description=description
                    )
                )
            except AttributeError:
                raise SchemaCreationError(
                    f"Invalid data_type '{dtype_str}' for custom property '{name}'. "
                    f"Use a valid wvc.DataType string (e.g., 'TEXT', 'INT', 'NUMBER')."
                )
            except Exception as e:
                raise SchemaCreationError(f"Error processing custom property '{name}': {e}")

    # 5. Combine properties
    all_properties = base_properties + custom_properties

    vector_config = None
    vectorizer_name_setting = settings.VECTORIZER.lower()

    logger.info("Configuring vectorizer: %s", vectorizer_name_setting)

    if vectorizer_name_setting == "huggingface" or vectorizer_name_setting == "openai_client":
        print(f"Python-based vectorizer ('{vectorizer_name_setting}') is active.")
        print("Setting Weaviate schema vectorizer to 'none'.")
        vector_config = wvc.Configure.Vectorizer.none()

    elif vectorizer_name_setting == "weaviate_module":
        module_name = settings.WEAVIATE_VECTORIZER_MODULE.lower()
        print(f"Using Weaviate internal module: '{module_name}'")

        if module_name == "text2vec-openai":
            vector_config = wvc.Configure.Vectorizer.text2vec_openai(
                vectorize_collection_name=settings.IS_VECTORIZE_COLLECTION_NAME
            )
        # (필요시 다른 Weaviate 모듈도 여기에 추가)
        else:
            raise SchemaCreationError(
                f"Unsupported WEAVIATE_VECTORIZER_MODULE: '{module_name}'.")

    elif vectorizer_name_setting == "none":
        # 벡터화 비활성화
        print("Vectorizer is set to 'none'.")
        vector_config = wvc.Configure.Vectorizer.none()

    else:
        raise SchemaCreationError(
            f"Invalid VECTORIZER setting: '{vectorizer_name_setting}'.")

    generative_config = None
    if settings.WEAVIATE_GENERATIVE_MODULE.lower() == "generative-openai":
        generative_config = wvc.Configure.Generative.openai()

    try:
        vectorwave_collection = client.collections.create(
            name=collection_name,
            properties=all_properties,

            # 7. Vectorizer Configuration
            vectorizer_config=vector_config,

            # 8. Generative Configuration (for RAG, etc.)
            generative_config=generative_config
        )
        return vectorwave_collection

    except Exception as e:
        # Raise a specific exception instead of returning None
        raise SchemaCreationError(f"Error during schema creation: {e}")


def create_execution_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    """
    Defines and creates the VectorWaveExecutions (dynamic) collection schema.
    """
    collection_name = settings.EXECUTION_COLLECTION_NAME

    if client.collections.exists(collection_name):
        logger.info("Collection '%s' already exists, skipping creation", collection_name)
        return client.collections.get(collection_name)

    logger.info("Creating collection '%s'", collection_name)

    properties = [
        wvc.Property(
            name="trace_id",
            data_type=wvc.DataType.TEXT,
            description="The unique ID for the entire trace/workflow"
        ),
        wvc.Property(
            name="span_id",
            data_type=wvc.DataType.TEXT,
            description="The unique ID for this specific span/function execution"
        ),
        wvc.Property(
            name="parent_span_id",
            data_type=wvc.DataType.TEXT,
            description="The span_id of the parent function that called this span"
        ),
        wvc.Property(
            name="function_name",
            data_type=wvc.DataType.TEXT,
            description="Name of the executed function (span name)"
        ),
        wvc.Property(
            name="function_uuid",
            data_type=wvc.DataType.UUID,
            description="The UUID of the executed function definition"
        ),
        wvc.Property(
            name="timestamp_utc",
            data_type=wvc.DataType.DATE,
            description="The UTC timestamp when the execution started"
        ),
        wvc.Property(
            name="duration_ms",
            data_type=wvc.DataType.NUMBER,
            description="Total execution time in milliseconds"
        ),
        wvc.Property(
            name="status",
            data_type=wvc.DataType.TEXT,  # "SUCCESS" or "ERROR"
            description="Execution status"
        ),
        wvc.Property(
            name="error_message",
            data_type=wvc.DataType.TEXT,
            description="Error message and traceback if status is 'ERROR'"
        ),
        wvc.Property(
            name="error_code",
            data_type=wvc.DataType.TEXT,
            description="Categorized error code for the failure (e.g., 'INVALID_INPUT', 'TIMEOUT')"
        ),
        wvc.Property(
            name="return_value",
            data_type=wvc.DataType.TEXT,
            description="The JSON-serialized or str() representation of the function's return value"
        )
    ]

    if settings.custom_properties:
        logger.info(
            "Adding %d custom properties: %s",
            len(settings.custom_properties),
            list(settings.custom_properties.keys())
        )
        for name, prop_details in settings.custom_properties.items():
            try:
                if not isinstance(prop_details, dict):
                    raise ValueError("Property details must be a dictionary.")

                dtype_str = prop_details.get("data_type")
                if not dtype_str:
                    raise ValueError("data_type is missing.")

                data_type = getattr(wvc.DataType, dtype_str.upper())
                description = prop_details.get("description")

                properties.append(
                    wvc.Property(
                        name=name,
                        data_type=data_type,
                        description=description
                    )
                )
            except Exception as e:
                logger.warning("Skipping custom property '%s' for '%s': %s", name, collection_name, e)

    try:
        execution_collection = client.collections.create(
            name=collection_name,
            properties=properties,
            vectorizer_config=wvc.Configure.Vectorizer.none(),
        )
        logger.info("Collection '%s' created successfully", collection_name)
        return execution_collection
    except Exception as e:
        raise SchemaCreationError(f"Error during execution schema creation: {e}")


def create_golden_dataset_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    """
    Creates a collection to store Golden Data (Best Practice/Ground Truth).
    """
    collection_name = settings.GOLDEN_COLLECTION_NAME

    if client.collections.exists(collection_name):
        logger.info("Collection '%s' already exists.", collection_name)
        return client.collections.get(collection_name)

    logger.info("Creating collection '%s'", collection_name)

    properties = [
        wvc.Property(name="original_uuid", data_type=wvc.DataType.TEXT,
                     description="UUID of the original execution log"),
        wvc.Property(name="function_name", data_type=wvc.DataType.TEXT),
        wvc.Property(name="function_uuid", data_type=wvc.DataType.UUID),

        wvc.Property(name="return_value", data_type=wvc.DataType.TEXT),

        wvc.Property(name="note", data_type=wvc.DataType.TEXT, description="User notes or reason for selection"),
        wvc.Property(name="created_at", data_type=wvc.DataType.DATE),
        wvc.Property(name="tags", data_type=wvc.DataType.TEXT_ARRAY, description="Tags like ['edge-case', 'baseline']")
    ]

    try:
        return client.collections.create(
            name=collection_name,
            properties=properties,
            vectorizer_config=wvc.Configure.Vectorizer.none(),
        )
    except Exception as e:
        raise SchemaCreationError(f"Error creating Golden Dataset schema: {e}")


def create_usage_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    """
    API call token analysis schema
    """
    collection_name = "VectorWaveTokenUsage"

    if client.collections.exists(collection_name):
        return client.collections.get(collection_name)

    logger.info("Creating collection '%s'", collection_name)

    properties = [
        wvc.Property(name="timestamp_utc", data_type=wvc.DataType.DATE),
        wvc.Property(name="model", data_type=wvc.DataType.TEXT),
        wvc.Property(name="usage_type", data_type=wvc.DataType.TEXT),  # "embedding", "generation" 등
        wvc.Property(name="category", data_type=wvc.DataType.TEXT),  # "execution_log", "auto_doc" 등
        wvc.Property(name="tokens", data_type=wvc.DataType.INT),
    ]

    try:
        return client.collections.create(
            name=collection_name,
            properties=properties,
            vectorizer_config=wvc.Configure.Vectorizer.none(),
        )
    except Exception as e:
        logger.error(f"Error creating usage schema: {e}")
        raise SchemaCreationError(f"Error during usage schema creation: {e}")


def initialize_database():
    """
    Helper function to initialize both the client and the two schemas.
    """
    try:
        settings = get_weaviate_settings()
        client = get_cached_client()
        if client:
            create_vectorwave_schema(client, settings)
            create_execution_schema(client, settings)
            create_usage_schema(client, settings)
            create_golden_dataset_schema(client, settings)
            return client
    except Exception as e:
        logger.error("Failed to initialize VectorWave database: %s", e)
        return None
