import logging
from functools import lru_cache
from typing import Optional

import weaviate
import weaviate.classes.config as wvc
from weaviate.classes.config import Tokenization
from vectorwave.exception.exceptions import (
    WeaviateConnectionError,
    WeaviateNotReadyError,
    SchemaCreationError
)
from vectorwave.models.db_config import WeaviateSettings
from vectorwave.models.db_config import get_weaviate_settings
from weaviate.config import AdditionalConfig
from weaviate.exceptions import WeaviateConnectionError as WeaviateClientConnectionError

logger = logging.getLogger(__name__)


def get_weaviate_client(
        settings: WeaviateSettings = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        grpc_port: Optional[int] = None,
        api_key: Optional[str] = None
) -> weaviate.WeaviateClient:
    try:
        if host is not None and api_key is not None:
            # Cloud (WCS) connection with API key
            client = weaviate.connect_to_wcs(
                cluster_url=host,
                auth_credentials=weaviate.auth.AuthApiKey(api_key),
                additional_config=AdditionalConfig(
                    timeout_retries=3
                )
            )
        elif host is not None:
            # Local connection with custom host/port
            client = weaviate.connect_to_local(
                host=host,
                port=port or 8080,
                grpc_port=grpc_port or 50051,
                additional_config=AdditionalConfig(
                    dynamic=True,
                    batch_size=20,
                    timeout_retries=3
                )
            )
        else:
            # Default: local connection from settings (existing behavior)
            if settings is None:
                settings = get_weaviate_settings()

            if settings.WEAVIATE_API_KEY:
                logger.info(f"Connecting to Weaviate Cloud: {settings.WEAVIATE_HOST}")
                client = weaviate.connect_to_wcs(
                    cluster_url=settings.WEAVIATE_HOST,
                    auth_credentials=weaviate.auth.AuthApiKey(settings.WEAVIATE_API_KEY),
                    additional_config=AdditionalConfig(
                        timeout_retries=3
                    )
                )
            else:
                logger.info(f"Connecting to Local Weaviate: {settings.WEAVIATE_HOST}")
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
def get_cached_client(
        host: Optional[str] = None,
        port: Optional[int] = None,
        grpc_port: Optional[int] = None,
        api_key: Optional[str] = None
) -> weaviate.WeaviateClient:
    if host is not None:
        logger.debug("Creating and caching dynamic Weaviate client for host=%s", host)
        client = get_weaviate_client(host=host, port=port, grpc_port=grpc_port, api_key=api_key)
    else:
        logger.debug("Creating and caching default Weaviate client instance")
        settings = get_weaviate_settings()
        client = get_weaviate_client(settings)
    return client


def _create_property_from_config(name: str, prop_details: dict) -> wvc.Property:
    dtype_str = prop_details.get("data_type")
    if not dtype_str:
        raise ValueError(f"Property '{name}' missing 'data_type'")

    if not hasattr(wvc.DataType, dtype_str.upper()):
        raise ValueError(f"Invalid data_type '{dtype_str}' for property '{name}'")

    data_type = getattr(wvc.DataType, dtype_str.upper())
    description = prop_details.get("description")

    tokenization = None
    token_str = prop_details.get("tokenization")

    if token_str:
        token_map = {
            "word": Tokenization.WORD,
            "whitespace": Tokenization.WHITESPACE,
            "field": Tokenization.FIELD,
            "lowercase": Tokenization.LOWERCASE
        }
        token_key = token_str.lower()
        if token_key in token_map:
            tokenization = token_map[token_key]
        else:
            logger.warning(f"Invalid tokenization '{token_str}' for '{name}'. Using default.")

    return wvc.Property(
        name=name,
        data_type=data_type,
        description=description,
        tokenization=tokenization
    )


def _get_existing_collection(client: weaviate.WeaviateClient, name: str):
    """Returns existing collection if it exists, or None."""
    if client.collections.exists(name):
        logger.info("Collection '%s' already exists, skipping creation", name)
        return client.collections.get(name)
    return None


def _build_custom_properties(settings: WeaviateSettings, collection_name: str):
    """Builds custom wvc.Property list from settings.custom_properties."""
    custom_properties = []
    if settings.custom_properties:
        logger.info(f"Adding {len(settings.custom_properties)} custom properties to '{collection_name}'")
        for name, prop_details in settings.custom_properties.items():
            try:
                prop = _create_property_from_config(name, prop_details)
                custom_properties.append(prop)
            except Exception as e:
                logger.error(f"Invalid property definition for '{name}': {e}")
                raise SchemaCreationError(f"Invalid property definition for '{name}': {e}")
    return custom_properties


def _create_collection_safe(client, name, properties, vectorizer_config, generative_config=None):
    """Creates a collection with standardized error handling."""
    logger.info("Creating collection '%s'", name)
    try:
        return client.collections.create(
            name=name,
            properties=properties,
            vectorizer_config=vectorizer_config,
            generative_config=generative_config
        )
    except Exception as e:
        raise SchemaCreationError(f"Error creating collection '{name}': {e}")


def create_vectorwave_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    collection_name = settings.COLLECTION_NAME

    existing = _get_existing_collection(client, collection_name)
    if existing is not None:
        return existing

    base_properties = [
        wvc.Property(name="function_name", data_type=wvc.DataType.TEXT),
        wvc.Property(name="module_name", data_type=wvc.DataType.TEXT),
        wvc.Property(name="docstring", data_type=wvc.DataType.TEXT),
        wvc.Property(name="source_code", data_type=wvc.DataType.TEXT),
        wvc.Property(name="search_description", data_type=wvc.DataType.TEXT),
        wvc.Property(name="sequence_narrative", data_type=wvc.DataType.TEXT),
        wvc.Property(name="file_path", data_type=wvc.DataType.TEXT),
    ]

    all_properties = base_properties + _build_custom_properties(settings, collection_name)

    vector_config = wvc.Configure.Vectorizer.none()
    generative_config = None

    if settings.VECTORIZER.lower() == "weaviate_module":
        if settings.WEAVIATE_VECTORIZER_MODULE == "text2vec-openai":
            vector_config = wvc.Configure.Vectorizer.text2vec_openai()
        if settings.WEAVIATE_GENERATIVE_MODULE == "generative-openai":
            generative_config = wvc.Configure.Generative.openai()

    elif settings.VECTORIZER.lower() not in ["none", "huggingface", "openai_client"]:
        raise SchemaCreationError(f"Invalid VECTORIZER setting: {settings.VECTORIZER}")

    return _create_collection_safe(client, collection_name, all_properties, vector_config, generative_config)


def create_execution_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    collection_name = settings.EXECUTION_COLLECTION_NAME

    existing = _get_existing_collection(client, collection_name)
    if existing is not None:
        return existing

    properties = [
        wvc.Property(name="trace_id", data_type=wvc.DataType.TEXT),
        wvc.Property(name="span_id", data_type=wvc.DataType.TEXT),
        wvc.Property(name="parent_span_id", data_type=wvc.DataType.TEXT),
        wvc.Property(name="function_name", data_type=wvc.DataType.TEXT),
        wvc.Property(name="function_uuid", data_type=wvc.DataType.UUID),
        wvc.Property(name="timestamp_utc", data_type=wvc.DataType.DATE),
        wvc.Property(name="duration_ms", data_type=wvc.DataType.NUMBER),
        wvc.Property(name="status", data_type=wvc.DataType.TEXT),
        wvc.Property(name="error_message", data_type=wvc.DataType.TEXT),
        wvc.Property(name="error_code", data_type=wvc.DataType.TEXT),
        wvc.Property(name="return_value", data_type=wvc.DataType.TEXT),
        wvc.Property(name="exec_source", data_type=wvc.DataType.TEXT)
    ]
    properties += _build_custom_properties(settings, collection_name)

    return _create_collection_safe(client, collection_name, properties, wvc.Configure.Vectorizer.none())


def create_golden_dataset_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    collection_name = settings.GOLDEN_COLLECTION_NAME

    existing = _get_existing_collection(client, collection_name)
    if existing is not None:
        return existing

    properties = [
        wvc.Property(name="original_uuid", data_type=wvc.DataType.TEXT),
        wvc.Property(name="function_name", data_type=wvc.DataType.TEXT),
        wvc.Property(name="function_uuid", data_type=wvc.DataType.UUID),
        wvc.Property(name="return_value", data_type=wvc.DataType.TEXT),
        wvc.Property(name="note", data_type=wvc.DataType.TEXT),
        wvc.Property(name="created_at", data_type=wvc.DataType.DATE),
        wvc.Property(name="tags", data_type=wvc.DataType.TEXT_ARRAY)
    ]
    properties += _build_custom_properties(settings, collection_name)

    return _create_collection_safe(client, collection_name, properties, wvc.Configure.Vectorizer.none())


def create_usage_schema(client: weaviate.WeaviateClient, settings: WeaviateSettings):
    collection_name = "VectorWaveTokenUsage"

    existing = _get_existing_collection(client, collection_name)
    if existing is not None:
        return existing

    properties = [
        wvc.Property(name="timestamp_utc", data_type=wvc.DataType.DATE),
        wvc.Property(name="model", data_type=wvc.DataType.TEXT),
        wvc.Property(name="usage_type", data_type=wvc.DataType.TEXT),
        wvc.Property(name="category", data_type=wvc.DataType.TEXT),
        wvc.Property(name="tokens", data_type=wvc.DataType.INT),
    ]

    return _create_collection_safe(client, collection_name, properties, wvc.Configure.Vectorizer.none())


def update_database_schema():
    """
    [Migration Utility]
    Updates the schema of the running database.
    Finds properties defined in .weaviate_properties that are missing from the DB and adds them.
    Used to apply new filtering features without data loss.
    """
    try:
        settings = get_weaviate_settings()
        client = get_cached_client()

        target_collections = [
            settings.EXECUTION_COLLECTION_NAME,
            settings.GOLDEN_COLLECTION_NAME
        ]

        logger.info("🚀 Starting Schema Migration...")

        for col_name in target_collections:
            if not client.collections.exists(col_name):
                logger.warning(f"Collection '{col_name}' does not exist. Skipping.")
                continue

            collection = client.collections.get(col_name)
            # 현재 DB에 존재하는 속성 이름 가져오기
            existing_props = {p.name for p in collection.config.get().properties}

            # 추가할 속성 확인
            if settings.custom_properties:
                for name, prop_details in settings.custom_properties.items():
                    if name not in existing_props:
                        logger.info(f"➕ Adding new property '{name}' to '{col_name}'...")
                        try:
                            new_prop = _create_property_from_config(name, prop_details)
                            collection.config.add_property(new_prop)
                            logger.info(f"   ✅ Property '{name}' added successfully.")
                        except Exception as e:
                            logger.error(f"   ❌ Failed to add property '{name}': {e}")
                    else:
                        logger.debug(f"   Pass: Property '{name}' already exists.")

        logger.info("✨ Schema Migration Completed!")
        return True

    except Exception as e:
        logger.error(f"Migration failed: {e}", exc_info=True)
        return False


def initialize_database():
    try:
        settings = get_weaviate_settings()
        client = get_cached_client()
        if client is not None:
            create_vectorwave_schema(client, settings)
            create_execution_schema(client, settings)
            create_usage_schema(client, settings)
            create_golden_dataset_schema(client, settings)
            return client
    except Exception as e:
        logger.error("Failed to initialize VectorWave database: %s", e)
        return None