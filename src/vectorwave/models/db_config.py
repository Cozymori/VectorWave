import logging
from pydantic_settings import BaseSettings, SettingsConfigDict
from functools import lru_cache
from typing import Dict, Optional, Any, Set
import json
import os

# Create module-level logger
logger = logging.getLogger(__name__)


class WeaviateSettings(BaseSettings):
    """
    Manages Weaviate database connection settings.

    Reads values from environment variables or a .env file.
    (e.g., WEAVIATE_HOST=10.0.0.1)
    """
    # If environment variables are not set, these default values will be used.
    WEAVIATE_HOST: str = "localhost"
    WEAVIATE_PORT: int = 8080
    WEAVIATE_GRPC_PORT: int = 50051
    WEAVIATE_API_KEY: Optional[str] = None
    COLLECTION_NAME: str = "VectorWaveFunctions"
    EXECUTION_COLLECTION_NAME: str = "VectorWaveExecutions"
    GOLDEN_COLLECTION_NAME: str = "VectorWaveGoldenDataset"
    IS_VECTORIZE_COLLECTION_NAME: bool = True

    # "weaviate_module", "huggingface", "openai_client", "none"
    VECTORIZER: str = "weaviate_module"

    # batch configs
    BATCH_THRESHOLD: int = 20
    FLUSH_INTERVAL_SECONDS: float = 2.0

    WEAVIATE_VECTORIZER_MODULE: str = "text2vec-openai"

    WEAVIATE_GENERATIVE_MODULE: str = "generative-openai"

    OPENAI_API_KEY: Optional[str] = None
    HF_MODEL_NAME: str = "sentence-transformers/all-MiniLM-L6-v2"

    CUSTOM_PROPERTIES_FILE_PATH: str = ".weaviate_properties"
    FAILURE_MAPPING_FILE_PATH: str = ".vectorwave_errors.json"
    IGNORE_ERROR_FILE_PATH: str = ".vtwignore"

    GITHUB_TOKEN: Optional[str] = None
    GITHUB_REPO_NAME: Optional[str] = None
    GITHUB_BASE_BRANCH: str = "main"

    custom_properties: Optional[Dict[str, Dict[str, Any]]] = None
    global_custom_values: Optional[Dict[str, Any]] = None
    failure_mapping: Optional[Dict[str, str]] = None

    ignored_error_codes: Set[str] = set()

    ALERTER_STRATEGY: str = "none"
    ALERTER_WEBHOOK_URL: Optional[str] = None
    ALERTER_MIN_LEVEL: str = "ERROR"

    DRIFT_DETECTION_ENABLED: bool = False
    DRIFT_DISTANCE_THRESHOLD: float = 0.25
    DRIFT_NEIGHBOR_AMOUNT: int = 5

    RECOMMENDATION_STEADY_MARGIN: float = 0.05
    RECOMMENDATION_DISCOVERY_MARGIN: float = 0.15

    SENSITIVE_FIELD_NAMES: str = "password,api_key,token,secret,auth_token"
    sensitive_keys: Set[str] = set()

    ASYNC_LOGGING: bool = False

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra='ignore')


# @lru_cache ensures this function creates the Settings object only once (Singleton pattern)
# and reuses the cached object on subsequent calls.
@lru_cache()
def get_weaviate_settings() -> WeaviateSettings:
    """
    Factory function that returns the settings object.
    """
    settings = WeaviateSettings()

    file_path = settings.CUSTOM_PROPERTIES_FILE_PATH

    if file_path and os.path.exists(file_path):
        logger.info("Loading custom properties schema from '%s'", file_path)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)

                if isinstance(loaded_data, dict):
                    settings.custom_properties = loaded_data
                else:
                    logger.warning(
                        "Content in '%s' is not a valid dictionary (JSON root), custom properties will not be loaded",
                        file_path
                    )
                    settings.custom_properties = None

        except json.JSONDecodeError as e:
            logger.warning("Could not parse JSON from '%s': %s", file_path, e)
            settings.custom_properties = None
        except Exception as e:
            logger.warning("Could not read file '%s': %s", file_path, e)
            settings.custom_properties = None

    elif file_path:
        logger.debug("Custom properties file not found at '%s', skipping", file_path)

    if settings.custom_properties:
        settings.global_custom_values = {}
        logger.debug("Loading global custom values from environment variables")

        for prop_name in settings.custom_properties.keys():
            env_var_name = prop_name.upper()
            value = os.environ.get(env_var_name)

            if value:
                settings.global_custom_values[prop_name] = value
                logger.debug("Loaded global value for '%s' from env var '%s'", prop_name, env_var_name)

    error_file_path = settings.FAILURE_MAPPING_FILE_PATH

    if error_file_path and os.path.exists(error_file_path):
        logger.info(f"Loading failure mapping from '{error_file_path}'...")
        try:
            with open(error_file_path, 'r', encoding='utf-8') as f:
                loaded_data = json.load(f)
                if isinstance(loaded_data, dict):
                    settings.failure_mapping = loaded_data
                else:
                    logger.warning(
                        f"Content in '{error_file_path}' is not a valid dictionary. Skipping failure mapping.")
        except Exception as e:
            logger.warning(f"Could not read or parse '{error_file_path}': {e}")
    elif error_file_path:
        logger.info(f"Note: Failure mapping file not found at '{error_file_path}'. Skipping.")

    ignore_file_path = settings.IGNORE_ERROR_FILE_PATH
    if ignore_file_path and os.path.exists(ignore_file_path):
        logger.info(f"Loading ignore error codes from '{ignore_file_path}'...")
        try:
            with open(ignore_file_path, 'r', encoding='utf-8') as f:
                codes = {line.strip() for line in f if line.strip() and not line.startswith('#')}
                settings.ignored_error_codes = codes
                logger.info(f"Loaded {len(codes)} ignored error codes.")
        except Exception as e:
            logger.warning(f"Could not read '{ignore_file_path}': {e}")

    try:
        settings.sensitive_keys = {
            key.strip().lower()
            for key in settings.SENSITIVE_FIELD_NAMES.split(',')
            if key.strip()
        }
        if settings.sensitive_keys:
            logger.info(f"Initialized with {len(settings.sensitive_keys)} sensitive keys for masking.")
    except Exception as e:
        logger.warning(f"Failed to parse SENSITIVE_FIELD_NAMES: {e}")
        settings.sensitive_keys = set()

    return settings
