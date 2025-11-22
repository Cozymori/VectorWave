import logging
from typing import List, Dict, Any

from ..database.db import get_cached_client
from ..models.db_config import get_weaviate_settings

logger = logging.getLogger(__name__)


def get_db_status() -> bool:
    """
    Checks the connection status of the Weaviate database.

    Returns:
        bool: True if connected and ready, False otherwise.
    """
    try:
        client = get_cached_client()
        return client.is_ready()
    except Exception as e:
        logger.warning(f"Failed to check DB status: {e}")
        return False


def get_registered_functions() -> List[Dict[str, Any]]:
    """
    Returns information for all applications (functions) registered with the @vectorize decorator.

    Returns:
        List[Dict]: A list of metadata for registered functions.
    """
    try:
        if not get_db_status():
            return []

        client = get_cached_client()
        settings = get_weaviate_settings()
        collection = client.collections.get(settings.COLLECTION_NAME)

        results = []

        for obj in collection.iterator(
                return_properties=["function_name", "module_name", "search_description"]
        ):
            results.append(obj.properties)

        return results

    except Exception as e:
        logger.error(f"Registered function list search failed: {e}")
        return []

    except Exception as e:
        logger.error(f"Failed to retrieve registered function list: {e}")
        return []
