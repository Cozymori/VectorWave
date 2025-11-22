import sys
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any

# --- Path Setup ---
# Assumes this file is in src/vectorwave/search/
# Adds the top-level 'src' folder to sys.path to import other modules
current_dir = os.path.dirname(os.path.abspath(__file__))
src_root = os.path.dirname(os.path.dirname(current_dir))  # src/
sys.path.insert(0, src_root)

try:
    # Import the low-level DB search function
    #
    from vectorwave.database.db_search import search_executions
    from vectorwave import initialize_database
    from vectorwave.database.db import get_cached_client
except ImportError as e:
    # Use logger for the error, but print is necessary if logger fails
    print(f"Failed to import VectorWave module: {e}")
    logging.error(f"Failed to import VectorWave module: {e}", exc_info=True)
    sys.exit(1)

# Set up a module-level logger
logger = logging.getLogger(__name__)


def find_executions(
        filters: Optional[Dict[str, Any]] = None,
        sort_by: str = "timestamp_utc",
        sort_ascending: bool = False,
        limit: int = 10
) -> List[Dict[str, Any]]:
    """
    A general wrapper function for searching the VectorWaveExecutions collection.

    Args:
        filters: A dictionary of Weaviate filters (e.g., {"status": "ERROR"})
        sort_by: The property to sort by (e.g., "duration_ms")
        sort_ascending: Whether to sort in ascending order
        limit: The maximum number of results to return

    Returns:
        A list of retrieved log objects (dictionaries)
    """
    logger.info(f"Querying executions. Filters: {filters}, SortBy: {sort_by}, Limit: {limit}")
    try:
        #
        return search_executions(
            limit=limit,
            filters=filters,
            sort_by=sort_by,
            sort_ascending=sort_ascending
        )
    except Exception as e:
        logger.error(f"An error occurred while searching execution logs: {e}", exc_info=True)
        return []


def find_recent_errors(
        minutes_ago: int = 5,
        limit: int = 20,
        error_codes: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    logger.info(f"--- Searching for error logs from the last {minutes_ago} minutes ---")

    time_limit_iso = (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).isoformat()

    filters = {
        "status": "ERROR",
        "timestamp_utc__gte": time_limit_iso
    }

    if error_codes:
        filters["error_code"] = error_codes

    all_errors = find_executions(
        filters=filters,
        sort_by="timestamp_utc",
        sort_ascending=False,
        limit=limit
    )

    logger.info(f"-> Found {len(all_errors)} matching errors.")
    return all_errors


def find_slowest_executions(
        limit: int = 5,
        min_duration_ms: float = 0.0
) -> List[Dict[str, Any]]:
    """
    Searches for the slowest execution logs. (For performance monitoring)
    """
    logger.info(f"\n--- Searching for Top {limit} Slowest Executions ---")

    filters = {}
    if min_duration_ms > 0:
        filters["duration_ms__gte"] = min_duration_ms

    return find_executions(
        filters=filters,
        sort_by="duration_ms",
        sort_ascending=False,
        limit=limit
    )


def find_by_trace_id(trace_id: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    Searches for all spans/logs belonging to a specific trace_id, sorted by time.
    """
    logger.info(f"\n--- Searching for Trace ID '{trace_id}' ---")
    filters = {"trace_id": trace_id}

    return find_executions(
        filters=filters,
        sort_by="timestamp_utc",
        sort_ascending=True,  # Sort chronologically
        limit=limit
    )
