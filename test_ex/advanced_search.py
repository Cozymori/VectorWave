import sys
import os
# import logging  <- logging module removed
from dotenv import load_dotenv

# --- 1. Path Setup (Moved to top, before imports) ---
# Since this script is in 'test_ex', add the parent 'src' folder to the path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

# --- Now import the vectorwave module ---
from vectorwave.search.execution_search import *

try:
    # --- 2. Import necessary modules ---
    from vectorwave import initialize_database
    from vectorwave.database.db import get_cached_client

    # (RAG Search) Import low-level function
    #
    from vectorwave.database.db_search import search_functions, search_errors_by_message

    # (New) Import high-level execution log search functions
    #
    from vectorwave.search.execution_search import (
        find_recent_errors,
        find_slowest_executions,
        find_by_trace_id,
        find_executions
    )
except ImportError as e:
    print(f"Module import failed: {e}")
    print("Ensure the script is in the 'test_ex' folder and the 'src' folder is in the parent directory.")
    sys.exit(1)


# Logging setup removed
# ...

def run_search_scenarios():
    """
    Runs various scenarios utilizing the new search modules.
    (Output changed to print statements)
    """
    try:
        # --- 3. Initialize DB ---
        print("Connecting to and initializing database...")
        #
        client = initialize_database()
        if not client:
            print("DB connection failed. Check if Weaviate is running and .env file is correct.")
            return

        # --- 4. Scenario 1: RAG Function Search ---
        print("\n" + "=" * 50)
        print("--- [Scenario 1: RAG Function Search] ---")
        print("=" * 50)
        query = "payment process function"
        print(f"Query: '{query}'")
        #
        funcs = search_functions(query=query, limit=3, filters={"team":"billing"})
        for i, func in enumerate(funcs):
            print(f"  [Result {i + 1}] Function: {func['properties']['function_name']} (Similarity: {func['metadata'].distance:.4f})")

        # --- 5. Scenario 2: Find Slowest Functions (Using new module) ---
        print("\n" + "=" * 50)
        print("--- [Scenario 2: Top 3 Slowest Functions] ---")
        print("=" * 50)
        slowest_logs = find_slowest_executions(limit=3)
        for log in slowest_logs:
            print(f"  - [Slow] {log['function_name']} ({log['duration_ms']:.2f} ms)")

        # --- 6. Scenario 3: Find Recent Errors (Using new module) ---
        print("\n" + "=" * 50)
        print("--- [Scenario 3: 'INVALID_INPUT' Errors in last 60 mins] ---")
        print("=" * 50)
        error_logs = find_recent_errors(minutes_ago=60, error_codes=["INVALID_INPUT"])
        if not error_logs:
            print("  -> No such errors found. (Run example.py to generate error data)")
        for log in error_logs:
            print(f"  - [Error] {log['function_name']} | {log.get('error_code')} | {log['timestamp_utc']}")

        # --- 7. Scenario 4: Distributed Tracing (Using new module) ---
        print("\n" + "=" * 50)
        print("--- [Scenario 4: Find all spans for 'process_payment' Trace ID] ---")
        print("=" * 50)
        #
        root_spans = search_executions(limit=1, filters={"function_name": "process_payment"})
        if root_spans:
            trace_id = root_spans[0].get('trace_id')
            print(f"Trace ID for '{root_spans[0]['function_name']}': {trace_id}")

            #
            spans = find_by_trace_id(trace_id)
            for span in spans:
                print(f"  - [Trace] {span['function_name']} | {span['status']} | {span['duration_ms']:.2f} ms")
        else:
            print("  -> 'process_payment' log not found. (Run example.py first)")

        # --- [NEW] Scenario 5: Semantic Error Search ---
        print("\n" + "=" * 50)
        print("--- [Scenario 5: Semantic Error Search (Vector)] ---")
        print("=" * 50)
        error_query = "problem with negative amount" 
        print(f"Query: '{error_query}'")

        try:
            similar_errors = search_errors_by_message(query=error_query, limit=3)

            if not similar_errors:
                print("  -> No semantically similar errors found.")
                print("     (Run example.py to generate errors, and ensure .env uses a Python-based vectorizer like 'huggingface' or 'openai_client')")
            for i, err in enumerate(similar_errors):
                full_error_msg = err['properties'].get('error_message', 'N/A')
                simple_msg = full_error_msg.strip().split('\n')[-1]

                print(f"  [Error Result {i + 1}] (Distance: {err['metadata'].distance:.4f})")
                print(f"  Function: {err['properties']['function_name']}")
                print(f"  Message: {simple_msg}")

        except Exception as e:
            print(f"  [Error] Semantic search failed: {e}")
            print("  (Note: This search requires .env VECTORIZER to be 'huggingface' or 'openai_client')")

    except Exception as e:
        print(f"AFatal error occurred during scenario execution: {e}")
    finally:
        client = get_cached_client()
        if client:
            client.close()
            print("\nDatabase connection closed.")


if __name__ == "__main__":
    # Load .env file (test_ex/.env)
    #
    load_dotenv()

    # Run main scenarios
    run_search_scenarios()