import sys
import os
import time
from typing import Callable, Dict, Any

# --- 1. Path Setup ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
os.chdir(current_script_dir)

# --- 2. VectorWave Module Import ---
from vectorwave import vectorize, initialize_database
from vectorwave.database.db import get_cached_client


# --- 3. DB Initialization ---
client = None
try:
    print("Attempting to initialize VectorWave database...")
    client = initialize_database()
    if client:
        print("✅ Database connection and schema initialization successful.")
    else:
        raise ConnectionError("Database initialization failed. Check your Weaviate server and .env file.")

except Exception as e:
    print(f"\n[Fatal Error] Script execution halted during DB init: {e}")
    sys.exit(1)


# --- 4. Define Comparison Functions (Same 0.5 sec Delay) ---

DELAY_SECONDS = 0.5
QUERY = "Analyze this high-cost query for a payment of 100 dollars."
AMOUNT = 100
NUM_RUNS = 10 # Number of runs per group

# 4-A. Caching Enabled Group (Target for Performance Improvement)
@vectorize(
    search_description="High-cost query processing function with caching enabled.",
    sequence_narrative="Uses cached results after the first call to improve performance.",
    semantic_cache=True,       # ⬅️ Enable Caching
    cache_threshold=0.9,
    capture_return_value=True
)
def heavy_cached_query(user_query: str, amount: int):
    # This log is only printed upon a cache miss (actual execution).
    print(f"  [CACHED GROUP] 🚀 Executing (Delay: {DELAY_SECONDS:.1f}s)...")
    time.sleep(DELAY_SECONDS)
    return {"status": "NEW_RUN"}


# 4-B. Caching Disabled Group (Baseline)
@vectorize(
    search_description="High-cost query processing function with caching disabled (Baseline).",
    sequence_narrative="Executes the actual function on every call for baseline time measurement.",
    semantic_cache=False,      # ⬅️ Disable Caching
    capture_return_value=True
)
def heavy_uncached_query(user_query: str, amount: int):
    # This log is printed every time since caching is disabled.
    print(f"  [UNCACHED GROUP] ❌ Executing (Delay: {DELAY_SECONDS:.1f}s)...")
    time.sleep(DELAY_SECONDS)
    return {"status": "UNCALCHED_RUN"}


# --- 5. Test Execution Helper ---

def run_test_group(func: Callable, group_name: str, num_runs: int = NUM_RUNS) -> float:
    """
    Executes the specified function multiple times and measures the total execution time.
    """
    print("\n" + "=" * 60)
    print(f"--- {group_name} Start ({num_runs} total calls, {DELAY_SECONDS:.1f} sec delay each) ---")
    print("=" * 60)

    start_time = time.time()

    for i in range(num_runs):
        # Calls made with identical arguments to induce cache hits/misses.
        print(f"  [{i+1}/{num_runs}] Calling...")
        func(user_query=QUERY, amount=AMOUNT)

    end_time = time.time()
    total_time = end_time - start_time

    print(f"\n✅ {group_name} Complete. Total execution time: {total_time:.2f} seconds")
    return total_time


# --- 6. Performance Test Execution and Comparison ---

if __name__ == "__main__":

    # 6-A. Cached Group Execution: 1 Miss + 9 Hits (Expected Time: ~0.5s)
    time_cached = run_test_group(heavy_cached_query, "Caching Group (Cache Group)")

    # 6-B. Uncached Group Execution: 10 Misses (Expected Time: ~5.0s)
    time_uncached = run_test_group(heavy_uncached_query, "Uncached Group (Uncached Group)")

    # --- 6-C. Result Comparison ---
    print("\n" + "#" * 60)
    print("                      ✨ Performance Comparison Results ✨")
    print("#" * 60)
    print(f"  [1] Total time for Caching Group (1 Miss + 9 Hits): {time_cached:.2f} seconds")
    print(f"  [2] Total time for Uncached Group (10 Misses):      {time_uncached:.2f} seconds")
    print("-" * 60)

    # Check if the speedup is significant (i.e., close to N-times faster)
    if time_uncached > time_cached * (NUM_RUNS - 1):
        print(f"  ➡️ Caching saved approximately {(time_uncached - time_cached):.2f} seconds! (Performance improved by approx. {time_uncached/time_cached:.1f}x)")
    else:
        print("  ⚠️ Performance improvement is less than expected. Check cache lookup overhead.")


    # --- 7. Connection Closure ---
    if client:
        print("\n" + "=" * 60)
        print("All scenarios complete. Closing database connection.")
        get_cached_client().close()
        print("=" * 60)