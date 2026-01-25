import sys
import os
import time
import logging

# [Setup] Prioritize source code path
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, '../src'))
sys.path.insert(0, src_path)

from vectorwave.core.decorator import vectorize
from vectorwave.database.db import initialize_database, get_cached_client
from vectorwave.models.db_config import get_weaviate_settings
from vectorwave.database.db_search import search_executions

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("IsolationTest")

def reset_and_init_db():
    print("\n🧹 Initializing DB and updating schema...")
    settings = get_weaviate_settings()
    client = get_cached_client()

    # Delete existing collections (to apply schema changes)
    for col in [settings.COLLECTION_NAME, settings.EXECUTION_COLLECTION_NAME, settings.GOLDEN_COLLECTION_NAME]:
        if client.collections.exists(col):
            client.collections.delete(col)

    initialize_database()
    print("✨ DB Ready!\n")

def run_isolation_test():
    reset_and_init_db()

    print("🚀 [Scenario] Starting AI Summary Isolation Test per Project")
    print("   Goal: Even with identical input, cache should not be shared if 'project_id' differs.\n")

    # --------------------------------------------------------------------------------
    # Define AI summary function (Scope restricted by project_id)
    # --------------------------------------------------------------------------------
    @vectorize(
        semantic_cache=True,
        semantic_cache_scope=['project_id'], # <--- Key: Use cache only when project ID matches
        capture_inputs=True,                 # Auto-capture arguments
        cache_threshold=0.85                 # Similarity threshold
    )
    def ai_summarize_meeting(project_id: str, meeting_content: str):
        # Simulate LLM call by consuming time
        print(f"      [System] 🤖 Generating summary for '{project_id}'... (LLM Call)")
        time.sleep(1.0)
        return f"Summary complete: {meeting_content[:20]}..."

    # Test Data
    shared_content = "In this sprint, we focused on strengthening authentication logic and improving API speed."

    # --------------------------------------------------------------------------------
    # 1. Project A: First Request (Cache Creation)
    # --------------------------------------------------------------------------------
    print("\n[Step 1] Project_A: First Summary Request (DB Save)")
    start = time.time()
    ai_summarize_meeting(project_id="Project_A", meeting_content=shared_content)
    print(f"   ⏱️ Duration: {time.time() - start:.2f}s (Executed)")

    print("   ⏳ Waiting for data save (4s)...")
    time.sleep(4.0)

    # --------------------------------------------------------------------------------
    # 2. Project A: Re-request (Verify Cache Hit)
    # --------------------------------------------------------------------------------
    print("\n[Step 2] Project_A: Re-requesting same content")
    start = time.time()
    ai_summarize_meeting(project_id="Project_A", meeting_content=shared_content)
    duration = time.time() - start
    print(f"   ⏱️ Duration: {duration:.2f}s")

    if duration < 0.5:
        print("   ✅ [Success] Successfully retrieved Project_A's cache.")
    else:
        print("   ❌ [Failure] Cache hit failed")

    # --------------------------------------------------------------------------------
    # 3. Project B: Same content request (Verify Isolation - Most Important!)
    # --------------------------------------------------------------------------------
    print("\n[Step 3] Project_B: Same content request (Security Isolation Test)")
    print("   ❓ Expected Result: Input content is same, but should not use A's cache because project differs.")

    start = time.time()
    # Request as Project_B
    ai_summarize_meeting(project_id="Project_B", meeting_content=shared_content)
    duration = time.time() - start
    print(f"   ⏱️ Duration: {duration:.2f}s")

    if duration >= 1.0:
        print("   ✅ [Success] Isolation works! Did not see Project_A's data and executed newly.")
    else:
        print("   ❌ [Danger] Data Leak! Fetched Project_A's cache.")

    # --------------------------------------------------------------------------------
    # 4. Project B: Re-request (Verify using its own cache)
    # --------------------------------------------------------------------------------
    print("\n   ⏳ Waiting for Project_B data save (4s)...")
    time.sleep(4.0)

    print("\n[Step 4] Project_B: Re-request")
    start = time.time()
    ai_summarize_meeting(project_id="Project_B", meeting_content=shared_content)
    duration = time.time() - start
    print(f"   ⏱️ Duration: {duration:.2f}s")

    if duration < 0.5:
        print("   ✅ [Success] Project_B is correctly using its own cache.")

if __name__ == "__main__":
    run_isolation_test()