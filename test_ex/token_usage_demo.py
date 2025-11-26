import sys
import os
import time
from dotenv import load_dotenv

# --- Path setup (to recognize the src folder) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

from vectorwave import initialize_database
from vectorwave.core.llm.factory import get_llm_client
from vectorwave.database.db_search import get_token_usage_stats
from vectorwave.database.db import get_cached_client

def run_token_usage_test():
    # 1. Initialize DB and check schema
    print("🔌 Connecting to the database...")
    client = initialize_database()
    if not client:
        print("❌ DB connection failed. Check if Weaviate is running.")
        return

    # 2. Retrieve statistics before execution
    print("\n📊 [Before] Current Token Usage Statistics:")
    stats_before = get_token_usage_stats()
    print(f"   -> {stats_before}")
    initial_total = stats_before.get('total_tokens', 0)

    # 3. Use LLM client (consume tokens)
    llm_client = get_llm_client()
    if not llm_client:
        print("❌ LLM Client initialization failed.")
        return

    print("\n🤖 [Action] Calling LLM API (consuming tokens)...")

    # 3-1. Chat Completion (Generation)
    response = llm_client.create_chat_completion(
        messages=[{"role": "user", "content": "Tell me a very short joke about programming."}],
        model="gpt-4-turbo",
        category="usage_test_chat"
    )
    print(f"   -> Chat Response: {response}")

    # 3-2. Embedding (Embedding)
    vector = llm_client.create_embedding(
        text="This text is for testing token usage logging.",
        model="text-embedding-3-small",
        category="usage_test_embed"
    )
    print(f"   -> Embedding created successfully (Dimensions: {len(vector)})")

    # 4. Wait for asynchronous saving (Batch Flush time consideration)
    print("\n⏳ Waiting 3 seconds for log saving...")
    time.sleep(3)

    # 5. Retrieve statistics after execution and compare
    print("\n📊 [After] Updated Token Usage Statistics:")
    stats_after = get_token_usage_stats()
    print(f"   -> {stats_after}")

    final_total = stats_after.get('total_tokens', 0)
    consumed = final_total - initial_total

    print("\n" + "="*40)
    if consumed > 0:
        print(f"✅ Test SUCCESS! A total of {consumed} tokens were additionally logged.")
    else:
        print("⚠️ Warning: Token usage did not increase. (Check batch configuration or DB connection)")
    print("="*40)

    get_cached_client().close()

if __name__ == "__main__":
    load_dotenv()  # Load .env
    run_token_usage_test()