import sys
import os
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.abspath(os.path.join(current_dir, "../src")))

from vectorwave.database.db import get_cached_client
from vectorwave.models.db_config import get_weaviate_settings

def clear_database():
    print("🧹 Starting Weaviate DB initialization...")
    client = get_cached_client()
    settings = get_weaviate_settings()

    collections = [
        settings.COLLECTION_NAME,
        settings.EXECUTION_COLLECTION_NAME,
        settings.GOLDEN_COLLECTION_NAME,

    ]

    for col_name in collections:
        try:
            # Attempt deletion without checking if the collection exists (try-except handles errors if missing)
            client.collections.delete(col_name)
            print(f"   ✅ Collection deletion complete: {col_name}")
        except Exception as e:
            print(f"   ⚠️ Deletion failed ({col_name}): {e}")

    print("✨ DB is clean.")

if __name__ == "__main__":
    clear_database()