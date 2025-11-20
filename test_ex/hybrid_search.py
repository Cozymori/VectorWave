import sys
import os
from dotenv import load_dotenv

# --- 1. Path setup (recognize src folder) ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

try:
    from vectorwave import initialize_database
    from vectorwave.database.db import get_cached_client
    # Import the newly added hybrid search function
    from vectorwave import search_functions_hybrid
except ImportError as e:
    print(f"Module import failed: {e}")
    print("Please check if search_functions_hybrid is added to src/vectorwave/__init__.py.")
    sys.exit(1)


def run_hybrid_test():
    # Initialize DB
    client = initialize_database()
    if not client:
        print("DB connection failed")
        return

    print("\n" + "=" * 60)
    print("🔍 [Hybrid Search Test] Comparison of search results by Alpha value")
    print("=" * 60)

    # Search query to test
    # Example: "pay" (keyword) vs "pay money" (semantic)
    # Choose a query that shows a distinct difference in results depending on the situation.
    query = "transfer cash money"

    # Alpha values to test
    # 0.0 ~ 1.0 (Closer to 0 is keyword-centric, closer to 1 is vector/semantic-centric)
    alpha_values = [0.1, 0.5, 0.9]

    for alpha in alpha_values:
        print(f"\n🔹 [Alpha: {alpha}] ", end="")
        if alpha < 0.3:
            print("(Keyword-centric)")
        elif alpha > 0.7:
            print("(Semantic/Vector-centric)")
        else:
            print("(Balanced)")

        try:
            results = search_functions_hybrid(
                query=query,
                limit=3,
                alpha=alpha
            )

            if not results:
                print("   -> No results")
            else:
                for i, res in enumerate(results):
                    score = res['metadata'].score or 0.0
                    func_name = res['properties']['function_name']
                    print(f"   {i + 1}. {func_name} (Score: {score:.4f})")

        except Exception as e:
            print(f"   -> Error during search: {e}")

    print("\n" + "=" * 60)

    # Close connection
    get_cached_client().close()


if __name__ == "__main__":
    load_dotenv()  # Load .env
    run_hybrid_test()
