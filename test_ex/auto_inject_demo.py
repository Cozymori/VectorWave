import sys
import os
import time

# Set src path
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)
src_path = os.path.join(os.path.dirname(current_dir), 'src')
sys.path.insert(0, src_path)

project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from vectorwave import initialize_database, VectorWaveAutoInjector, generate_and_register_metadata


def main():
    print("🚀 Starting Real-World Auto-Injection Demo...")

    if not initialize_database():
        return

    # 1. Global configuration (auto=True: Save later in batch)
    VectorWaveAutoInjector.configure(
        auto=True,
        team="billing-team",
        priority=1
    )

    print("\n--- 1. Injecting Modules ---")
    # At injection time, the module is imported and function metadata is collected in memory.
    VectorWaveAutoInjector.inject("test_ex.pure_logic")

    # =========================================================
    # [Moved] Save to DB before running logic!
    # =========================================================
    print("\n--- 2. Registering Metadata (Pre-flight) ---")
    generate_and_register_metadata()
    print("✅ Function Metadata Saved! (Ready to execute)")

    print("\n--- 3. Importing & Running Business Logic ---")
    # Since it's already injected, just import and use it.
    import test_ex.pure_logic as my_logic

    # Now execution logs (Executions) will be recorded.
    result = my_logic.process_payment("user_real_world", 200)
    print(f"Result: {result}")
    time.sleep(5)

    print("\n✨ Done! All traces captured.")


if __name__ == "__main__":
    main()
