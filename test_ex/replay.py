import sys
import os
import time
current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)
src_path = os.path.abspath(os.path.join(current_dir, "../src"))

if src_path not in sys.path:
    sys.path.insert(0, src_path)

from vectorwave.utils.replayer import VectorWaveReplayer


def run_replay_test():
    print("🚀 [Replay Test] Starting regression test for example.py functions...")

    replayer = VectorWaveReplayer()

    # Target module path to test (where example.py is located)
    # Note: The main logic of example.py may execute when importing this file.
    target_module = "test_ex.example"

    # -----------------------------------------------------
    # Test 1: validate_payment (Unit Function Test)
    # -----------------------------------------------------
    func_full_name = f"{target_module}.step_1_validate_payment"
    print(f"\n▶️ Testing '{func_full_name}'...")

    # Find and execute 'step_1_validate_payment' logs from the DB
    result_1 = replayer.replay(func_full_name, limit=10)
    print_summary(result_1)

    # -----------------------------------------------------
    # Test 2: process_payment (Main Business Logic Test)
    # -----------------------------------------------------
    # Since this function calls step_1, step_2, etc. internally,
    # verification is performed to see if this flow is reproduced during Replay.
    func_full_name = f"{target_module}.process_payment"
    print(f"\n▶️ Testing '{func_full_name}'...")

    result_2 = replayer.replay(func_full_name, limit=10)
    print_summary(result_2)

    # -----------------------------------------------------
    # Test 3: generate_report (Simple Return Value Test)
    # -----------------------------------------------------
    func_full_name = f"{target_module}.generate_report"
    print(f"\n▶️ Testing '{func_full_name}'...")

    result_3 = replayer.replay(func_full_name, limit=10)
    print_summary(result_3)

def print_summary(result):
    if "error" in result:
        print(f"   ❌ Error: {result['error']}")
        return

    total = result.get('total', 0)
    passed = result.get('passed', 0)
    failed = result.get('failed', 0)

    print(f"   Result: Total {total} | ✅ Passed: {passed} | ❌ Failed: {failed}")

    if failed > 0:
        print("   🔍 Detailed Failure Information:")
        for fail in result.get('failures', []):
            print(f"      - UUID: {fail.get('uuid')}")
            print(f"      - Expected: {fail.get('expected')}")
            print(f"      - Actual:   {fail.get('actual')}")
            if 'error' in fail:
                print(f"      - Error Msg: {fail['error']}")

if __name__ == "__main__":
    # Add the parent directory to the path to recognize the example.py path as a Python package
    # (Set the vectorwave folder as the package root)
    project_root = os.path.abspath(os.path.join(current_dir, ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    try:
        run_replay_test()
        time.sleep(10)
    except Exception as e:
        print(f"\n❌ Error during test execution: {e}")