import sys
import os
import time

current_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(current_dir)
src_path = os.path.abspath(os.path.join(current_dir, "../src"))

if src_path not in sys.path:
    sys.path.insert(0, src_path)

project_root = os.path.abspath(os.path.join(current_dir, ".."))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from vectorwave.utils.replayer_semantic import SemanticReplayer

def run_test():
    print("🚀 Starting Semantic Replay Test (LLM Text Mode)...")

    try:
        replayer = SemanticReplayer()
    except Exception as e:
        print(f"❌ Initialization Failed: {e}")
        return

    target_func = "test_ex.example.generate_review_summary"

    print(f"\n🎯 Target Function: {target_func}")
    print("   (This function returns slightly different text every time, simulating an LLM)")

    print(f"\n[Test 1] Vector Similarity Check")
    res_vec = replayer.replay(
        target_func,
        limit=5,
        similarity_threshold=0.85
    )
    print(f"Result: ✅ Passed {res_vec['passed']} / ❌ Failed {res_vec['failed']}")

    # 2. LLM 의미론적 판단 테스트
    print(f"\n[Test 2] LLM Semantic Check")
    res_llm = replayer.replay(
        target_func,
        limit=5,
        semantic_eval=True
    )
    print(f"Result: ✅ Passed {res_llm['passed']} / ❌ Failed {res_llm['failed']}")

if __name__ == "__main__":
    run_test()
