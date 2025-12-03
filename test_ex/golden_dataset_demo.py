import sys
import os
import time

# --- 경로 설정 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
src_path = os.path.abspath(os.path.join(current_dir, "../src"))

if src_path not in sys.path:
    sys.path.insert(0, src_path)

# --- 모듈 임포트 ---
from vectorwave import initialize_database
from vectorwave.utils.replayer_semantic import SemanticReplayer
from vectorwave.database.dataset import VectorWaveDatasetManager
from vectorwave.search.execution_search import search_executions

# 기존에 정의된 함수 사용 (새로 만들지 않음)
from test_ex.example import generate_review_summary

def run_test():
    print("🚀 Starting Golden Dataset Test (Integration)...")

    # 1. DB 초기화
    client = initialize_database()
    if not client:
        print("❌ DB Connection Failed.")
        return

    dataset_manager = VectorWaveDatasetManager()

    # 테스트 대상 함수 (기존 example.py에 있는 함수)
    target_func_name = "test_ex.example.generate_review_summary"
    short_func_name = "generate_review_summary"

    print(f"\n🎯 Target Function: {target_func_name}")

    # 2. 데이터 생성 (로그가 없을 경우를 대비해 실행)
    print("\n[Step 1] Generating Execution Logs...")
    sample_review = "The product quality is amazing and delivery was super fast!"
    generate_review_summary(review_text=sample_review)

    print("  ⏳ Waiting 4s for async indexing...")
    time.sleep(4)

    # 3. Golden Data 등록
    print("\n[Step 2] Registering Golden Data...")

    # 최신 로그 조회
    logs = search_executions(limit=5, filters={"function_name": short_func_name})
    target_log = next((log for log in logs if log.get('review_text') == sample_review), None)

    if target_log:
        log_uuid = target_log['uuid']
        print(f"  -> Found Log UUID: {log_uuid}")

        success = dataset_manager.register_as_golden(
            log_uuid=log_uuid,
            note="Best practice review summary",
            tags=["golden-test"]
        )
        if success:
            print("  ✅ Log registered as Golden Data.")
        else:
            print("  ❌ Failed to register Golden Data.")
    else:
        print("  ⚠️ Target log not found. Skipping registration.")

    # 4. Semantic Replayer 실행 (Golden Data 우선순위 확인)
    print("\n[Step 3] Running Semantic Replay (Golden Priority Check)")
    print("  -> Watch the logs for 'Golden Data test cases' or '[GOLDEN]' tag.")

    try:
        replayer = SemanticReplayer()

        # LLM 의미론적 비교 (Golden Data가 포함되어 테스트되는지 확인)
        result = replayer.replay(
            target_func_name,
            limit=5,
            semantic_eval=True  # LLM 평가 활성화
        )

        print(f"\n📊 Result: Total {result['total']} | ✅ Passed {result['passed']} | ❌ Failed {result['failed']}")

        # 실패 목록에서 Golden 여부 확인
        for fail in result.get('failures', []):
            is_golden = fail.get('is_golden', False)
            print(f"   - Fail UUID: {fail['uuid']} (Golden: {is_golden})")

    except Exception as e:
        print(f"❌ Replay Failed: {e}")

if __name__ == "__main__":
    run_test()