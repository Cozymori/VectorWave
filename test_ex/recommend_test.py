import sys
import os
import time
from dotenv import load_dotenv

# --- 경로 설정 ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
os.environ["RECOMMENDATION_STEADY_MARGIN"] = "0.25"
os.environ["RECOMMENDATION_DISCOVERY_MARGIN"] = "0.40"

from vectorwave import vectorize, initialize_database
from vectorwave.database.dataset import VectorWaveDatasetManager
from vectorwave.search.execution_search import search_executions
from vectorwave.database.db import get_cached_client



# ✅ [핵심] capture_return_value=True가 있어야 벡터가 생성됩니다.
@vectorize(
    search_description="Golden Dataset Recommendation Test",
    team="qa-team",
    attributes_to_capture=['query'],
    capture_return_value=True
)
def golden_test_func(query: str):
    print(f"  [EXEC] Processing: '{query}'")
    time.sleep(0.05)
    return f"Result: {query}"

def run_recommendation_test():
    print("=" * 60)
    print("🧪 Density-Based Recommendation System Test (Multi-Data)")
    print("=" * 60)

    # 1. 초기화
    client = initialize_database()
    if not client:
        print("❌ DB Connection Failed.")
        return

    dataset_manager = VectorWaveDatasetManager()
    target_func = "golden_test_func"

    # 2. Golden Data 확인 (기준점: "Standard guide for usage")
    print("\n[Step 1] Checking Golden Data Baseline...")
    from vectorwave.models.db_config import get_weaviate_settings
    settings = get_weaviate_settings()
    golden_col = client.collections.get(settings.GOLDEN_COLLECTION_NAME)

    try:
        check = golden_col.query.fetch_objects(limit=1)
        if not check.objects:
            print("  ⚠️ No Golden Data found. Creating a baseline...")
            baseline_query = "Standard guide for usage"
            golden_test_func(baseline_query)
            print("  ⏳ Waiting 2s for indexing...")
            time.sleep(2)

            logs = search_executions(limit=1, filters={"function_name": target_func})
            if logs:
                dataset_manager.register_as_golden(logs[0]['uuid'], note="Baseline for test")
                print("  ✅ Baseline registered: 'Standard guide for usage'")
            else:
                return
        else:
            print("  ✅ Golden Data found. Using existing baseline.")

    except Exception as e:
        print(f"  ❌ Error: {e}")
        return

    # 3. 다량의 테스트 데이터 생성
    print("\n[Step 2] Generating Multiple Candidate Logs...")

    test_scenarios = [
        # [Group A] Steady: 기준점과 매우 유사한 문장들 (예상: STEADY or DISCOVERY)
        "Standard usage manual for beginners",
        "Guide for standard operational usage",
        "Basic instructions for usage guide",

        # [Group B] Discovery: 관련은 있지만 주제가 조금 다른 기술 문장들 (예상: DISCOVERY)
        "Advanced vector search optimization techniques",
        "Database connection timeout troubleshooting",
        "System performance tuning guide",
        "API authentication protocol v2",

        # [Group C] Ignore: 완전히 쌩뚱맞은 문장들 (예상: 제외됨)
        "Delicious pepperoni pizza recipe with extra cheese",
        "The weather in Seoul is sunny today",
        "Movie review: The latest superhero film was amazing"
    ]

    for i, query in enumerate(test_scenarios):
        print(f"  ({i+1}/{len(test_scenarios)}) Generating log: '{query[:40]}...'")
        golden_test_func(query)

    print(f"  ⏳ Waiting 5s for embedding generation & indexing...")
    time.sleep(5)

    # 4. 추천 실행
    print("\n[Step 3] Running Recommendation Engine...")

    try:
        recommendations = []
        for attempt in range(3):
            print(f"  🔎 Analyzing candidates (Attempt {attempt+1}/3)...")
            # limit를 20으로 늘려 모든 결과를 확인
            recommendations = dataset_manager.recommend_candidates(target_func, limit=20)
            if recommendations:
                break
            time.sleep(2)

        if not recommendations:
            print("  -> No recommendations found.")
        else:
            print(f"\n  📊 Recommendation Results ({len(recommendations)} found):")
            print(f"  {'Type':<12} | {'Dist':<8} | {'Input Query'}")
            print("-" * 70)

            # 보기 좋게 거리순 정렬
            recommendations.sort(key=lambda x: x['distance_to_center'])

            for rec in recommendations:
                rec_type = rec['type']
                dist = rec['distance_to_center']
                ret_val = rec.get('return_value', '')
                # "Result: " 제거하고 깔끔하게 출력
                input_text = ret_val.replace("Result: ", "").replace('"', '')

                icon = "🟢" if rec_type == "STEADY" else "🔵" if rec_type == "DISCOVERY" else "⚪"
                print(f"  {icon} {rec_type:<9} | {dist:.4f}   | {input_text}")

            print("-" * 70)
            print("  * 🟢 STEADY: 기존 패턴과 유사 (안정적)")
            print("  * 🔵 DISCOVERY: 새로운 패턴 발견 (유의미한 변화)")
            print("  * (목록에 없음): IGNORE (관련 없음)")

    except Exception as e:
        print(f"❌ Error during recommendation: {e}")

    print("\n✨ Test Completed.")

if __name__ == "__main__":
    load_dotenv()
    run_recommendation_test()