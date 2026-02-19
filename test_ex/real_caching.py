import sys
import os
import time
import statistics

# --- 1. Path Setup ---
current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)

# --- 2. VectorWave Import ---
from vectorwave import vectorize, initialize_database
from vectorwave.database.db import get_cached_client

# 경고 끄기
os.environ["TOKENIZERS_PARALLELISM"] = "false"

# DB 초기화
client = initialize_database()

# ==========================================
# 🧪 실험 대상 함수 정의 (아주 가벼운 연산)
# ==========================================

# 1. 순수 파이썬 함수 (기준점)
def pure_function(x: int):
    return x * x

# 2. VectorWave 적용 함수 (오버헤드 측정용)
@vectorize(unique_key="overhead_test_v1")
def vw_function(x: int):
    return x * x

# ==========================================
# 🏃‍♂️ 벤치마크 실행 로직
# ==========================================

def run_benchmark(label, func, input_val, count=10000):
    print(f"\n🚀 [{label}] 테스트 시작 ({count}회 반복)...")

    times = []

    # 예열 (Warm-up): 첫 실행은 DB 인덱싱/초기화 때문에 오래 걸리므로 제외할 수도 있음
    # 여기서는 "첫 실행(Miss)"과 "이후 실행(Hit)"을 구분하지 않고 전체 평균을 봅니다.
    # 단, VectorWave는 첫 실행 후 2초 대기하여 캐시를 확실히 만듭니다.

    print("   🔥 예열 중 (First Run)...")
    func(input_val)
    if "VectorWave" in label:
        time.sleep(2) # DB 인덱싱 대기

    print("   ⏱️ 측정 시작...")
    for i in range(count):
        start = time.time()
        func(input_val) # 실행
        end = time.time()
        times.append(end - start)

        # 진행 상황 표시 (10번마다)
        if (i+1) % 10 == 0:
            print(f".", end="", flush=True)

    avg_time = statistics.mean(times)
    min_time = min(times)
    max_time = max(times)

    print(f"\n   ✅ 완료!")
    print(f"   - 평균 소요 시간: {avg_time:.6f}초")
    print(f"   - 최소 소요 시간: {min_time:.6f}초")
    print(f"   - 최대 소요 시간: {max_time:.6f}초")

    return avg_time

# ==========================================
# 🏁 메인 실행
# ==========================================

if __name__ == "__main__":
    try:
        INPUT_VALUE = 42
        ITERATIONS = 50 # 50번만 해도 충분함 (네트워크 통신이라)

        # 1. 순수 파이썬 측정
        t_pure = run_benchmark("Pure Python", pure_function, INPUT_VALUE, ITERATIONS)

        # 2. VectorWave 측정 (캐시 히트 상황)
        t_vw = run_benchmark("VectorWave (Cache Hit)", vw_function, INPUT_VALUE, ITERATIONS)

        # 3. 결론 도출
        print("\n" + "="*50)
        print("📊 오버헤드(Overhead) 분석 결과")
        print("="*50)
        print(f"1. 순수 함수 실행 시간 : {t_pure:.6f}s (거의 0초)")
        print(f"2. VectorWave 실행 시간: {t_vw:.6f}s (네트워크/DB 비용)")

        overhead = t_vw - t_pure
        print("-" * 50)
        print(f"💡 VectorWave 기본 오버헤드: 약 {overhead:.4f}초")
        print("   (이 시간은 임베딩 생성 + 벡터 DB 검색에 걸리는 '최소 비용'입니다.)")
        print("-" * 50)

        threshold = 0.5 # 0.5초 기준
        if overhead < threshold:
            print(f"✅ 결론: 오버헤드가 {overhead:.4f}초로 매우 적습니다.")
            print(f"   따라서, 실행 시간이 {threshold}초 이상 걸리는 LLM/API 함수에 쓰면 무조건 이득입니다!")
        else:
            print(f"⚠️ 결론: 오버헤드가 {overhead:.4f}초로 다소 높습니다. 네트워크 상태를 확인하세요.")

    finally:
        get_cached_client().close()