import time
import os
import sys
import logging
from unittest.mock import MagicMock, patch

# 프로젝트 루트 경로 추가 (모듈 임포트용)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src')))

from vectorwave import vectorize
from vectorwave.models.db_config import get_weaviate_settings
from vectorwave.vectorizer.factory import get_vectorizer

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Benchmark")

# --- 1. 느린 Vectorizer 시뮬레이션 (네트워크 지연 1초 가정) ---
class SlowVectorizer:
    def embed(self, text):
        time.sleep(1.0)  # 1초 지연 (OpenAI API 호출 시뮬레이션)
        return [0.1] * 1536

# Vectorizer를 Mocking하여 강제로 교체
mock_vectorizer = SlowVectorizer()

@patch('vectorwave.monitoring.tracer.get_vectorizer', return_value=mock_vectorizer)
@patch('vectorwave.core.decorator.get_vectorizer', return_value=mock_vectorizer)
def run_benchmark(mock_get_vec1, mock_get_vec2):
    settings = get_weaviate_settings()

    # 테스트할 타겟 함수
    @vectorize(search_description="Test function", capture_return_value=True)
    def my_fast_function(x, y):
        return x + y

    print("\n" + "="*50)
    print("🚀  VectorWave Zero-Latency Benchmark")
    print("="*50)
    print(f"[*] 시뮬레이션: Vectorizer 임베딩 시간 1.0초 고정\n")

    # --- Case 1: 동기 모드 (Sync) ---
    settings.ASYNC_LOGGING = False
    print("🔴 1. [동기 모드] 실행 (ASYNC_LOGGING=False)")

    start_time = time.perf_counter()
    result = my_fast_function(10, 20)
    end_time = time.perf_counter()

    sync_duration = end_time - start_time
    print(f"   -> 실행 시간: {sync_duration:.4f}초 (예상: 1초 이상)")
    print(f"   -> 결과: {result}")

    # --- Case 2: 비동기 모드 (Async) ---
    settings.ASYNC_LOGGING = True
    print("\n🟢 2. [비동기 모드] 실행 (ASYNC_LOGGING=True)")

    start_time = time.perf_counter()
    result = my_fast_function(30, 40)
    end_time = time.perf_counter()

    async_duration = end_time - start_time
    print(f"   -> 실행 시간: {async_duration:.4f}초 (예상: 0.1초 미만)")
    print(f"   -> 결과: {result}")

    # --- 결론 ---
    print("\n" + "-"*50)
    if async_duration < 0.1 and sync_duration > 1.0:
        print("✅  SUCCESS: 비동기 로깅이 정상 작동하여 지연 시간이 제거되었습니다!")
        print(f"🚀  속도 향상: {sync_duration / async_duration:.1f}배 더 빠름")
    else:
        print("❌  FAILURE: 비동기 로깅이 적용되지 않았거나 오버헤드가 큽니다.")
    print("="*50 + "\n")

    # 백그라운드 스레드가 작업을 마칠 때까지 잠시 대기 (로그 확인용)
    print("[*] 백그라운드 작업 완료 대기 중 (2초)...")
    time.sleep(7)

if __name__ == "__main__":
    # DB 연결 설정을 무시하거나 Mocking이 필요할 수 있으나,
    # 여기서는 로직 흐름만 테스트하므로 DB 에러는 로그로만 찍히고 넘어갑니다.
    run_benchmark()