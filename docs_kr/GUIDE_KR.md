
# 🌊 VectorWave 상세 사용 가이드

**VectorWave**는 파이썬 함수의 입력/출력 및 실행 흐름을 벡터 데이터베이스(Weaviate)에 자동으로 저장하고, 이를 기반으로 검색(RAG), 모니터링, 테스트 자동화를 지원하는 프레임워크입니다.

-----

## 1\. 시작하기 (Getting Started)

### 1.1 설치 및 필수 요구사항

VectorWave는 Weaviate 데이터베이스에 의존합니다. 프로젝트 루트에 `docker-compose` 파일이 포함되어 있습니다.

1.  **Weaviate 실행:**
    업로드된 `vw_docker.yml` 파일을 사용하여 Weaviate 컨테이너를 실행합니다.

    ```bash
    docker-compose -f test_ex/vw_docker.yml up -d
    ```

2.  **환경 변수 설정 (.env):**
    프로젝트 루트 또는 실행 스크립트 위치에 `.env` 파일을 생성합니다. 벡터화 방식(`huggingface` 또는 `openai_client`)을 선택해야 합니다.

    ```ini
    # .env 예시
    WEAVIATE_HOST=localhost
    WEAVIATE_PORT=8080
    WEAVIATE_GRPC_PORT=50051

    # 옵션 1: HuggingFace (로컬 실행, API 키 불필요)
    VECTORIZER="huggingface"
    HF_MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2"

    # 옵션 2: OpenAI (고성능, API 키 필요)
    # VECTORIZER="openai_client"
    # OPENAI_API_KEY="sk-..."

    # 커스텀 태그 설정 파일 경로
    CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
    ```

3.  **DB 초기화:**
    코드 실행 시 최초 1회 초기화가 필요합니다.

    ```python
    from vectorwave import initialize_database
    client = initialize_database()
    ```

-----

## 2\. 핵심 기능: 함수 벡터화 (@vectorize)

함수의 소스코드와 실행 결과를 자동으로 저장하려면 `@vectorize` 데코레이터를 사용합니다.

### 사용법

```python
from vectorwave import vectorize

@vectorize(
    search_description="사용자 결제를 처리하는 함수",  # 검색을 위한 설명
    sequence_narrative="결제 검증 후 영수증을 발송함", # 전후 맥락 설명
    team="billing",     # 커스텀 태그 (실행 로그에 저장됨)
    priority=1          # 커스텀 태그
)
def process_payment(user_id: str, amount: int):
    # 비즈니스 로직
    return {"status": "success"}
```

* **정적 저장:** 함수 정의 시 소스코드와 설명이 `VectorWaveFunctions` 컬렉션에 저장됩니다.
* **동적 로깅:** 함수 실행 시 입력값, 반환값, 성공/실패 여부, 실행 시간이 `VectorWaveExecutions` 컬렉션에 저장됩니다.

-----

## 3\. 모니터링: 분산 추적 (Distributed Tracing)

복잡한 호출 흐름을 하나의 `trace_id`로 묶어서 추적할 수 있습니다. 메인 함수에는 `@vectorize`를, 하위 함수에는 `@trace_span`을 사용합니다.

### 사용법

```python
from vectorwave import vectorize, trace_span

@trace_span(attributes_to_capture=['user_id']) # user_id 인자를 로그에 남김
def validate_user(user_id):
    pass

@trace_span
def send_email():
    pass

@vectorize(search_description="회원가입 처리") # Root Span 역할
def signup_workflow(user_id):
    validate_user(user_id) # 자동으로 부모의 trace_id를 상속받음
    send_email()
```

### 추적 로그 검색

특정 `trace_id`에 속한 모든 실행 로그를 시간순으로 조회할 수 있습니다.

```python
from vectorwave.search.execution_search import find_by_trace_id

logs = find_by_trace_id(trace_id="your-trace-id-uuid")
for log in logs:
    print(f"{log['function_name']} -> {log['status']}")
```

-----

## 4\. 성능 최적화: 시맨틱 캐싱 (Semantic Caching)

입력값의 의미(Vector Similarity)가 유사하면 함수를 실행하지 않고 캐시된 결과를 반환합니다. 비용이 높은 연산(예: LLM 호출)에 유용합니다.

### 설정 방법

```python
@vectorize(
    semantic_cache=True,      # 캐싱 활성화
    cache_threshold=0.95,     # 유사도 95% 이상일 때만 캐시 히트
    capture_return_value=True # 반환값 저장은 필수
)
def expensive_llm_task(prompt: str):
    # ... 고비용 연산 ...
    return result
```

* `prompt`가 이전에 요청된 내용과 의미적으로 유사하면, 실제 함수 실행을 건너뛰고 DB에 저장된 값을 즉시 반환합니다.

-----

## 5\. 자동화 기능: AI 문서화 & 자가 치유

### 5.1 AI 자동 문서화 (Auto-Doc)

함수 설명을 수동으로 적지 않고 LLM이 코드를 분석해 생성하게 합니다.

1.  함수에 `auto=True` 설정:
    ```python
    @vectorize(auto=True)
    def complex_algorithm(data):
        """복잡한 알고리즘을 수행하는 함수"""
        pass
    ```
2.  스크립트 마지막에 생성 함수 호출:
    ```python
    from vectorwave import generate_and_register_metadata
    generate_and_register_metadata() # 보류된 함수들의 메타데이터를 일괄 생성
    ```

### 5.2 자가 치유 (Healer)

함수에서 에러가 발생했을 때, 과거의 성공 로그와 현재 에러 로그를 비교하여 수정 코드를 제안합니다.

```python
from vectorwave import VectorWaveHealer

healer = VectorWaveHealer(model="gpt-4-turbo")
suggestion = healer.diagnose_and_heal(function_name="buggy_function", lookback_minutes=60)
print(suggestion) # 수정된 코드 제안 출력
```

-----

## 6\. 테스트: 리플레이 & 회귀 테스트 (Replay)

운영 환경에서 실행된 실제 데이터를 사용하여 로직 변경 후에도 결과가 동일한지 검증합니다.

1.  **데이터 수집:** `@vectorize(replay=True)`로 설정하여 입력/출력을 저장합니다.
2.  **테스트 실행:**
    ```python
    from vectorwave import VectorWaveReplayer

    replayer = VectorWaveReplayer()
    # 과거 10개의 성공 케이스를 가져와 현재 코드값으로 재실행 및 비교
    result = replayer.replay("module_name.function_name", limit=10)

    print(f"Passed: {result['passed']}, Failed: {result['failed']}")
    ```
3.  **Baseline 업데이트:** 로직 변경이 의도된 것이라면 `update_baseline=True`로 DB의 정답 데이터를 갱신할 수 있습니다.

-----

## 7\. RAG & 검색 (Search)

저장된 함수 코드나 실행 로그를 자연어(한국어/영어)로 검색하고 분석할 수 있습니다.

* **코드 검색 및 질문:**
  ```python
  from vectorwave import search_and_answer
  answer = search_and_answer("결제 처리 로직이 어떻게 돼?", language='ko')
  ```
* **실행 로그 분석:**
  ```python
  from vectorwave import analyze_trace_log
  analysis = analyze_trace_log(trace_id="...", language='ko')
  ```

-----

## 8\. 설정 심화: 커스텀 속성 및 알림

* **커스텀 속성:** `.weaviate_properties` 파일에 `team`, `project_id` 등의 필드를 정의하면, 데코레이터나 환경 변수(`TEAM=backend`)를 통해 자동으로 태깅됩니다.
* **실시간 알림 (Webhook):** 에러 발생 시 Discord 등으로 알림을 보내려면 `.env`에 다음을 추가합니다.
  ```ini
  ALERTER_STRATEGY="webhook"
  ALERTER_WEBHOOK_URL="https://discord.com/api/webhooks/..."
  ```

이 가이드는 `src/vectorwave` 내부의 핵심 모듈들과 `test_ex` 폴더의 예제 코드를 기반으로 작성되었습니다.