# VectorWave: Seamless Auto-Vectorization Framework

[](https://www.google.com/search?q=LICENSE)

## 🌟 프로젝트 소개 (Overview)

**VectorWave**는 파이썬 함수/메서드의 출력을 **데코레이터**를 사용하여 자동으로 **벡터 데이터베이스(Vector DB)**에 저장하고 관리하는 혁신적인 프레임워크입니다. 개발자는 데이터 수집, 임베딩 생성, 벡터 DB 저장의 복잡한 과정을 신경 쓸 필요 없이, 단 한 줄의 코드(`@vectorize`)로 함수 출력을 지능적인 벡터 데이터로 변환할 수 있습니다.

---

## ✨ 주요 특징 (Features)

* **`@vectorize` 데코레이터:**
  1.  **정적 데이터 수집:** 스크립트 로드 시, 함수의 소스 코드, 독스트링, 메타데이터를 `VectorWaveFunctions` 컬렉션에 1회 저장합니다.
  2.  **동적 데이터 로깅:** 함수가 호출될 때마다 실행 시간, 성공/실패 상태, 에러 로그, 그리고 '동적 태그'를 `VectorWaveExecutions` 컬렉션에 기록합니다.
* **시맨틱 캐싱 및 성능 최적화 (Semantic Caching and Performance Optimization):**
    * 함수 입력의 의미적 유사성(semantic similarity)을 기반으로 캐시 적중(cache hit)을 판별하여, 동일하거나 매우 유사한 입력에 대한 함수 실행을 우회하고 저장된 결과를 즉시 반환합니다.
    * 이는 특히 고비용 계산 함수(예: LLM 호출, 복잡한 데이터 처리)의 **실행 지연 시간(latency)을 크게 단축**하고 비용을 절감하는 데 효과적입니다.
* **분산 추적 (Distributed Tracing):** `@vectorize`와 `@trace_span` 데코레이터를 결합하여 복잡한 다단계 워크플로우의 실행을 하나의 **`trace_id`**로 묶어 분석할 수 있습니다.
* **검색 인터페이스:** 저장된 벡터 데이터(함수 정의)와 로그(실행 기록)를 검색하는 `search_functions` 및 `search_executions` 함수를 제공하여 RAG 및 모니터링 시스템 구축을 용이하게 합니다.

---

## 🚀 사용법 (Usage)

VectorWave는 데코레이터를 통한 '저장'과 함수를 통한 '검색'으로 구성되며, 이제 **실행 흐름 추적** 기능이 포함됩니다.

### 1. (필수) 데이터베이스 초기화 및 설정

```python
import time
from vectorwave import (
    vectorize, 
    initialize_database, 
    search_functions, 
    search_executions
)
# [추가] 분산 추적을 위해 trace_span을 별도로 임포트합니다.
from vectorwave.monitoring.tracer import trace_span 

# 스크립트 시작 시 한 번만 호출하면 됩니다.
try:
    client = initialize_database()
    print("VectorWave DB 초기화 성공.")
except Exception as e:
    print(f"DB 초기화 실패: {e}")
    exit()
````

### 2\. [저장] `@vectorize`와 분산 추적 사용

`@vectorize`는 트레이싱의 **루트(Root)** 역할을 수행하며, 내부 함수에 `@trace_span`을 적용하여 워크플로우 실행을 \*\*하나의 `trace_id`\*\*로 묶습니다.

```python
# --- 하위 스팬 함수: 인자를 캡처합니다 ---
@trace_span(attributes_to_capture=['user_id', 'amount'])
def step_1_validate_payment(user_id: str, amount: int):
    """(스팬) 결제 유효성 검사. user_id와 amount를 로그에 기록합니다."""
    print(f"  [SPAN 1] Validating payment for {user_id}...")
    time.sleep(0.1)
    return True

@trace_span(attributes_to_capture=['user_id', 'receipt_id'])
def step_2_send_receipt(user_id: str, receipt_id: str):
    """(스팬) 영수증 발송."""
    print(f"  [SPAN 2] Sending receipt {receipt_id}...")
    time.sleep(0.2)


# --- 루트 함수 (@trace_root 역할) ---
@vectorize(
    search_description="사용자 결제를 처리하고 영수증을 반환합니다.",
    sequence_narrative="결제가 완료되면 이메일로 영수증이 발송됩니다.",
    team="billing",  # ⬅️ 커스텀 태그 (모든 실행 로그에 기록됨)
    priority=1       # ⬅️ 커스텀 태그 (실행 중요도)
)
def process_payment(user_id: str, amount: int):
    """(루트 스팬) 사용자 결제 워크플로우를 실행합니다."""
    print(f"  [ROOT EXEC] process_payment: Starting workflow for {user_id}...")
    
    # 하위 함수 호출 시, 동일한 trace_id가 ContextVar를 통해 자동으로 상속됩니다.
    step_1_validate_payment(user_id=user_id, amount=amount) 
    
    receipt_id = f"receipt_{user_id}_{amount}"
    step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)

    print(f"  [ROOT DONE] process_payment")
    return {"status": "success", "receipt_id": receipt_id}

# --- 함수 실행 ---
print("Now calling 'process_payment'...")
# 이 하나의 호출은 DB에 총 3개의 실행 로그(스팬)를 기록하며,
# 세 로그는 하나의 'trace_id'로 묶입니다.
process_payment("user_789", 5000)
```

#### 시맨틱 캐싱 활용 예시 (Semantic Caching Example)

함수 입력이 유사할 경우 재실행을 방지하고 캐시된 결과를 반환하도록 설정합니다.

```python
from vectorwave import vectorize
import time

@vectorize(
    search_description="LLM을 이용한 고비용 요약 작업",
    sequence_narrative="LLM Summarization Step",
    semantic_cache=True,            # 캐싱 활성화
    cache_threshold=0.95,           # 95% 이상 유사할 경우 캐시 적중
    capture_return_value=True       # 캐싱을 위해 반환 값 저장 필수
)
def summarize_document(document_text: str):
    # 실제 LLM 호출 또는 고비용 계산 로직 (예: 0.5초 지연)
    time.sleep(0.5)
    print("--- [Cache Miss] Document is being summarized by LLM...")
    return f"Summary of: {document_text[:20]}..."

# 첫 번째 호출 (Cache Miss) - 0.5초 소요, DB에 결과 저장
result_1 = summarize_document("The first quarter results showed strong growth in Europe and Asia...")

# 두 번째 호출 (Cache Hit) - 0.0초 소요, 캐시된 값 반환
# "Q1 results"가 "first quarter results"와 의미적으로 유사하여 캐시에 적중될 수 있습니다.
result_2 = summarize_document("The Q1 results demonstrated strong growth in Europe and Asia...") 

# result_2는 실제 함수 실행 없이 result_1의 저장된 값을 반환합니다.
```

### 3\. [검색 ①] 함수 정의 검색 (RAG 용도)

```python
# '결제'와 관련된 함수를 자연어(벡터)로 검색합니다.
print("\n--- '결제' 관련 함수 검색 ---")
payment_funcs = search_functions(
    query="사용자 결제 처리 기능",
    limit=3
)
for func in payment_funcs:
    print(f"  - 함수명: {func['properties']['function_name']}")
    print(f"  - 설명: {func['properties']['search_description']}")
    print(f"  - 유사도(거리): {func['metadata'].distance:.4f}")
```

### 4\. [검색 ②] 실행 로그 검색 (모니터링 및 추적)

`search_executions` 함수는 이제 `trace_id`를 기준으로 관련된 모든 실행 로그(스팬)를 검색할 수 있습니다.

```python
# 1. 특정 워크플로우(process_payment)의 Trace ID를 찾습니다.
latest_payment_span = search_executions(
    limit=1, 
    filters={"function_name": "process_payment"},
    sort_by="timestamp_utc",
    sort_ascending=False
)
trace_id = latest_payment_span[0]["trace_id"] 

# 2. 해당 Trace ID에 속한 모든 스팬을 시간순으로 검색합니다.
print(f"\n--- Trace ID ({trace_id[:8]}...) 전체 추적 ---")
trace_spans = search_executions(
    limit=10,
    filters={"trace_id": trace_id},
    sort_by="timestamp_utc",
    sort_ascending=True # 워크플로우 흐름 분석을 위해 오름차순 정렬
)

for i, span in enumerate(trace_spans):
    print(f"  - [Span {i+1}] {span['function_name']} ({span['duration_ms']:.2f}ms)")
    # 하위 스팬의 캡처된 인자(user_id, amount 등)도 함께 표시됩니다.
    
# 예상 결과:
# - [Span 1] step_1_validate_payment (100.81ms)
# - [Span 2] step_2_send_receipt (202.06ms)
# - [Span 3] process_payment (333.18ms)
```

-----

## ⚙️ 설정 (Configuration)

VectorWave는 Weaviate 데이터베이스 연결 정보와 **벡터화 전략**을 **환경 변수** 또는 `.env` 파일을 통해 자동으로 읽어옵니다.

라이브러리를 사용하는 당신의 프로젝트 루트 디렉터리(예: `test_ex/example.py`가 있는 곳)에 `.env` 파일을 생성하고 필요한 값들을 설정하세요.

### 벡터화 전략 설정 (VECTORIZER)

`test_ex/.env` 파일의 `VECTORIZER` 환경 변수 설정을 통해 텍스트 벡터화 방식을 선택할 수 있습니다.

| `VECTORIZER` 설정 | 설명 | 필요한 추가 설정 |
| :--- | :--- | :--- |
| **`huggingface`** | (기본 권장) 로컬 CPU에서 `sentence-transformers` 라이브러리를 사용해 벡터화합니다. API 키가 필요 없어 즉시 테스트 가능합니다. | `HF_MODEL_NAME` (예: "sentence-transformers/all-MiniLM-L6-v2") |
| **`openai_client`** | (고성능) OpenAI Python 클라이언트를 사용하여 `text-embedding-3-small` 같은 최신 모델로 벡터화합니다. | `OPENAI_API_KEY` (유효한 OpenAI API 키) |
| **`weaviate_module`** | (Docker 위임) 벡터화 작업을 Weaviate 도커 컨테이너의 내장 모듈 (예: `text2vec-openai`)에 위임합니다. | `WEAVIATE_VECTORIZER_MODULE`, `OPENAI_API_KEY` |
| **`none`** | 벡터화를 수행하지 않습니다. 데이터는 벡터 없이 저장됩니다. | 없음 |

#### ⚠️ 시맨틱 캐싱 필수 조건 및 설정

`semantic_cache=True`를 사용하려면 다음 조건이 충족되어야 합니다.

* **벡터라이저 필수:** 라이브러리 설정(`VECTORIZER` 환경 변수)에서 **Python 기반의 벡터라이저** (`huggingface` 또는 `openai_client`)가 구성되어 있어야 합니다. `weaviate_module` 또는 `none` 설정 시 캐싱이 자동으로 비활성화됩니다.
* **반환 값 캡처 필수:** `semantic_cache=True` 활성화 시 `capture_return_value` 매개변수는 자동으로 `True`로 설정됩니다.

### .env 파일 적용 예시

사용하려는 전략에 맞춰 `.env` 파일의 내용을 구성하세요.

#### 예시 1: `huggingface` 사용 (로컬, API 키 불필요)

로컬 머신에서 `sentence-transformers` 모델을 사용합니다. API 키가 필요 없어 즉시 테스트에 용이합니다.

```ini
# .env (HuggingFace 사용 시)
# --- 기본 Weaviate 연결 설정 ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [전략 1] HuggingFace 설정 ---
VECTORIZER="huggingface"
HF_MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2"

# (이 모드에서는 OPENAI_API_KEY가 필요하지 않습니다)
OPENAI_API_KEY=sk-...

# --- [고급] 커스텀 속성 설정 ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
FAILURE_MAPPING_FILE_PATH=.vectorwave_errors.json
RUN_ID=test-run-001
```

#### 예시 2: `openai_client` 사용 (Python 클라이언트, 고성능)

`openai` Python 라이브러리를 통해 직접 OpenAI API를 호출합니다.

```ini
# .env (OpenAI Python Client 사용 시)
# --- 기본 Weaviate 연결 설정 ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [전략 2] OpenAI Client 설정 ---
VECTORIZER="openai_client"

# [필수] 유효한 OpenAI API 키를 입력해야 합니다.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# (이 모드에서는 HF_MODEL_NAME이 사용되지 않습니다)
HF_MODEL_NAME=...

# --- [고급] 커스텀 속성 설정 ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
RUN_ID=test-run-001
```

#### 예시 3: `weaviate_module` 사용 (Docker 위임)

벡터화 작업을 Python이 아닌 Weaviate 도커 컨테이너에 위임합니다. (`vw_docker.yml` 설정 참조)

```ini
# .env (Weaviate Module 위임 시)
# --- 기본 Weaviate 연결 설정 ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [전략 3] Weaviate Module 설정 ---
VECTORIZER="weaviate_module"
WEAVIATE_VECTORIZER_MODULE=text2vec-openai
WEAVIATE_GENERATIVE_MODULE=generative-openai

# [필수] Weaviate 컨테이너가 이 API 키를 읽어 사용합니다.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# --- [고급] 커스텀 속성 설정 ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
RUN_ID=test-run-001
```

-----

### 🚀 고급 실패 추적 (Error Code)

단순히 `status: "ERROR"`로 기록하는 것을 넘어, `VectorWaveExecutions` 로그에 `error_code` 속성을 추가하여 실패 원인을 세분화합니다.

`@vectorize` 또는 `@trace_span`으로 감싸인 함수가 실패할 때, `error_code`는 다음 3가지 우선순위에 따라 자동으로 결정됩니다.

1.  **커스텀 예외 속성 (우선순위 1):**
    가장 구체적인 방법입니다. 발생한 예외 객체 `e`가 `e.error_code` 속성을 가지고 있다면, 해당 값을 `error_code`로 사용합니다.

    ```python
    class PaymentError(Exception):
        def __init__(self, message, error_code):
            super().__init__(message)
            self.error_code = error_code # ⬅️ 이 속성을 감지합니다.

    @vectorize(...)
    def process_payment(amount):
        if amount < 0:
            raise PaymentError("Amount < 0", error_code="PAYMENT_NEGATIVE_AMOUNT")

    # 실행 시 DB 로그: { "status": "ERROR", "error_code": "PAYMENT_NEGATIVE_AMOUNT" }
    ```

2.  **전역 매핑 파일 (우선순위 2):**
    `ValueError` 등 일반적인 예외를 중앙에서 관리합니다. `.env` 파일에 `FAILURE_MAPPING_FILE_PATH` (기본값: `.vectorwave_errors.json`)로 지정된 JSON 파일에서 예외 클래스 이름을 키로 찾아 매핑합니다.

    **`.vectorwave_errors.json` 예시:**

    ```json
    {
      "ValueError": "INVALID_INPUT",
      "KeyError": "CONFIG_MISSING",
      "TypeError": "INVALID_INPUT"
    }
    ```

    ```python
    @vectorize(...)
    def get_config(key):
        return os.environ[key] # ⬅️ KeyError 발생

    # 실행 시 DB 로그: { "status": "ERROR", "error_code": "CONFIG_MISSING" }
    ```

3.  **기본값 (우선순위 3):**
    위 1, 2번에 해당하지 않는 모든 예외는 예외 클래스의 이름(예: `"ZeroDivisionError"`)이 `error_code`로 자동 저장됩니다.

**[활용] 실패 로그 검색:**
이제 `search_executions`에서 `error_code`를 필터링하여 특정 유형의 실패만 집계할 수 있습니다.

```python
# "INVALID_INPUT"으로 분류된 모든 실패 로그 검색
invalid_logs = search_executions(
  filters={"error_code": "INVALID_INPUT"},
  limit=10
)
```

-----

### 커스텀 속성 및 동적 실행 태깅

VectorWave는 정적 데이터(함수 정의)와 동적 데이터(실행 로그) 외에 사용자가 정의한 추가 메타데이터를 저장할 수 있습니다. 이는 두 단계로 작동합니다.

#### 1단계: 커스텀 스키마 정의 (태그 "허용 목록")

`.env` 파일의 `CUSTOM_PROPERTIES_FILE_PATH`에 지정된 경로(기본값: `.weaviate_properties`)에 JSON 파일을 생성합니다.

이 파일은 Weaviate 컬렉션에 \*\*새로운 속성(열)\*\*을 추가하도록 VectorWave에 지시합니다. 이 파일은 모든 커스텀 태그의 **"허용 목록(allow-list)"** 역할을 합니다.

**`.weaviate_properties` 예시:**

```json
{
  "run_id": {
    "data_type": "TEXT",
    "description": "The ID of the specific test run"
  },
  "experiment_id": {
    "data_type": "TEXT",
    "description": "Identifier for the experiment"
  },
  "team": {
    "data_type": "TEXT",
    "description": "이 함수를 담당하는 팀"
  },
  "priority": {
    "data_type": "INT",
    "description": "실행 우선순위"
  }
}
```

* 위와 같이 정의하면 `VectorWaveFunctions`와 `VectorWaveExecutions` 컬렉션 모두에 `run_id`, `experiment_id`, `team`, `priority` 속성이 추가됩니다.

#### 2단계: 동적 실행 태깅 (값 추가하기)

함수가 실행될 때, VectorWave는 `VectorWaveExecutions` 로그에 태그를 추가합니다. 이 태그는 두 가지 방식으로 수집된 후 병합됩니다.

**1. 전역 태그 (환경 변수)**
VectorWave는 1단계에서 정의된 키의 **대문자 이름**(예: `RUN_ID`, `EXPERIMENT_ID`)과 일치하는 환경 변수를 찾습니다. 발견된 값은 `global_custom_values`로 로드되어 *모든* 실행 로그에 추가됩니다. 스크립트 실행 전반에 걸친 메타데이터에 이상적입니다.

**2. 함수별 태그 (데코레이터)**
`@vectorize` 데코레이터에 직접 키워드 인수(`**execution_tags`)로 태그를 전달할 수 있습니다. 이는 함수별 메타데이터에 이상적입니다.

```python
# --- .env 파일 ---
# RUN_ID=global-run-abc
# TEAM=default-team

@vectorize(
    search_description="결제 처리",
    sequence_narrative="...",
    team="billing",  # <-- 함수별 태그
    priority=1       # <-- 함수별 태그
)
def process_payment():
    pass

@vectorize(
    search_description="다른 함수",
    sequence_narrative="...",
    run_id="override-run-xyz" # <-- 전역 태그를 덮어씀
)
def other_function():
    pass
```

**태그 병합 및 유효성 검사 규칙**

1.  **유효성 검사 (중요):** 태그(전역 또는 함수별)는 **반드시** `.weaviate_properties` 파일(1단계)에 키(예: `run_id`, `team`, `priority`)가 먼저 정의된 경우에만 Weaviate에 저장됩니다. 스키마에 정의되지 않은 태그는 **무시**되며, 스크립트 시작 시 경고가 출력됩니다.

2.  **우선순위 (덮어쓰기):** 만약 태그 키가 두 곳 모두에 정의된 경우(예: `.env`의 전역 `RUN_ID`와 데코레이터의 `run_id="override-xyz"`), **데코레이터에 명시된 함수별 태그가 항상 이깁니다**.

**결과 로그:**

* `process_payment()` 실행 로그: `{"run_id": "global-run-abc", "team": "billing", "priority": 1}`
* `other_function()` 실행 로그: `{"run_id": "override-run-xyz", "team": "default-team"}`

-----

### 🚀 실시간 에러 알림 (Webhook)

`VectorWave`는 단순한 로그 저장을 넘어, **에러 발생 즉시** 웹훅(Webhook)을 통해 실시간 알림을 보낼 수 있습니다. 이 기능은 `tracer`에 내장되어 있으며, 별도 설정 없이 `.env` 파일 수정만으로 활성화할 수 있습니다.

**작동 방식:**

1.  `@trace_span` 또는 `@vectorize` 데코레이터가 적용된 함수에서 예외(Exception)가 발생합니다.
2.  `tracer`가 `except` 블록에서 에러를 감지하는 즉시, `alerter` 객체를 호출합니다.
3.  `alerter`는 `.env` 설정을 읽어 `WebhookAlerter`를 사용, 설정된 URL로 에러 정보를 발송합니다.
4.  알림은 **Discord Embed** 형식에 최적화되어, 에러 코드, 트레이스 ID, 캡처된 속성(`user_id` 등) 및 전체 스택 트레이스를 포함한 상세한 리포트를 전송합니다.

**활성화 방법:**
`test_ex/.env` 파일 (또는 환경 변수)에 다음 두 변수를 추가하세요.

```ini
# .env 파일

# 1. 알림 전략을 'webhook'으로 설정합니다. (기본값: "none")
ALERTER_STRATEGY="webhook"

# 2. Discord 또는 Slack 등에서 발급받은 웹훅 URL을 입력합니다.
ALERTER_WEBHOOK_URL="[https://discord.com/api/webhooks/YOUR_HOOK_ID/](https://discord.com/api/webhooks/YOUR_HOOK_ID/)..."
이 두 줄만 추가하고 test_ex/example.py를 실행하면, CustomValueError가 발생하는 시점에 즉시 Discord로 알림이 전송됩니다.

확장성 (전략 패턴): 이 알림 시스템은 전략 패턴으로 설계되었습니다. BaseAlerter 인터페이스를 구현하여 이메일, PagerDuty 등 원하는 다른 알림 채널로 쉽게 확장할 수 있습니다.
```

## 🤝 기여 (Contributing)

버그 보고, 기능 요청, 코드 기여 등 모든 형태의 기여를 환영합니다. 자세한 내용은 [CONTRIBUTING.md](https://www.google.com/search?q=httpsS://www.google.com/search%3Fq%3DCONTRIBUTING.md)를 참고해 주세요.

## 📜 라이선스 (License)

이 프로젝트는 MIT 라이선스에 따라 배포됩니다. 자세한 내용은 [LICENSE](https://www.google.com/search?q=LICENSE) 파일을 확인하세요.

