
# VectorWave: Seamless Auto-Vectorization Framework

[LICENSE](https://www.google.com/search?q=LICENSE)

## 🌟 프로젝트 소개 (Overview)

**VectorWave**는 파이썬 함수/메서드의 출력을 **데코레이터**를 사용하여 자동으로 **벡터 데이터베이스(Vector DB)**에 저장하고 관리하는 혁신적인 프레임워크입니다. 개발자는 데이터 수집, 임베딩 생성, 벡터 DB 저장의 복잡한 과정을 신경 쓸 필요 없이, 단 한 줄의 코드(`@vectorize`)로 함수 출력을 지능적인 벡터 데이터로 변환할 수 있습니다.

---

## ✨ 주요 특징 (Features)

* **`@vectorize` 데코레이터:**
    1.  **정적 데이터 수집:** 스크립트 로드 시, 함수의 소스 코드, 독스트링, 메타데이터를 `VectorWaveFunctions` 컬렉션에 1회 저장합니다.
    2.  **동적 데이터 로깅:** 함수가 호출될 때마다 실행 시간, 성공/실패 상태, 에러 로그, 그리고 '동적 태그'를 `VectorWaveExecutions` 컬렉션에 기록합니다.
* **(NEW) AI 기반 함수 문서화:** LLM(Large Language Model)을 사용하여 **`search_description`** 및 **`sequence_narrative`**를 자동으로 생성합니다. 이는 수동 작업 부담을 획기적으로 줄이고 검색 품질을 향상시킵니다.
* **(NEW) 지연된 등록 (Deferred Registration):** LLM 문서 생성은 명시적인 호출 시에만 실행되어, 애플리케이션 시작 시 발생하는 **지연 시간(Latency)을 완벽하게 방지**합니다.
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

-----

### 2.1. 💡 AI Documentation Setup (LLM 설정)

LLM 기능을 사용하기 위해 필요한 종속성과 환경 변수를 명시해야 합니다.

#### AI 자동 문서화 필수 조건 (Prerequisites)

AI 기반 문서화 기능을 사용하려면 `openai` 라이브러리가 설치되어 있어야 하며, API 키가 설정되어 있어야 합니다.

1. **라이브러리 설치:**
    ```bash
    pip install openai
    ```

2. **API 키 설정:** `.env` 파일에 유효한 OpenAI API 키를 추가해야 합니다.
    ```ini
    OPENAI_API_KEY="sk-proj-YOUR_API_KEY_HERE"
    # WEAVIATE_GENERATIVE_MODULE="generative-openai" (OpenAI LLM 사용 시 Weaviate 모듈도 활성화해야 함)
    ```

### 2.2. 🚀 사용법: 자동 함수 메타데이터 생성 (Auto=True)

`search_description`과 `sequence_narrative`를 수동으로 정의하는 대신, `auto=True` 플래그를 사용할 수 있습니다.

#### 3. 자동 함수 메타데이터 생성 절차

1. **함수 마킹:** `auto=True`를 설정합니다. LLM의 분석 품질을 높이기 위해 **Docstring을 상세하게 작성하는 것을 강력히 권장합니다.**

    ```python
    # vectorwave/test_ex/example.py 내의 코드
    @vectorize(auto=True, team="loyalty-program")
    def calculate_loyalty_points(purchase_amount: int, is_vip: bool):
        """
        구매 금액에 따른 포인트 적립 계산 함수.
        VIP 고객은 포인트를 2배로 적립받습니다.
        """
        points = purchase_amount // 10
        if is_vip:
            points *= 2
        return {"points": points, "tier": "VIP" if is_vip else "Regular"}
    ```

2. **생성 실행 트리거:** 모든 `@vectorize` 함수 정의가 완료된 **직후**에 `generate_and_register_metadata()` 함수를 호출합니다. 이 함수는 LLM을 호출하고, 생성된 메타데이터를 벡터화하여 DB에 등록합니다.

    ```python
    # ... (위의 calculate_loyalty_points 함수 정의 후)

    # [필수] 모든 함수 정의가 완료된 후 호출되어야 합니다.
    print("🚀 Checking for functions needing auto-documentation...")
    generate_and_register_metadata()
    ```

> **참고:** 이 프로세스는 LLM API 호출을 포함하므로, 서버 시작 시 실행하면 **지연 시간(Latency)**이 발생할 수 있습니다. 운영 환경에서는 별도의 관리 스크립트나 관리자 API를 통해 실행하는 것을 권장합니다.
-----

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

이번 업데이트에서는 **멀티 테넌시(Multi-tenancy) 지원**과 **성능 최적화**에 중점을 두었습니다.

### 1. 시맨틱 캐싱 스코프(Scope) 및 격리 기능
이제 프로젝트나 사용자 단위로 캐시를 격리하여 데이터 프라이버시를 보장할 수 있습니다.
- **기능**: `@vectorize` 데코레이터에 `semantic_cache_scope` 파라미터가 추가되었습니다.
- **동작**: 지정된 필드(예: `project_id`, `user_id`)가 일치하는 경우에만 캐시를 반환합니다.
- **자동 캡처**: 스코프로 지정된 인자는 별도로 `capture_inputs`에 명시하지 않아도 자동으로 DB에 저장됩니다.

**예시:**
```python
@vectorize(
    semantic_cache=True,
    semantic_cache_scope=['project_id'],  # project_id가 같을 때만 캐시 사용
    tokenization="field" # .weaviate_properties 설정 권장
)
def summarize(project_id, content):
    ...
2. ID 필드 검색 정확도 향상 (tokenization: field)
기존의 word 단위 토큰화로 인해 ID 값(예: user_A)이 부분 일치로 잘못 검색되는 문제를 해결했습니다.

설정: .weaviate_properties 파일에서 tokenization: "field" 옵션을 사용하여 문자열 전체를 정확하게 매칭(Exact Match)하도록 설정할 수 있습니다.

호환성: Weaviate v4 클라이언트와의 호환성을 위해 내부 매핑 로직을 개선했습니다.

3. 추적기(Tracer) 성능 최적화
함수 호출 시 발생하는 오버헤드를 최소화했습니다.

최적화: inspect.signature 호출에 LRU Cache를 적용하여, 반복되는 함수 호출 시 서명 분석 비용을 제거했습니다.

안정성: 데코레이터가 주입하는 메타데이터(kwargs)와 실제 함수 인자 간의 충돌로 인해 로그 저장이 실패하던 버그를 수정했습니다.

4. 운영 환경을 위한 무중단 스키마 마이그레이션
서비스 운영 중에도 데이터를 유지하며 새로운 필터링 속성을 추가할 수 있습니다.

유틸리티: vectorwave.database.db.update_database_schema() 함수가 추가되었습니다.

사용법: .weaviate_properties에 새로운 속성을 정의한 후, 이 함수를 실행하면 DB에 즉시 반영됩니다.

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

`VectorWave`는 단순히 로그 저장을 넘어, **에러 발생 즉시** 웹훅(Webhook)을 통해 실시간 알림을 보낼 수 있습니다. 이 기능은 `tracer`에 내장되어 있으며, 별도 설정 없이 `.env` 파일 수정만으로 활성화할 수 있습니다.

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
ALERTER_WEBHOOK_URL="[https://discord.com/api/webhooks/YOUR_HOOK_ID/](https://www.google.com/search?q=https://discord.com/api/webhooks/YOUR_HOOK_ID/)..."
이 두 줄만 추가하고 test_ex/example.py를 실행하면, CustomValueError가 발생하는 시점에 즉시 Discord로 알림이 전송됩니다.

확장성 (전략 패턴): 이 알림 시스템은 전략 패턴으로 설계되었습니다. BaseAlerter 인터페이스를 구현하여 이메일, PagerDuty 등 원하는 다른 알림 채널로 쉽게 확장할 수 있습니다.
```

-----

## 📝 Readme.md 추가 내용 (한국어)

### 🧪 고급 기능: 테스트 및 유지보수 (Advanced Usage)

VectorWave는 저장된 운영 데이터를 테스트와 유지보수에 활용할 수 있는 강력한 도구를 제공합니다.

### 1\. 자동 회귀 테스트 (Replay)

**운영 환경의 로그를 테스트 케이스로 변신시키세요.**
VectorWave는 함수 실행 시의 \*\*입력값(Arguments)\*\*과 \*\*반환값(Return Value)\*\*을 기록합니다. `Replayer`는 이 데이터를 사용하여 함수를 재실행하고, 결과가 과거와 동일한지 검증하여 코드 변경으로 인한 \*\*회귀(Regression, 기존 기능 파손)\*\*를 자동으로 감지합니다.

#### Replay 모드 활성화

`@vectorize` 데코레이터에 `replay=True` 옵션을 추가하세요. 입력값과 반환값이 자동으로 캡처됩니다.

```python
@vectorize(
    search_description="결제 금액 계산",
    sequence_narrative="사용자 유효성을 검사하고 총 금액을 반환함",
    replay=True  # <--- 이 옵션을 켜면 Replay 준비 완료!
)
def calculate_total(user_id: str, price: int, tax: float):
    return price + (price * tax)
```

#### 테스트 실행 (Replay Test)

별도의 테스트 스크립트에서 `VectorWaveReplayer`를 사용하여, 과거의 성공한 실행 이력을 바탕으로 현재 코드를 검증합니다.

```python
from vectorwave.utils.replayer import VectorWaveReplayer

replayer = VectorWaveReplayer()

# 'my_module.calculate_total' 함수의 최근 성공 로그 10개를 가져와 테스트
result = replayer.replay("my_module.calculate_total", limit=10)

print(f"통과(Passed): {result['passed']}, 실패(Failed): {result['failed']}")

if result['failed'] > 0:
    for fail in result['failures']:
        print(f"불일치 발생! UUID: {fail['uuid']}, 기대값: {fail['expected']}, 실제값: {fail['actual']}")
```

#### 베이스라인 업데이트 (Update Baseline)

로직 변경으로 인해 결과값이 바뀌는 것이 의도된 사항이라면, `update_baseline=True` 옵션을 사용하여 현재의 실행 결과를 새로운 정답(Baseline)으로 DB에 저장할 수 있습니다.

```python
# DB에 저장된 반환값을 현재 함수의 실행 결과로 업데이트합니다.
replayer.replay("my_module.calculate_total", update_baseline=True)
```

### 2\. 데이터 아카이빙 및 파인튜닝 (Archiver)

**데이터베이스 용량을 관리하고 학습 데이터셋을 확보하세요.**
오래된 실행 로그를 **JSONL 포맷**(LLM 파인튜닝에 적합)으로 내보내거나, 데이터베이스에서 삭제하여 저장 공간을 확보할 수 있습니다.

```python
from vectorwave.database.archiver import VectorWaveArchiver

archiver = VectorWaveArchiver()

# 1. JSONL로 내보내고 DB에서 삭제 (Export & Clear)
archiver.export_and_clear(
    function_name="my_module.calculate_total",
    output_file="data/training_dataset.jsonl",
    clear_after_export=True  # 내보내기가 성공하면 DB에서 로그 삭제
)

# 2. 삭제만 수행 (Purge)
archiver.export_and_clear(
    function_name="my_module.calculate_total",
    output_file="",
    delete_only=True
)
```

**생성된 JSONL 예시:**

```json
{"messages": [{"role": "user", "content": "{\"price\": 100, \"tax\": 0.1}"}, {"role": "assistant", "content": "110.0"}]}
```


## 🌊 자동 주입 (Auto-Injection): 코드 수정 없는 통합

비즈니스 로직 코드를 직접 수정하지 않고도 `VectorWaveAutoInjector`를 사용하여 외부에서 `VectorWave` 기능을 주입할 수 있습니다.

### 사용 방법

1.  **전역 설정 (Configure):** `team`, `priority`, `auto` (대기 모드) 등 기본값을 설정합니다.
2.  **모듈 주입 (Inject):** 대상 모듈의 경로(문자열)를 지정하여 주입합니다.

```python
from vectorwave import initialize_database, VectorWaveAutoInjector, generate_and_register_metadata

# 1. DB 초기화
initialize_database()

# 2. AutoInjector 설정 (전역 설정)
VectorWaveAutoInjector.configure(
    team="billing-team",
    priority=1,
    auto=True  # True: 메타데이터를 메모리에 대기(Pending), False: 즉시 DB 저장
)

# 3. 모듈에 VectorWave 주입
# ('my_service.payment' 코드 내에 @vectorize를 붙일 필요가 없습니다!)
VectorWaveAutoInjector.inject("my_service.payment")

# 4. 메타데이터 등록 (auto=True인 경우 필수)
# 서버 시작 전이나 로직 실행 전에 호출하여 DB에 함수 정보를 저장합니다.
generate_and_register_metadata()

# 5. 비즈니스 로직 실행
import my_service.payment
my_service.payment.process_transaction()
```

## 🌌 에코시스템 (Ecosystem)

VectorWave는 관측 가능성(Observability)부터 테스트까지, AI 엔지니어링의 전체 수명 주기를 최적화하기 위해 설계된 더 큰 생태계의 일부입니다.

### 🏄‍♂️ [VectorSurfer](https://github.com/cozymori/vectorsurfer)
> **AI의 실행 흐름을 시각적으로 확인하세요.**
**VectorSurfer**는 VectorWave를 위한 종합 웹 대시보드입니다. 직관적인 인터페이스를 통해 복잡한 실행 추적(Trace)을 시각화하고, 실시간으로 에러율을 모니터링하며, 자가 치유(Healer) 프로세스를 손쉽게 관리할 수 있습니다.
* **Trace 시각화:** 복잡한 실행 흐름(Span)과 지연 시간(Latency) 워터폴 차트를 한눈에 파악합니다.
* **에러 모니터링:** 에러 발생 추이를 추적하고 상세 실패 로그를 분석합니다.
* **Healer 인터페이스:** VectorWave Healer가 제안한 코드 수정 사항을 웹에서 검토하고 적용합니다.

### ✅ [VectorCheck](https://github.com/cozymori/vectorcheck)
> **단순 문자열이 아닌, AI의 "의도(Intent)"를 테스트하세요.**
**VectorCheck**는 AI 에이전트를 위한 회귀 테스트(Regression Testing) 프레임워크입니다. 깨지기 쉬운 단순 문자열 비교(`assert a == b`) 대신, 벡터 유사도를 사용하여 AI의 출력이 의도한 정답("Golden Data")과 의미적으로 일치하는지 검증합니다.
* **의미론적 검증(Semantic Assertions):** 단어 선택이 조금 다르더라도, 출력이 예상 결과와 *의미적으로* 유사하면 테스트를 통과시킵니다.
* **골든 데이터 리플레이(Golden Data Replay):** 검증된 운영 환경의 성공 로그를 자동으로 가져와 재실행(Replay)함으로써, 코드 변경 후에도 기능이 정상 작동하는지 확인합니다.
* **CLI 대시보드:** 복잡한 설정 없이 터미널에서 즉시 테스트를 실행하고 결과를 직관적으로 확인합니다.

## 🤝 기여 (Contributing)

버그 보고, 기능 요청, 코드 기여 등 모든 형태의 기여를 환영합니다. 자세한 내용은 [CONTRIBUTING.md](https://www.google.com/search?q=httpsS://www.google.com/search%3Fq%3DCONTRIBUTING.md)를 참고해 주세요.

## 📜 라이선스 (License)

이 프로젝트는 MIT 라이선스에 따라 배포됩니다. 자세한 내용은 [LICENSE](https://www.google.com/search?q=LICENSE) 파일을 확인하세요.

