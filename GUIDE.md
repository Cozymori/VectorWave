
# 🌊 VectorWave Detailed User Guide

**VectorWave** is a framework that automatically stores Python function inputs/outputs and execution flows into a **Vector Database (Weaviate)**, supporting search (RAG), monitoring, and test automation based on this data.

-----

## 1\. Getting Started

### 1.1 Installation & Prerequisites

VectorWave depends on the Weaviate database. A `docker-compose` file is included in the project root.

1.  **Run Weaviate:**
    Run the Weaviate container using the uploaded `vw_docker.yml` file.

    ```bash
    docker-compose -f test_ex/vw_docker.yml up -d
    ```

2.  **Configure Environment Variables (.env):**
    Create a `.env` file in the project root or the location of your execution script. You must select a vectorization method (`huggingface` or `openai_client`).

    ```ini
    # .env Example
    WEAVIATE_HOST=localhost
    WEAVIATE_PORT=8080
    WEAVIATE_GRPC_PORT=50051

    # Option 1: HuggingFace (Local execution, no API key required)
    VECTORIZER="huggingface"
    HF_MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2"

    # Option 2: OpenAI (High performance, API key required)
    # VECTORIZER="openai_client"
    # OPENAI_API_KEY="sk-..."

    # Path to custom tag configuration file
    CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
    ```

3.  **Initialize DB:**
    Initialization is required once when running the code for the first time.

    ```python
    from vectorwave import initialize_database
    client = initialize_database()
    ```

-----

## 2\. Core Feature: Function Vectorization (@vectorize)

Use the `@vectorize` decorator to automatically store the function's source code and execution results.

### Usage

```python
from vectorwave import vectorize

@vectorize(
    search_description="Function to process user payment",  # Description for search
    sequence_narrative="Validates payment then sends receipt", # Contextual explanation
    team="billing",     # Custom tag (stored in execution logs)
    priority=1          # Custom tag
)
def process_payment(user_id: str, amount: int):
    # Business logic
    return {"status": "success"}
```

* **Static Storage:** Upon function definition, the source code and description are stored in the `VectorWaveFunctions` collection.
* **Dynamic Logging:** Upon function execution, inputs, return values, success/failure status, and execution duration are stored in the `VectorWaveExecutions` collection.

-----

## 3\. Monitoring: Distributed Tracing

Complex call flows can be grouped and traced under a single `trace_id`. Use `@vectorize` for the main function and `@trace_span` for child functions.

### Usage

```python
from vectorwave import vectorize, trace_span

@trace_span(attributes_to_capture=['user_id']) # Logs the user_id argument
def validate_user(user_id):
    pass

@trace_span
def send_email():
    pass

@vectorize(search_description="Sign-up workflow") # Acts as Root Span
def signup_workflow(user_id):
    validate_user(user_id) # Automatically inherits parent trace_id
    send_email()
```

### Searching Trace Logs

You can query all execution logs belonging to a specific `trace_id` in chronological order.

```python
from vectorwave.search.execution_search import find_by_trace_id

logs = find_by_trace_id(trace_id="your-trace-id-uuid")
for log in logs:
    print(f"{log['function_name']} -> {log['status']}")
```

-----

## 4\. Performance Optimization: Semantic Caching

If the meaning (Vector Similarity) of the input value is similar, the function execution is skipped, and the cached result is returned. This is useful for high-cost operations (e.g., LLM calls).

### Configuration

```python
@vectorize(
    semantic_cache=True,      # Enable caching
    cache_threshold=0.95,     # Cache hit only if similarity >= 95%
    capture_return_value=True # Saving return value is mandatory
)
def expensive_llm_task(prompt: str):
    # ... High-cost operation ...
    return result
```

* If the `prompt` is semantically similar to previously requested content, the actual function execution is bypassed, and the value stored in the DB is returned immediately.

-----

## 5\. Automation Features: AI Documentation & Self-Healing

### 5.1 AI Auto-Documentation (Auto-Doc)

Instead of manually writing function descriptions, let the LLM analyze the code and generate them.

1.  Set `auto=True` on the function:
    ```python
    @vectorize(auto=True)
    def complex_algorithm(data):
        """Function that performs a complex algorithm"""
        pass
    ```
2.  Call the generation function at the end of the script:
    ```python
    from vectorwave import generate_and_register_metadata
    generate_and_register_metadata() # Batch generate metadata for pending functions
    ```

### 5.2 Self-Healing (Healer)

When an error occurs in a function, it compares past successful logs with the current error log to suggest code fixes.

```python
from vectorwave import VectorWaveHealer

healer = VectorWaveHealer(model="gpt-4-turbo")
suggestion = healer.diagnose_and_heal(function_name="buggy_function", lookback_minutes=60)
print(suggestion) # Output suggested code fix
```

-----

## 6\. Testing: Replay & Regression Testing

Use actual data executed in the production environment to verify if the results remain the same even after logic changes.

1.  **Data Collection:** Set `@vectorize(replay=True)` to store inputs/outputs.
2.  **Execute Test:**
    ```python
    from vectorwave import VectorWaveReplayer

    replayer = VectorWaveReplayer()
    # Retrieve past 10 successful cases and re-execute with current code for comparison
    result = replayer.replay("module_name.function_name", limit=10)

    print(f"Passed: {result['passed']}, Failed: {result['failed']}")
    ```
3.  **Update Baseline:** If the logic change is intentional, use `update_baseline=True` to update the correct answer data in the DB.

-----

## 7\. RAG & Search

You can search and analyze stored function code or execution logs using natural language (Korean/English).

* **Code Search & QA:**
  ```python
  from vectorwave import search_and_answer
  answer = search_and_answer("How does the payment logic work?", language='en')
  ```
* **Trace Log Analysis:**
  ```python
  from vectorwave import analyze_trace_log
  analysis = analyze_trace_log(trace_id="...", language='en')
  ```

-----

## 8\. Advanced Configuration: Custom Properties & Alerts

* **Custom Properties:** Define fields like `team` or `project_id` in the `.weaviate_properties` file, and they will be automatically tagged via decorators or environment variables (`TEAM=backend`).
* **Real-time Alerts (Webhook):** To send notifications to Discord etc. when an error occurs, add the following to your `.env`.
  ```ini
  ALERTER_STRATEGY="webhook"
  ALERTER_WEBHOOK_URL="https://discord.com/api/webhooks/..."
  ```

This guide was written based on the core modules within `src/vectorwave` and the example code in the `test_ex` folder.