
# VectorWave: Seamless Auto-Vectorization Framework

[](https://opensource.org/licenses/MIT)

## 🌟 Overview

**VectorWave** is an innovative framework that uses **decorators** to automatically save and manage the output of Python functions/methods in a **Vector Database (Vector DB)**. Developers can convert function outputs into intelligent vector data with just a single line of code (`@vectorize`), without worrying about the complex processes of data collection, embedding generation, and Vector DB storage.

-----

## ✨ Features

* **`@vectorize` Decorator:**
  1.  **Static Data Collection:** Upon script load, the function's source code, docstring, and metadata are saved once to the `VectorWaveFunctions` collection.
  2.  **Dynamic Data Logging:** Each time the function is called, its execution time, success/failure status, error logs, and "dynamic tags" are recorded in the `VectorWaveExecutions` collection.
* **Semantic Caching and Performance Optimization:**
    * Determines cache hits based on the **semantic similarity** of function inputs, bypassing actual execution for identical or highly similar inputs and returning stored results immediately.
    * This significantly **reduces latency** and costs, especially for high-cost computation functions (e.g., LLM calls, complex data processing).
* **Distributed Tracing:** Combines `@vectorize` and `@trace_span` decorators to bundle the execution of complex, multi-step workflows under a single **`trace_id`** for analysis.
* **Automated Regression Testing (Replay):** Replay past execution logs to automatically verify that code changes haven't broken existing functionality.
* **Data Archiving & Maintenance:** Export execution logs to JSONL format for LLM fine-tuning or archive old data to manage database storage.
* **Search Interface:** Provides `search_functions` and `search_executions` to query the stored vector data (function definitions) and logs (execution history), facilitating the construction of RAG and monitoring systems.

-----

## 🚀 Usage

VectorWave consists of "storage" via decorators and "retrieval" via functions, and now includes **execution flow tracing**.

### 1\. (Required) Database Initialization and Setup

```python
import time
from vectorwave import (
    vectorize, 
    initialize_database, 
    search_functions, 
    search_executions
)
# [New] Import trace_span separately for distributed tracing.
from vectorwave.monitoring.tracer import trace_span 

# Needs to be called only once at script startup.
try:
    client = initialize_database()
    print("VectorWave DB initialization successful.")
except Exception as e:
    print(f"DB initialization failed: {e}")
    exit()
````

### 2\. [Storage] Using `@vectorize` and Distributed Tracing

`@vectorize` acts as the **Root** of the trace, and applying `@trace_span` to internal functions bundles the workflow execution under a **single `trace_id`**.

```python
# --- Child Span Function: Captures arguments ---
@trace_span(attributes_to_capture=['user_id', 'amount'])
def step_1_validate_payment(user_id: str, amount: int):
    """(Span) Validates payment. Logs user_id and amount."""
    print(f"  [SPAN 1] Validating payment for {user_id}...")
    time.sleep(0.1)
    return True

@trace_span(attributes_to_capture=['user_id', 'receipt_id'])
def step_2_send_receipt(user_id: str, receipt_id: str):
    """(Span) Sends receipt."""
    print(f"  [SPAN 2] Sending receipt {receipt_id}...")
    time.sleep(0.2)


# --- Root Function (acts as @trace_root) ---
@vectorize(
    search_description="Processes a user payment and returns a receipt.",
    sequence_narrative="After payment is complete, a receipt is sent via email.",
    team="billing",  # ⬅️ Custom tag (logged on all executions)
    priority=1       # ⬅️ Custom tag (execution importance)
)
def process_payment(user_id: str, amount: int):
    """(Root Span) Executes the user payment workflow."""
    print(f"  [ROOT EXEC] process_payment: Starting workflow for {user_id}...")
    
    # When child functions are called, the same trace_id is automatically inherited via ContextVar.
    step_1_validate_payment(user_id=user_id, amount=amount) 
    
    receipt_id = f"receipt_{user_id}_{amount}"
    step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)

    print(f"  [ROOT DONE] process_payment")
    return {"status": "success", "receipt_id": receipt_id}

# --- Function Execution ---
print("Now calling 'process_payment'...")
# This single call will record a total of 3 execution logs (spans) in the DB,
# and all three logs will be tied to a single 'trace_id'.
process_payment("user_789", 5000)
```

#### Semantic Caching Example

Configure a function to return a cached result if the input is semantically similar to a previous execution.

```python
from vectorwave import vectorize
import time

@vectorize(
    search_description="High-cost LLM summarization task",
    sequence_narrative="LLM Summarization Step",
    semantic_cache=True,            # Enable caching
    cache_threshold=0.95,           # Cache hit if similarity >= 0.95
    capture_return_value=True       # Required to save the result
)
def summarize_document(document_text: str):
    # Simulate an LLM call or heavy computation (e.g., 0.5 sec delay)
    time.sleep(0.5)
    print("--- [Cache Miss] Document is being summarized by LLM...")
    return f"Summary of: {document_text[:20]}..."

# First call (Cache Miss) - takes ~0.5s, saves result to DB
result_1 = summarize_document("The first quarter results showed strong growth in Europe and Asia...")

# Second call (Cache Hit) - takes ~0.0s, returns cached value
# "Q1 results" is semantically similar to "first quarter results"
result_2 = summarize_document("The Q1 results demonstrated strong growth in Europe and Asia...") 

# result_2 returns the stored value without executing the function's body.
```

### 3\. [Retrieval ①] Search Function Definitions (for RAG)

```python
# Search for functions related to 'payment' using natural language (vector).
print("\n--- Searching for 'payment' related functions ---")
payment_funcs = search_functions(
    query="User payment processing feature",
    limit=3
)
for func in payment_funcs:
    print(f"  - Function: {func['properties']['function_name']}")
    print(f"  - Description: {func['properties']['search_description']}")
    print(f"  - Similarity (Distance): {func['metadata'].distance:.4f}")
```

### 4\. [Retrieval ②] Search Execution Logs (for Monitoring & Tracing)

`search_executions` can now retrieve all related execution logs (spans) based on a `trace_id`.

```python
# 1. Find the Trace ID of a specific workflow (process_payment).
latest_payment_span = search_executions(
    limit=1, 
    filters={"function_name": "process_payment"},
    sort_by="timestamp_utc",
    sort_ascending=False
)
trace_id = latest_payment_span[0]["trace_id"] 

# 2. Retrieve all spans belonging to that Trace ID in chronological order.
print(f"\n--- Full Trace for ID ({trace_id[:8]}...) ---")
trace_spans = search_executions(
    limit=10,
    filters={"trace_id": trace_id},
    sort_by="timestamp_utc",
    sort_ascending=True # Sort ascending to analyze workflow
)

for i, span in enumerate(trace_spans):
    print(f"  - [Span {i+1}] {span['function_name']} ({span['duration_ms']:.2f}ms)")
    # Captured arguments (user_id, amount, etc.) from child spans will also be visible.
    
# Expected Output:
# - [Span 1] step_1_validate_payment (100.81ms)
# - [Span 2] step_2_send_receipt (202.06ms)
# - [Span 3] process_payment (333.18ms)
```

-----

## ⚙️ Configuration

VectorWave automatically reads Weaviate database connection info and **vectorization strategy** from **environment variables** or a `.env` file.

Create a `.env` file in your project's root directory (e.g., where `test_ex/example.py` is located) and set the required values.

### Vectorizer Strategy (VECTORIZER)

You can select the text vectorization method via the `VECTORIZER` environment variable in your `test_ex/.env` file.

| `VECTORIZER` Setting | Description | Required Additional Settings |
| :--- | :--- | :--- |
| **`huggingface`** | (Default Recommended) Uses the `sentence-transformers` library to vectorize on your local CPU. No API key is needed. | `HF_MODEL_NAME` (e.g., "sentence-transformers/all-MiniLM-L6-v2") |
| **`openai_client`** | (High-Performance) Uses the OpenAI Python client to vectorize with models like `text-embedding-3-small`. | `OPENAI_API_KEY` (A valid OpenAI API key) |
| **`weaviate_module`** | (Docker Delegate) Delegates vectorization to Weaviate's built-in module (e.g., `text2vec-openai`). | `WEAVIATE_VECTORIZER_MODULE`, `OPENAI_API_KEY` |
| **`none`** | Disables vectorization. Data is stored without vectors. | None |

#### ⚠️ Semantic Caching Prerequisites and Configuration

To use `semantic_cache=True`, the following conditions must be met:

* **Vectorizer Required:** A **Python-based vectorizer** (`huggingface` or `openai_client`) must be configured in your environment (`VECTORIZER` environment variable). Caching is automatically disabled if set to `weaviate_module` or `none`.
* **Return Value Capture:** The `capture_return_value` parameter is automatically set to `True` when `semantic_cache=True` is enabled.

### .env File Examples

Configure your `.env` file according to the strategy you want to use.

#### Example 1: Using `huggingface` (Local, No API Key)

Uses a `sentence-transformers` model on your local machine. Ideal for testing without API keys.

```ini
# .env (Using HuggingFace)
# --- Basic Weaviate Connection ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 1] HuggingFace Config ---
VECTORIZER="huggingface"
HF_MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2"

# (OPENAI_API_KEY is not required for this mode)
OPENAI_API_KEY=sk-...

# --- [Advanced] Custom Properties ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
FAILURE_MAPPING_FILE_PATH=.vectorwave_errors.json
RUN_ID=test-run-001
```

#### Example 2: Using `openai_client` (Python Client, High-Performance)

Directly calls the OpenAI API via the `openai` Python library.

```ini
# .env (Using OpenAI Python Client)
# --- Basic Weaviate Connection ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 2] OpenAI Client Config ---
VECTORIZER="openai_client"

# [Required] You must enter a valid OpenAI API key.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# (HF_MODEL_NAME is not used in this mode)
HF_MODEL_NAME=...

# --- [Advanced] Custom Properties ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
FAILURE_MAPPING_FILE_PATH=.vectorwave_errors.json
RUN_ID=test-run-001
```

#### Example 3: Using `weaviate_module` (Docker Delegate)

Delegates vectorization to the Weaviate Docker container instead of Python. (See `vw_docker.yml` config).

```ini
# .env (Delegating to Weaviate Module)
# --- Basic Weaviate Connection ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 3] Weaviate Module Config ---
VECTORIZER="weaviate_module"
WEAVIATE_VECTORIZER_MODULE=text2vec-openai
WEAVIATE_GENERATIVE_MODULE=generative-openai

# [Required] The Weaviate container will read this API key.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# --- [Advanced] Custom Properties ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
FAILURE_MAPPING_FILE_PATH=.vectorwave_errors.json
RUN_ID=test-run-001
```

### 🚀 Advanced Failure Tracing (Error Code)

This enhances `VectorWaveExecutions` logs beyond a simple `status: "ERROR"`. An `error_code` property is added to the schema for granular failure analysis.

When a function wrapped by `@vectorize` or `@trace_span` fails, the `error_code` is automatically determined based on three priorities:

1.  **Custom Exception Attribute (Priority 1):**
    The most specific method. If the raised exception object `e` has an `e.error_code` attribute, its value is used.

    ```python
    class PaymentError(Exception):
        def __init__(self, message, error_code):
            super().__init__(message)
            self.error_code = error_code # ⬅️ This attribute is detected.

    @vectorize(...)
    def process_payment(amount):
        if amount < 0:
            raise PaymentError("Amount < 0", error_code="PAYMENT_NEGATIVE_AMOUNT")

    # DB Log on execution: { "status": "ERROR", "error_code": "PAYMENT_NEGATIVE_AMOUNT" }
    ```

2.  **Global Mapping File (Priority 2):**
    Centrally manage common exceptions. VectorWave loads a JSON file specified by `FAILURE_MAPPING_FILE_PATH` in your `.env` (default: `.vectorwave_errors.json`) and maps the exception class name to a code.

    **`.vectorwave_errors.json` Example:**

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
        return os.environ[key] # ⬅️ Raises KeyError

    # DB Log on execution: { "status": "ERROR", "error_code": "CONFIG_MISSING" }
    ```

3.  **Default (Priority 3):**
    If neither of the above applies, the exception's class name (e.g., `"ZeroDivisionError"`) is stored as the default `error_code`.

**[Usage] Searching for Failures:**
You can now filter for specific failure types using `search_executions`.

```python
# Find all failure logs categorized as "INVALID_INPUT"
invalid_logs = search_executions(
    filters={"error_code": "INVALID_INPUT"},
    limit=10
)
```

-----

### Custom Properties and Dynamic Execution Tagging

VectorWave can store user-defined metadata in addition to static data (function definitions) and dynamic data (execution logs). This works in two steps.

#### Step 1: Define Custom Schema (Tag "Allow-list")

Create a JSON file at the path specified by `CUSTOM_PROPERTIES_FILE_PATH` in your `.env` file (default: `.weaviate_properties`).

This file instructs VectorWave to add **new properties (columns)** to the Weaviate collections. This file acts as an **"allow-list"** for all custom tags.

**`.weaviate_properties` Example:**

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
    "description": "The team responsible for this function"
  },
  "priority": {
    "data_type": "INT",
    "description": "Execution priority level"
  }
}
```

* This definition will add `run_id`, `experiment_id`, `team`, and `priority` properties to both the `VectorWaveFunctions` and `VectorWaveExecutions` collections.

#### Step 2: Dynamic Execution Tagging (Adding Values)

When a function is executed, VectorWave adds tags to the `VectorWaveExecutions` log. These tags are collected and merged from two sources.

**1. Global Tags (Environment Variables)**
VectorWave looks for environment variables matching the **UPPERCASE name** of the keys defined in Step 1 (e.g., `RUN_ID`, `EXPERIMENT_ID`). Found values are loaded as `global_custom_values` and added to *all* execution logs. Ideal for run-wide metadata.

**2. Function-Specific Tags (Decorator)**
You can pass tags as keyword arguments (`**execution_tags`) directly to the `@vectorize` decorator. This is ideal for function-specific metadata.

```python
# --- .env file ---
# RUN_ID=global-run-abc
# TEAM=default-team

@vectorize(
    search_description="Process payment",
    sequence_narrative="...",
    team="billing",  # <-- Function-specific tag
    priority=1       # <-- Function-specific tag
)
def process_payment():
    pass

@vectorize(
    search_description="Another function",
    sequence_narrative="...",
    run_id="override-run-xyz" # <-- Overrides the global tag
)
def other_function():
    pass
```

1.  **Validation (Important):** Tags (global or function-specific) will **only** be saved to Weaviate if their key (e.g., `run_id`, `team`, `priority`) was first defined in the `.weaviate_properties` file (Step 1). Tags not defined in the schema are **ignored**, and a warning is logged at startup.

2.  **Priority (Override):** If a tag key is defined in both places (e.g., global `RUN_ID` in `.env` and `run_id="override-xyz"` in the decorator), the **function-specific tag from the decorator always wins**.

**Resulting Logs:**

* `process_payment()` execution log: `{"run_id": "global-run-abc", "team": "billing", "priority": 1}`
* `other_function()` execution log: `{"run_id": "override-run-xyz", "team": "default-team"}`

-----

### 🚀 Real-time Error Alerting (Webhook)

Beyond just logging, `VectorWave` can send **real-time notifications via webhook** the instant an error occurs. This functionality is built directly into the tracer and can be activated simply by updating your `.env` file.

**How it Works:**

1.  An exception is raised within a function decorated by `@trace_span` or `@vectorize`.
2.  The tracer catches the exception in its `except` block and immediately calls the `alerter` object.
3.  The alerter reads the `.env` configuration and uses the `WebhookAlerter` to dispatch the error details to your specified URL.
4.  The notification is optimized for **Discord Embeds**, sending a rich report including the error code, trace ID, captured attributes (`user_id`, etc.), and the full stack trace.

**How to Enable:**
Add the following two variables to your `test_ex/.env` file (or environment variables):

```ini
# .env file

# 1. Set the alerter strategy to 'webhook'. (Default: "none")
ALERTER_STRATEGY="webhook"

# 2. Provide your webhook URL from Discord, Slack, etc.
ALERTER_WEBHOOK_URL="[https://discord.com/api/webhooks/YOUR_HOOK_ID/](https://discord.com/api/webhooks/YOUR_HOOK_ID/)..."
```

With just these two lines, running `test_ex/example.py` will now instantly send a Discord alert when the `CustomValueError` is raised.

Extensibility (Strategy Pattern): The alerting system is built on a Strategy Pattern. You can easily extend it by implementing the `BaseAlerter` interface to support other channels like email, PagerDuty, or more.

-----

## 🧪 Advanced Usage: Testing & Maintenance

VectorWave provides tools to leverage your stored production data for testing and maintenance.

### 1\. Automated Regression Testing (Replay)

**Turn your production logs into test cases.**
VectorWave can record the input arguments and return values of your functions. The `Replayer` then uses this data to re-execute the function and verify that the output remains consistent, ensuring code changes haven't introduced regressions.

#### Enable Replay Mode

Add `replay=True` to your `@vectorize` decorator. This automatically enables capturing of all input arguments and the return value.

```python
@vectorize(
    search_description="Calculate payment total",
    sequence_narrative="Validates user and returns total amount",
    replay=True  # <--- Enables automatic capture for Replay!
)
def calculate_total(user_id: str, price: int, tax: float):
    return price + (price * tax)
```

#### Running the Replay Test

Use `VectorWaveReplayer` in a separate test script to fetch past successful logs and verify the current code.

```python
from vectorwave.utils.replayer import VectorWaveReplayer

replayer = VectorWaveReplayer()

# Fetch the last 10 successful logs for 'my_module.calculate_total' and test them.
result = replayer.replay("my_module.calculate_total", limit=10)

print(f"Passed: {result['passed']}, Failed: {result['failed']}")

if result['failed'] > 0:
    for fail in result['failures']:
        print(f"Mismatch! UUID: {fail['uuid']}, Expected: {fail['expected']}, Actual: {fail['actual']}")
```

#### Updating the Baseline

If a logic change intentionally alters the return value, you can update the stored logs to reflect the new "correct" answer (Baseline) using `update_baseline=True`.

```python
# Updates the stored return values in the DB to match the current function's output.
replayer.replay("my_module.calculate_total", update_baseline=True)
```

### 2\. Data Archiving & Fine-tuning (Archiver)

**Manage storage and create training datasets.**
You can export old execution logs to **JSONL format** (suitable for LLM fine-tuning) or delete them from the database to free up space.

```python
from vectorwave.database.archiver import VectorWaveArchiver

archiver = VectorWaveArchiver()

# 1. Export to JSONL and Delete from DB (Export & Clear)
archiver.export_and_clear(
    function_name="my_module.calculate_total",
    output_file="data/training_dataset.jsonl",
    clear_after_export=True  # Removes logs from DB after successful export
)

# 2. Delete Only (Purge)
archiver.export_and_clear(
    function_name="my_module.calculate_total",
    output_file="",
    delete_only=True
)
```

**Generated JSONL Example:**

```json
{"messages": [{"role": "user", "content": "{\"price\": 100, \"tax\": 0.1}"}, {"role": "assistant", "content": "110.0"}]}
```

-----

## 🤝 Contributing

Bug reports, feature requests, and code contributions are all welcome. For details, please see [CONTRIBUTING.md](https://www.google.com/search?q=https://github.com/junyeong0619/vectorwave/blob/main/Contributing.md).

## 📜 License

This project is distributed under the MIT License. See the [LICENSE](https://www.google.com/search?q=https://github.com/junyeong0619/vectorwave/blob/main/LICENSE) file for details.

