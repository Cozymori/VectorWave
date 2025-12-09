
# VectorWave: Seamless Auto-Vectorization Framework

[LICENSE](https://www.google.com/search?q=LICENSE)

## 🌟 Project Overview

**VectorWave** is an innovative framework that uses **decorators** to automatically store and manage the output of Python functions/methods in a **Vector Database (Vector DB)**. Developers can transform function output into intelligent vector data with a single line of code (`@vectorize`), without worrying about the complex processes of data collection, embedding generation, and vector database storage.

---

## ✨ Key Features

* **`@vectorize` Decorator:**
    1.  **Static Data Collection:** Upon script load, the function's source code, docstring, and metadata are stored once in the `VectorWaveFunctions` collection.
    2.  **Dynamic Data Logging:** Every time the function is called, execution time, success/failure status, error logs, and 'dynamic tags' are recorded in the `VectorWaveExecutions` collection.
* **(NEW) AI-Powered Function Documentation:** Uses a Large Language Model (LLM) to automatically generate the **`search_description`** and **`sequence_narrative`**. This significantly reduces manual effort and improves semantic search quality.
* **(NEW) Deferred Registration:** LLM documentation generation is deferred and runs only upon explicit command, **completely avoiding latency** during application startup.
* **Semantic Caching and Performance Optimization:**
    * Determines a cache hit based on the **semantic similarity** of function inputs, bypassing function execution for identical or highly similar inputs and returning stored results instantly.
    * This is highly effective in **significantly reducing execution latency** and cost, especially for high-cost computational functions (e.g., LLM calls, complex data processing).
* **Distributed Tracing:** Combines `@vectorize` and `@trace_span` decorators to group the execution of complex multi-step workflows under a single **`trace_id`** for analysis.
* **Search Interface:** Provides `search_functions` and `search_executions` to query stored vector data (function definitions) and logs (execution history), facilitating the building of RAG and monitoring systems.

---

## 🚀 Usage

VectorWave consists of 'storing' via decorators and 'searching' via functions, now including **execution flow tracing**.

### 1. (Required) Database Initialization and Setup

```python
import time
from vectorwave import (
    vectorize, 
    initialize_database, 
    search_functions, 
    search_executions,
    generate_and_register_metadata # Added for Auto-Doc
)
# [Add] Import trace_span separately for distributed tracing.
from vectorwave.monitoring.tracer import trace_span 

# Should only be called once when the script starts.
try:
    client = initialize_database()
    print("VectorWave DB initialization successful.")
except Exception as e:
    print(f"DB initialization failed: {e}")
    exit()
````

### 2\. [Store] Using `@vectorize` and Distributed Tracing

`@vectorize` acts as the **Root** of the tracing, and applying `@trace_span` to internal functions groups the workflow execution under a **single `trace_id`**.

```python
# --- Sub-span function: Captures arguments ---
@trace_span(attributes_to_capture=['user_id', 'amount'])
def step_1_validate_payment(user_id: str, amount: int):
    """(Span) Validates payment. Records user_id and amount to logs."""
    print(f"  [SPAN 1] Validating payment for {user_id}...")
    time.sleep(0.1)
    return True

@trace_span(attributes_to_capture=['user_id', 'receipt_id'])
def step_2_send_receipt(user_id: str, receipt_id: str):
    """(Span) Sends the receipt."""
    print(f"  [SPAN 2] Sending receipt {receipt_id}...")
    time.sleep(0.2)


# --- Root function (Acts as @trace_root) ---
@vectorize(
    search_description="Process user payment and return a receipt.",
    sequence_narrative="After payment is complete, a receipt is sent via email.",
    team="billing",  # ⬅️ Custom Tag (Recorded in all execution logs)
    priority=1       # ⬅️ Custom Tag (Execution Priority)
)
def process_payment(user_id: str, amount: int):
    """(Root Span) Executes the user payment workflow."""
    print(f"  [ROOT EXEC] process_payment: Starting workflow for {user_id}...")
    
    # When calling sub-functions, the same trace_id is automatically inherited via ContextVar.
    step_1_validate_payment(user_id=user_id, amount=amount) 
    
    receipt_id = f"receipt_{user_id}_{amount}"
    step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)

    print(f"  [ROOT DONE] process_payment")
    return {"status": "success", "receipt_id": receipt_id}

# --- Function Execution ---
print("Now calling 'process_payment'...")
# This single call records 3 execution logs (spans) in the DB,
# and all three logs are grouped under one 'trace_id'.
process_payment("user_789", 5000)
```

-----

### 2.1. 💡 AI Documentation Setup (LLM Configuration)

To use the LLM feature, you must specify dependencies and environment variables.

#### Prerequisites for AI Auto-Documentation

To use the AI-powered documentation feature, you must have the `openai` library installed and configure your API key.

1. **Install Library:**
    ```bash
    pip install openai
    ```

2. **Set API Key:** Add your valid OpenAI API key to your `.env` file.
    ```ini
    OPENAI_API_KEY="sk-proj-YOUR_API_KEY_HERE"
    # WEAVIATE_GENERATIVE_MODULE="generative-openai" (Required to enable the Weaviate module when using OpenAI LLM)
    ```

### 2.2. 🚀 Usage: Auto-Generating Function Metadata (Auto=True)

Instead of manually defining `search_description` and `sequence_narrative`, you can use the `auto=True` flag.

#### 3. Automatic Function Metadata Generation (Auto=True)

You can use the `auto=True` flag instead of manually defining `search_description` and `sequence_narrative`.

1. **Mark Function:** Set `auto=True`. It is **strongly recommended to include a detailed Docstring** to enhance the LLM's analysis quality.

    ```python
    # Code from vectorwave/test_ex/example.py
    @vectorize(auto=True, team="loyalty-program")
    def calculate_loyalty_points(purchase_amount: int, is_vip: bool):
        """
        Function to calculate loyalty points based on purchase amount.
        VIP customers earn double points.
        """
        points = purchase_amount // 10
        if is_vip:
            points *= 2
        return {"points": points, "tier": "VIP" if is_vip else "Regular"}
    ```

2. **Trigger Generation:** Call `generate_and_register_metadata()` **immediately after** all `@vectorize` function definitions are complete. This function calls the LLM, vectorizes the generated metadata, and registers it to the DB.

    ```python
    # ... (After defining the calculate_loyalty_points function above)

    # [Mandatory] Must be called after all function definitions are complete.
    print("🚀 Checking for functions needing auto-documentation...")
    generate_and_register_metadata()
    ```

> **Note:** Since this process involves LLM API calls, it can cause **latency** if run during server startup. It is recommended to execute this via a separate management script or an admin API endpoint in a production environment.
-----

#### Semantic Caching Example

Configure the system to prevent re-execution for similar inputs and return cached results.

```python
from vectorwave import vectorize
import time

@vectorize(
    search_description="High-cost summarization task using LLM",
    sequence_narrative="LLM Summarization Step",
    semantic_cache=True,            # Enable caching
    cache_threshold=0.95,           # Cache hit if similarity is > 95%
    capture_return_value=True       # Must capture return value for caching
)
def summarize_document(document_text: str):
    # Actual LLM call or high-cost computation logic (e.g., 0.5s delay)
    time.sleep(0.5)
    print("--- [Cache Miss] Document is being summarized by LLM...")
    return f"Summary of: {document_text[:20]}..."

# First call (Cache Miss) - Takes 0.5s, result stored in DB
result_1 = summarize_document("The first quarter results showed strong growth in Europe and Asia...")

# Second call (Cache Hit) - Takes 0.0s, returns cached value
# "Q1 results" may semantically match "first quarter results", leading to a cache hit.
result_2 = summarize_document("The Q1 results demonstrated strong growth in Europe and Asia...") 

# result_2 returns the stored value from result_1 without actual function execution.
```

### 3\. [Search ①] Function Definition Search (for RAG)

```python
# Semantically search for functions related to 'payment'.
print("\n--- Searching for 'payment' related functions ---")
payment_funcs = search_functions(
    query="User payment processing function",
    limit=3
)
for func in payment_funcs:
    print(f"  - Function Name: {func['properties']['function_name']}")
    print(f"  - Description: {func['properties']['search_description']}")
    print(f"  - Similarity (Distance): {func['metadata'].distance:.4f}")
```

### 4\. [Search ②] Execution Log Search (Monitoring and Tracing)

The `search_executions` function can now search for all related execution logs (spans) based on the `trace_id`.

```python
# 1. Find the Trace ID of the latest 'process_payment' workflow.
latest_payment_span = search_executions(
    limit=1, 
    filters={"function_name": "process_payment"},
    sort_by="timestamp_utc",
    sort_ascending=False
)
trace_id = latest_payment_span[0]["trace_id"] 

# 2. Search all spans belonging to that Trace ID, sorted chronologically.
print(f"\n--- Full Trace ({trace_id[:8]}...) ---")
trace_spans = search_executions(
    limit=10,
    filters={"trace_id": trace_id},
    sort_by="timestamp_utc",
    sort_ascending=True # Ascending sort for workflow flow analysis
)

for i, span in enumerate(trace_spans):
    print(f"  - [Span {i+1}] {span['function_name']} ({span['duration_ms']:.2f}ms)")
    # Captured arguments (user_id, amount, etc.) are also displayed.
    
# Expected result:
# - [Span 1] step_1_validate_payment (100.81ms)
# - [Span 2] step_2_send_receipt (202.06ms)
# - [Span 3] process_payment (333.18ms)
```

-----

## ⚙️ Configuration

VectorWave automatically reads Weaviate database connection information and **vectorization strategy** from **environment variables** or a `.env` file.

Create a `.env` file in your project's root directory (e.g., where `test_ex/example.py` is located) and set the necessary values.

### Vectorization Strategy Settings (VECTORIZER)

You can select the text vectorization method via the `VECTORIZER` environment variable in your `test_ex/.env` file.

| `VECTORIZER` Setting | Description | Required Additional Settings |
| :--- | :--- | :--- |
| **`huggingface`** | (Recommended Default) Uses the `sentence-transformers` library on the local CPU for vectorization. No API key needed for testing. | `HF_MODEL_NAME` (e.g., "sentence-transformers/all-MiniLM-L6-v2") |
| **`openai_client`** | (High-Performance) Calls the OpenAI API directly via the Python client for models like `text-embedding-3-small`. | `OPENAI_API_KEY` (Valid OpenAI API key) |
| **`weaviate_module`** | (Docker Delegation) Delegates vectorization to the module built into the Weaviate Docker container (e.g., `text2vec-openai`). | `WEAVIATE_VECTORIZER_MODULE`, `OPENAI_API_KEY` |
| **`none`** | No vectorization is performed. Data is stored without vectors. | None |

#### ⚠️ Semantic Caching Prerequisites and Settings

To use `semantic_cache=True`, the following conditions must be met:

* **Vectorizer Required:** A **Python-based vectorizer** (`huggingface` or `openai_client`) must be configured in the library settings (`VECTORIZER` env var). Caching is automatically disabled if `weaviate_module` or `none` is set.
* **Return Value Capture Mandatory:** When `semantic_cache=True` is enabled, the `capture_return_value` parameter is automatically set to `True`.

### .env File Examples

Configure the contents of your `.env` file according to the strategy you intend to use.

#### Example 1: Using `huggingface` (Local, No API Key Needed)

Uses a `sentence-transformers` model on the local machine. Easy for testing as no API key is needed.

```ini
# .env (For HuggingFace)
# --- Basic Weaviate Connection Settings ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 1] HuggingFace Settings ---
VECTORIZER="huggingface"
HF_MODEL_NAME="sentence-transformers/all-MiniLM-L6-v2"

# (OPENAI_API_KEY is not needed in this mode)
OPENAI_API_KEY=sk-...

# --- [Advanced] Custom Property Settings ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
FAILURE_MAPPING_FILE_PATH=.vectorwave_errors.json
RUN_ID=test-run-001
```

#### Example 2: Using `openai_client` (Python Client, High-Performance)

Calls the OpenAI API directly via the `openai` Python library.

```ini
# .env (For OpenAI Python Client)
# --- Basic Weaviate Connection Settings ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 2] OpenAI Client Settings ---
VECTORIZER="openai_client"

# [REQUIRED] Must enter a valid OpenAI API key.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# (HF_MODEL_NAME is not used in this mode)
HF_MODEL_NAME=...

# --- [Advanced] Custom Property Settings ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
RUN_ID=test-run-001
```

#### Example 3: Using `weaviate_module` (Docker Delegation)

Delegates vectorization to the Weaviate Docker container instead of Python. (Refer to `vw_docker.yml` settings)

```ini
# .env (For Weaviate Module Delegation)
# --- Basic Weaviate Connection Settings ---
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8080
WEAVIATE_GRPC_PORT=50051

# --- [Strategy 3] Weaviate Module Settings ---
VECTORIZER="weaviate_module"
WEAVIATE_VECTORIZER_MODULE=text2vec-openai
WEAVIATE_GENERATIVE_MODULE=generative-openai

# [REQUIRED] The Weaviate container reads and uses this API key.
OPENAI_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxxxxxx

# --- [Advanced] Custom Property Settings ---
CUSTOM_PROPERTIES_FILE_PATH=.weaviate_properties
RUN_ID=test-run-001
```

-----

### 🚀 Advanced Failure Tracking (Error Code)

Beyond simple `status: "ERROR"` recording, `error_code` attributes are added to `VectorWaveExecutions` logs to categorize failure causes.

When an exception occurs in a function decorated with `@vectorize` or `@trace_span`, the `error_code` is automatically determined based on the following 3 priorities:

1.  **Custom Exception Attribute (Priority 1):**
    The most specific method. If the raised exception object `e` has an `e.error_code` attribute, that value is used as the `error_code`.

    ```python
    class PaymentError(Exception):
        def __init__(self, message, error_code):
            super().__init__(message)
            self.error_code = error_code # ⬅️ This attribute is detected.

    @vectorize(...)
    def process_payment(amount):
        if amount < 0:
            raise PaymentError("Amount < 0", error_code="PAYMENT_NEGATIVE_AMOUNT")

    # DB Log upon execution: { "status": "ERROR", "error_code": "PAYMENT_NEGATIVE_AMOUNT" }
    ```

2.  **Global Mapping File (Priority 2):**
    Manages common exceptions like `ValueError` centrally. The exception class name is looked up as a key in the JSON file specified by `FAILURE_MAPPING_FILE_PATH` (default: `.vectorwave_errors.json`) in the `.env` file.

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

    # DB Log upon execution: { "status": "ERROR", "error_code": "CONFIG_MISSING" }
    ```

3.  **Default Value (Priority 3):**
    Any exception not covered by P1 or P2 has its exception class name (e.g., `"ZeroDivisionError"`) automatically saved as the `error_code`.

**[Usage] Searching Failure Logs:**
You can now filter `search_executions` by `error_code` to aggregate only specific types of failures.

```python
# Search all failure logs categorized as "INVALID_INPUT"
invalid_logs = search_executions(
  filters={"error_code": "INVALID_INPUT"},
  limit=10
)
```

-----

### Custom Properties and Dynamic Execution Tagging

VectorWave can store user-defined additional metadata alongside static data (function definitions) and dynamic data (execution logs). This works in two steps.

#### Step 1: Define Custom Schema (Tag "Allow-List")

Create a JSON file at the path specified by `CUSTOM_PROPERTIES_FILE_PATH` (default: `.weaviate_properties`) in the `.env` file.

This file instructs VectorWave to add **new properties (columns)** to the Weaviate collections. This file acts as the **"allow-list"** for all custom tags.

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
    "description": "Execution priority"
  }
}
```

* Properties defined above will be added to both `VectorWaveFunctions` and `VectorWaveExecutions` collections.

#### Step 2: Dynamic Execution Tagging (Adding Values)

When a function executes, VectorWave adds tags to the `VectorWaveExecutions` log. These tags are collected and merged in two ways:

**1. Global Tags (Environment Variables)**
VectorWave looks for environment variables whose **uppercase names** (e.g., `RUN_ID`, `EXPERIMENT_ID`) match the keys defined in Step 1. Found values are loaded as `global_custom_values` and added to *all* execution logs. Ideal for metadata spanning the entire script execution.

**2. Per-Function Tags (Decorator)**
Tags can be passed directly to the `@vectorize` decorator as keyword arguments (`**execution_tags`). Ideal for function-specific metadata.

```python
# --- .env file ---
# RUN_ID=global-run-abc
# TEAM=default-team

@vectorize(
    search_description="Process payment",
    sequence_narrative="...",
    team="billing",  # <-- Per-function tag
    priority=1       # <-- Per-function tag
)
def process_payment():
    pass

@vectorize(
    search_description="Another function",
    sequence_narrative="...",
    run_id="override-run-xyz" # <-- Overrides global tag
)
def other_function():
    pass
```

**Tag Merging and Validation Rules**

1.  **Validation (Important):** Tags (global or per-function) are **only** stored in Weaviate if their key (e.g., `run_id`, `team`, `priority`) is first defined in the `.weaviate_properties` file (Step 1). Tags not defined in the schema are **ignored**, and a warning is printed upon script startup.

2.  **Precedence (Override):** If a tag key is defined in both places (e.g., global `RUN_ID` from `.env` and per-function `run_id="override-xyz"`), the **per-function tag explicitly defined in the decorator always wins**.

**Resulting Logs:**

* `process_payment()` execution log: `{"run_id": "global-run-abc", "team": "billing", "priority": 1}`
* `other_function()` execution log: `{"run_id": "override-run-xyz", "team": "default-team"}`

-----

### 🚀 Real-time Error Alerting (Webhook)

Beyond simple log storage, `VectorWave` can send real-time alerts via **Webhook** immediately upon error occurrence. This feature is built into the `tracer` and can be enabled simply by modifying the `.env` file.

**How it Works:**

1.  An exception occurs in a function decorated with `@trace_span` or `@vectorize`.
2.  The `tracer` immediately detects the error and calls the `alerter` object.
3.  The `alerter` reads the `.env` settings, uses `WebhookAlerter`, and dispatches the error information to the configured URL.
4.  The notification is optimized for **Discord Embed** format, sending a detailed report including the error code, trace ID, captured attributes (`user_id`, etc.), and the full stack trace.

**How to Enable:**
Add these two variables to your `test_ex/.env` file (or environment variables).

```ini
# .env File

# 1. Set the alert strategy to 'webhook'. (Default: "none")
ALERTER_STRATEGY="webhook"

# 2. Enter your Webhook URL obtained from Discord or Slack.
ALERTER_WEBHOOK_URL="[https://discord.com/api/webhooks/YOUR_HOOK_ID/](https://www.google.com/search?q=https://discord.com/api/webhooks/YOUR_HOOK_ID/)..."
Adding just these two lines and running test_ex/example.py will immediately send an alert to Discord when a CustomValueError occurs.

Extensibility (Strategy Pattern): This alert system is designed with the Strategy Pattern, allowing easy extension to other notification channels like email or PagerDuty by implementing the BaseAlerter interface.
```

-----

## 🧪 Advanced Features: Testing and Maintenance

VectorWave provides powerful tools to utilize stored operational data for testing and maintenance.

### 1\. Automated Regression Testing (Replay)

**Transform production logs into test cases.**
VectorWave records the **input arguments** and **return value** of the function upon execution. The `Replayer` uses this data to re-execute the function and verify if the result matches the past outcome, automatically detecting **regression** (breakage of existing functionality) due to code changes.

#### Enable Replay Mode

Add the `replay=True` option to the `@vectorize` decorator. Input arguments and return values will be automatically captured.

```python
@vectorize(
    search_description="Calculate payment amount",
    sequence_narrative="Validate user and return total amount",
    replay=True  # <--- Turn on this option to enable Replay!
)
def calculate_total(user_id: str, price: int, tax: float):
    return price + (price * tax)
```

#### Execute Tests (Replay Test)

Use the `VectorWaveReplayer` in a separate test script to validate the current code against past successful execution history.

```python
from vectorwave.utils.replayer import VectorWaveReplayer

replayer = VectorWaveReplayer()

# Test the latest 10 successful logs of 'my_module.calculate_total'
result = replayer.replay("my_module.calculate_total", limit=10)

print(f"Passed: {result['passed']}, Failed: {result['failed']}")

if result['failed'] > 0:
    for fail in result['failures']:
        print(f"Mismatch! UUID: {fail['uuid']}, Expected: {fail['expected']}, Actual: {fail['actual']}")
```

#### Update Baseline

If the change in result is intentional due to logic modification, use the `update_baseline=True` option to save the current execution result as the new correct answer (Baseline) in the DB.

```python
# Update the stored return value in the DB to the current function execution result.
replayer.replay("my_module.calculate_total", update_baseline=True)
```

### 2\. Data Archiving and Fine-tuning (Archiver)

**Manage database capacity and secure training datasets.**
Export old execution logs in **JSONL format** (suitable for LLM fine-tuning) or delete them from the database to save storage space.

```python
from vectorwave.database.archiver import VectorWaveArchiver

archiver = VectorWaveArchiver()

# 1. Export to JSONL and Clear from DB (Export & Clear)
archiver.export_and_clear(
    function_name="my_module.calculate_total",
    output_file="data/training_dataset.jsonl",
    clear_after_export=True  # Delete logs from DB after successful export
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

## 🌊 Auto-Injection (Zero-Code Change Integration)

You don't need to modify your business logic to use VectorWave. Use `VectorWaveAutoInjector` to inject functionality externally.

### How to use

1. **Configure Global Settings**: Set default values like `team`, `priority`, or `auto` (pending mode).
2. **Inject Modules**: Specify the target module path string.

```python
from vectorwave import initialize_database, VectorWaveAutoInjector, generate_and_register_metadata

# 1. Initialize DB
initialize_database()

# 2. Configure AutoInjector (Global Settings)
VectorWaveAutoInjector.configure(
    team="billing-team",
    priority=1,
    auto=True  # True: Collect metadata in memory (Pending), False: Save immediately to DB
)

# 3. Inject VectorWave into your module
# (No need to add @vectorize decorators in 'my_service.payment' code!)
VectorWaveAutoInjector.inject("my_service.payment")

# 4. Register Metadata (If auto=True)
generate_and_register_metadata()

# 5. Run your business logic
import my_service.payment
my_service.payment.process_transaction()
```

## 🤝 Contributing

We welcome all forms of contributions, including bug reports, feature requests, and code contributions. Please refer to [CONTRIBUTING.md](https://www.google.com/search?q=httpsS://www.google.com/search%3Fq%3DCONTRIBUTING.md) for details.

## 📜 License

This project is distributed under the MIT License. Please check the [LICENSE](https://www.google.com/search?q=LICENSE) file for details.
