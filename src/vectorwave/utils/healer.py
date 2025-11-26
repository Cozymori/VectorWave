import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta, timezone

# Import VectorWave internal modules
from ..search.execution_search import find_executions
from ..database.db_search import search_functions_hybrid
from ..models.db_config import get_weaviate_settings
from ..core.llm.factory import get_llm_client

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

logger = logging.getLogger(__name__)

class VectorWaveHealer:
    """
    Self-Healing agent that analyzes functions with errors and suggests
    corrected code based on past successful executions.
    """
    def __init__(self, model: str = "gpt-4-turbo"):
        self.settings = get_weaviate_settings()
        self.model = model
        self.client = get_llm_client()

    def diagnose_and_heal(self, function_name: str, lookback_minutes: int = 60) -> str:
        """
        Analyzes recent errors of a specific function and suggests corrected code.
        """
        if not self.client:
            return "❌ OpenAI client initialization failed."

        print(f"🕵️ Analyzing function: '{function_name}'...")

        # 1. Retrieve original function source code
        func_defs = search_functions_hybrid(query=function_name, limit=1, alpha=0.1)
        if not func_defs:
            return f"❌ Function definition not found: {function_name}"

        source_code = func_defs[0]['properties'].get('source_code', '')
        if not source_code:
            return "❌ No stored source code found."

        # 2. Collect recent error logs
        time_limit = (datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)).isoformat()
        error_logs = find_executions(
            filters={
                "function_name": function_name,
                "status": "ERROR",
                "timestamp_utc__gte": time_limit
            },
            limit=3,
            sort_by="timestamp_utc",
            sort_ascending=False
        )

        if not error_logs:
            return f"✅ No errors found for '{function_name}' in the last {lookback_minutes} minutes."

        # 3. Collect success logs
        success_logs = find_executions(
            filters={
                "function_name": function_name,
                "status": "SUCCESS"
            },
            limit=2,
            sort_by="timestamp_utc",
            sort_ascending=False
        )

        # 4. Construct prompt
        prompt_context = self._construct_prompt(function_name, source_code, error_logs, success_logs, lookback_minutes)

        # 5. Call LLM
        print("🤖 Generating fix via LLM...")
        try:
            response_text = self.client.create_chat_completion(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are an expert Python debugger."
                                                  " Analyze the code and errors provided,"
                                                  " then generate a fixed version of the code."},
                    {"role": "user", "content": prompt_context}
                ],
                temperature=0.1,
                category="healer"
            )

            if response_text:
                return response_text
            else:
                return "❌ LLM returned no response."

        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return f"❌ Error occurred during LLM call: {e}"

    def _construct_prompt(self, func_name, source_code, errors, successes, lookback_minutes) -> str:
        """
        Generates a detailed debugging report to send to the LLM.
        """
        error_details = []
        for err in errors:
            inputs = {k: v for k, v in err.items() if k not in ['trace_id', 'span_id', 'error_message', 'source_code', 'return_value']}
            error_details.append(f"""
- Timestamp: {err.get('timestamp_utc')}
- Error Code: {err.get('error_code')}
- Error Message: {err.get('error_message')}
- Inputs causing error: {json.dumps(inputs, default=str)}
            """)

        success_details = []
        for suc in successes:
            inputs = {k: v for k, v in suc.items() if k not in ['trace_id', 'span_id', 'return_value']}
            output = suc.get('return_value')
            success_details.append(f"""
- Inputs: {json.dumps(inputs, default=str)}
- Output: {output}
            """)

        prompt = fr'''
# Debugging Task for Function: `{func_name}`

## 1. Context
You are an expert Python debugger. Your goal is to fix a buggy function based on its source code and execution logs.

## 2. Current Source Code
(Note: The code below may contain decorators like @vectorize, which should NOT be included in your output.)
\`\`\`python
{source_code}
\`\`\`

## 3. Recent Errors (last {lookback_minutes} minutes)
{''.join(error_details)}

## 4. Successful Executions (Reference)
{''.join(success_details) if success_details else "No success logs available."}

## 5. Instructions
1. **Analyze**: Infer the intended functionality of `{func_name}` based on its name and current logic.
2. **Diagnose**: Identify the root cause of the "Recent Errors".
3. **Fix**: Rewrite the function to resolve the error.
    - Fix the logic that causes the crash (e.g., type mismatch, index error).
    - If the code contains clearly incorrect logic (like intentional bug injections for testing), correct it to match the intended behavior.
    - Refactor the code to be clean and idiomatic Python.
4. **Constraint**:
    - Return **ONLY** the full, corrected function definition.
    - Start exactly with `def {func_name}(...):`.
    - **DO NOT** include the `@vectorize` decorator or any other decorators in the output.
    - **DO NOT** include any markdown formatting (like ```python), comments outside the function, or explanations.
'''
        return prompt