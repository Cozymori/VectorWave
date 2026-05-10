import logging
import json
import inspect
import importlib
import os
import ast
import re
from typing import Optional, List, Dict, Any, Tuple
from datetime import datetime, timedelta, timezone

# Import VectorWave internal modules
from ..search.execution_search import find_executions
from ..database.db_search import search_functions_hybrid
from ..models.db_config import get_weaviate_settings
from ..core.llm.factory import get_llm_client
from .github_pr import PRManager

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

    def diagnose_and_heal(self, function_name: str, lookback_minutes: int = 60, create_pr: bool = False) -> str:
        """
        Analyzes recent errors of a specific function and suggests corrected code.
        If create_pr is True, it attempts to create a GitHub Pull Request with the fix.
        """
        if self.client is None:
            return "❌ OpenAI client initialization failed."

        print(f"🕵️ Analyzing function: '{function_name}'...")

        # 1. Retrieve original function source code
        func_defs = search_functions_hybrid(query=function_name, limit=1, alpha=0.1)
        if not func_defs:
            return f"❌ Function definition not found: {function_name}"

        module_name = func_defs[0]['properties'].get('module_name')
        file_path = func_defs[0]['properties'].get('file_path')
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
            suggested_code = self.client.create_chat_completion(
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

            if not suggested_code:
                return "❌ LLM returned no response."

            # [Cleanup 1] 마크다운 제거
            suggested_code = self._clean_llm_response(suggested_code)

            # 6. Handle PR Creation if requested
            if create_pr and ("def " in suggested_code or "async def " in suggested_code):
                print("🚀 Initiating PR creation sequence...")
                pr_result = self._handle_pr_creation(module_name, file_path, function_name, suggested_code)
                return f"{suggested_code}\n\n{pr_result}"

            return suggested_code

        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            return f"❌ Error occurred during LLM call: {e}"

    def _clean_llm_response(self, text: str) -> str:
        """
        Removes Markdown code block formatting (```python ... ```) from the LLM response.
        """
        text = text.strip()
        code_block_pattern = r"```(?:\w+)?\s*(.*?)```"
        match = re.search(code_block_pattern, text, re.DOTALL)
        if match:
            return match.group(1).strip()
        return text

    def _handle_pr_creation(self, module_name: str, file_path: str, function_name: str, new_func_code: str) -> str:
        """
        Locates the file using stored path (or module name), creates a patched version of the content,
        and calls PRManager to open a PR.
        """
        target_path = file_path

        if not target_path:
            if not module_name:
                return "❌ No file path or module name found."
            try:
                module = importlib.import_module(module_name)
                target_path = inspect.getsourcefile(module)
            except ImportError:
                return f"❌ Could not locate file via module: {module_name}"
            except Exception as e:
                return f"❌ Error locating file via module: {e}"

        if not target_path or not os.path.exists(target_path):
            return f"❌ File not found at: {target_path}"

        try:
            # 여기서 패치 적용 (임포트 호이스팅 포함)
            new_full_content = self._apply_patch_to_file_content(target_path, function_name, new_func_code)

            if not new_full_content:
                return "❌ Failed to apply patch to file content."

            try:
                rel_path = os.path.relpath(target_path, os.getcwd())
            except ValueError:
                rel_path = target_path

            pr_manager = PRManager()
            result = pr_manager.create_fix_pr(
                file_path=rel_path,
                function_name=function_name,
                new_file_content=new_full_content,
                diagnosis="Automated fix by VectorWave Healer based on error logs."
            )
            return result

        except Exception as e:
            logger.error(f"PR creation process failed: {e}")
            return f"❌ PR creation process failed: {e}"

    def _separate_imports_and_code(self, new_code: str) -> Tuple[List[str], str]:
        """
        AI 응답에서 Import와 함수 본문만 남기고,
        중간에 낀 전역 변수나 잡다한 코드는 제거합니다.
        """
        lines = new_code.strip().splitlines()
        imports = []
        func_lines = []

        found_def = False

        for line in lines:
            stripped = line.strip()

            # 1. 함수 정의가 시작되면 그 뒤는 무조건 함수 본문
            if stripped.startswith("def ") or stripped.startswith("async def "):
                found_def = True

            if found_def:
                func_lines.append(line)
            else:
                # 2. 함수 정의 전: import 문만 골라냄
                if stripped.startswith("import ") or stripped.startswith("from "):
                    imports.append(line)
                # [핵심] import가 아닌 다른 코드(GLOBAL_STATE = ... 등)는 여기서 무시됨(버려짐)

        return imports, "\n".join(func_lines).strip()

    def _apply_patch_to_file_content(self, file_path: str, func_name: str, new_code: str) -> Optional[str]:
        """
        Reads the file, replaces the target function with new_code,
        AND hoists any new imports to the top of the file.
        """
        try:
            # 1. AI 코드에서 임포트와 함수 분리
            imports_to_add, cleaned_func_code = self._separate_imports_and_code(new_code)

            with open(file_path, 'r', encoding='utf-8') as f:
                source = f.read()

            tree = ast.parse(source)
            target_node = None
            for node in ast.walk(tree):
                if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == func_name:
                    target_node = node
                    break

            if not target_node:
                logger.warning(f"Function '{func_name}' not found in file '{file_path}' via AST.")
                return None

            start_line = target_node.lineno - 1
            end_line = target_node.end_lineno

            lines = source.splitlines(keepends=True)

            original_def_line = lines[start_line]
            original_indent = original_def_line[:len(original_def_line) - len(original_def_line.lstrip())]

            new_lines_raw = cleaned_func_code.strip().splitlines()
            new_lines_indented = []

            for line in new_lines_raw:
                if line.strip():
                    new_lines_indented.append(original_indent + line + "\n")
                else:
                    new_lines_indented.append("\n")

            real_def_line_idx = start_line
            for i in range(start_line, end_line):
                stripped_line = lines[i].strip()
                if stripped_line.startswith("def ") or stripped_line.startswith("async def "):
                    real_def_line_idx = i
                    break

            # 2. 파일 내용 재조립 (함수 교체)
            content_with_new_func = (
                    "".join(lines[:real_def_line_idx]) +
                    "".join(new_lines_indented) +
                    "".join(lines[end_line:])
            )

            # 3. [Import Hoisting] 임포트 구문 최상단 추가
            if imports_to_add:
                # 기존 파일 내용에 이미 해당 임포트가 있는지 단순 텍스트 매칭으로 확인 (중복 방지)
                # (더 정교하게 하려면 AST를 써야 하지만, 이 정도면 충분합니다)
                final_imports = []
                for imp in imports_to_add:
                    if imp.strip() not in source:
                        final_imports.append(imp)

                if final_imports:
                    # 파일 맨 위에 추가
                    final_content = "\n".join(final_imports) + "\n" + content_with_new_func
                else:
                    final_content = content_with_new_func
            else:
                final_content = content_with_new_func

            # 4. Refuse to ship a patch that doesn't even parse. The LLM is
            # prone to producing slightly-wrong Python at low temperature, and
            # without this guard a syntax error would be opened as a PR.
            try:
                ast.parse(final_content)
            except SyntaxError as syn:
                logger.error(
                    f"Refusing to apply LLM patch: result is not valid Python "
                    f"(line {syn.lineno}, col {syn.offset}): {syn.msg}"
                )
                return None

            return final_content

        except Exception as e:
            logger.error(f"Patch application failed: {e}")
            return None

    def _construct_prompt(self, func_name, source_code, errors, successes, lookback_minutes) -> str:
        # (기존 코드 유지)
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
3. **Fix**: Rewrite the function so that it returns correct results for ALL inputs, including those that previously caused errors.
    - If you need new libraries (e.g., asyncio, time), include the `import` statements at the very top of your response.
    - Fix the root logic itself. DO NOT simply add defensive `raise` statements or wrap code in `try/except` as a workaround.
    - Use the "Successful Executions" above to infer the expected input→output pattern.
    - Refactor the code to be clean and idiomatic Python.
4. **Constraint**:
    - Return **ONLY** the full, corrected function definition.
    - Start with any necessary imports, then `def {func_name}(...):` or `async def {func_name}(...):`.
    - **DO NOT** include the `@vectorize` decorator in the output.
    - **DO NOT** include any markdown formatting (like ```python), comments outside the function, or explanations.
'''
        return prompt