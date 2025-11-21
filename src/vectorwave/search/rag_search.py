import logging

from ..database.db_search import search_functions
from ..models.db_config import get_weaviate_settings
from ..search.execution_search import find_by_trace_id

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None

logger = logging.getLogger(__name__)


def _get_openai_client():
    """Loads the OpenAI client (includes configuration check)."""
    if OpenAI is None:
        logger.error("OpenAI library not found. Install with 'pip install openai'.")
        return None

    settings = get_weaviate_settings()
    if not settings.OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY is missing in environment variables.")
        return None

    return OpenAI(api_key=settings.OPENAI_API_KEY)


def search_and_answer(query: str, model: str = "gpt-4-turbo", language: str = "en") -> str:
    """
    [Code RAG] Retrieves a function and generates an answer based on its content.
    This is a high-level wrapper around existing search_functions().

    Args:
        query: The user's natural language question.
        model: The LLM model to use (default: gpt-4-turbo).
        language: The language for the answer ('en' for English, 'ko' for Korean).
    """
    # 1. Retrieve (Use existing DB search)
    logger.info(f"🔍 Searching codebase for: '{query}'...")
    search_results = search_functions(query=query, limit=1)

    if not search_results:
        msg = "❌ No relevant functions found. The DB might be empty or the query is unclear."
        return msg if language == 'en' else "❌ 관련 함수를 찾을 수 없습니다. DB에 내용이 없거나 검색어가 명확하지 않습니다."

    best_match = search_results[0]
    props = best_match['properties']

    # 2. Augment (Construct Prompt)
    context = f"""
    [Target Function]: {props.get('function_name')}
    [Description]: {props.get('search_description')}
    [Docstring]: {props.get('docstring')}
    
    [Source Code]:
    ```python
    {props.get('source_code')}
    ```
    """

    # Dynamic System Prompt based on language
    if language == 'ko':
        system_instruction = (
            "You are a helpful code assistant provided by VectorWave. "
            "Answer the user's question based ONLY on the provided function context. "
            "Explain the logic clearly in **Korean**."
        )
    else:
        system_instruction = (
            "You are a helpful code assistant provided by VectorWave. "
            "Answer the user's question based ONLY on the provided function context. "
            "Explain the logic clearly in **English**."
        )

    user_message = f"Context:\n{context}\n\nQuestion: {query}"

    # 3. Generate (LLM Response)
    client = _get_openai_client()
    if not client:
        msg = "❌ OpenAI client could not be initialized. (Check .env settings)"
        return msg if language == 'en' else "❌ OpenAI 클라이언트를 초기화할 수 없습니다. (.env 설정 확인 필요)"

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": user_message}
            ],
            temperature=0.1
        )
        return response.choices[0].message.content

    except Exception as e:
        logger.error(f"LLM generation failed: {e}")
        error_msg = f"❌ Error during answer generation: {e}"
        return error_msg if language == 'en' else f"❌ 답변 생성 중 오류 발생: {e}"


def analyze_trace_log(trace_id: str, model: str = "gpt-4-turbo", language: str = "en") -> str:
    """
    [Trace RAG] Retrieves all logs for a specific Trace ID and analyzes the cause.

    Args:
        trace_id: The target Trace ID.
        model: The LLM model to use.
        language: The language for the analysis ('en' for English, 'ko' for Korean).
    """
    # 1. Retrieve (Use existing execution search)
    logger.info(f"🔍 Fetching trace logs for ID: {trace_id}...")
    spans = find_by_trace_id(trace_id=trace_id)

    if not spans:
        msg = f"❌ Could not find logs for Trace ID '{trace_id}'."
        return msg if language == 'en' else f"❌ Trace ID '{trace_id}'에 대한 로그를 찾을 수 없습니다."

    # 2. Augment (Textualize Logs)
    log_summary = "Execution Flow:\n"
    for i, span in enumerate(spans):
        status = "✅ SUCCESS" if span.get('status') == 'SUCCESS' else "❌ ERROR"
        log_summary += f"{i + 1}. {span.get('function_name')} [{status}] ({span.get('duration_ms')}ms)\n"

        if span.get('status') == 'ERROR':
            log_summary += f"   -> Error Code: {span.get('error_code')}\n"
            log_summary += f"   -> Message: {span.get('error_message')}\n"

    # Dynamic System Prompt based on language
    if language == 'ko':
        system_instruction = (
            "You are an AI debugger. Analyze the execution flow below. "
            "Summarize what happened, and if there was an error, pinpoint the root cause function and reason. "
            "Please respond in **Korean**."
        )
    else:
        system_instruction = (
            "You are an AI debugger. Analyze the execution flow below. "
            "Summarize what happened, and if there was an error, pinpoint the root cause function and reason. "
            "Please respond in **English**."
        )

    # 3. Generate
    client = _get_openai_client()
    if not client:
        msg = "❌ OpenAI client initialization failed."
        return msg if language == 'en' else "❌ OpenAI 클라이언트 초기화 실패"

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": log_summary}
            ],
            temperature=0.1
        )
        return response.choices[0].message.content

    except Exception as e:
        error_msg = f"❌ Error during analysis: {e}"
        return error_msg if language == 'en' else f"❌ 분석 중 오류 발생: {e}"