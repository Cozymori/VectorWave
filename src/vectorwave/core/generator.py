import logging
import json
from typing import Optional, Dict, Any

from ..utils.function_cache import function_cache_manager
from ..models.db_config import get_weaviate_settings
from ..batch.batch import get_batch_manager
from ..vectorizer.factory import get_vectorizer
from .decorator import PENDING_FUNCTIONS
from .llm.factory import get_llm_client

logger = logging.getLogger(__name__)

try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


def generate_metadata_via_llm(source_code: str, func_name: str) -> Optional[Dict[str, str]]:
    """Call LLM to generate description and narrative from source code."""
    settings = get_weaviate_settings()
    client = get_llm_client()
    if not client:
        return None

    prompt = f"""
    Analyze the Python function below and generate a JSON object with two keys.
    Ensure the values are **single strings**, not nested objects.

    1. "search_description": A concise summary of what this function does (for vector search).
    2. "sequence_narrative": A brief explanation of the context, inputs, and outputs as a narrative text.
    
    Function Name: {func_name}
    Code:
    ```python
    {source_code}
    ```
    """

    try:
        # Refactored to use BaseLLMClient interface
        response_text = client.create_chat_completion(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a technical documentation assistant. Output only JSON."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0,
            response_format={"type": "json_object"},
            category="auto_doc"
        )

        if response_text:
            return json.loads(response_text)
        return None

    except Exception as e:
        logger.error(f"LLM generation failed for '{func_name}': {e}")
        return None


def generate_and_register_metadata():
    """
    [Entry Point] Processes all functions in PENDING_FUNCTIONS.
    """
    if not PENDING_FUNCTIONS:
        logger.info("No pending functions for auto-generation.")
        return

    logger.info(f"🚀 Processing {len(PENDING_FUNCTIONS)} functions for auto-documentation...")

    settings = get_weaviate_settings()
    batch = get_batch_manager()
    vectorizer = get_vectorizer()

    processed_count = 0

    for item in PENDING_FUNCTIONS:
        func_name = item["func_name"]
        func_uuid = item["func_uuid"]
        func_identifier = item["func_identifier"]
        static_props = item["static_properties"]

        current_hash = function_cache_manager.calculate_content_hash(func_identifier, static_props)

        cached_meta = function_cache_manager.get_cached_metadata(func_uuid, current_hash)

        final_desc = ""
        final_narr = ""

        if cached_meta:
            logger.info(f"✅ [Cache Hit] Loaded metadata for '{func_name}'.")
            final_desc = cached_meta.get("search_description")
            final_narr = cached_meta.get("sequence_narrative")
        else:
            logger.info(f"🤖 [Auto-Gen] Generating metadata for '{func_name}' via LLM...")
            generated = generate_metadata_via_llm(static_props["source_code"], func_name)

            if generated:
                final_desc = generated.get("search_description", "")
                final_narr = generated.get("sequence_narrative", "")

                if not isinstance(final_desc, str):
                    final_desc = json.dumps(final_desc, ensure_ascii=False)
                if not isinstance(final_narr, str):
                    final_narr = json.dumps(final_narr, ensure_ascii=False)

                # Update Cache with new metadata
                function_cache_manager.update_cache_with_metadata(
                    func_uuid, current_hash,
                    {"search_description": final_desc, "sequence_narrative": final_narr}
                )
            else:
                logger.warning(f"⚠️ Skipping registration for '{func_name}' due to generation failure.")
                continue

        # 3. Update Properties
        static_props["search_description"] = final_desc
        static_props["sequence_narrative"] = final_narr

        # 4. Vectorize the Description
        vector_to_add = None
        if vectorizer and final_desc:
            try:
                vector_to_add = vectorizer.embed(final_desc)
            except Exception as e:
                logger.warning(f"Vectorization failed for '{func_name}': {e}")

        # 5. Register to DB
        batch.add_object(
            collection=settings.COLLECTION_NAME,
            properties=static_props,
            uuid=func_uuid,
            vector=vector_to_add
        )
        processed_count += 1

    PENDING_FUNCTIONS.clear()
    logger.info(f"✨ Auto-generation complete. Registered {processed_count} functions.")