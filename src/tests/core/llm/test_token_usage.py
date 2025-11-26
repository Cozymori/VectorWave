import pytest
from unittest.mock import MagicMock, patch
from vectorwave.core.llm.openai_client import VectorWaveOpenAIClient

@pytest.fixture
def mock_deps(monkeypatch):
    """
    Mocks the dependencies (BatchManager, OpenAI, Settings) of VectorWaveOpenAIClient.
    """
    # 1. Mock BatchManager (Target for verifying token usage persistence)
    mock_batch = MagicMock()
    mock_batch.add_object = MagicMock()
    mock_get_batch = MagicMock(return_value=mock_batch)
    monkeypatch.setattr("vectorwave.core.llm.openai_client.get_batch_manager", mock_get_batch)

    # 2. Mock Settings (Bypasses API Key check)
    mock_settings = MagicMock()
    mock_settings.OPENAI_API_KEY = "sk-test-key"
    mock_get_settings = MagicMock(return_value=mock_settings)
    monkeypatch.setattr("vectorwave.core.llm.openai_client.get_weaviate_settings", mock_get_settings)

    # 3. Mock OpenAI Class (Bypasses actual API calls)
    mock_openai_cls = MagicMock()
    monkeypatch.setattr("vectorwave.core.llm.openai_client.OpenAI", mock_openai_cls)

    # Note: Clearing the lru_cache for the singleton instance is typically
    # needed if the client is imported globally. We assume the current test
    # setup allows for fresh initialization here.

    return {
        "batch": mock_batch,
        "openai_cls": mock_openai_cls
    }

def test_chat_completion_logs_token_usage(mock_deps):
    """
    [Case 1] Verify that token usage is logged as 'generation' type during chat completion.
    """
    # Arrange
    # Get the mock client instance returned when OpenAI() is called
    mock_client_instance = mock_deps["openai_cls"].return_value

    # Mock OpenAI Response structure
    mock_response = MagicMock()
    mock_response.choices = [MagicMock(message=MagicMock(content="Test response"))]
    mock_response.usage.total_tokens = 123  # Token count to test
    mock_client_instance.chat.completions.create.return_value = mock_response

    # Act
    # Create a new client instance for the test
    client = VectorWaveOpenAIClient()
    result = client.create_chat_completion(
        messages=[{"role": "user", "content": "Hi"}],
        model="gpt-4-test",
        category="test_chat_category"
    )

    # Assert
    # 1. Verify that the function returns the expected result
    assert result == "Test response"

    # 2. Verify that BatchManager.add_object was called (log saved)
    mock_batch = mock_deps["batch"]
    mock_batch.add_object.assert_called_once()

    # 3. Verify the properties of the saved log entry
    call_kwargs = mock_batch.add_object.call_args.kwargs
    props = call_kwargs["properties"]

    assert call_kwargs["collection"] == "VectorWaveTokenUsage"
    assert props["tokens"] == 123
    assert props["model"] == "gpt-4-test"
    assert props["category"] == "test_chat_category"
    assert props["usage_type"] == "generation"

def test_create_embedding_logs_token_usage(mock_deps):
    """
    [Case 2] Verify that token usage is logged as 'embedding' type during embedding creation.
    """
    # Arrange
    mock_client_instance = mock_deps["openai_cls"].return_value

    # Mock OpenAI Response structure
    mock_response = MagicMock()
    mock_response.data = [MagicMock(embedding=[0.1, 0.2, 0.3])]
    mock_response.usage.total_tokens = 45  # Token count to test
    mock_client_instance.embeddings.create.return_value = mock_response

    # Act
    client = VectorWaveOpenAIClient()
    result = client.create_embedding(
        text="Test text",
        model="text-embedding-3-small",
        category="test_embed_category"
    )

    # Assert
    # 1. Verify that the function returns the expected result
    assert result == [0.1, 0.2, 0.3]

    # 2. Verify that BatchManager.add_object was called
    mock_batch = mock_deps["batch"]
    mock_batch.add_object.assert_called_once()

    # 3. Verify the properties of the saved log entry
    call_kwargs = mock_batch.add_object.call_args.kwargs
    props = call_kwargs["properties"]

    assert call_kwargs["collection"] == "VectorWaveTokenUsage"
    assert props["tokens"] == 45
    assert props["usage_type"] == "embedding"
    assert props["category"] == "test_embed_category"