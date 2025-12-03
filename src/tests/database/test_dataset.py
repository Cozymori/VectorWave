import pytest
from unittest.mock import MagicMock, patch
import math
from vectorwave.database.dataset import VectorWaveDatasetManager
from vectorwave.models.db_config import WeaviateSettings

@pytest.fixture
def mock_dataset_deps(monkeypatch):
    """Mocking dependencies for DatasetManager tests"""
    # 1. Settings Mock
    mock_settings = WeaviateSettings(
        EXECUTION_COLLECTION_NAME="Executions",
        GOLDEN_COLLECTION_NAME="GoldenData",
        RECOMMENDATION_STEADY_MARGIN=0.1,
        RECOMMENDATION_DISCOVERY_MARGIN=0.2
    )
    mock_get_settings = MagicMock(return_value=mock_settings)

    # 2. Client & Collections Mock
    mock_client = MagicMock()
    mock_exec_col = MagicMock()
    mock_golden_col = MagicMock()

    def get_collection_side_effect(name):
        if name == "Executions": return mock_exec_col
        if name == "GoldenData": return mock_golden_col
        return MagicMock()

    mock_client.collections.get.side_effect = get_collection_side_effect
    mock_get_client = MagicMock(return_value=mock_client)

    # 3. Patching
    monkeypatch.setattr("vectorwave.database.dataset.get_cached_client", mock_get_client)
    monkeypatch.setattr("vectorwave.database.dataset.get_weaviate_settings", mock_get_settings)

    return {
        "exec_col": mock_exec_col,
        "golden_col": mock_golden_col,
        "settings": mock_settings
    }

def create_mock_obj(uuid_str, props=None, vector=None):
    obj = MagicMock()
    obj.uuid = uuid_str
    obj.properties = props or {}
    if vector:
        obj.vector = {"default": vector}
    return obj

def test_register_as_golden_success(mock_dataset_deps):
    """[Case 1] Test successful Golden Data registration"""
    manager = VectorWaveDatasetManager()

    # Arrange: Simulate successful retrieval of original log
    mock_log = create_mock_obj("log-uuid-1", {"function_name": "test_func", "return_value": "res"}, vector=[0.1, 0.2])
    mock_dataset_deps["exec_col"].query.fetch_object_by_id.return_value = mock_log

    # Act
    result = manager.register_as_golden("log-uuid-1", note="Best case")

    # Assert
    assert result is True
    # Verify if insert was called on Golden Collection
    mock_dataset_deps["golden_col"].data.insert.assert_called_once()
    call_kwargs = mock_dataset_deps["golden_col"].data.insert.call_args.kwargs
    assert call_kwargs["properties"]["original_uuid"] == "log-uuid-1"
    assert call_kwargs["properties"]["note"] == "Best case"
    assert call_kwargs["vector"] == [0.1, 0.2] # Check if vector was copied

def test_recommend_candidates_logic(mock_dataset_deps):
    """[Case 2] Test density-based recommendation logic (Steady/Discovery)"""
    manager = VectorWaveDatasetManager()

    # Arrange 1: Golden Data (Set reference point)
    # Centroid: [1.0, 1.0], Avg Dist: 0.0 (Assuming all points are identical)
    golden_vec = [1.0, 1.0]
    golden_objs = [create_mock_obj("gold-1", {"original_uuid": "origin-1"}, golden_vec)]
    mock_dataset_deps["golden_col"].query.fetch_objects.return_value.objects = golden_objs

    # Cand A: [1.05, 1.05] -> Dist ≈ 0.07 (Steady Range: <= 0.1)
    # Cand B: [1.2, 1.2]   -> Dist ≈ 0.28 (Discovery Range: 0.1 < d <= 0.3)
    # Cand C: [2.0, 2.0]   -> Dist ≈ 1.41 (Ignore Range: > 0.3)
    cand_a = create_mock_obj("cand-a", {"return_value": "A"}, [1.05, 1.05])
    cand_b = create_mock_obj("cand-b", {"return_value": "B"}, [1.2, 1.2])
    cand_c = create_mock_obj("cand-c", {"return_value": "C"}, [2.0, 2.0])

    mock_dataset_deps["exec_col"].query.near_vector.return_value.objects = [cand_a, cand_b, cand_c]

    # Act
    recommendations = manager.recommend_candidates("test_func")

    # Assert
    assert len(recommendations) == 2
    assert recommendations[0]["uuid"] == "cand-a"
    assert recommendations[0]["type"] == "STEADY"
    assert recommendations[1]["uuid"] == "cand-b"
    assert recommendations[1]["type"] == "DISCOVERY"