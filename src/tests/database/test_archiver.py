import json
import pytest
from datetime import datetime, timezone
from uuid import uuid4

from vectorwave.database.archiver import VectorWaveArchiver
from vectorwave.database.db import get_weaviate_client, create_execution_schema
from vectorwave.models.db_config import WeaviateSettings


@pytest.fixture
def e2e_settings() -> WeaviateSettings:
    s = WeaviateSettings()
    s.IS_VECTORIZE_COLLECTION_NAME = False
    s.VECTORIZER = "none"
    return s


def _seed(coll, *, function_name, return_value, status="SUCCESS", input_payload=None):
    props = {
        "function_uuid": str(uuid4()),
        "function_name": function_name,
        "return_value": return_value,
        "status": status,
        "duration_ms": 10,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
    }
    if input_payload:
        props.update(input_payload)
    return coll.data.insert(properties=props)


def _read_jsonl(path):
    with open(path, "r", encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


# ---------------------------------------------------------------------------
# Unit test — pure JSONL conversion, no DB needed
# ---------------------------------------------------------------------------

def test_convert_to_training_format_excludes_metadata_keys():
    """`_convert_to_training_format` filters out internal fields and serializes inputs."""
    from types import SimpleNamespace

    obj = SimpleNamespace(
        uuid=uuid4(),
        properties={
            "function_name": "calc",
            "status": "SUCCESS",
            "input_a": 10,
            "input_b": 20,
            "return_value": 30,
            "duration_ms": 100,
        },
    )
    formatted = VectorWaveArchiver.__new__(VectorWaveArchiver)._convert_to_training_format(obj)

    messages = formatted["messages"]
    assert len(messages) == 2
    user_content = json.loads(messages[0]["content"])
    assert user_content == {"input_a": 10, "input_b": 20}
    assert messages[1]["content"] == "30"


# ---------------------------------------------------------------------------
# E2E — real Weaviate, real file IO
# ---------------------------------------------------------------------------

@pytest.mark.e2e
def test_export_writes_jsonl_and_clears_when_requested(clean_weaviate, e2e_settings, tmp_path):
    """export_and_clear with clear_after_export=True writes JSONL and removes the rows."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="test_func", return_value="r1")
        _seed(coll, function_name="test_func", return_value="r2")

        out = tmp_path / "exports" / "dataset.jsonl"
        archiver = VectorWaveArchiver()
        result = archiver.export_and_clear(
            function_name="test_func",
            output_file=str(out),
            clear_after_export=True,
        )

        assert result["exported"] == 2
        assert result["deleted"] == 2

        rows = _read_jsonl(out)
        assert len(rows) == 2
        assert all("messages" in r for r in rows)

        remaining = coll.query.fetch_objects(limit=10).objects
        assert len(remaining) == 0
    finally:
        client.close()


@pytest.mark.e2e
def test_export_only_does_not_delete(clean_weaviate, e2e_settings, tmp_path):
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="keep_me", return_value="r1")

        out = tmp_path / "backup.jsonl"
        archiver = VectorWaveArchiver()
        result = archiver.export_and_clear(
            function_name="keep_me",
            output_file=str(out),
            clear_after_export=False,
        )

        assert result["exported"] == 1
        assert result["deleted"] == 0
        assert len(coll.query.fetch_objects(limit=10).objects) == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_delete_only_skips_file_and_purges(clean_weaviate, e2e_settings, tmp_path):
    """delete_only mode purges all matching rows including non-SUCCESS ones."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="purge_me", return_value="ok", status="SUCCESS")
        _seed(coll, function_name="purge_me", return_value="bad", status="FAILURE")

        out = tmp_path / "should_not_be_created.jsonl"
        archiver = VectorWaveArchiver()
        result = archiver.export_and_clear(
            function_name="purge_me",
            output_file=str(out),
            delete_only=True,
        )

        assert result["exported"] == 0
        assert result["deleted"] == 2
        assert not out.exists()
        assert len(coll.query.fetch_objects(limit=10).objects) == 0
    finally:
        client.close()


@pytest.mark.e2e
def test_file_write_failure_aborts_delete(clean_weaviate, e2e_settings, monkeypatch):
    """If file write fails, no rows are deleted (safety invariant)."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="safety", return_value="r1")

        original_open = open

        def failing_open(*args, **kwargs):
            if args and args[0] and "safety_export.jsonl" in str(args[0]):
                raise IOError("Disk Full")
            return original_open(*args, **kwargs)

        monkeypatch.setattr("builtins.open", failing_open)

        archiver = VectorWaveArchiver()
        result = archiver.export_and_clear(
            function_name="safety",
            output_file="safety_export.jsonl",
            clear_after_export=True,
        )

        assert result["exported"] == 0
        assert result["deleted"] == 0
        assert len(coll.query.fetch_objects(limit=10).objects) == 1
    finally:
        client.close()


@pytest.mark.e2e
def test_export_only_includes_success_status(clean_weaviate, e2e_settings, tmp_path):
    """Non-delete-only mode filters to status=SUCCESS rows for training export."""
    client = get_weaviate_client()
    try:
        create_execution_schema(client, e2e_settings)
        coll = client.collections.get(e2e_settings.EXECUTION_COLLECTION_NAME)
        _seed(coll, function_name="mixed", return_value="ok1", status="SUCCESS")
        _seed(coll, function_name="mixed", return_value="ok2", status="SUCCESS")
        _seed(coll, function_name="mixed", return_value="bad", status="FAILURE")

        out = tmp_path / "training.jsonl"
        archiver = VectorWaveArchiver()
        result = archiver.export_and_clear(
            function_name="mixed",
            output_file=str(out),
        )

        assert result["exported"] == 2
        rows = _read_jsonl(out)
        assert len(rows) == 2
    finally:
        client.close()
