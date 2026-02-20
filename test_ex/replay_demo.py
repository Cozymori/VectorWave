"""
replay_demo.py - Replay 기능 실제 동작 테스트 (외부 호출 없음)

실행 방법:
  python3 test_ex/replay_demo.py
  pytest test_ex/replay_demo.py -v
"""

import importlib
import json
import inspect
import sys
from unittest.mock import MagicMock, patch

# test_ex/replay_fixtures 가 항상 동일한 경로로 임포트되도록 보장
import test_ex.replay_fixtures as _fx


# ── Weaviate Mock 구성 ────────────────────────────────────────────────────────

def _make_mock_weaviate(exec_logs=None):
    mock_settings = MagicMock()
    mock_settings.EXECUTION_COLLECTION_NAME = "VectorWaveExecutions"
    mock_settings.GOLDEN_COLLECTION_NAME = "VectorWaveGoldenDataset"

    mock_exec_col = MagicMock()
    mock_golden_col = MagicMock()

    mock_exec_col.query.fetch_objects.return_value = MagicMock(objects=exec_logs or [])
    mock_exec_col.query.fetch_object_by_id.return_value = None
    mock_exec_col.data = MagicMock()

    mock_golden_col.query.fetch_objects.return_value = MagicMock(objects=[])
    mock_golden_col.data = MagicMock()

    mock_client = MagicMock()

    def _get_col(name):
        if name == "VectorWaveGoldenDataset":
            return mock_golden_col
        return mock_exec_col

    mock_client.collections.get.side_effect = _get_col
    return mock_client, mock_settings, mock_exec_col


def _make_log(uuid_str: str, inputs: dict, return_value) -> MagicMock:
    obj = MagicMock()
    obj.uuid = uuid_str
    props = inputs.copy()
    props["return_value"] = (
        json.dumps(return_value) if not isinstance(return_value, str) else return_value
    )
    props["timestamp_utc"] = "2024-01-01T00:00:00Z"
    obj.properties = props
    return obj


def _run_replay(func_full_name: str, func_obj, logs: list,
                mocks=None, update_baseline: bool = False) -> dict:
    mock_client, mock_settings, mock_exec_col = _make_mock_weaviate(exec_logs=logs)

    func_name = func_full_name.rsplit(".", 1)[1]
    mock_module = MagicMock()
    setattr(mock_module, func_name, func_obj)

    mock_importlib = MagicMock(spec=importlib)
    mock_importlib.import_module.return_value = mock_module

    with patch("vectorwave.utils.replayer.get_cached_client", return_value=mock_client), \
         patch("vectorwave.utils.replayer.get_weaviate_settings", return_value=mock_settings), \
         patch("vectorwave.utils.replayer.importlib", mock_importlib):

        from vectorwave.utils.replayer import VectorWaveReplayer  # noqa: PLC0415
        replayer = VectorWaveReplayer()

        return replayer.replay(
            func_full_name, limit=len(logs),
            mocks=mocks, update_baseline=update_baseline
        ), mock_exec_col


# ── 테스트 케이스 ─────────────────────────────────────────────────────────────

def test_순수로직_매치():
    """외부 호출 없는 순수 함수: 예상값과 일치 → PASSED"""
    logs = [_make_log("uuid-1", {"a": 3, "b": 4}, 7)]
    result, _ = _run_replay("test_ex.replay_fixtures.add", _fx.add, logs)

    assert result["passed"] == 1
    assert result["failed"] == 0
    print("[OK] test_순수로직_매치")


def test_순수로직_불일치():
    """함수 결과가 기록된 기대값과 다를 때 → FAILED (회귀 감지)"""
    def buggy_add(a: int, b: int) -> int:
        return a + b + 100  # 버그

    buggy_add.__signature__ = inspect.signature(_fx.add)

    logs = [_make_log("uuid-2", {"a": 3, "b": 4}, 7)]
    result, _ = _run_replay("test_ex.replay_fixtures.add", buggy_add, logs)

    assert result["passed"] == 0
    assert result["failed"] == 1
    assert result["failures"][0]["expected"] == 7
    assert result["failures"][0]["actual"] == 107
    print("[OK] test_순수로직_불일치")


def test_외부호출_mocks로_차단():
    """
    process_order 는 _external_payment_api 를 호출한다.
    mocks 파라미터로 해당 호출을 차단하고 가짜 응답을 주입한다.
    """
    expected_payment = {"status": "approved", "amount": 50.0}
    logs = [
        _make_log(
            "uuid-3",
            {"item": "book", "quantity": 2, "price_per_unit": 25.0},
            {"item": "book", "quantity": 2, "payment": expected_payment},
        )
    ]

    result, _ = _run_replay(
        "test_ex.replay_fixtures.process_order",
        _fx.process_order,
        logs,
        mocks={
            "test_ex.replay_fixtures._external_payment_api": {
                "return_value": expected_payment
            }
        },
    )

    assert result["passed"] == 1
    assert result["failed"] == 0
    print("[OK] test_외부호출_mocks로_차단")


def test_외부호출_차단없이_실행시_예외처리():
    """mocks 없이 외부 호출 함수 실행 → RuntimeError → failed 로 기록됨"""
    logs = [
        _make_log(
            "uuid-4",
            {"item": "pen", "quantity": 1, "price_per_unit": 5.0},
            {"item": "pen", "payment": {"status": "approved"}},
        )
    ]

    result, _ = _run_replay(
        "test_ex.replay_fixtures.process_order",
        _fx.process_order,
        logs,
        mocks=None,
    )

    assert result["failed"] == 1
    assert "EXCEPTION_RAISED" in str(result["failures"][0]["actual"])
    print("[OK] test_외부호출_차단없이_실행시_예외처리")


def test_mocks_side_effect():
    """side_effect 로 입력값 기반 동적 응답 주입"""
    def dynamic_payment(amount):
        return {"status": "approved", "amount": amount * 0.9}

    logs = [
        _make_log(
            "uuid-5",
            {"item": "hat", "quantity": 2, "price_per_unit": 30.0},
            {"item": "hat", "quantity": 2,
             "payment": {"status": "approved", "amount": 54.0}},
        )
    ]

    result, _ = _run_replay(
        "test_ex.replay_fixtures.process_order",
        _fx.process_order,
        logs,
        mocks={
            "test_ex.replay_fixtures._external_payment_api": {
                "side_effect": dynamic_payment
            }
        },
    )

    assert result["passed"] == 1
    assert result["failed"] == 0
    print("[OK] test_mocks_side_effect")


def test_update_baseline():
    """update_baseline=True: 결과가 달라도 DB를 업데이트하고 PASSED 처리"""
    logs = [_make_log("uuid-6", {"name": "Alice"}, "Hello, Bob!")]  # 기대값이 틀림

    mock_client, mock_settings, mock_exec_col = _make_mock_weaviate(exec_logs=logs)

    mock_module = MagicMock()
    mock_module.greet = _fx.greet
    mock_importlib = MagicMock(spec=importlib)
    mock_importlib.import_module.return_value = mock_module

    with patch("vectorwave.utils.replayer.get_cached_client", return_value=mock_client), \
         patch("vectorwave.utils.replayer.get_weaviate_settings", return_value=mock_settings), \
         patch("vectorwave.utils.replayer.importlib", mock_importlib):

        from vectorwave.utils.replayer import VectorWaveReplayer  # noqa: PLC0415
        replayer = VectorWaveReplayer()

        result = replayer.replay(
            "test_ex.replay_fixtures.greet",
            limit=1,
            update_baseline=True,
        )

    assert result["updated"] == 1
    assert result["passed"] == 1
    assert result["failed"] == 0
    mock_exec_col.data.update.assert_called_once()
    print("[OK] test_update_baseline")


# ── 직접 실행 ─────────────────────────────────────────────────────────────────

TESTS = [
    test_순수로직_매치,
    test_순수로직_불일치,
    test_외부호출_mocks로_차단,
    test_외부호출_차단없이_실행시_예외처리,
    test_mocks_side_effect,
    test_update_baseline,
]

if __name__ == "__main__":
    print("=" * 60)
    print("VectorWave Replayer 동작 테스트 (외부 호출 없음)")
    print("=" * 60)

    passed_cnt, failed_cnt = 0, 0
    for test_fn in TESTS:
        try:
            test_fn()
            passed_cnt += 1
        except Exception as e:
            print(f"[FAIL] {test_fn.__name__}: {e}")
            failed_cnt += 1

    print(f"\n결과: {passed_cnt} passed / {failed_cnt} failed")
    sys.exit(1 if failed_cnt else 0)
