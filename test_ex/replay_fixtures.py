"""
replay_demo에서 사용하는 테스트 대상 함수들.
별도 모듈로 분리해야 patch 경로('test_ex.replay_fixtures.*')가 항상 일정하다.
"""


def _external_payment_api(amount: float) -> dict:
    """실제 환경에서는 외부 결제 API를 호출 - 테스트에서 차단해야 함"""
    raise RuntimeError("실제 결제 API 호출됨! 테스트에서는 차단되어야 합니다.")


def process_order(item: str, quantity: int, price_per_unit: float) -> dict:
    """주문 처리 함수. 외부 결제 API를 호출한다."""
    subtotal = quantity * price_per_unit
    payment_result = _external_payment_api(subtotal)
    return {"item": item, "quantity": quantity, "payment": payment_result}


def add(a: int, b: int) -> int:
    """순수 로직 함수 (외부 호출 없음)"""
    return a + b


def greet(name: str) -> str:
    """순수 문자열 처리 함수"""
    return f"Hello, {name}!"
