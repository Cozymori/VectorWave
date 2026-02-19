import sys
import os
import time
import random # random 모듈 임포트 위치 조정

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
os.chdir(current_script_dir)

# [MODIFIED] Import generate_and_register_metadata
from vectorwave import vectorize, initialize_database, generate_and_register_metadata
from vectorwave.monitoring.tracer import trace_span

# Exception 클래스와 함수 정의는 모듈 레벨에 유지해야 Replayer가 찾을 수 있습니다.
class CustomValueError(Exception):
    def __init__(self, message, error_code):
        super().__init__(message)
        self.error_code = error_code

@trace_span(attributes_to_capture=['user_id', 'amount'], capture_return_value=True)
def step_1_validate_payment(user_id: str, amount: int):
    print(f"  [SPAN 1] Validating payment for {user_id}...")
    if amount < 0:
        raise CustomValueError("Amount cannot be negative", "INVALID_INPUT")
    time.sleep(0.1)
    print(f"  [SPAN 1] Validation complete.")
    return True

@trace_span(attributes_to_capture=['user_id', 'receipt_id'], capture_return_value=True)
def step_2_send_receipt(user_id: str, receipt_id: str):
    print(f"  [SPAN 2] Sending receipt {receipt_id} to {user_id}...")
    time.sleep(0.2)
    print(f"  [SPAN 2] Receipt sent.")

@trace_span(attributes_to_capture=['user_id'], capture_return_value=True)
def step_3_get_user_details(user_id: str):
    print(f"  [SPAN 3] Fetching user details for {user_id}...")
    time.sleep(0.05)
    return {"username": "user_A", "email": "user_a@example.com", "is_active": True}

@trace_span(capture_return_value=True)
def step_4_get_user_roles():
    print(f"  [SPAN 4] Fetching user roles...")
    time.sleep(0.02)
    return ["admin", "billing_user", "support_agent"]

@trace_span(capture_return_value=True)
def step_5_get_user_balance():
    print(f"  [SPAN 5] Fetching user balance...")
    time.sleep(0.01)
    return 15000

@vectorize(
    search_description="Process user payment and return a receipt.",
    sequence_narrative="After payment is complete, a receipt is sent via email.",
    team="billing",
    priority=1,
    replay=True,
    capture_inputs=True
)
def process_payment(user_id: str, amount: int):
    print(f"  [ROOT EXEC] process_payment: Processing payment...")

    step_1_validate_payment(user_id=user_id, amount=amount)

    user_details = step_3_get_user_details(user_id=user_id)
    user_roles = step_4_get_user_roles()
    user_balance = step_5_get_user_balance()
    print(f"  [INFO] Got details: {user_details['username']}, Roles: {len(user_roles)}, Balance: {user_balance}")

    receipt_id = f"receipt_{int(time.time())}"

    step_2_send_receipt(user_id=user_id, receipt_id=receipt_id)

    print(f"  [ROOT DONE] process_payment")
    return {"status": "success", "receipt_id": receipt_id}

@vectorize(
    search_description="Generate a report for data analysis.",
    sequence_narrative="After the report is generated, an admin is notified.",
    team="data-science",
    replay=True
)
def generate_report():
    print(f"  [ROOT EXEC] generate_report: Generating report...")
    time.sleep(0.3)
    print(f"  [ROOT DONE] generate_report")
    return {"report_url": "/reports/analytics.pdf"}

# [NEW] Auto-Doc Test Function
@vectorize(auto=True, team="loyalty-program")
def calculate_loyalty_points(purchase_amount: int, is_vip: bool):
    """
    구매 금액에 따른 포인트 적립 계산 함수.
    VIP 고객은 포인트를 2배로 적립받습니다.
    """
    print(f"  [Auto-Doc Func] calculate_loyalty_points called.")
    points = purchase_amount // 10
    if is_vip:
        points *= 2
    return {"points": points, "tier": "VIP" if is_vip else "Regular"}

@vectorize(
    search_description="Generate a summary of customer review.",
    team="ai-service",
    priority=2,
    replay=True,
    capture_return_value=True
)
def generate_review_summary(review_text: str):
    """
    (LLM 시뮬레이션) 리뷰 요약 함수.
    호출될 때마다 문장 표현이 조금씩 달라지지만 의미는 같습니다.
    """
    print(f"  [AI] Summarizing review: {review_text[:10]}...")

    responses = [
        "The customer is highly satisfied with the product quality and fast shipping.",
        "User expressed great satisfaction regarding quality and delivery speed.",
        "Great product quality and fast delivery made the customer happy."
    ]

    return random.choice(responses)


# =================================================================
# [중요] 실행 로직을 main 블록으로 이동
# 이 파일이 직접 실행될 때만 아래 코드가 동작합니다.
# Replayer가 import 할 때는 함수 정의만 읽어갑니다.
# =================================================================
if __name__ == "__main__":
    client = None
    try:
        print("Attempting to initialize VectorWave database...")
        client = initialize_database()

        if client:
            print("Database connection and schema initialization successful.")
        else:
            raise ConnectionError("Database initialization failed. Check your settings.")

        # [NEW] Auto-Documentation Trigger
        print("🚀 Checking for functions needing auto-documentation...")
        generate_and_register_metadata()

        print("=" * 30)
        print("Function definitions complete (static data collected).")
        print("=" * 30)

        print("Now calling 'process_payment' (trace_id 1개 + 하위 span 2개 생성)...")

        # 정상 케이스
        process_payment(user_id="user_A", amount=10000)

        print("\nNow calling 'generate_report' (별개의 trace_id 1개 생성)...")
        generate_report()

        print("\nNow calling 'calculate_loyalty_points' (Auto-Doc & Execution)...")
        calculate_loyalty_points(purchase_amount=5000, is_vip=True)

        print("\nNow calling 'process_payment' (INVALID_INPUT case)...")
        try:
            process_payment(user_id="user_B", amount=-50)
        except CustomValueError as ve:
            print(f"  -> Intended error caught: {ve}")

        # AI 요약 함수 반복 호출
        for _ in range(6):
            generate_review_summary(review_text='I really loved this item! It arrived so fast...')

        time.sleep(10) # 이 대기 시간이 Replay 로그와의 시간차 원인이었습니다.

        print("\nFunction calls completed.")

    except Exception as e:
        print(f"\nError during function execution: {e}")

    finally:
        print("=" * 30)
        print("Script will be terminating soon.")
        if not client:
            print("Client was not successfully initialized.")