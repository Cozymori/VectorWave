import sys
import os
import time

current_script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_script_dir)
src_path = os.path.join(project_root, 'src')
sys.path.insert(0, src_path)
os.chdir(current_script_dir)

from vectorwave import vectorize, initialize_database
from vectorwave.monitoring.tracer import trace_span

client = None


class CustomValueError(Exception):
    def __init__(self, message, error_code):
        super().__init__(message)
        self.error_code = error_code


try:
    print("Attempting to initialize VectorWave database...")
    client = initialize_database()

    if client:
        print("Database connection and schema initialization successful.")
    else:
        raise ConnectionError("Database initialization failed. Check your settings.")


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
        priority=1
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
        team="data-science"
    )
    def generate_report():
        print(f"  [ROOT EXEC] generate_report: Generating report...")
        time.sleep(0.3)
        print(f"  [ROOT DONE] generate_report")
        return {"report_url": "/reports/analytics.pdf"}


    print("=" * 30)
    print("Function definitions complete (static data collected).")
    print("=" * 30)

    print("Now calling 'process_payment' (trace_id 1개 + 하위 span 2개 생성)...")
    try:
        process_payment(user_id="user_A", amount=10000)

        print("\nNow calling 'generate_report' (별개의 trace_id 1개 생성)...")
        generate_report()

        print("\nNow calling 'generate_report' (별개의 trace_id 1개 생성)...")
        generate_report()

        print("\nNow calling 'process_payment' (INVALID_INPUT case)...")
        try:
            process_payment(user_id="user_B", amount=-50)
        except CustomValueError as ve:
            print(f"  -> Intended error caught: {ve}")

        print("\nFunction calls completed.")

    except Exception as e:
        print(f"\nError during function execution: {e}")

except Exception as e:
    print(f"\n[Fatal Error] Script execution halted: {e}")

finally:
    print("=" * 30)
    print("Script will be terminating soon.")
    if not client:
        print("Client was not successfully initialized.")
