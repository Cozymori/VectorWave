# test_ex/pure_logic.py
import time
import random

def validate_user(user_id):
    """Function to validate user ID"""
    print(f"  [Logic] Validating user: {user_id}")
    time.sleep(0.1)  # Simulate processing time
    return True

def calculate_discount(amount):
    """Function to calculate discount rate"""
    print(f"  [Logic] Calculating discount for ${amount}...")
    if amount > 100:
        return amount * 0.1
    return 0

def process_payment(user_id, amount):
    """
    Main function to process payment.
    Internally calls validate_user and calculate_discount.
    """
    print(f"\n[Logic] Starting payment process for {user_id} (${amount})")

    # 1. User validation (Should be a Child Span)
    if not validate_user(user_id):
        return {"status": "failed", "reason": "Invalid User"}

    # 2. Discount calculation (Should be a Child Span)
    discount = calculate_discount(amount)
    final_amount = amount - discount

    print(f"  [Logic] Payment successful! Final amount: ${final_amount}")
    return {"status": "success", "paid": final_amount}