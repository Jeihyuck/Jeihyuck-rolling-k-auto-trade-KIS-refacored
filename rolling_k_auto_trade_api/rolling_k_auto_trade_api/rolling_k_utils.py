def calculate_target_price(prev_high, prev_low, today_open, k):
    """
    K 돌파 타겟 가격 계산 함수
    """
    return today_open + (prev_high - prev_low) * k
