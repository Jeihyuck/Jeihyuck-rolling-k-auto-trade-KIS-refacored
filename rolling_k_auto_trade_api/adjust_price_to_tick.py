def adjust_price_to_tick(price: float) -> int:
    """한국거래소 호가 단위 규칙에 따라 가격을 조정"""
    price = float(price)
    if price < 1000:
        tick = 1
    elif price < 5000:
        tick = 5
    elif price < 10000:
        tick = 10
    elif price < 50000:
        tick = 50
    elif price < 100000:
        tick = 100
    elif price < 500000:
        tick = 500
    else:
        tick = 1000

    return int(round(price / tick) * tick)
