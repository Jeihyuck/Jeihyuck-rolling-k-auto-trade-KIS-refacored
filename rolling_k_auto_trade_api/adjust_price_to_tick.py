# rolling_k_auto_trade_api/adjust_price_to_tick.py

def adjust_price_to_tick(price: float, code: str | None = None) -> int:
    """
    한국 주식 시장 호가단위(틱사이즈)에 맞게 가격을 자동 보정.
    - KOSDAQ/코스피 기준 호가단위 (1원 ~ 1000원 단위까지 지원)
    - code 인자는 호가 단위 결정에 사용하지 않으나, 호출부 호환성을 위해 허용한다.
    """
    # 2024년 기준 틱사이즈 테이블 (KOSPI/KOSDAQ 공통)
    tick_table = [
        (1_000_000, 1_000),
        (500_000, 500),
        (100_000, 100),
        (10_000, 50),
        (1_000, 10),
        (100, 1),
        (0, 1),
    ]
    price = float(price)
    for base, tick in tick_table:
        if price >= base:
            # price를 해당 tick단위로 내림 (주문 오류 방지)
            return int(price // tick * tick)
    return int(price)

# (보조) 소수점 가격(ETF, ETN 등)용
def adjust_price_to_tick_decimal(price: float) -> float:
    """
    소수점 주식/ETF/ETN 등에서 쓸 수 있는 틱 단위 보정 (1원, 0.5원 등)
    """
    tick_table = [
        (100_000, 100),
        (10_000, 10),
        (1_000, 1),
        (100, 0.1),
        (0, 0.01),
    ]
    price = float(price)
    for base, tick in tick_table:
        if price >= base:
            return round((price // tick) * tick, 2)
    return round(price, 2)

