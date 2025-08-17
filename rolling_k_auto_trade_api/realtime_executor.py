# realtime_executor.py
import time
from datetime import datetime
import logging
import os

from trader.strategy_atr import ATRStrategyEngine, StrategyATRParams
from .kis_api import get_price_data, send_order_wrapper as send_order, get_cash_balance_wrapper as get_cash_balance

logger = logging.getLogger(__name__)

# ATR 전략 초기화 (환경변수로 파라미터 조정 가능)
atr_engine = ATRStrategyEngine(StrategyATRParams())

def monitor_and_trade_with_atr(stocks, top_n=None):
    """
    ATR 전략 기반 Top N 매수/매도 실시간 실행
    stocks: [{'stock_code', 'best_k', 'avg_return_pct', '목표가'}]
    top_n: None이면 환경변수 TOP_N_STOCKS 사용, 기본=5
    """
    if not stocks:
        logger.warning("[ATR매매] 리밸런싱 종목이 없습니다.")
        return

    if top_n is None:
        top_n = int(os.getenv("TOP_N_STOCKS", 5))

    # 수익률 기준 Top N 선정
    selected = sorted(stocks, key=lambda x: x.get('avg_return_pct', 0), reverse=True)[:top_n]
    logger.info(f"[ATR매매] Top {top_n} 종목 선정: {[s['stock_code'] for s in selected]}")

    # 예수금 기반 수량 계산
    cash_info = get_cash_balance()
    total_cash = cash_info.get("cash") if isinstance(cash_info, dict) else cash_info
    try:
        total_cash = float(total_cash)
    except:
        total_cash = 0

    if total_cash <= 0:
        logger.warning("[ATR매매] 예수금이 없습니다. 매매를 종료합니다.")
        return

    cash_per_stock = total_cash / len(selected)

    # 장중 실시간 매매
    while datetime.now().hour < 15:
        for stock in selected:
            code = stock["stock_code"]
            target_price = stock.get("목표가") or stock.get("target_price") or 0

            # 현재가 조회
            try:
                data = get_price_data(code)
                current_price = data.get("price")
                if current_price is None:
                    raise ValueError("price is None")
            except Exception as e:
                logger.warning(f"[가격조회실패] {code}: {e}")
                continue

            pos_state = atr_engine.get_position_state(code)

            # 매수 조건: 목표가 돌파 + 미보유
            if not pos_state and current_price > target_price:
                qty = max(1, int(cash_per_stock // current_price))
                send_order("buy", code, qty)
                atr_engine.on_buy_filled(code, qty, current_price)
                logger.info(f"[매수] {code} {qty}주 @ {current_price} (목표가 {target_price})")
                continue

            # 보유 시 ATR 전략 매도 판단
            if pos_state:
                decision = atr_engine.feed_tick_and_decide(code, current_price)
                if decision.type == "SELL_ALL":
                    send_order("sell", code, pos_state["qty"])
                    atr_engine.on_sell_filled(code, pos_state["qty"])
                    logger.info(f"[전량매도] {code} {pos_state['qty']}주 @ {current_price} 사유={decision.reason}")
                elif decision.type == "SELL_PARTIAL" and decision.qty > 0:
                    send_order("sell", code, decision.qty)
                    atr_engine.on_sell_filled(code, decision.qty)
                    logger.info(f"[부분매도] {code} {decision.qty}주 @ {current_price} 사유={decision.reason}")

        time.sleep(10)  # 10초 대기
