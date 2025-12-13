# auto_trade_signal.py
import time
import logging
from datetime import datetime, timedelta
from typing import List, Dict

from rolling_k_auto_trade_api.kis_api import send_order, get_price_quote
from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_krx_topn, get_price_data_segments
from rolling_k_auto_trade_api.adjust_price_to_tick import adjust_price_to_tick

logger = logging.getLogger(__name__)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _get_price_context(stock_code: str, rebalance_date: datetime.date) -> Dict[str, float]:
    """KIS 시세를 우선 사용해 목표가 계산에 필요한 값들을 반환."""

    try:
        quote = get_price_quote(stock_code)
    except Exception as e:
        logger.warning(f"[QUOTE_FAIL] {stock_code} 시세 조회 실패: {e}")
        quote = {}

    context: Dict[str, float] = {
        "current_price": _safe_float(quote.get("stck_prpr"), 0.0),
        "today_open": _safe_float(quote.get("stck_oprc"), 0.0),
        "prev_high": _safe_float(quote.get("prdy_hgpr"), 0.0),
        "prev_low": _safe_float(quote.get("prdy_lwpr"), 0.0),
    }

    if context["prev_high"] and context["prev_low"] and context["today_open"]:
        return context

    # 보완: 최근 확정 일봉에서 전일 고가/저가를 채움
    segments = get_price_data_segments(stock_code, rebalance_date)
    month_records = sorted(segments.get("month", []), key=lambda x: x.get("date"))
    if month_records:
        last = month_records[-1]
        if not context["prev_high"]:
            context["prev_high"] = _safe_float(last.get("high"), 0.0)
        if not context["prev_low"]:
            context["prev_low"] = _safe_float(last.get("low"), 0.0)
        if not context["today_open"]:
            context["today_open"] = _safe_float(last.get("close"), 0.0)

    return context


def rolling_k_auto_trade_loop(
    rebalance_date: str,
    invest_amount: int = 10_000_000,
    order_test: bool = False,
    dryrun: bool = True
):
    """
    실시간 자동매매 시그널 loop (실전/모의)
    - 투자 대상 종목군/Best-K: get_best_k_for_krx_topn(rebalance_date) 결과 활용
    - 실시간 목표가 계산/매수/매도
    - dryrun=True: 주문 실행X 로그만
    - order_test=True: 강제 매수
    """
    logger.info(f"[실시간 AUTO LOOP] {rebalance_date} 시작 (test={order_test}, dryrun={dryrun})")
    target_stocks: List[Dict] = get_best_k_for_krx_topn(rebalance_date)
    logger.info(f"[AUTO] 투자 대상 = {len(target_stocks)}종목")

    for s in target_stocks:
        code, name, best_k = s["code"], s["name"], s["best_k"]
        weight = _safe_float(s.get("weight"), 0.0)
        logger.info(f"[LOOP] {name}({code}) - K={best_k}")
        # 실시간 가격 데이터/목표가 산출
        price_ctx = _get_price_context(code, datetime.strptime(rebalance_date, "%Y-%m-%d").date())
        if not price_ctx["today_open"] or not price_ctx["prev_high"] or not price_ctx["prev_low"]:
            logger.warning(f"[SKIP] {code} 목표가 계산에 필요한 시세 없음")
            continue

        target_price = adjust_price_to_tick(
            round(price_ctx["today_open"] + (price_ctx["prev_high"] - price_ctx["prev_low"]) * best_k, 2)
        )

        current_price = price_ctx["current_price"]
        logger.info(f"[RT] {code} 목표가={target_price}, 현재가={current_price}")
        # 매수 시그널
        if order_test or (current_price > 0 and current_price >= target_price):
            allocated = invest_amount * weight if weight > 0 else invest_amount / max(len(target_stocks), 1)
            qty = max(int(allocated // int(current_price)), 1)
            if dryrun:
                logger.info(f"[DRYRUN] {code} {name}: 매수신호 qty={qty} 목표가={target_price} 현재가={current_price}")
            else:
                try:
                    resp = send_order(code, qty=qty, price=target_price, side="buy")
                    logger.info(f"[ORDER] {code} {name}: {resp}")
                    time.sleep(3)
                except Exception as e:
                    logger.exception(f"[ORDER_FAIL] {code} 주문실패: {e}")
        else:
            logger.info(f"[WAIT] {code} 목표가 미충족 or 데이터 부족")
