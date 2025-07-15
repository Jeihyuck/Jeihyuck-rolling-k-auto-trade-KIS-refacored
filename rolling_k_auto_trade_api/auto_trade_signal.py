# auto_trade_signal.py
import os
import time
import logging
import requests
import pandas as pd
from datetime import datetime, timedelta
from typing import List, Dict

from rolling_k_auto_trade_api.kis_api import send_order
from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50, get_price_data_segments

logger = logging.getLogger(__name__)


def get_latest_price(stock_code: str) -> float:
    """실시간 가격 데이터: 한국투자 OpenAPI가 없으면 FDR로 임시 구현"""
    # TODO: 실전에서는 websocket/REST 실시간 연동 필요
    import FinanceDataReader as fdr
    try:
        df = fdr.DataReader(stock_code, datetime.now()-timedelta(days=5), datetime.now())
        if not df.empty:
            price = float(df.iloc[-1]["Close"])
            logger.debug(f"[RT] {stock_code} FDR 최신가: {price}")
            return price
    except Exception as e:
        logger.warning(f"[RT] FDR 가격조회 실패: {e}")
    return 0.0


def rolling_k_auto_trade_loop(
    rebalance_date: str,
    invest_amount: int = 10_000_000,
    order_test: bool = False,
    dryrun: bool = True
):
    """
    실시간 자동매매 시그널 loop (실전/모의)
    - 투자 대상 종목군/Best-K: get_best_k_for_kosdaq_50(rebalance_date) 결과 활용
    - 실시간 목표가 계산/매수/매도
    - dryrun=True: 주문 실행X 로그만
    - order_test=True: 강제 매수
    """
    logger.info(f"[실시간 AUTO LOOP] {rebalance_date} 시작 (test={order_test}, dryrun={dryrun})")
    target_stocks: List[Dict] = get_best_k_for_kosdaq_50(rebalance_date)
    logger.info(f"[AUTO] 투자 대상 = {len(target_stocks)}종목")
    each_invest = invest_amount // max(len(target_stocks), 1)

    for s in target_stocks:
        code, name, best_k = s["code"], s["name"], s["best_k"]
        logger.info(f"[LOOP] {name}({code}) - K={best_k}")
        # 실시간 가격 데이터/목표가 산출
        price_segments = get_price_data_segments(code, datetime.strptime(rebalance_date, "%Y-%m-%d").date())
        # 돌파목표가: 오늘 시가 + (전일고가-전일저가)*best_k
        today = datetime.today().date()
        today_prices = [p for p in price_segments["month"] if p["date"] == today]
        if not today_prices:
            logger.warning(f"[SKIP] {code} 오늘 시가 없음")
            continue
        today_open = today_prices[0]["open"]
        yesterday_prices = [p for p in price_segments["month"] if p["date"] == today - timedelta(days=1)]
        if not yesterday_prices:
            logger.warning(f"[SKIP] {code} 전일 데이터 없음")
            continue
        prev_high = yesterday_prices[0]["high"]
        prev_low  = yesterday_prices[0]["low"]
        target_price = round(today_open + (prev_high - prev_low) * best_k, 2)

        current_price = get_latest_price(code)
        logger.info(f"[RT] {code} 목표가={target_price}, 현재가={current_price}")
        # 매수 시그널
        if order_test or (current_price > 0 and current_price >= target_price):
            qty = max(each_invest // int(current_price), 1)
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
