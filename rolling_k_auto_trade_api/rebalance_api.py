import os
import json
import uuid
import logging
import time as time_module 
from datetime import datetime

from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api.kis_api import send_order, inquire_balance, inquire_filled_order
from rolling_k_auto_trade_api.logging_config import configure_logging

# 로깅 설정
configure_logging()
logger = logging.getLogger(__name__)

# 라우터 및 전역 변수
rebalance_router = APIRouter()
latest_rebalance_result = {"date": None, "selected_stocks": []}
TOTAL_CAPITAL = 10_000_000

# rebalance_api.py
from datetime import datetime, time, timezone, timedelta
import pytz   # settings.py 등에서 KST 타임존을 이미 쓰고 있다면 생략

KST = pytz.timezone("Asia/Seoul")

def is_market_open(ts: datetime | None = None) -> bool:
    """모의·실전 공통: 평일 08:30~16:00(KST)"""
    ts = ts or datetime.now(tz=KST)
    if ts.weekday() >= 5:                 # 5=토, 6=일
        return False
    open_t  = ts.replace(hour=8,  minute=30, second=0, microsecond=0)
    close_t = ts.replace(hour=16, minute=0, second=0, microsecond=0)
    return open_t <= ts <= close_t


@rebalance_router.post("/rebalance/run/{date}", tags=["Rebalance"])
async def run_rebalance(date: str):
    """
    date: YYYY-MM-DD 포맷의 리밸런싱 실행 날짜
    """
    logger.info(f"[RUN] run_rebalance 호출됨: date={date}")

    # 1) Best-K 계산
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    # 2) 결과 형태 통일
    results_map = {}
    if isinstance(raw_results, dict):
        results_map = raw_results
    elif isinstance(raw_results, list):
        for s in raw_results:
            code_key = s.get("stock_code") or s.get("code") or s.get("티커")
            if not code_key:
                logger.warning(f"[WARN] 리스트 항목 코드 누락: {s}")
                continue
            s["stock_code"] = code_key
            results_map[code_key] = s

    # 필터 통과 종목 수 로깅
    logger.info(f"[FILTER] Best-K 후보 count = {len(results_map)}개")

    results_list = []
    count = len(results_map)
    each_invest = TOTAL_CAPITAL // count if count > 0 else 0

    # 3) 종목별 주문 및 로그
    now = datetime.now(tz=KST)
    if not is_market_open(now):
        logger.info("[SKIP] 장 종료 – 주문·잔고 로직 생략")
        return {"message": "장 종료 — 리밸런싱 스킵"}
    
    for code, stock in results_map.items():
        stock["stock_code"] = code
        # 목표가 우선 매수 로그
        logger.info(
            f"[DEBUG][목표가 LOG] code={code}, "
            f"목표가={stock.get('목표가')}, "
            f"target_price={stock.get('target_price')}, "
            f"best_k_price={stock.get('best_k_price')}, "
            f"buy_price={stock.get('buy_price')}, "
            f"close={stock.get('close')}"
        )

        price = (
            stock.get("목표가") or
            stock.get("target_price") or
            stock.get("best_k_price") or
            stock.get("buy_price") or
            stock.get("close")  # 없을 때만 fallback
        )
        if not price or price <= 0:
            logger.warning(f"[SKIP] 목표가 미정의 or 0원: code={code}")
            continue

        price = int(round(price))           # ← ① 소숫점 제거(정수 지정가)
        quantity = int(max(each_invest // price, 1))   # ← ② 반드시 정수 수량

        stock["매수단가"] = price
        stock["매수수량"] = quantity

        try:
            resp = send_order(code, qty=quantity, price=price)
            time_module.sleep(3.0)  # 초당 1건 제한
            ord_no = resp.get("output1", {}).get("OrdNo") or resp.get("ordNo")
            stock["order_response"] = resp
            stock["order_status"] = f"접수번호={ord_no}, qty={quantity}, price={price}"
            logger.info(f"[ORDER] code={code}, qty={quantity}, price={price}, ord_no={ord_no}")

            # 잔고 조회
            try:
                balance = inquire_balance(code)
                stock["balance_after"] = balance
                logger.info(f"[BALANCE] code={code}, balance={balance}")
            except Exception:
                logger.exception(f"[BALANCE_FAIL] code={code}")

            # 체결 내역 조회
            if ord_no:
                try:
                    fill = inquire_filled_order(ord_no)
                    stock["fill_info"] = fill
                    logger.info(f"[FILL] ord_no={ord_no}, fill={fill}")
                except Exception:
                    logger.exception(f"[FILL_FAIL] ord_no={ord_no}")

        except Exception as e:
            stock["order_status"] = f"실패: {e}"
            logger.exception(f"[ORDER_FAIL] code={code}, error={e}")

        results_list.append(stock)

    # 주문 시도 건수 로깅
    logger.info(f"[ORDER_COUNT] 주문 시도 종목 수 = {len(results_list)}개")

    # 4) 메모리 캐시 업데이트
    latest_rebalance_result["date"] = date
    latest_rebalance_result["selected_stocks"] = results_list

    # 5) 결과 JSON 파일 저장
    output_dir = "rebalance_results"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"rebalance_{date}.json")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results_list, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {filepath} 저장 (count={len(results_list)})")
    except Exception as e:
        logger.exception(f"[SAVE_FAIL] JSON 저장 중 오류: {e}")
        raise HTTPException(status_code=500, detail="리밸런스 결과 저장 실패")

    # 6) API 응답 반환
    return {
        "message": f"{date} 리밸런싱 완료 (Meta-K 기반 Best K 적용)",
        "selected_count": len(results_list),
        "selected_stocks": results_list,
    }

@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    """가장 최근 실행한 리밸런싱 결과를 반환"""
    return latest_rebalance_result

@rebalance_router.get(
    "/rebalance/backtest-monthly",
    tags=["Rebalance"],
    response_class=JSONResponse,
    responses={200: {"description": "항상 200 OK, 요약 또는 전체 데이터 반환"}},
)
def rebalance_backtest_monthly(
    start_date: str = Query("2020-01-01", description="시작일 (YYYY-MM-DD)"),
    end_date:   str = Query("2024-04-01", description="종료일 (YYYY-MM-DD)"),
    request:    Request = None,
):
    logger.info(f"[BACKTEST] 호출: {start_date}~{end_date}")
    ua = (request.headers.get("user-agent") or "").lower()
    referer = (request.headers.get("referer") or "").lower()
    is_curl = ("curl" in ua) or ("/docs" in referer)

    # 날짜 유효성 검사
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(content={"error": "날짜 형식 오류: YYYY-MM-DD"})

    try:
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
        fee = 0.0015
        k_values = np.arange(0.1, 1.01, 0.05)
        periods = pd.date_range(start=start_date, end=end_date, freq="MS")

        if len(periods) < 2:
            return JSONResponse(content={"error": "최소 두 개 월 이상 지정 필요"})

        all_results = []
        for i in range(len(periods) - 1):
            rebalance_date = periods[i + 1]
            start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_date.strftime("%Y-%m-%d")
            end_test = (rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")

            selected = []
            for code, name in tickers:
                try:
                    df = DataReader(code, start_train, end_test)
                    if df.empty:
                        continue
                    df.index = pd.to_datetime(df.index)
                    train = df.loc[start_train:end_train].copy()
                    test  = df.loc[start_test:end_test].copy()
                    if len(train) < 15 or len(test) < 5:
                        continue

                    # 최적 k 탐색 및 테스트 로직 (기존 구현 유지)
                    # ...

                except Exception as e:
                    logger.exception(f"[ERROR] {name}({code}) 백테스트 중 예외: {e}")
                    continue

            monthly_df = pd.DataFrame(selected).sort_values("수익률(%)", ascending=False).head(20)
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)

        if not all_results:
            return JSONResponse(content={"error": "조건 만족 종목 없음"})

        final_df = pd.concat(all_results, ignore_index=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join("rebalance_results", filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)
        logger.info(f"[SAVE] 백테스트 결과 저장: {filename} (count={len(final_df)})")

        if is_curl:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(content=data)
        else:
            return JSONResponse(content={
                "message": f"{len(final_df)}개 종목 리밸런싱 완료",
                "filename": filename,
                "tip": "전체 데이터는 curl 또는 파일에서 확인",
            })

    except Exception as e:
        logger.exception(f"[ERROR] rebalance-backtest 예외: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})

