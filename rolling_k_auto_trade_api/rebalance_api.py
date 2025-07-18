import os
import json
import uuid
import logging
import time as time_module
from datetime import datetime, time as dtime, timedelta

import pytz
from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api.kis_api import send_order, inquire_balance, inquire_filled_order
from rolling_k_auto_trade_api.logging_config import configure_logging
from rolling_k_auto_trade_api.kis_api import inquire_cash_balance


# ──────────────────────────────────────────────────────────────
# 0. 로깅 설정 & 전역 상수
# ──────────────────────────────────────────────────────────────
configure_logging()
logger = logging.getLogger(__name__)

rebalance_router = APIRouter()
latest_rebalance_result: dict[str, any] = {"date": None, "selected_stocks": []}
TOTAL_CAPITAL = 10_000_000  # 투자금 고정 (1,000만원)

# ──────────────────────────────────────────────────────────────
# 1. KST 타임존 & 장 운영시간 헬퍼
# ──────────────────────────────────────────────────────────────
KST = pytz.timezone("Asia/Seoul")
MARKET_OPEN: dtime = dtime(8, 30)
MARKET_CLOSE: dtime = dtime(16, 0)

def is_market_open(ts: datetime | None = None) -> bool:
    """현재 시각(기본=now KST)이 장중(08:30~16:00)인지 여부"""
    ts = ts or datetime.now(tz=KST)
    if ts.weekday() >= 5:
        return False
    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE

# ──────────────────────────────────────────────────────────────
# 2. 리밸런싱 실행 엔드포인트
# ──────────────────────────────────────────────────────────────
@rebalance_router.post("/rebalance/run/{date}", tags=["Rebalance"])
async def run_rebalance(
    date: str,
    force_order: bool = Query(
        False,
        description="True 로 주면 장 종료 후에도 주문 로직 강제 실행 (테스트 용)",
    ),
):
    """KOSDAQ‑50 대상 Meta‑K 전략 리밸런싱 수행

    Args:
        date: YYYY‑MM‑DD 포맷 리밸런싱 기준일
        force_order: 장 종료 이후에도 주문 처리 실행 여부
    """
    logger.info(f"[RUN] run_rebalance 호출: date={date}, force_order={force_order}")

    # 1) Best-K 계산
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    # 2) 결과 통일 (code → info dict)
    results_map: dict[str, dict] = {}
    if isinstance(raw_results, dict):
        results_map = raw_results
    else:
        for s in raw_results:
            code_key = s.get("stock_code") or s.get("code") or s.get("티커")
            if not code_key:
                logger.warning(f"[WARN] 코드 누락: {s}")
                continue
            s["stock_code"] = code_key
            results_map[code_key] = s

    logger.info(f"[FILTER] 후보 종목 수 = {len(results_map)}개")

    results_list: list[dict] = []
    count = len(results_map)
    each_invest = TOTAL_CAPITAL // count if count > 0 else 0

    # 3) 장중 여부 및 force 플래그
    now = datetime.now(tz=KST)
    allow_after = force_order or os.getenv("ALLOW_AFTER_HOURS", "0") == "1"
    if not is_market_open(now) and not allow_after:
        logger.info("[SKIP] 장 종료 – 주문·잔고 로직 생략 (force_order=False)")
        return {
            "status": "skipped",
            "reason": "market_closed",
            "candidates": list(results_map.values()),
        }

    # 1) 예수금 확인
    # 변경 후
    cash = inquire_cash_balance()

    if cash <= 0:
        logger.error(f"[REBALANCE_ABORT] 예수금 부족: {cash:,}원")
        return {"error": "예수금이 0원입니다. 모의투자 계좌에 예수금을 충전하세요."}

    logger.info(f"[REBALANCE] 시작예수금: {cash:,}원")        

    # 4) 주문 실행
    for code, stock in results_map.items():
        stock["stock_code"] = code
        price = (
            stock.get("목표가") or
            stock.get("target_price") or
            stock.get("best_k_price") or
            stock.get("buy_price") or
            stock.get("close")
        )
        if not price or price <= 0:
            logger.warning(f"[SKIP] 목표가 미정의/0원: {code}")
            continue

        price = int(round(price))
        quantity = max(each_invest // price, 1)
        stock["매수단가"] = price
        stock["매수수량"] = quantity

        logger.info(f"[DEBUG] 주문예정 code={code}, price={price}, qty={quantity}")
        try:
            resp = send_order(code, qty=quantity, price=price, side="buy")

            time_module.sleep(3.0)
            ord_no = resp.get("output1", {}).get("OrdNo") or resp.get("ordNo")
            stock["order_response"] = resp
            stock["order_status"] = f"접수번호={ord_no}, qty={quantity}, price={price}"
            logger.info(f"[ORDER] code={code}, qty={quantity}, price={price}, ord_no={ord_no}")

            # 잔고 조회
            try:
                bal = inquire_balance(code)
                stock["balance_after"] = bal
                logger.info(f"[BALANCE] code={code}, balance={bal}")
            except Exception:
                logger.exception(f"[BALANCE_FAIL] code={code}")

            # 체결 조회
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

    logger.info(f"[ORDER_COUNT] 주문 시도 종목 수 = {len(results_list)}개")

    # 5) 캐시 업데이트 및 파일 저장
    latest_rebalance_result.update({"date": date, "selected_stocks": results_list})
    out_dir = "rebalance_results"
    os.makedirs(out_dir, exist_ok=True)
    fp = os.path.join(out_dir, f"rebalance_{date}.json")
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(results_list, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {fp} 저장 (count={len(results_list)})")
    except Exception as e:
        logger.exception(f"[SAVE_FAIL] JSON 저장 오류: {e}")
        raise HTTPException(status_code=500, detail="리밸런스 결과 저장 실패")

    return {
        "status": "orders_sent",
        "selected_count": len(results_list),
        "selected_stocks": results_list,
        "force_order": allow_after,
    }

# ──────────────────────────────────────────────────────────────
# 3. 최근 리밸런스 결과 조회
# ──────────────────────────────────────────────────────────────
@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    """최근 run_rebalance 결과 캐시 반환"""
    return latest_rebalance_result

# ──────────────────────────────────────────────────────────────
# 4. 월단위 백테스트 엔드포인트 (원본 로직 유지)
# ──────────────────────────────────────────────────────────────
@rebalance_router.get(
    "/rebalance/backtest-monthly",
    tags=["Rebalance"],
    response_class=JSONResponse,
    response_model=None,
    responses={200: {"description": "항상 200 OK, 요약 또는 전체 데이터 반환"}},
)
def rebalance_backtest_monthly(
    start_date: str = Query("2020-01-01", description="시작일 (YYYY-MM-DD)"),
    end_date:   str = Query("2024-04-01", description="종료일 (YYYY-MM-DD)"),
    request:    Request = None,
):
    """월별 리밸런스 백테스트
    반환은 요약(HTML/docs) 또는 전체 JSON(curl) 두 형태 지원"""
    logger.info(f"[BACKTEST] 호출: {start_date}~{end_date}")
    ua = (request.headers.get("user-agent") or "").lower() if request else ""
    referer = (request.headers.get("referer") or "").lower() if request else ""
    is_curl = ("curl" in ua) or ("/docs" in referer)

    # 날짜 형식 검사
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date,   "%Y-%m-%d")
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
            rebalance_dt = periods[i + 1]
            start_train = (rebalance_dt - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train   = (rebalance_dt - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test  = rebalance_dt.strftime("%Y-%m-%d")
            end_test    = (rebalance_dt + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")

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
