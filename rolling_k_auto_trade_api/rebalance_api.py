# File: rolling_k_auto_trade_api/rebalance_api.py

import os
import json
import uuid
import logging
from datetime import datetime

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api.kis_api import send_order

# 로거 세팅
logger = logging.getLogger(__name__)

# 라우터 및 전역 변수
rebalance_router = APIRouter()
latest_rebalance_result = {"date": None, "selected_stocks": []}
TOTAL_CAPITAL = 10_000_000


@rebalance_router.get("/rebalance/run/{date}", tags=["Rebalance"])
def run_rebalance(date: str):
    """
    기준일(date)에 따라 Best-K 전략으로 종목을 선별하고
    자동으로 매수 주문을 보냅니다.
    """
    logger.info(f"[RUN] run_rebalance 호출됨: date={date}")
    try:
        raw_results = get_best_k_for_kosdaq_50(date)

        # 결과가 리스트로 올 수도 있으므로 dict 형태로 통일
        if isinstance(raw_results, list):
            results_map = {s.get("stock_code"): s for s in raw_results}
        else:
            results_map = raw_results or {}

        results_list = []
        each_invest = TOTAL_CAPITAL // len(results_map) if results_map else 0

        for code, stock in results_map.items():
            stock["stock_code"] = code
            try:
                # 실제 실전연동 시에는 실제 가격 조회 로직으로 대체하세요
                price = stock.get("close", 10000)
                quantity = max(each_invest // price, 1)
                send_order(code, qty=quantity, side="buy")
                stock["order_status"] = f"{quantity}주 주문 완료"
                logger.info(f"Order success: code={code}, qty={quantity}")
            except Exception as e:
                stock["order_status"] = f"실패: {e}"
                logger.exception(f"Order failed: code={code}")
            results_list.append(stock)

        # 메모리 캐시 업데이트
        latest_rebalance_result["date"] = date
        latest_rebalance_result["selected_stocks"] = results_list

        # 파일 저장
        os.makedirs("rebalance_results", exist_ok=True)
        filepath = f"rebalance_results/rebalance_{date}.json"
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results_list, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {filepath} 저장, count={len(results_list)}")

        return {
            "message": f"{date} 리밸런싱 완료 (Meta-K 기반 Best K 적용)",
            "selected_count": len(results_list),
            "selected_stocks": results_list,
        }

    except Exception as e:
        logger.exception(f"[ERROR] run_rebalance 실패: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    """
    가장 최근 실행한 리밸런싱 결과를 반환합니다.
    """
    return latest_rebalance_result


@rebalance_router.get(
    "/rebalance/backtest-monthly",
    tags=["Rebalance"],
    response_class=JSONResponse,
    responses={200: {"description": "항상 200 OK, 요약 또는 전체 데이터 반환"}},
)
def rebalance_backtest_monthly(
    start_date: str = Query("2020-01-01", description="시작일 (YYYY-MM-DD)"),
    end_date: str = Query("2024-04-01", description="종료일 (YYYY-MM-DD)"),
    request: Request = None,
):
    """
    월별 백테스트를 수행하고,
    curl 요청엔 전체 데이터를, 브라우저 요청엔 요약만 반환합니다.
    """
    logger.info(f"[BACKTEST] rebalance_backtest_monthly 호출됨: {start_date} ~ {end_date}")

    # 1) 요청 출처 검사
    ua = (request.headers.get("user-agent") or "").lower()
    referer = (request.headers.get("referer") or "").lower()
    is_curl = ("curl" in ua) or ("/docs" in referer)
    logger.debug(f"User-Agent='{ua}', Referer='{referer}', is_curl={is_curl}")

    # 2) 날짜 유효성 검사
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        logger.warning(f"[WARN] 잘못된 날짜 형식: {start_date}, {end_date}")
        return JSONResponse(content={"error": "날짜 형식 오류: YYYY-MM-DD 형식을 사용하세요."})

    try:
        # 3) KOSDAQ 시총 Top50
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
        logger.debug(f"Top50 count: {len(tickers)}")

        fee = 0.0015
        k_values = np.arange(0.1, 1.01, 0.05)
        periods = pd.date_range(start=start_date, end=end_date, freq="MS")
        logger.debug(f"Periods: {list(periods)}")

        if len(periods) < 2:
            logger.warning("기간 부족: 최소 두 개 월 이상 필요")
            return JSONResponse(content={"error": "리밸런싱을 수행하려면 최소 두 개 월 이상을 지정해야 합니다."})

        all_results = []
        for i in range(len(periods) - 1):
            rebalance_date = periods[i + 1]
            start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train   = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test  = rebalance_date.strftime("%Y-%m-%d")
            end_test    = (rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            logger.debug(f"{i+1}th rebalance: train {start_train}~{end_train}, test {start_test}~{end_test}")

            selected = []
            for code, name in tickers:
                try:
                    df = DataReader(code, start_train, end_test)
                    if df.empty:
                        continue
                    df.index = pd.to_datetime(df.index)

                    train = df[(df.index >= start_train) & (df.index <= end_train)].copy()
                    test  = df[(df.index >= start_test)  & (df.index <= end_test)].copy()
                    if len(train) < 15 or len(test) < 5:
                        continue

                    # 최적 k 서치
                    best_k, best_ret = 0, -np.inf
                    for k in k_values:
                        temp = train.copy()
                        temp["range"] = temp["High"].shift(1) - temp["Low"].shift(1)
                        temp["target"] = temp["Open"] + temp["range"] * k
                        temp["buy_signal"] = temp["High"] > temp["target"]
                        temp["buy_price"] = np.where(temp["buy_signal"], temp["target"], np.nan)
                        temp["sell_price"] = temp["Close"]
                        temp["strategy_return"] = np.where(
                            temp["buy_signal"],
                            (temp["sell_price"] - temp["buy_price"]) / temp["buy_price"] - fee,
                            0,
                        )
                        temp["cumulative_return"] = (1 + temp["strategy_return"].fillna(0)).cumprod() - 1
                        final_ret = temp["cumulative_return"].iloc[-1]
                        if final_ret > best_ret:
                            best_ret = final_ret
                            best_k = k

                    # 테스트 기간 성능
                    test["range"] = test["High"].shift(1) - test["Low"].shift(1)
                    test["target"] = test["Open"] + test["range"] * best_k
                    test["buy_signal"] = test["High"] > test["target"]
                    test["buy_price"] = np.where(test["buy_signal"], test["target"], np.nan)
                    test["sell_price"] = test["Close"]
                    test["strategy_return"] = np.where(
                        test["buy_signal"],
                        (test["sell_price"] - test["buy_price"]) / test["buy_price"] - fee,
                        0,
                    )
                    test["cumulative"] = (1 + test["strategy_return"].fillna(0)).cumprod()
                    if test["cumulative"].empty:
                        continue

                    mdd = ((test["cumulative"] - test["cumulative"].cummax()) / test["cumulative"].cummax()).min() * 100
                    wins = (test["strategy_return"] > 0).sum()
                    total = test["strategy_return"].notnull().sum()
                    win_rate = wins / total if total > 0 else 0
                    final_ret = (test["cumulative"].iloc[-1] - 1) * 100

                    if final_ret > 0 and win_rate > 0.2 and mdd < 30:
                        selected.append({
                            "리밸런싱시점": start_test,
                            "티커": code,
                            "종목명": name,
                            "최적k": best_k,
                            "수익률(%)": round(final_ret, 2),
                            "MDD(%)": round(mdd, 2),
                            "승률(%)": round(win_rate * 100, 2),
                        })
                except Exception as e:
                    logger.exception(f"[ERROR] {name}({code}) 백테스트 중 예외: {e}")
                    continue

            monthly_df = pd.DataFrame(selected).sort_values("수익률(%)", ascending=False).head(20)
            logger.debug(f"{start_test} 월 선택된 종목 수: {len(monthly_df)}")
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)
                logger.debug(f"all_results에 추가, 현재 크기: {len(all_results)}")

        if not all_results:
            logger.warning("all_results 비어 있음 - 조건 만족 종목 없음")
            return JSONResponse(content={"error": "조건을 만족하는 종목이 없습니다."})

        final_df = pd.concat(all_results, ignore_index=True)
        logger.debug(f"최종 final_df 크기: {final_df.shape}")

        os.makedirs("rebalance_results", exist_ok=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join("rebalance_results", filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)
        logger.info(f"final_df 생성 완료: {len(final_df)}개 종목 → 파일: {filename}")

        if is_curl:
            logger.debug("is_curl=True → 전체 JSON 반환")
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(content=data)

        logger.debug("is_curl=False → 요약만 반환")
        return JSONResponse(content={
            "message": f"{len(final_df)}개 종목 리밸런싱 완료",
            "filename": filename,
            "tip": "전체 데이터는 curl 또는 파일에서 확인하세요",
        })

    except Exception as e:
        logger.exception(f"[ERROR] backtest-monthly 처리 중 예외 발생: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
