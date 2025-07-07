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

    # 1) Best-K 계산
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 산출 중 예외: {e}")
        return JSONResponse(status_code=500, content={"error": "Best K 계산 실패"})

    # 2) 결과 형태 통일: dict 또는 list 처리
    if isinstance(raw_results, dict):
        results_map = raw_results
    elif isinstance(raw_results, list):
        results_map = {}
        for s in raw_results:
            # 리스트 항목에 'stock_code', '티커', 또는 'code' 키 중 하나가 있어야 합니다.
            code_key = s.get("stock_code") or s.get("티커") or s.get("code")
            if not code_key:
                logger.warning(f"[WARN] 리스트 항목에 종목 코드 누락: {s}")
                continue
            s['stock_code'] = code_key
            results_map[code_key] = s
    else:
        results_map = {}

    results_list = []
    count = len(results_map)
    each_invest = TOTAL_CAPITAL // count if count > 0 else 0

    # 3) 종목별 주문 실행
    for code, stock in results_map.items():
        stock["stock_code"] = code
        try:
            price = stock.get("close", 10000)
            quantity = max(each_invest // price, 1)
            send_order(code, qty=quantity, side="buy")
            stock["order_status"] = f"{quantity}주 주문 완료"
            logger.info(f"[ORDER] code={code}, qty={quantity}")
        except Exception as e:
            stock["order_status"] = f"실패: {e}"
            logger.exception(f"[ORDER_FAIL] code={code}, error={e}")
        results_list.append(stock)

    # 4) 메모리 캐시 업데이트
    latest_rebalance_result["date"] = date
    latest_rebalance_result["selected_stocks"] = results_list

    # 5) 결과 JSON 파일 저장 (루프 외부에서 한 번만)
    output_dir = "rebalance_results"
    os.makedirs(output_dir, exist_ok=True)
    filepath = os.path.join(output_dir, f"rebalance_{date}.json")
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(results_list, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {filepath} 저장, count={len(results_list)}")
    except Exception as e:
        logger.exception(f"[SAVE_FAIL] JSON 저장 중 예외: {e}")
        return JSONResponse(status_code=500, content={"error": "리밸런스 결과 저장 실패"})

    # 6) 응답 반환
    return {
        "message": f"{date} 리밸런싱 완료 (Meta-K 기반 Best K 적용)",
        "selected_count": len(results_list),
        "selected_stocks": results_list,
    }

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
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
        fee = 0.0015
        k_values = np.arange(0.1, 1.01, 0.05)
        periods = pd.date_range(start=start_date, end=end_date, freq="MS")
        if len(periods) < 2:
            return JSONResponse(content={"error": "리밸런싱을 수행하려면 최소 두 개 월 이상을 지정해야 합니다."})

        all_results = []
        for i in range(len(periods) - 1):
            rebalance_date = periods[i + 1]
            start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_date.strftime("%Y-%m-%d")
            end_test = (rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            logger.debug(f"{i+1}th rebalance: train {start_train}~{end_train}, test {start_test}~{end_test}")

            selected = []
            for code, name in tickers:
                try:
                    df = DataReader(code, start_train, end_test)
                    if df.empty:
                        continue
                    df.index = pd.to_datetime(df.index)

                    train = df[(df.index >= start_train) & (df.index <= end_train)].copy()
                    test = df[(df.index >= start_test) & (df.index <= end_test)].copy()
                    if len(train) < 15 or len(test) < 5:
                        continue

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
                    total_trades = test["strategy_return"].notnull().sum()
                    win_rate = wins / total_trades if total_trades > 0 else 0
                    final_ret_pct = (test["cumulative"].iloc[-1] - 1) * 100

                    if final_ret_pct > 0 and win_rate > 0.2 and mdd < 30:
                        selected.append({
                            "리밸런싱시점": start_test,
                            "티커": code,
                            "종목명": name,
                            "최적k": best_k,
                            "수익률(%)": round(final_ret_pct, 2),
                            "MDD(%)": round(mdd, 2),
                            "승률(%)": round(win_rate * 100, 2),
                        })
                except Exception as e:
                    logger.exception(f"[ERROR] {name}({code}) 백테스트 중 예외: {e}")
                    continue

            monthly_df = pd.DataFrame(selected).sort_values("수익률(%)", ascending=False).head(20)
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)

        if not all_results:
            return JSONResponse(content={"error": "조건을 만족하는 종목이 없습니다."})

        final_df = pd.concat(all_results, ignore_index=True)

        os.makedirs("rebalance_results", exist_ok=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join("rebalance_results", filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)
        logger.info(f"final_df 생성 완료: {len(final_df)}개 종목 → 파일: {filename}")

        if is_curl:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(content=data)

        return JSONResponse(content={
            "message": f"{len(final_df)}개 종목 리밸런싱 완료",
            "filename": filename,
            "tip": "전체 데이터는 curl 또는 파일에서 확인하세요",
        })

    except Exception as e:
        logger.exception(f"[ERROR] backtest-monthly 처리 중 예외 발생: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
