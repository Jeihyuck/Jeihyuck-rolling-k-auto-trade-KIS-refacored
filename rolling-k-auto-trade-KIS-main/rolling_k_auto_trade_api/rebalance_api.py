# File: rolling_k_auto_trade_api/rebalance_api.py

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse
from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api.kis_api import send_order
from FinanceDataReader import StockListing, DataReader
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
import uuid

print("rebalance_api.py 로딩됨")  # 이 메시지가 찍히면 수정된 파일이 로드된 것임

rebalance_router = APIRouter()
latest_rebalance_result = {"date": None, "selected_stocks": []}
TOTAL_CAPITAL = 10000000

@rebalance_router.get("/rebalance/run/{date}", tags=["Rebalance"])
def run_rebalance(date: str):
    results = get_best_k_for_kosdaq_50(date)
    results_list = []
    each_invest = TOTAL_CAPITAL // len(results) if results else 0

    for code, stock in results.items():
        stock["stock_code"] = code
        try:
            price = 10000  # placeholder
            quantity = max(each_invest // price, 1)
            send_order(code, qty=quantity, side="buy")
            stock["order_status"] = f"{quantity}주 주문 완료"
        except Exception as e:
            stock["order_status"] = f"실패: {str(e)}"
        results_list.append(stock)

    latest_rebalance_result["date"] = date
    latest_rebalance_result["selected_stocks"] = results_list

    os.makedirs("rebalance_results", exist_ok=True)
    with open(f"rebalance_results/rebalance_{date}.json", "w", encoding="utf-8") as f:
        json.dump(results_list, f, ensure_ascii=False, indent=2)

    return {
        "message": f"{date} 리밸런싱 완료 (Meta-K 기반 Best K 적용)",
        "selected_count": len(results_list),
        "selected_stocks": results_list
    }

@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    return latest_rebalance_result

@rebalance_router.get(
    "/rebalance/backtest-monthly",
    tags=["Rebalance"],
    response_class=JSONResponse,
    responses={200: {"description": "항상 200 OK, 요약 또는 전체 데이터 반환"}}
)
def rebalance_backtest_monthly(
    start_date: str = Query("2020-01-01", description="시작일 (YYYY-MM-DD)"),
    end_date: str = Query("2024-04-01", description="종료일 (YYYY-MM-DD)"),
    request: Request = None
):
    # 1) 호출 로그
    print(f"[✔ API 실행] rebalance_backtest_monthly 호출됨: {start_date} ~ {end_date}")

    # 2) User-Agent, Accept, Referer 헤더 검사
    ua = request.headers.get("user-agent", "").lower()
    accept = request.headers.get("accept", "").lower()
    referer = request.headers.get("referer", "").lower()
    # “curl” 또는 Swagger UI에서 오는 요청(Referer에 '/docs' 포함)을 True로 간주
    is_curl = ("curl" in ua) or ("/docs" in referer)
    print(f"[DEBUG] User-Agent='{ua}', Accept='{accept}', Referer='{referer}', is_curl={is_curl}")

    # 3) 날짜 형식 검증 (오류 발생 시에도 HTTP 200으로 error 반환)
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        print("[❌ 날짜 형식 오류 - 200 OK로 error 반환]")
        return JSONResponse(content={
            "error": "날짜 형식 오류: YYYY-MM-DD 형식을 사용하세요."
        })

    try:
        # 4) KOSDAQ Top50 조회
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
        print(f"[DEBUG] Top50 count: {len(tickers)}")

        fee = 0.0015
        k_values = np.arange(0.1, 1.01, 0.05)
        periods = pd.date_range(start=start_date, end=end_date, freq="MS")
        print(f"[DEBUG] Periods: {list(periods)}")

        # 5) 기간 부족 검사 (오류 발생 시에도 HTTP 200으로 error 반환)
        if len(periods) < 2:
            print("[❌ 기간 부족 - 200 OK로 error 반환]")
            return JSONResponse(content={
                "error": "리밸런싱을 수행하려면 최소 두 개 월 이상을 지정해야 합니다."
            })

        all_results = []
        print(f"[DEBUG] 시작 all_results 길이: {len(all_results)}")

        # 6) 월별 백테스트 수행
        for i in range(len(periods) - 1):
            rebalance_date = periods[i + 1]
            start_train = (rebalance_date - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train = (rebalance_date - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_date.strftime("%Y-%m-%d")
            end_test = (rebalance_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")

            print(f"[DEBUG] {i+1}번째 월 리밸런싱: train {start_train}~{end_train}, test {start_test}~{end_test}")
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
                            0
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
                        0
                    )
                    test["cumulative"] = (1 + test["strategy_return"].fillna(0)).cumprod()
                    if len(test["cumulative"]) == 0:
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
                            "승률(%)": round(win_rate * 100, 2)
                        })  
                except Exception as e:
                    print(f"[예외] {name} ({code}) → {str(e)}")
                    continue

            monthly_df = pd.DataFrame(selected).sort_values("수익률(%)", ascending=False).head(20)
            print(f"[DEBUG] {start_test} 월 선택된 종목 수: {len(monthly_df)}")
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)
                print(f"[DEBUG] all_results에 추가, 현재 크기: {len(all_results)}")

        # 7) 종목 미선정 시
        if not all_results:
            print("[❗] all_results 비어 있음 - 200 OK로 error 반환]")
            return JSONResponse(content={
                "error": "조건을 만족하는 종목이 없습니다."
            })

        final_df = pd.concat(all_results, ignore_index=True)
        print(f"[DEBUG] 최종 final_df 크기: {final_df.shape}")

        os.makedirs("rebalance_results", exist_ok=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join("rebalance_results", filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)

        print(f"[SUCCESS] final_df 생성 완료, 총 {len(final_df)}개 종목 → 파일: {filename}")

        # 8) curl 또는 Swagger 요청 분기
        if is_curl:
            print("[DEBUG] is_curl=True → 전체 JSON 반환]")
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            return JSONResponse(content=data)

        # Swagger/브라우저 요청이라면 요약만 반환
        print("[DEBUG] is_curl=False → 요약만 반환]")
        return JSONResponse(content={
            "message": f"{len(final_df)}개 종목 리밸런싱 완료",
            "filename": filename,
            "tip": "전체 데이터는 curl 또는 파일에서 확인하세요"
        })

    except Exception as e:
        print(f"[❌ ERROR] 백테스트 처리 중 예외 발생: {e}")
        return JSONResponse(content={
            "error": f"백테스트 예외 발생: {str(e)}"
        })
