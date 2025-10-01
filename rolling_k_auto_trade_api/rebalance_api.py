import os
import json
import uuid
import logging
from datetime import datetime, time as dtime, timedelta
from typing import Any

import pytz
from fastapi import APIRouter, Query, HTTPException, Request
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_50
from rolling_k_auto_trade_api.logging_config import configure_logging

# ──────────────────────────────────────────────────────────────
# 로깅 및 전역
# ──────────────────────────────────────────────────────────────
configure_logging()
logger = logging.getLogger(__name__)

rebalance_router = APIRouter()
# 호환성을 위해 selected_stocks와 signals 둘 다 저장
latest_rebalance_result: dict[str, Any] = {"date": None, "selected_stocks": [], "signals": []}
REBALANCE_OUT_DIR = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
os.makedirs(REBALANCE_OUT_DIR, exist_ok=True)

KST = pytz.timezone("Asia/Seoul")
MARKET_OPEN: dtime = dtime(8, 30)
MARKET_CLOSE: dtime = dtime(16, 0)


def is_market_open(ts: datetime | None = None) -> bool:
    ts = ts or datetime.now(tz=KST)
    if ts.weekday() >= 5:
        return False
    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE


# -----------------------------------------------------------------
# Signals-only 리밸런서
# - 주문/잔고 로직 제거
# - trader가 매일 자체적으로 목표가(전일종가 + K*(high-low)) 계산하도록 OHLC을 포함한 시그널 제공
# - backward compatibility: 기존 소비자가 "selected_stocks" 키를 요청할 수 있으므로 둘 다 채움
# -----------------------------------------------------------------
@rebalance_router.post("/rebalance/run/{date}", tags=["Rebalance"])
async def run_rebalance(date: str, force_generate: bool = Query(False, description="강제 생성 (장중/장외 상관없이)")):
    """Signals-only 리밸런서

    반환: status, selected_count, signals (list)
    각 signal은 최소한 다음 필드를 포함합니다:
      - stock_code, name, best_k, base_close_date, base_close, base_high, base_low, meta

    파일 저장: rebalance_results/rebalance_signals_{date}.json
    """
    logger.info(f"[RUN] run_rebalance 호출: date={date}, force_generate={force_generate}")

    # 1) Best-K 계산
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    # 2) 결과 표준화
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

    # 후보 -> signals 생성 (필터 적용 가능)
    signals: list[dict] = []

    def _get_base_ohlc(code: str, ref_date: str) -> dict:
        """리밸런싱 기준일(ref_date) 이전(또는 동일)의 마지막 영업일 OHLC를 반환
        반환값: base_close_date(YYYY-MM-DD), base_close, base_high, base_low
        """
        try:
            ref_dt = datetime.strptime(ref_date, "%Y-%m-%d")
            # 안전 범위: 최근 10거래일 이내
            start = (ref_dt - timedelta(days=14)).strftime("%Y-%m-%d")
            end = (ref_dt).strftime("%Y-%m-%d")
            df = DataReader(code, start, end)
            if df is None or df.empty:
                return {"base_close_date": None, "base_close": None, "base_high": None, "base_low": None}
            df.index = pd.to_datetime(df.index)
            df_filtered = df[df.index <= pd.to_datetime(ref_date)]
            if df_filtered.empty:
                return {"base_close_date": None, "base_close": None, "base_high": None, "base_low": None}
            row = df_filtered.iloc[-1]
            # DataReader 컬럼 표준화: Open/High/Low/Close 또는 영문/한글
            close = None
            if "Close" in row.index:
                close = row.get("Close")
            elif "Adj Close" in row.index:
                close = row.get("Adj Close")
            elif "종가" in row.index:
                close = row.get("종가")
            high = row.get("High") if "High" in row.index else row.get("고가")
            low = row.get("Low") if "Low" in row.index else row.get("저가")
            return {
                "base_close_date": str(row.name.date()),
                "base_close": float(close) if close is not None and not pd.isna(close) else None,
                "base_high": float(high) if high is not None and not pd.isna(high) else None,
                "base_low": float(low) if low is not None and not pd.isna(low) else None,
            }
        except Exception as e:
            logger.warning(f"[WARN] OHLC 조회 실패: {code} {e}")
            return {"base_close_date": None, "base_close": None, "base_high": None, "base_low": None}

    for code, raw in results_map.items():
        info = dict(raw)
        name = info.get("name") or info.get("종목명") or ""
        best_k = info.get("best_k") or info.get("K") or info.get("k")
        try:
            best_k = float(best_k) if best_k is not None else None
        except Exception:
            best_k = None

        # 우선 raw에 이미 base OHLC가 있으면 사용, 없으면 DataReader로 조회
        base_close = info.get("base_close") or info.get("종가")
        base_high = info.get("base_high") or info.get("high") or info.get("고가")
        base_low = info.get("base_low") or info.get("low") or info.get("저가")
        base_close_date = info.get("base_close_date") or info.get("date")

        if not (base_close and base_high and base_low and base_close_date):
            ohlc = _get_base_ohlc(code, date)
            base_close_date = base_close_date or ohlc.get("base_close_date")
            base_close = base_close or ohlc.get("base_close")
            base_high = base_high or ohlc.get("base_high")
            base_low = base_low or ohlc.get("base_low")

        meta = info.get("meta") or {}
        # numeric meta 정리
        for k in ["cumulative_return_pct", "win_rate_pct", "mdd_pct"]:
            if k in meta:
                try:
                    meta[k] = float(meta[k])
                except Exception:
                    pass

        signal = {
            "stock_code": code,
            "name": name,
            "best_k": best_k,
            "base_close_date": base_close_date,
            "base_close": base_close,
            "base_high": base_high,
            "base_low": base_low,
            "meta": meta,
        }

        signals.append(signal)

    # 캐시(호환성: selected_stocks) 및 파일 저장
    latest_rebalance_result.update({"date": date, "signals": signals, "selected_stocks": signals})
    fp = os.path.join(REBALANCE_OUT_DIR, f"rebalance_signals_{date}.json")
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(signals, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {fp} 저장 (count={len(signals)})")
    except Exception as e:
        logger.exception(f"[SAVE_FAIL] JSON 저장 오류: {e}")

    return {"status": "signals_generated", "selected_count": len(signals), "signals": signals, "selected_stocks": signals}


# ──────────────────────────────────────────────────────────────
# 조회 엔드포인트
# ──────────────────────────────────────────────────────────────
@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    """최근 run_rebalance(시그널) 결과 캐시 반환"""
    return latest_rebalance_result


@rebalance_router.get("/rebalance/selected/{date}", tags=["Rebalance"], response_class=JSONResponse)
def get_selected_stocks(date: str):
    """
    리밸런싱 실행 후 selected_stocks(시그널 리스트)만 반환하는 API
    """
    if latest_rebalance_result.get("date") == date and latest_rebalance_result.get("selected_stocks"):
        selected = latest_rebalance_result.get("selected_stocks", [])
        return {"status": "ready", "rebalance_date": date, "selected": selected}

    # 파일 폴백: rebalance_signals_{date}.json 또는 rebalance_{date}.json
    candidates = [
        os.path.join(REBALANCE_OUT_DIR, f"rebalance_signals_{date}.json"),
        os.path.join(REBALANCE_OUT_DIR, f"rebalance_{date}.json"),
    ]
    for fp in candidates:
        if os.path.exists(fp):
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    data = json.load(f)
                return {"status": "ready", "rebalance_date": date, "selected": data}
            except Exception as e:
                logger.exception(f"[LOAD_FAIL] {fp} 불러오기 실패: {e}")

    return {"status": "not_ready", "rebalance_date": date, "selected": [], "message": "먼저 /rebalance/run/{date} 실행 또는 파일 확인"}


# 기존 월간 백테스트 엔드포인트(원본 로직 유지)
@rebalance_router.get("/rebalance/backtest-monthly", tags=["Rebalance"], response_class=JSONResponse)
def rebalance_backtest_monthly(start_date: str = Query("2020-01-01"), end_date: str = Query("2024-04-01")):
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
            rebalance_dt = periods[i + 1]
            start_train = (rebalance_dt - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train = (rebalance_dt - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_dt.strftime("%Y-%m-%d")
            end_test = (rebalance_dt + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime("%Y-%m-%d")

            selected = []
            for code, name in tickers:
                try:
                    df = DataReader(code, start_train, end_test)
                    if df.empty:
                        continue
                    df.index = pd.to_datetime(df.index)
                    train = df.loc[start_train:end_train].copy()
                    test = df.loc[start_test:end_test].copy()
                    if len(train) < 15 or len(test) < 5:
                        continue

                    try:
                        start_price = test["Open"].iloc[0]
                        end_price = test["Close"].iloc[-1]
                        rtn_pct = (end_price / start_price - 1) * 100
                    except Exception as e:
                        logger.warning(f"[WARN] {name}({code}) 수익률 계산 실패: {e}")
                        continue

                    selected.append({
                        "code": code,
                        "name": name,
                        "수익률(%)": round(rtn_pct, 2),
                        "시작일": start_test,
                        "종료일": end_test,
                        "종가": int(end_price),
                    })
                except Exception as e:
                    logger.exception(f"[ERROR] {name}({code}) 백테스트 중 예외: {e}")
                    continue

            if not selected:
                logger.warning(f"[SKIP] {rebalance_dt.strftime('%Y-%m')} : 조건 만족 종목 없음")
                continue

            df = pd.DataFrame(selected)
            monthly_df = df.sort_values("수익률(%)", ascending=False).head(20)
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)

        if not all_results:
            return JSONResponse(content={"error": "조건 만족 종목 없음"})

        final_df = pd.concat(all_results, ignore_index=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join(REBALANCE_OUT_DIR, filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)
        logger.info(f"[SAVE] 백테스트 결과 저장: {filename} (count={len(final_df)})")
        return JSONResponse(content={"message": f"{len(final_df)}개 종목 리밸런싱 완료", "filename": filename})

    except Exception as e:
        logger.exception(f"[ERROR] rebalance-backtest 예외: {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})
