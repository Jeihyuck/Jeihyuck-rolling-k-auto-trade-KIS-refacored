# rolling_k_auto_trade_api/rebalance_api.py
# Signals-only Rebalance API (KOSDAQ Top 50, RK-Max)
# - 주문/체결/잔고 로직 제거
# - 시그널(목표가 계산 가능한 OHLC+best_k)만 산출 및 저장
# - 기존 워크플로/스크립트 호환을 위해 selected_stocks 키와
#   /rebalance/generate?date=... 엔드포인트 제공

from __future__ import annotations

import os
import json
import uuid
import logging
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List

import pytz
from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import (
    get_best_k_for_kosdaq_50,
)
from rolling_k_auto_trade_api.logging_config import configure_logging

# ──────────────────────────────────────────────────────────────
# 로깅 및 전역
# ──────────────────────────────────────────────────────────────
configure_logging()
logger = logging.getLogger(__name__)

rebalance_router = APIRouter()

# 호환성을 위해 selected_stocks와 signals 둘 다 유지
latest_rebalance_result: Dict[str, Any] = {
    "date": None,
    "selected_stocks": [],
    "signals": [],
}

REBALANCE_OUT_DIR = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
os.makedirs(REBALANCE_OUT_DIR, exist_ok=True)

KST = pytz.timezone("Asia/Seoul")
MARKET_OPEN: dtime = dtime(8, 30)
MARKET_CLOSE: dtime = dtime(16, 0)


def is_market_open(ts: datetime | None = None) -> bool:
    """한국장(기본 08:30~16:00) 개장 여부 간단 판정."""
    _ts = ts or datetime.now(tz=KST)
    if _ts.weekday() >= 5:  # 5=Sat, 6=Sun
        return False
    return (MARKET_OPEN <= _ts.time() <= MARKET_CLOSE)


# -----------------------------------------------------------------
# 내부 유틸
# -----------------------------------------------------------------

def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        if isinstance(x, (int, float)) and not pd.isna(x):
            return float(x)
        # 문자열 등
        v = float(str(x))
        if pd.isna(v):
            return None
        return v
    except Exception:
        return None


def _get_base_ohlc(code: str, ref_date: str) -> Dict[str, Any]:
    """리밸런싱 기준일(ref_date) 이전(또는 동일)의 마지막 영업일 OHLC를 반환
    반환:
      - base_close_date(YYYY-MM-DD), base_close, base_high, base_low
    """
    try:
        ref_dt = datetime.strptime(ref_date, "%Y-%m-%d")
        # 안전 범위: 최근 14일 조회
        start = (ref_dt - timedelta(days=14)).strftime("%Y-%m-%d")
        end = ref_dt.strftime("%Y-%m-%d")
        df = DataReader(code, start, end)
        if df is None or df.empty:
            return {
                "base_close_date": None,
                "base_close": None,
                "base_high": None,
                "base_low": None,
            }
        df.index = pd.to_datetime(df.index)
        df_filtered = df[df.index <= pd.to_datetime(ref_date)]
        if df_filtered.empty:
            return {
                "base_close_date": None,
                "base_close": None,
                "base_high": None,
                "base_low": None,
            }
        row = df_filtered.iloc[-1]

        # 열 이름 표준화 대응
        close = (
            row.get("Close")
            if "Close" in row.index
            else row.get("Adj Close")
            if "Adj Close" in row.index
            else row.get("종가")
        )
        high = row.get("High") if "High" in row.index else row.get("고가")
        low = row.get("Low") if "Low" in row.index else row.get("저가")

        return {
            "base_close_date": str(row.name.date()),
            "base_close": _safe_float(close),
            "base_high": _safe_float(high),
            "base_low": _safe_float(low),
        }
    except Exception as e:
        logger.warning("[WARN] OHLC 조회 실패: %s %s", code, e)
        return {
            "base_close_date": None,
            "base_close": None,
            "base_high": None,
            "base_low": None,
        }


# -----------------------------------------------------------------
# Signals-only 리밸런서
# -----------------------------------------------------------------
@rebalance_router.post("/rebalance/run/{date}", tags=["Rebalance"])
async def run_rebalance(
    date: str,
    force_generate: bool = Query(
        False, description="강제 생성 (장중/장외 상관없이)"
    ),
) -> Dict[str, Any]:
    """Signals-only 리밸런서

    반환: { status, selected_count, signals(list), selected_stocks(list) }
    각 시그널 필드:
      - stock_code, name, best_k, base_close_date, base_close, base_high, base_low, meta

    파일 저장 형식:
      rebalance_results/rebalance_signals_{date}.json
      {
        "date": "YYYY-MM-DD",
        "selected": [...],
        "selected_stocks": [...]
      }
    """
    logger.info("[RUN] run_rebalance 호출: date=%s force_generate=%s", date, force_generate)

    # 1) Best-K 계산
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception("[ERROR] Best K 계산 실패: %s", e)
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    # 2) 결과 표준화 (code -> dict)
    results_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_results, dict):
        results_map = raw_results
    else:
        for s in raw_results or []:
            code_key = s.get("stock_code") or s.get("code") or s.get("티커")
            if not code_key:
                logger.warning("[WARN] 코드 누락: %s", s)
                continue
            s["stock_code"] = code_key
            results_map[code_key] = s

    logger.info("[FILTER] 후보 종목 수 = %d개", len(results_map))

    # 후보 -> signals 생성
    signals: List[Dict[str, Any]] = []
    for code, raw in results_map.items():
        info = dict(raw)
        name = info.get("name") or info.get("종목명") or ""
        best_k = _safe_float(info.get("best_k") or info.get("K") or info.get("k"))

        # 우선 raw에 이미 base OHLC가 있으면 사용, 없으면 조회
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
        # 숫자형 메타 정리
        for k in ("cumulative_return_pct", "win_rate_pct", "mdd_pct"):
            if k in meta:
                v = _safe_float(meta.get(k))
                if v is not None:
                    meta[k] = v

        signal = {
            "stock_code": code,
            "name": name,
            "best_k": best_k,
            "base_close_date": base_close_date,
            "base_close": _safe_float(base_close),
            "base_high": _safe_float(base_high),
            "base_low": _safe_float(base_low),
            "meta": meta,
        }
        signals.append(signal)

    # 캐시 및 파일 저장 (호환 키 포함)
    latest_rebalance_result.update(
        {"date": date, "signals": signals, "selected_stocks": signals}
    )

    out_path = os.path.join(REBALANCE_OUT_DIR, f"rebalance_signals_{date}.json")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(
                {"date": date, "selected": signals, "selected_stocks": signals},
                f,
                ensure_ascii=False,
                indent=2,
            )
        logger.info("[SAVE] %s 저장 (count=%d)", out_path, len(signals))
    except Exception as e:
        logger.exception("[SAVE_FAIL] JSON 저장 오류: %s", e)

    return {
        "status": "signals_generated",
        "selected_count": len(signals),
        "signals": signals,
        "selected_stocks": signals,
    }


# ──────────────────────────────────────────────────────────────
# 호환 엔드포인트 (/rebalance/generate?date=...)
# ──────────────────────────────────────────────────────────────
@rebalance_router.post("/rebalance/generate", tags=["Rebalance"])
async def generate(date: str = Query(..., description="YYYY-MM-DD")) -> Dict[str, Any]:
    """기존 워크플로 호환용: /rebalance/generate?date=YYYY-MM-DD"""
    # 내부적으로 run_rebalance 호출
    return await run_rebalance(date=date, force_generate=True)


# ──────────────────────────────────────────────────────────────
# 조회 엔드포인트
# ──────────────────────────────────────────────────────────────
@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance() -> Dict[str, Any]:
    """최근 run_rebalance 결과 캐시 반환"""
    return latest_rebalance_result


@rebalance_router.get(
    "/rebalance/selected/{date}", tags=["Rebalance"], response_class=JSONResponse
)
def get_selected_stocks(date: str) -> JSONResponse:
    """리밸런싱 실행 후 selected_stocks(시그널 리스트) 반환 (캐시→파일 순)"""
    # 메모리 캐시 우선
    if (
        latest_rebalance_result.get("date") == date
        and latest_rebalance_result.get("selected_stocks")
    ):
        selected = latest_rebalance_result.get("selected_stocks", [])
        return JSONResponse(
            content={"status": "ready", "rebalance_date": date, "selected": selected}
        )

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
                # data가 리스트이거나, dict에 selected/selected_stocks 키가 있을 수 있음
                if isinstance(data, list):
                    selected = data
                elif isinstance(data, dict):
                    selected = data.get("selected") or data.get("selected_stocks") or []
                else:
                    selected = []
                return JSONResponse(
                    content={
                        "status": "ready",
                        "rebalance_date": date,
                        "selected": selected,
                    }
                )
            except Exception as e:
                logger.exception("[LOAD_FAIL] %s 불러오기 실패: %s", fp, e)

    return JSONResponse(
        content={
            "status": "not_ready",
            "rebalance_date": date,
            "selected": [],
            "message": "먼저 /rebalance/run/{date} 실행 또는 파일 확인",
        }
    )


# ──────────────────────────────────────────────────────────────
# 기존 월간 백테스트 엔드포인트(원본 로직 유지/간소)
# ──────────────────────────────────────────────────────────────
@rebalance_router.get(
    "/rebalance/backtest-monthly", tags=["Rebalance"], response_class=JSONResponse
)
def rebalance_backtest_monthly(
    start_date: str = Query("2020-01-01"),
    end_date: str = Query("2024-04-01"),
) -> JSONResponse:
    # 날짜 형식 검증
    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
    except ValueError:
        return JSONResponse(content={"error": "날짜 형식 오류: YYYY-MM-DD"})

    try:
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))

        periods = pd.date_range(start=start_date, end=end_date, freq="MS")
        if len(periods) < 2:
            return JSONResponse(content={"error": "최소 두 개 월 이상 지정 필요"})

        all_results: List[pd.DataFrame] = []
        for i in range(len(periods) - 1):
            rebalance_dt = periods[i + 1]
            start_train = (rebalance_dt - pd.DateOffset(months=1)).strftime("%Y-%m-%d")
            end_train = (rebalance_dt - pd.DateOffset(days=1)).strftime("%Y-%m-%d")
            start_test = rebalance_dt.strftime("%Y-%m-%d")
            end_test = (rebalance_dt + pd.DateOffset(months=1) - pd.DateOffset(days=1)).strftime(
                "%Y-%m-%d"
            )

            selected: List[Dict[str, Any]] = []
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
                        start_price = float(test["Open"].iloc[0])
                        end_price = float(test["Close"].iloc[-1])
                        rtn_pct = (end_price / start_price - 1.0) * 100.0
                    except Exception as e:
                        logger.warning("[WARN] %s(%s) 수익률 계산 실패: %s", name, code, e)
                        continue

                    selected.append(
                        {
                            "code": code,
                            "name": name,
                            "수익률(%)": round(rtn_pct, 2),
                            "시작일": start_test,
                            "종료일": end_test,
                            "종가": int(end_price),
                        }
                    )
                except Exception as e:
                    logger.exception("[ERROR] %s(%s) 백테스트 중 예외: %s", name, code, e)
                    continue

            if not selected:
                logger.warning("[SKIP] %s : 조건 만족 종목 없음", rebalance_dt.strftime("%Y-%m"))
                continue

            df_sel = pd.DataFrame(selected)
            monthly_df = df_sel.sort_values("수익률(%)", ascending=False).head(20)
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100.0 / len(monthly_df), 2)
                all_results.append(monthly_df)

        if not all_results:
            return JSONResponse(content={"error": "조건 만족 종목 없음"})

        final_df = pd.concat(all_results, ignore_index=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        filepath = os.path.join(REBALANCE_OUT_DIR, filename)
        final_df.to_json(filepath, force_ascii=False, orient="records", indent=2)
        logger.info("[SAVE] 백테스트 결과 저장: %s (count=%d)", filename, len(final_df))
        return JSONResponse(
            content={"message": f"{len(final_df)}개 종목 리밸런싱 완료", "filename": filename}
        )

    except Exception as e:
        logger.exception("[ERROR] rebalance-backtest 예외: %s", e)
        return JSONResponse(status_code=500, content={"error": str(e)})
