import os
import json
import uuid
import logging
from datetime import datetime, time as dtime
from typing import Any, Dict, List, Optional

import pytz
from fastapi import APIRouter, Query, Request, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import numpy as np
from FinanceDataReader import StockListing, DataReader

from rolling_k_auto_trade_api.best_k_meta_strategy import get_best_k_for_kosdaq_topn
from rolling_k_auto_trade_api.logging_config import configure_logging

configure_logging()
logger = logging.getLogger(__name__)

rebalance_router = APIRouter()
latest_rebalance_result: Dict[str, Any] = {"date": None, "selected_stocks": []}

TOTAL_CAPITAL = int(os.getenv("TOTAL_CAPITAL", "10000000"))
MIN_WINRATE = float(os.getenv("MIN_WINRATE", "50"))
MAX_MDD = float(os.getenv("MAX_MDD", "10"))
MIN_CUMRET = float(os.getenv("MIN_CUMRET", "2"))
TOP_K_LIMIT = int(os.getenv("TOP_K_LIMIT", "20"))
REBALANCE_OUT_DIR = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
REBALANCE_STORE = os.getenv("REBALANCE_STORE", "./data/selected_stocks.json")

KST = pytz.timezone("Asia/Seoul")
MARKET_OPEN: dtime = dtime(9, 0)
MARKET_CLOSE: dtime = dtime(15, 20)

def is_market_open(ts: Optional[datetime] = None) -> bool:
    ts = ts or datetime.now(tz=KST)
    if ts.weekday() >= 5:
        return False
    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE

def _assign_weights(selected: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not selected:
        return []
    scores: List[float] = []
    for it in selected:
        try:
            win = float(it.get("win_rate_pct", it.get("승률(%)", 0))) / 100.0
            ret = float(it.get("avg_return_pct", it.get("수익률(%)", 0))) / 100.0
            mdd = abs(float(it.get("mdd_pct", it.get("MDD(%)", 0)))) / 100.0
        except Exception:
            win, ret, mdd = 0.5, 0.1, 0.1
        vol = float(it.get("prev_volume", 1))
        score = (0.6 * win + 0.6 * ret + 0.1 * np.log1p(vol)) / max(0.05, (0.4 * mdd))
        scores.append(max(0.0, score))
    s = sum(scores) or 1.0
    weights = [sc / s for sc in scores]
    out: List[Dict[str, Any]] = []
    for it, w in zip(selected, weights):
        o = dict(it)
        o["weight"] = round(float(w), 6)
        out.append(o)
    return out

def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

@rebalance_router.post("/rebalance/run/{date}", tags=["Rebalance"])
async def run_rebalance(date: str):
    logger.info(f"[RUN] run_rebalance 호출: date={date}")

    try:
        raw_results = get_best_k_for_kosdaq_topn(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    results_map: Dict[str, Dict[str, Any]] = {}
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

    candidates: List[Dict[str, Any]] = []
    selected: List[Dict[str, Any]] = []
    for code, info0 in results_map.items():
        info = dict(info0)
        candidates.append(info)
        try:
            cumret = float(info.get("cumulative_return_pct", info.get("수익률(%)", 0)))
            win = float(info.get("win_rate_pct", info.get("승률(%)", 0)))
            mdd = float(info.get("mdd_pct", info.get("MDD(%)", 0)))
        except Exception as e:
            logger.warning(f"[WARN] 수치 변환 실패: {code}: {e}")
            continue
        if (
            cumret > MIN_CUMRET and
            win > MIN_WINRATE and
            mdd <= MAX_MDD
        ):
            selected.append(info)
    # 거래량 내림차순 정렬
    candidates_with_vol = sorted(candidates, key=lambda x: x.get("prev_volume", 0), reverse=True)
    # === [anchor] 거래량 Top5(상승 종목만) 산출 및 강제편입 ===
    # 1. 거래량 상위 후보 중에서, 전일 '상승(양봉)' 종목만 필터
    rising_vol_candidates = [
        c for c in candidates_with_vol if c.get("prev_close", 0) > c.get("prev_open", 0)
    ]
    # 2. 거래량 기준 내림차순으로 Top5 추출
    top5_rising_vol = rising_vol_candidates[:5]
    # 3. 기존 selected에 없는 경우만 강제 편입
    codes_in_selected = {s["code"] for s in selected}
    for cand in top5_rising_vol:
        if cand["code"] not in codes_in_selected:
            selected.append(cand)


    if TOP_K_LIMIT and len(selected) > TOP_K_LIMIT:
        selected = sorted(
            selected,
            key=lambda x: (
                float(x.get("avg_return_pct", x.get("수익률(%)", 0))),
                float(x.get("win_rate_pct", x.get("승률(%)", 0))),
                -float(abs(x.get("mdd_pct", x.get("MDD(%)", 0))))
            ),
            reverse=True,
        )[:TOP_K_LIMIT]

    logger.info(f"[SELECTED] 필터 통과 종목 수 = {len(selected)}개")
    logger.debug(f"selected codes: {[s.get('code') or s.get('stock_code') for s in selected]}")

    if len(selected) == 0:
        latest_rebalance_result.update({"date": date, "selected_stocks": []})
        return {
            "status": "skipped",
            "reason": "no_qualified_candidates",
            "candidates": candidates,
            "selected": [],
        }

    has_weight = any("weight" in s for s in selected)
    if not has_weight:
        selected = _assign_weights(selected)

    enriched: List[Dict[str, Any]] = []
    for info in selected:
        code = info.get("stock_code") or info.get("code")
        name = info.get("name") or info.get("stock_name")
        row = dict(info)
        row.setdefault("code", code)
        row.setdefault("name", name)
        try:
            df = DataReader(code)
            if df is not None and len(df) >= 2:
                prev = df.iloc[-2]
                row["prev_open"] = float(prev.get("Open", 0))
                row["prev_high"] = float(prev.get("High", 0))
                row["prev_low"] = float(prev.get("Low", 0))
                row["prev_close"] = float(prev.get("Close", 0))
                row["prev_volume"] = float(prev.get("Volume", 0))
                row["prev_turnover"] = float(prev.get("Close", 0)) * float(prev.get("Volume", 0))
        except Exception as e:
            logger.warning(f"[REBAL] OHLC enrich fail {code}: {e}")
        enriched.append(row)

    latest_rebalance_result.update({"date": date, "selected_stocks": enriched})
    os.makedirs(REBALANCE_OUT_DIR, exist_ok=True)
    fp = os.path.join(REBALANCE_OUT_DIR, f"rebalance_{date}.json")
    try:
        with open(fp, "w", encoding="utf-8") as f:
            json.dump(enriched, f, ensure_ascii=False, indent=2)
        logger.info(f"[SAVE] {fp} 저장 (count={len(enriched)})")
    except Exception as e:
        logger.exception(f"[SAVE_FAIL] JSON 저장 오류: {e}")
        raise HTTPException(status_code=500, detail="리밸런스 결과 저장 실패")

    os.makedirs(os.path.dirname(REBALANCE_STORE), exist_ok=True)
    with open(REBALANCE_STORE, "w", encoding="utf-8") as f:
        json.dump({"date": date, "selected_stocks": enriched}, f, ensure_ascii=False, indent=2)

    return {
        "status": "saved",
        "selected_count": len(enriched),
        "selected_stocks": enriched,
        "store": REBALANCE_STORE,
        "out_file": fp,
        "total_capital_hint": TOTAL_CAPITAL,
    }

@rebalance_router.get("/rebalance/latest", tags=["Rebalance"])
def get_latest_rebalance():
    return latest_rebalance_result

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
    logger.info(f"[BACKTEST] 호출: {start_date}~{end_date}")
    ua = (request.headers.get("user-agent") or "").lower() if request else ""
    referer = (request.headers.get("referer") or "").lower() if request else ""
    is_curl = ("curl" in ua) or ("/docs" in referer)

    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date,   "%Y-%m-%d")
    except ValueError:
        return JSONResponse(content={"error": "날짜 형식 오류: YYYY-MM-DD"})

    try:
        top50_df = StockListing("KOSDAQ").sort_values("Marcap", ascending=False).head(50)
        tickers = list(zip(top50_df["Code"], top50_df["Name"]))
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
            if "수익률(%)" not in df.columns:
                logger.error(f"[ERROR] '수익률(%)' 컬럼 없음! keys={list(df.columns)}")
                continue
            monthly_df = df.sort_values("수익률(%)", ascending=False).head(20)
            if not monthly_df.empty:
                monthly_df["포트비중(%)"] = round(100 / len(monthly_df), 2)
                all_results.append(monthly_df)

        if not all_results:
            return JSONResponse(content={"error": "조건 만족 종목 없음"})

        final_df = pd.concat(all_results, ignore_index=True)
        filename = f"backtest_result_{uuid.uuid4().hex}.json"
        os.makedirs(REBALANCE_OUT_DIR, exist_ok=True)
        filepath = os.path.join(REBALANCE_OUT_DIR, filename)
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

@rebalance_router.get(
    "/rebalance/selected/{date}",
    tags=["Rebalance"],
    response_class=JSONResponse,
)
def get_selected_stocks(date: str):
    if latest_rebalance_result.get("date") == date:
        selected = latest_rebalance_result.get("selected_stocks", [])
        return {
            "status": "ready",
            "rebalance_date": date,
            "selected": selected
        }
    else:
        return {
            "status": "not_ready",
            "rebalance_date": date,
            "selected": [],
            "message": "먼저 /rebalance/run/{date} 엔드포인트로 리밸런싱을 실행하세요."
        }
# TOTAL_LINES: 320
