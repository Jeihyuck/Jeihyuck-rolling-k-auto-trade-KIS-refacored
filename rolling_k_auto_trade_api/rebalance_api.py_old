import os
import json
import uuid
import logging
import time as time_module
from datetime import datetime, time as dtime, timedelta
from typing import Any, Dict, List, Optional

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
latest_rebalance_result: Dict[str, Any] = {"date": None, "selected_stocks": []}

# 투자금/파라미터 환경변수화
TOTAL_CAPITAL = int(os.getenv("TOTAL_CAPITAL", "10000000"))  # 기본 1,000만원
MIN_QTY_PER_TICKET = int(os.getenv("MIN_QTY_PER_TICKET", "1"))
ALLOW_AFTER_HOURS_ENV = os.getenv("ALLOW_AFTER_HOURS", "0")

# 필터 파라미터(추가 보호)
MIN_WINRATE = float(os.getenv("MIN_WINRATE", "50"))     # 최소 승률(%)
MAX_MDD = float(os.getenv("MAX_MDD", "10"))             # 최대 허용 MDD(%)
MIN_CUMRET = float(os.getenv("MIN_CUMRET", "2"))         # 최소 누적수익률(%)
TOP_K_LIMIT = int(os.getenv("TOP_K_LIMIT", "20"))        # 상위 N개 컷(선택)

# ──────────────────────────────────────────────────────────────
# 1. KST 타임존 & 장 운영시간 헬퍼
# ──────────────────────────────────────────────────────────────
KST = pytz.timezone("Asia/Seoul")
# 실전 기준(안전 마진 포함): 09:00 ~ 15:20
MARKET_OPEN: dtime = dtime(9, 0)
MARKET_CLOSE: dtime = dtime(15, 20)


def is_market_open(ts: Optional[datetime] = None) -> bool:
    """현재 시각(기본=now KST)이 장중인지 여부"""
    ts = ts or datetime.now(tz=KST)
    if ts.weekday() >= 5:
        return False
    return MARKET_OPEN <= ts.time() <= MARKET_CLOSE


# ──────────────────────────────────────────────────────────────
# 내부 유틸: 비중 산출 (best_k 결과에 weight 없을 때 대비)
# ──────────────────────────────────────────────────────────────

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
        score = (0.6 * win + 0.6 * ret) / max(0.05, (0.4 * mdd))
        scores.append(max(0.0, score))
    s = sum(scores) or 1.0
    weights = [sc / s for sc in scores]
    out: List[Dict[str, Any]] = []
    for it, w in zip(selected, weights):
        o = dict(it)
        o["weight"] = round(float(w), 6)
        out.append(o)
    return out


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

    # 1) Best-K 계산 (weight/목표가 포함 반환)
    try:
        raw_results = get_best_k_for_kosdaq_50(date)
    except Exception as e:
        logger.exception(f"[ERROR] Best K 계산 실패: {e}")
        raise HTTPException(status_code=500, detail="Best K 계산 실패")

    # 2) 결과 통일 (code → info dict)
    results_map: Dict[str, Dict[str, Any]] = {}
    if isinstance(raw_results, dict):
        results_map = raw_results  # 이미 코드 맵 형태
    else:
        for s in raw_results:
            code_key = s.get("stock_code") or s.get("code") or s.get("티커")
            if not code_key:
                logger.warning(f"[WARN] 코드 누락: {s}")
                continue
            s["stock_code"] = code_key
            results_map[code_key] = s

    logger.info(f"[FILTER] 후보 종목 수 = {len(results_map)}개")

    # 후보/선택 분리 및 1차 품질 필터
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

    # 상위 K 컷(선택)
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

    # 3) 비중(Weight) 보정 — best_k가 weight를 제공하지 않으면 현지 계산
    has_weight = any("weight" in s for s in selected)
    if not has_weight:
        selected = _assign_weights(selected)

    # 4) 장중 여부 및 force 플래그
    now = datetime.now(tz=KST)
    allow_after = force_order or (ALLOW_AFTER_HOURS_ENV == "1")
    logger.info(f"[DEBUG] is_market_open={is_market_open(now)}, force_order={force_order}, allow_after={allow_after}")

    if not is_market_open(now) and not allow_after:
        logger.info("[SKIP] 장 종료 – 주문·잔고 로직 생략 (force_order=False)")
        return {
            "status": "skipped",
            "reason": "market_closed",
            "candidates": candidates,
            "selected": selected,   # 필터/비중 결과는 제공
        }

    # 5) 예수금 확인
    cash = inquire_cash_balance()
    if cash <= 0:
        logger.error(f"[REBALANCE_ABORT] 예수금 부족: {cash:,}원")
        return {"error": "예수금이 0원입니다. 모의투자 계좌에 예수금을 충전하세요."}

    logger.info(f"[REBALANCE] 시작예수금: {cash:,}원, 총 투자한도: {TOTAL_CAPITAL:,}원")

    # 6) 주문 실행 — 각 종목별 weight × TOTAL_CAPITAL
    results_list: List[Dict[str, Any]] = []

    for info in selected:
        code = info.get("stock_code") or info.get("code")
        if not code:
            continue
        target_px = (
            info.get("목표가") or info.get("target_price") or info.get("best_k_price") or info.get("buy_price")
        )
        if not target_px or float(target_px) <= 0:
            # 목표가 없으면 최근 종가 기준으로 보수적 진입가격 설정
            target_px = float(info.get("close", 0) or 0)
            if target_px <= 0:
                logger.warning(f"[SKIP] 목표가/종가 미정의: {code}")
                continue

        try:
            weight = float(info.get("weight", 0.0))
        except Exception:
            weight = 0.0

        # 최소 수량/최소 금액 방어
        budget = max(0, int(TOTAL_CAPITAL * weight))
        px = int(round(float(target_px)))
        qty = max(MIN_QTY_PER_TICKET, budget // max(px, 1))
        if qty <= 0:
            logger.info(f"[SKIP] {code}: weight={weight:.4f} 예산 부족")
            continue

        info["매수단가"] = px
        info["매수수량"] = qty
        info["size_budget"] = budget

        logger.info(f"[DEBUG] 주문예정 code={code}, price={px}, qty={qty}, weight={weight:.4f}, budget={budget}")

        # 주문 실행
        try:
            resp = send_order(code, qty=qty, price=px, side="buy")
            if not resp or not isinstance(resp, dict):
                info["order_status"] = "실패: 주문 API None/비 dict 응답"
                logger.error(f"[ORDER_FAIL] code={code}, resp is None/비 dict")
                results_list.append(info)
                continue

            # ODNO 필드로 주문번호 파싱
            ord_no = None
            if resp.get("output") and resp["output"].get("ODNO"):
                ord_no = resp["output"]["ODNO"]
            elif resp.get("ordNo"):
                ord_no = resp["ordNo"]

            info["order_response"] = resp
            if ord_no:
                info["order_status"] = f"접수번호={ord_no}, qty={qty}, price={px}"
            else:
                logger.error(f"[PARSE_FAIL] code={code}, resp missing ODNO: {resp}")
                info["order_status"] = "실패: 응답에서 주문번호(ODNO) 없음"

            logger.info(f"[ORDER] code={code}, qty={qty}, price={px}, ord_no={ord_no}")

            # 잔고 조회
            try:
                bal = inquire_balance(code)
                info["balance_after"] = bal
                logger.info(f"[BALANCE] code={code}, balance={bal}")
            except Exception:
                logger.exception(f"[BALANCE_FAIL] code={code}")

            # 체결 조회
            if ord_no:
                try:
                    fill = inquire_filled_order(ord_no)
                    info["fill_info"] = fill
                    logger.info(f"[FILL] ord_no={ord_no}, fill={fill}")
                except Exception:
                    logger.exception(f"[FILL_FAIL] ord_no={ord_no}")

        except Exception as e:
            info["order_status"] = f"실패: {e}"
            logger.exception(f"[ORDER_FAIL] code={code}, error={e}")

        results_list.append(info)
        time_module.sleep(float(os.getenv("ORDER_THROTTLE_SEC", "0.3")))

    logger.info(f"[ORDER_COUNT] 주문 시도 종목 수 = {len(results_list)}개")
    logger.info(f"[CANDIDATES_RAW] {json.dumps(candidates, ensure_ascii=False)}")
    logger.info(f"[SELECTED_RAW] {json.dumps(selected, ensure_ascii=False)}")

    # 7) 캐시 업데이트 및 파일 저장 - 반드시 selected 결과만!
    latest_rebalance_result.update({"date": date, "selected_stocks": results_list})
    out_dir = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
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
        out_dir = os.getenv("REBALANCE_OUT_DIR", "rebalance_results")
        os.makedirs(out_dir, exist_ok=True)
        filepath = os.path.join(out_dir, filename)
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


# ──────────────────────────────────────────────────────────────
# 5. selected_stocks만 반환하는 엔드포인트 (선택적)
# ──────────────────────────────────────────────────────────────
@rebalance_router.get(
    "/rebalance/selected/{date}",
    tags=["Rebalance"],
    response_class=JSONResponse,
)
def get_selected_stocks(date: str):
    """
    리밸런싱 실행 후 selected_stocks(실매매 종목 리스트)만 반환하는 API
    """
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
