# FILE: trader/report_ceo.py
from __future__ import annotations
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple

LOG_DIR = Path(__file__).parent / "logs"
REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)

# =============================================================
# 기본 로더/헬퍼
# =============================================================

def load_trades(date: datetime) -> List[dict]:
    """해당 일자의 trade 로그(json lines)를 모두 로드."""
    log_file = LOG_DIR / f"trades_{date.strftime('%Y-%m-%d')}.json"
    trades: List[dict] = []
    if log_file.exists():
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    trades.append(json.loads(line))
                except Exception:
                    continue
    return trades


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    v = os.getenv(name)
    if v is None or str(v).strip() == "":
        return default
    return str(v).strip()


def _parse_hhmm(hhmm: Optional[str], fallback: Tuple[int, int]) -> dtime:
    if not hhmm:
        return dtime(*fallback, 0)
    try:
        h, m = hhmm.split(":")
        return dtime(int(h), int(m), 0)
    except Exception:
        return dtime(*fallback, 0)


def is_market_open(now: datetime) -> bool:
    """한국 기준 평일 09:00~(SELL_FORCE_TIME 또는 15:20)."""
    if now.weekday() >= 5:
        return False
    open_t = dtime(9, 0)
    close_t = _parse_hhmm(_env("SELL_FORCE_TIME", "15:20"), (15, 20))
    return open_t <= now.time() <= close_t


def _to_native(val):
    """pandas / numpy 타입을 순수 python 타입으로 변환 (JSON 직렬화 안전)"""
    if pd.isna(val):
        return None
    if isinstance(val, (np.integer,)):
        return int(val)
    if isinstance(val, (np.floating,)):
        return float(val)
    if isinstance(val, (np.bool_, bool)):
        return bool(val)
    if isinstance(val, (pd.Timestamp, datetime)):
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    return val


def _pct_int(x: Optional[float]) -> str:
    """정수 % 문자열 (None→'0%')"""
    if x is None:
        return "0%"
    try:
        return f"{int(round(x))}%"
    except Exception:
        return "0%"


# =============================================================
# 전략/환경 설명 섹션 (옵션 B 반영)
# =============================================================

def _strategy_desc() -> Dict[str, str]:
    return {
        "운영K": "전월 rolling-K 최적값에 ATR20/60 블렌딩(RK-Max) 적용",
        "리밸런싱": "2주 무빙 윈도우, 매 거래일 08:50 스냅샷으로 Top 승률 10개 선정",
        "매수": "목표가(시가 + K×(전일 고-저) 보정) 돌파 시 지정가→시장가 폴백, 슬리피지 가드",
        "매도": (
            "TP1/TP2 분할익절, 트레일링 스탑, FAST_STOP(진입 5분내 -1%), "
            "ATR_STOP(1.5×ATR), TIME_STOP(13:00 손실 지속 시), 커트오프 강제전량매도"
        ),
        "집행": "일일 집행금 기준/가중치→수량 산출, 주문 재시도/레이트리밋/HashKey 적용",
    }


def _env_runtime_params() -> Dict[str, Any]:
    keys = [
        # 청산/리스크
        "PARTIAL1", "PARTIAL2", "TRAIL_PCT", "FAST_STOP", "ATR_STOP", "TIME_STOP_HHMM",
        # 스케줄
        "REBALANCE_ANCHOR", "REBALANCE_SNAPSHOT_TIME_KST", "MIDDAY_SNAPSHOT_TIME_KST",
        "SELL_FORCE_TIME", "ACTION_KILL_TIME",
        # 집행
        "DAILY_CAPITAL", "SLIPPAGE_ENTER_GUARD_PCT", "W_MAX_ONE", "W_MIN_ONE",
        # 선택 전략 변수
        "TOP_K", "LOOKBACK_DAYS", "SPREAD_REPRICE_PCT",
    ]
    out: Dict[str, Any] = {}
    for k in keys:
        out[k] = _env(k)
    return out


# =============================================================
# 리포트 본체
# =============================================================

def _match_trades(df: pd.DataFrame) -> pd.DataFrame:
    """BUY와 SELL을 시간순 매칭하여 수익/수익률/사유를 채운다 (옵션 B 호환)."""
    df = df.copy()
    # 필수 컬럼 보정
    for col, default in [
        ("side", None), ("code", None), ("name", None), ("qty", 0),
        ("price", 0.0), ("amount", None), ("K", None), ("target_price", None),
        ("strategy", None), ("result", None), ("reason", None), ("datetime", None)
    ]:
        if col not in df.columns:
            df[col] = default

    if df["amount"].isna().any():
        df["amount"] = df.get("price", 0).fillna(0).astype(float) * df.get("qty", 0).fillna(0).astype(float)

    # 타입 정규화
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0).astype(float)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(int)
    df["K"] = pd.to_numeric(df["K"], errors="coerce")
    df["target_price"] = pd.to_numeric(df["target_price"], errors="coerce")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # 결과 컬럼 초기화
    for c in ("수익", "수익률"):
        df[c] = 0.0 if c == "수익률" else 0
    df["매도단가"] = None
    df["매도시간"] = None
    df["청산사유"] = None

    buy_df = df[df["side"] == "BUY"].copy().sort_values("datetime")
    sell_df = df[df["side"] == "SELL"].copy().sort_values("datetime")
    sell_df["_matched"] = False

    for buy_idx, buy_row in buy_df.iterrows():
        # (1) 엄격: code + qty + 시간 + 체결가>0
        candidates = sell_df[(~sell_df["_matched"]) &
                             (sell_df["code"] == buy_row["code"]) &
                             (sell_df["qty"] == buy_row["qty"]) &
                             (sell_df["price"] > 0) &
                             (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))]
        # (2) 완화: code + 시간 + 체결가>0
        if candidates.shape[0] == 0:
            candidates = sell_df[(~sell_df["_matched"]) &
                                 (sell_df["code"] == buy_row["code"]) &
                                 (sell_df["price"] > 0) &
                                 (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))]
        if candidates.shape[0] == 0:
            continue

        sidx = candidates.index[0]
        sell_row = sell_df.loc[sidx]
        sell_df.at[sidx, "_matched"] = True

        sell_price = float(sell_row["price"]) or 0.0
        buy_price = float(buy_row["price"]) or 0.0
        qty = int(buy_row["qty"]) or 0

        profit = int(round((sell_price - buy_price) * qty))
        profit_pct = ((sell_price - buy_price) / buy_price * 100) if buy_price != 0 else 0.0

        df.loc[buy_idx, "매도단가"] = sell_price
        df.loc[buy_idx, "매도시간"] = sell_row["datetime"]
        df.loc[buy_idx, "수익"] = profit
        df.loc[buy_idx, "수익률"] = float(profit_pct)

        reason = sell_row.get("reason")
        if pd.isna(reason) or reason in (None, "", "None"):
            reason = sell_row.get("result")
        df.loc[buy_idx, "청산사유"] = None if pd.isna(reason) else str(reason)

    return df


def _rkmax_indicators(buy_trades: pd.DataFrame) -> Dict[str, Any]:
    # 1) K 사용값 통계/빈도
    k_series = pd.to_numeric(buy_trades.get("K"), errors="coerce").dropna()
    k_summary = {
        "평균": _to_native(round(k_series.mean(), 3)) if not k_series.empty else None,
        "중앙": _to_native(round(k_series.median(), 3)) if not k_series.empty else None,
        "최소": _to_native(round(k_series.min(), 3)) if not k_series.empty else None,
        "최대": _to_native(round(k_series.max(), 3)) if not k_series.empty else None,
    }
    k_freq_top = []
    if not k_series.empty:
        vc = k_series.round(3).value_counts().sort_values(ascending=False).head(5)
        for k, c in vc.items():
            k_freq_top.append({"K": _to_native(k), "거래건수": _to_native(c)})

    # 2) 진입 슬리피지(%): (매수가-목표가)/목표가 * 100
    slip_summary: Dict[str, Any] = {"평균%": None, "중앙%": None, "최대불리%": None, "최대유리%": None, "표본수": 0}
    if "target_price" in buy_trades.columns:
        tp = pd.to_numeric(buy_trades["target_price"], errors="coerce")
        valid = (tp > 0) & (~buy_trades["price"].isna())
        slippage_series = ((buy_trades.loc[valid, "price"] - tp.loc[valid]) / tp.loc[valid]) * 100
        if not slippage_series.empty:
            slip_summary = {
                "평균%": _to_native(int(round(slippage_series.mean()))),
                "중앙%": _to_native(int(round(slippage_series.median()))),
                "최대불리%": _to_native(int(round(slippage_series.max()))),
                "최대유리%": _to_native(int(round(slippage_series.min()))),
                "표본수": _to_native(int(slippage_series.shape[0])),
            }

    # 3) 청산 사유 분포 및 승률
    reason_perf: List[Dict[str, Any]] = []
    if "청산사유" in buy_trades.columns:
        tmp = buy_trades.dropna(subset=["청산사유"]).copy()
        if not tmp.empty:
            grp = tmp.groupby("청산사유")
            for reason, g in grp:
                cnt = int(g.shape[0])
                win = (g["수익"] > 0).mean() * 100 if cnt > 0 else 0.0
                avg_ret = g["수익률"].mean() if cnt > 0 else 0.0
                reason_perf.append({
                    "사유": _to_native(reason),
                    "건수": cnt,
                    "승률": _to_native(int(round(win))),
                    "평균수익률%": _to_native(int(round(avg_ret))),
                })
            reason_perf = sorted(reason_perf, key=lambda x: x["건수"], reverse=True)

    # 4) 전략별 성과
    strat_perf: List[Dict[str, Any]] = []
    if "strategy" in buy_trades.columns and not buy_trades.empty:
        grp = buy_trades.groupby(buy_trades["strategy"].fillna("N/A"))
        for strat, g in grp:
            cnt = int(g.shape[0])
            pnl = int(g["수익"].sum())
            win = (g["수익"] > 0).mean() * 100 if cnt > 0 else 0.0
            avg_ret = g["수익률"].mean() if cnt > 0 else 0.0
            strat_perf.append({
                "전략": _to_native(strat),
                "건수": cnt,
                "실현손익": _to_native(pnl),
                "승률": _to_native(int(round(win))),
                "평균수익률%": _to_native(int(round(avg_ret))),
            })
        strat_perf = sorted(strat_perf, key=lambda x: x["실현손익"], reverse=True)

    return {
        "K_요약": k_summary,
        "K_빈도_TOP": k_freq_top,
        "진입_슬리피지_요약": slip_summary,
        "청산사유_분포": reason_perf,
        "전략별_성과": strat_perf,
    }


def _carryover_stats(buys: pd.DataFrame, matched_mask: pd.Series) -> Dict[str, Any]:
    pending = buys[~matched_mask].copy()
    return {
        "미청산_건수": int(pending.shape[0]),
        "미청산_종목수": int(pending["code"].nunique()) if not pending.empty else 0,
        "미청산_목록": [
            {
                "code": _to_native(r.get("code")),
                "name": _to_native(r.get("name")),
                "qty": _to_native(r.get("qty")),
                "매수단가": _to_native(r.get("price")),
                "매수시간": _to_native(r.get("datetime")),
                "목표가": _to_native(r.get("target_price")),
                "K": _to_native(r.get("K")),
            }
            for _, r in pending.iterrows()
        ],
    }


def ceo_report(date: Optional[datetime] = None, period: str = "daily") -> Dict[str, Any]:
    if not date:
        date = datetime.now()

    # 기간 구성
    if period == "daily":
        trade_days = [date]
        title = f"{date.strftime('%Y-%m-%d')} Rolling K 실전 리포트 (일간)"
    elif period == "weekly":
        week_start = date - timedelta(days=date.weekday())
        trade_days = [week_start + timedelta(days=i) for i in range(7)]
        title = f"{week_start.strftime('%Y-%m-%d')}~{(week_start + timedelta(days=6)).strftime('%Y-%m-%d')} Rolling K 리포트 (주간)"
    elif period == "monthly":
        month_start = date.replace(day=1)
        next_month = (month_start + timedelta(days=32)).replace(day=1)
        trade_days = [month_start + timedelta(days=i) for i in range((next_month - month_start).days)]
        title = f"{month_start.strftime('%Y-%m')} Rolling K 리포트 (월간)"
    else:
        raise ValueError("period must be daily, weekly, monthly")

    # 로그 수집
    trades: List[dict] = []
    for d in trade_days:
        trades.extend(load_trades(d))

    market_status = is_market_open(datetime.now())
    runtime_params = _env_runtime_params()

    if not trades:
        report_msg = "거래 내역 없음 (시장 미개장 또는 휴일/주말)" if not market_status else "거래 내역 없음 (장 열림/평일)"
        리포트 = {
            "title": title,
            "msg": report_msg,
            "전략설정": _strategy_desc(),
            "운영파라미터": runtime_params,
            "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "market_open": market_status,
            "거래세부내역": []
        }
        filename = f"ceo_report_{period}_{date.strftime('%Y-%m-%d')}.json"
        with open(REPORT_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(리포트, f, indent=2, ensure_ascii=False)
        return 리포트

    # DataFrame 변환 및 매칭
    df = pd.DataFrame(trades)
    df = _match_trades(df)

    # BUY만 집계
    buy_trades = df[df["side"] == "BUY"].copy()

    # 집계값
    total_invest = int(buy_trades.get("amount", 0).sum()) if not buy_trades.empty else 0
    total_pnl = int(buy_trades.get("수익", 0).sum()) if not buy_trades.empty else 0
    win_rate = (buy_trades["수익"] > 0).mean() * 100 if len(buy_trades) > 0 else 0.0
    total_trade_count = int(len(df))
    symbol_count = int(buy_trades["code"].nunique()) if not buy_trades.empty else 0

    # MDD (실현 기준)
    mdd_str = "-"
    try:
        realized = buy_trades.dropna(subset=["매도시간"]).copy().sort_values("매도시간")
        equity = realized["수익"].cumsum()
        if not equity.empty:
            running_max = equity.cummax()
            drawdown = equity - running_max
            mdd_abs = int(drawdown.min())
            if total_invest > 0:
                mdd_pct = (mdd_abs / total_invest) * 100
                mdd_str = f"{mdd_abs:,}원 ({int(round(mdd_pct))}%)"
            else:
                mdd_str = f"{mdd_abs:,}원"
    except Exception:
        mdd_str = "-"

    # TOP/LOSE
    top_win = buy_trades.sort_values("수익", ascending=False).head(5)
    top_lose = buy_trades.sort_values("수익", ascending=True).head(3)

    # RK-Max 지표
    rkmax = _rkmax_indicators(buy_trades)

    # 일일 자금 사용률
    daily_capital_val = None
    daily_capital_usage = None
    if runtime_params.get("DAILY_CAPITAL"):
        try:
            daily_capital_val = int(float(runtime_params["DAILY_CAPITAL"]))
            if daily_capital_val > 0:
                daily_capital_usage = (total_invest / daily_capital_val) * 100
        except Exception:
            pass

    # 미청산(캐리오버) 집계: 매도단가가 비어있는 BUY
    carry_mask = buy_trades["매도단가"].isna()
    carry_stats = _carryover_stats(buy_trades, matched_mask=~carry_mask)

    # 강제/규칙 청산 카운트(품질 모니터링)
    def _count_reason(prefix: str) -> int:
        if "청산사유" not in buy_trades.columns:
            return 0
        return int(buy_trades[buy_trades["청산사유"].fillna("").str.contains(prefix)].shape[0])

    forced_cut = _count_reason("강제전량매도")
    time_stop = _count_reason("TIME_STOP")
    fast_stop = _count_reason("FAST_STOP")
    trail_cnt = _count_reason("TRAIL")
    atr_stop = _count_reason("ATR_STOP")
    tp_exits = _count_reason("TP")  # TP1/TP2 포함

    # 종목별 상세
    종목별상세: List[Dict[str, Any]] = []
    for _, row in buy_trades.iterrows():
        slip = None
        if not pd.isna(row.get("target_price")) and row.get("target_price") not in (None, 0):
            try:
                slip = ((float(row.get("price")) - float(row.get("target_price"))) / float(row.get("target_price"))) * 100
                slip = int(round(slip))
            except Exception:
                slip = None
        종목별상세.append({
            "code": _to_native(row.get("code")),
            "name": _to_native(row.get("name")),
            "K": _to_native(row.get("K", "-")),
            "목표가": _to_native(row.get("target_price", "-")),
            "매수수량": _to_native(row.get("qty")),
            "매수단가": _to_native(row.get("price")),
            "매수시간": _to_native(row.get("datetime")),
            "매도단가": _to_native(row.get("매도단가")),
            "매도시간": _to_native(row.get("매도시간")),
            "수익률": f"{int(round(_to_native(row.get('수익률')) or 0))}%",
            "실현손익": _to_native(row.get("수익")),
            "청산사유": _to_native(row.get("청산사유")),
            "전략설명": _to_native(row.get("strategy", "N/A")),
            "슬리피지%": _to_native(slip),
        })

    # 거래세부내역: df -> records
    records: List[Dict[str, Any]] = []
    for _, r in df.iterrows():
        rec = {k: _to_native(v) for k, v in r.items()}
        rec["수익률"] = int(round(rec.get("수익률") or 0)) if rec.get("수익률") is not None else 0
        records.append(rec)

    pnl_pct_total = (total_pnl / total_invest) * 100 if total_invest else 0

    리포트 = {
        "title": title,
        "전략설정": _strategy_desc(),
        "운영파라미터": runtime_params,
        "요약": {
            "총투자금액": f"{total_invest:,}원",
            "실현수익": f"{total_pnl:,}원 ({int(round(pnl_pct_total))}%)",
            "MDD": mdd_str,
            "승률": _pct_int(win_rate),
            "체결종목수": int(symbol_count),
            "매매횟수": int(total_trade_count),
            "미청산건수": carry_stats["미청산_건수"],
            "강제전량매도건수": forced_cut,
            "TIME_STOP건수": time_stop,
            "FAST_STOP건수": fast_stop,
            "TRAIL_EXIT건수": trail_cnt,
            "ATR_STOP건수": atr_stop,
            "TP_EXIT건수": tp_exits,
        },
        "수익TOP": [
            {
                "종목명": _to_native(r["name"]),
                "K": _to_native(r.get("K", "-")),
                "매수": _to_native(r["price"]),
                "매도": _to_native(r.get("매도단가")),
                "수익률": f"{int(round(_to_native(r.get('수익률')) or 0))}%",
                "매수시간": _to_native(r.get("datetime")),
                "매도시간": _to_native(r.get("매도시간"))
            }
            for _, r in top_win.iterrows()
        ],
        "손실TOP": [
            {
                "종목명": _to_native(r["name"]),
                "K": _to_native(r.get("K", "-")),
                "매수": _to_native(r["price"]),
                "매도": _to_native(r.get("매도단가")),
                "수익률": f"{int(round(_to_native(r.get('수익률')) or 0))}%",
                "매수시간": _to_native(r.get("datetime")),
                "매도시간": _to_native(r.get("매도시간"))
            }
            for _, r in top_lose.iterrows()
        ],
        "캐리오버": carry_stats,
        "종목별 상세": 종목별상세,
        "RKMAX_지표": {
            **rkmax,
            "일일_자금사용률": _pct_int(daily_capital_usage) if daily_capital_usage is not None else None,
            "일일_자금한도": f"{daily_capital_val:,}원" if daily_capital_val is not None else None,
        },
        "거래세부내역": records,
        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "market_open": market_status
    }

    filename = f"ceo_report_{period}_{date.strftime('%Y-%m-%d')}.json"
    with open(REPORT_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(리포트, f, indent=2, ensure_ascii=False)

    return 리포트


# 간편 실행
if __name__ == "__main__":
    today = datetime.now()
    print(json.dumps(ceo_report(today, period="daily"), ensure_ascii=False, indent=2))
    print(json.dumps(ceo_report(today, period="weekly"), ensure_ascii=False, indent=2))
    print(json.dumps(ceo_report(today, period="monthly"), ensure_ascii=False, indent=2))
