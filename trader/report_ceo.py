import os
import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

LOG_DIR = Path(__file__).parent / "logs"
REPORT_DIR = Path(__file__).parent / "reports"
REPORT_DIR.mkdir(exist_ok=True)


def load_trades(date: datetime) -> List[dict]:
    log_file = LOG_DIR / f"trades_{date.strftime('%Y-%m-%d')}.json"
    trades = []
    if log_file.exists():
        with open(log_file, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    trades.append(json.loads(line))
                except Exception:
                    continue
    return trades


def is_market_open(date: datetime) -> bool:
    # 장이 열렸는지 날짜 기준 체크 (한국 기준 평일 09:00~15:30)
    if date.weekday() >= 5:
        return False
    open_time = date.replace(hour=9, minute=0, second=0, microsecond=0)
    close_time = date.replace(hour=15, minute=30, second=0, microsecond=0)
    return open_time <= date <= close_time


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
        # ISO 형식으로 직렬화
        try:
            return val.isoformat()
        except Exception:
            return str(val)
    return val


def _fmt_pct(x: Optional[float], ndigits: int = 2) -> str:
    """None 안전 퍼센트 문자열 (소수 포함 % 표현, 0→'0%')"""
    if x is None:
        return "0%"
    try:
        return f"{round(x, ndigits)}%"
    except Exception:
        return "0%"


def ceo_report(date: Optional[datetime] = None, period: str = "daily") -> Dict[str, Any]:
    # period: daily, weekly, monthly 지원
    if not date:
        date = datetime.now()
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

    trades: List[dict] = []
    for d in trade_days:
        trades.extend(load_trades(d))

    market_status = is_market_open(date)
    if not trades:
        report_msg = "거래 내역 없음 (시장 미개장 또는 휴일/주말)" if not market_status else "거래 내역 없음 (장 열림/평일)"
        리포트 = {
            "title": title,
            "msg": report_msg,
            "전략설정": {
                "운영K": "종목별 전월 rolling K, 백테스트 기반 자동적용",
                "리밸런싱방식": "월초 리밸런싱 + 실시간 매수, 당일 장마감 익일 매도",
                "매수기준": "목표가 돌파시 실시간 시장가/지정가 매수",
                "매도기준": "당일 장마감 전 전량 매도",
            },
            "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            "market_open": market_status,
            "거래세부내역": []
        }
        filename = f"ceo_report_{period}_{date.strftime('%Y-%m-%d')}.json"
        with open(REPORT_DIR / filename, "w", encoding="utf-8") as f:
            json.dump(리포트, f, indent=2, ensure_ascii=False)
        return 리포트

    # DataFrame 생성 및 타입 정리
    df = pd.DataFrame(trades)

    # 필수 컬럼 보정 (없으면 기본값 채움)
    for col, default in [
        ("side", None),
        ("code", None),
        ("name", None),
        ("qty", 0),
        ("price", 0.0),
        ("amount", None),
        ("K", None),
        ("target_price", None),
        ("strategy", None),
        ("result", None),
        ("reason", None),
    ]:
        if col not in df.columns:
            df[col] = default

    # amount 없을 경우 price * qty 로 추정
    if df["amount"].isna().any():
        df["amount"] = df.get("price", 0).fillna(0).astype(float) * df.get("qty", 0).fillna(0).astype(float)

    # 타입 정규화
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0).astype(float)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(int)
    df["K"] = pd.to_numeric(df["K"], errors="coerce")
    df["target_price"] = pd.to_numeric(df["target_price"], errors="coerce")
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # 수익/수익률/매도단가/매도시간/청산사유 컬럼 초기화
    df["수익"] = 0
    df["수익률"] = 0.0
    df["매도단가"] = None
    df["매도시간"] = None
    df["청산사유"] = None

    # 분리
    buy_df = df[df["side"] == "BUY"].copy().sort_values("datetime")
    sell_df = df[df["side"] == "SELL"].copy().sort_values("datetime")
    sell_df["_matched"] = False

    # 매수-매도 연결 로직 (기본: 같은 종목, 같은 수량, 매수 이후 첫 SELL)
    for buy_idx, buy_row in buy_df.iterrows():
        matched = False
        # 1) 엄격: code + qty + 시간
        candidates = sell_df[
            (~sell_df["_matched"])
            & (sell_df["code"] == buy_row["code"])
            & (sell_df["qty"] == buy_row["qty"])
            & (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))
        ]
        # 2) 완화: code + 시간 (qty 불신뢰 로그 대비)
        if candidates.shape[0] == 0:
            candidates = sell_df[
                (~sell_df["_matched"])
                & (sell_df["code"] == buy_row["code"])
                & (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))
            ]

        if candidates.shape[0] > 0:
            sidx = candidates.index[0]
            sell_row = sell_df.loc[sidx]
            sell_df.at[sidx, "_matched"] = True

            sell_price = float(sell_row["price"])
            buy_price = float(buy_row["price"])
            qty = int(buy_row["qty"])
            profit = int(round((sell_price - buy_price) * qty))
            try:
                profit_pct = round((sell_price - buy_price) / buy_price * 100, 2) if buy_price != 0 else 0.0
            except Exception:
                profit_pct = 0.0

            df.loc[buy_idx, "매도단가"] = sell_price
            df.loc[buy_idx, "매도시간"] = sell_row["datetime"]
            df.loc[buy_idx, "수익"] = profit
            df.loc[buy_idx, "수익률"] = profit_pct
            # 청산사유(SELL의 reason 또는 result 사용)
            reason = sell_row.get("reason")
            if pd.isna(reason) or reason in (None, "", "None"):
                reason = sell_row.get("result")
            df.loc[buy_idx, "청산사유"] = None if pd.isna(reason) else str(reason)
            matched = True

        if not matched:
            continue  # 미매칭은 0으로 남김

    # buy_trades 기준 요약
    buy_trades = df[df["side"] == "BUY"].copy()

    total_invest = int(buy_trades["amount"].sum()) if not buy_trades.empty else 0
    total_pnl = int(buy_trades["수익"].sum()) if not buy_trades.empty else 0
    win_rate = (buy_trades["수익"] > 0).mean() * 100 if len(buy_trades) > 0 else 0.0
    total_trade_count = int(len(df))
    symbol_count = int(buy_trades["code"].nunique()) if not buy_trades.empty else 0

    # MDD (매칭된 매도시간 기준 누적 손익 커브로 계산)
    mdd_str = "-"
    try:
        realized = buy_trades.dropna(subset=["매도시간"]).copy()
        realized = realized.sort_values("매도시간")
        equity = realized["수익"].cumsum()
        if not equity.empty:
            running_max = equity.cummax()
            drawdown = equity - running_max
            mdd_abs = int(drawdown.min())  # 음수
            if total_invest > 0:
                mdd_pct = round((mdd_abs / total_invest) * 100, 2)
                mdd_str = f"{mdd_abs:,}원 ({mdd_pct}%)"
            else:
                mdd_str = f"{mdd_abs:,}원"
    except Exception:
        mdd_str = "-"

    top_win = buy_trades.sort_values("수익", ascending=False).head(5)
    top_lose = buy_trades.sort_values("수익", ascending=True).head(3)

    # ===== RK-Max 보강 지표 =====
    # 1) K 사용값 통계/빈도
    k_series = pd.to_numeric(buy_trades["K"], errors="coerce").dropna()
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
    slippage_series = None
    if "target_price" in buy_trades.columns:
        tp = pd.to_numeric(buy_trades["target_price"], errors="coerce")
        valid = (tp > 0) & (~buy_trades["price"].isna())
        slippage_series = ((buy_trades.loc[valid, "price"] - tp.loc[valid]) / tp.loc[valid]) * 100
    slip_summary = {
        "평균%": _to_native(round(slippage_series.mean(), 2)) if slippage_series is not None and not slippage_series.empty else None,
        "중앙%": _to_native(round(slippage_series.median(), 2)) if slippage_series is not None and not slippage_series.empty else None,
        "최대불리%": _to_native(round(slippage_series.max(), 2)) if slippage_series is not None and not slippage_series.empty else None,
        "최대유리%": _to_native(round(slippage_series.min(), 2)) if slippage_series is not None and not slippage_series.empty else None,
        "표본수": _to_native(int(slippage_series.shape[0])) if slippage_series is not None else 0,
    }

    # 3) 청산 사유 분포 및 승률
    reason_perf = []
    if "청산사유" in buy_trades.columns:
        tmp = buy_trades.dropna(subset=["청산사유"]).copy()
        if not tmp.empty:
            grp = tmp.groupby("청산사유")
            for reason, g in grp:
                cnt = int(g.shape[0])
                win = float((g["수익"] > 0).mean() * 100) if cnt > 0 else 0.0
                avg_ret = float(g["수익률"].mean()) if cnt > 0 else 0.0
                reason_perf.append({
                    "사유": _to_native(reason),
                    "건수": cnt,
                    "승률": _to_native(round(win, 1)),
                    "평균수익률%": _to_native(round(avg_ret, 2)),
                })
            reason_perf = sorted(reason_perf, key=lambda x: x["건수"], reverse=True)

    # 4) 전략별 성과
    strat_perf = []
    if "strategy" in buy_trades.columns and not buy_trades.empty:
        grp = buy_trades.groupby(buy_trades["strategy"].fillna("N/A"))
        for strat, g in grp:
            cnt = int(g.shape[0])
            pnl = int(g["수익"].sum())
            win = float((g["수익"] > 0).mean() * 100) if cnt > 0 else 0.0
            avg_ret = float(g["수익률"].mean()) if cnt > 0 else 0.0
            strat_perf.append({
                "전략": _to_native(strat),
                "건수": cnt,
                "실현손익": _to_native(pnl),
                "승률": _to_native(round(win, 1)),
                "평균수익률%": _to_native(round(avg_ret, 2)),
            })
        strat_perf = sorted(strat_perf, key=lambda x: x["실현손익"], reverse=True)

    # 5) 일일 자금 사용률(환경변수 DAILY_CAPITAL 기반)
    daily_capital_env = os.getenv("DAILY_CAPITAL")
    daily_capital_val = None
    daily_capital_usage = None
    if daily_capital_env is not None:
        try:
            daily_capital_val = int(float(daily_capital_env))
            if daily_capital_val > 0:
                daily_capital_usage = round((total_invest / daily_capital_val) * 100, 2)
        except Exception:
            daily_capital_val = None
            daily_capital_usage = None

    # 종목별 상세
    종목별상세 = []
    for idx, row in buy_trades.iterrows():
        # 개별 진입 슬리피지
        slip = None
        if not pd.isna(row.get("target_price")) and row.get("target_price") not in (None, 0):
            try:
                slip = round(((float(row.get("price")) - float(row.get("target_price"))) / float(row.get("target_price"))) * 100, 2)
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
            "수익률": f'{_to_native(row.get("수익률"))}%' if row.get("수익률") is not None else None,
            "실현손익": _to_native(row.get("수익")),
            "청산사유": _to_native(row.get("청산사유")),
            "전략설명": _to_native(row.get("strategy", "N/A")),
            "슬리피지%": _to_native(slip),
        })

    전략설명 = {
        "운영K": "종목별 전월 rolling K, 백테스트 기반 자동적용",
        "리밸런싱방식": "월초 리밸런싱 + 실시간 매수, 당일 장마감 익일 매도",
        "매수기준": "목표가 돌파시 실시간 시장가/지정가 매수",
        "매도기준": "당일 장마감 전 전량 매도",
    }

    # 거래세부내역: df -> records with native types
    records = []
    for _, r in df.iterrows():
        rec = {}
        for k, v in r.items():
            rec[k] = _to_native(v)
        records.append(rec)

    # 요약
    pnl_pct_total = round((total_pnl / total_invest) * 100, 2) if total_invest else 0
    리포트 = {
        "title": title,
        "전략설정": 전략설명,
        "요약": {
            "총투자금액": f"{total_invest:,}원",
            "실현수익": f"{total_pnl:,}원 ({pnl_pct_total}%)",
            "MDD": mdd_str,
            "승률": f"{round(win_rate, 1)}%",
            "체결종목수": int(symbol_count),
            "매매회수": int(total_trade_count)
        },
        "수익TOP": [
            {
                "종목명": _to_native(r["name"]),
                "K": _to_native(r.get("K", "-")),
                "매수": _to_native(r["price"]),
                "매도": _to_native(r.get("매도단가")),
                "수익률": f'{_to_native(r.get("수익률"))}%' if r.get("수익률") is not None else None,
                "매수시간": _to_native(r.get("datetime")),
                "매도시간": _to_native(r.get("매도시간"))
            } for _, r in top_win.iterrows()
        ],
        "손실TOP": [
            {
                "종목명": _to_native(r["name"]),
                "K": _to_native(r.get("K", "-")),
                "매수": _to_native(r["price"]),
                "매도": _to_native(r.get("매도단가")),
                "수익률": f'{_to_native(r.get("수익률"))}%' if r.get("수익률") is not None else None,
                "매수시간": _to_native(r.get("datetime")),
                "매도시간": _to_native(r.get("매도시간"))
            } for _, r in top_lose.iterrows()
        ],
        "종목별 상세": 종목별상세,
        "RKMAX_지표": {
            "K_요약": k_summary,
            "K_빈도_TOP": k_freq_top,
            "진입_슬리피지_요약": slip_summary,
            "청산사유_분포": reason_perf,
            "전략별_성과": strat_perf,
            "일일_자금사용률": _fmt_pct(daily_capital_usage) if daily_capital_usage is not None else None,
            "일일_자금한도": f"{daily_capital_val:,}원" if daily_capital_val is not None else None,
        },
        "거래세부내역": records,
        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "market_open": market_status
    }

    # 저장
    filename = f"ceo_report_{period}_{date.strftime('%Y-%m-%d')}.json"
    with open(REPORT_DIR / filename, "w", encoding="utf-8") as f:
        json.dump(리포트, f, indent=2, ensure_ascii=False)

    return 리포트


if __name__ == "__main__":
    today = datetime.now()
    print(ceo_report(today, period="daily"))   # 일간
    print(ceo_report(today, period="weekly"))  # 주간
    print(ceo_report(today, period="monthly")) # 월간
