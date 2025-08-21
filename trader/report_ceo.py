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
    if "side" not in df.columns:
        df["side"] = None
    if "code" not in df.columns:
        df["code"] = None
    if "name" not in df.columns:
        df["name"] = None
    if "qty" not in df.columns:
        df["qty"] = 0
    if "price" not in df.columns:
        df["price"] = 0.0
    if "amount" not in df.columns:
        # 로그에 amount가 없을 경우 price * qty 로 추정
        df["amount"] = df.get("price", 0).fillna(0).astype(float) * df.get("qty", 0).fillna(0).astype(float)

    # 타입 정규화: 안전하게 숫자/날짜로 변환
    df["qty"] = pd.to_numeric(df["qty"], errors="coerce").fillna(0).astype(int)
    df["price"] = pd.to_numeric(df["price"], errors="coerce").fillna(0.0).astype(float)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0).astype(int)
    # datetime 파싱
    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")

    # 수익/수익률/매도단가/매도시간 컬럼 초기화
    df["수익"] = 0
    df["수익률"] = 0.0
    df["매도단가"] = None
    df["매도시간"] = None

    # 분리
    buy_df = df[df["side"] == "BUY"].copy().sort_values("datetime")
    sell_df = df[df["side"] == "SELL"].copy().sort_values("datetime")
    sell_df["_matched"] = False

    # 매수-매도 연결 로직:
    # 각 BUY 행에 대해, 같은 종목(code)이고 같은 수량(qty)이며 매수 시간 이후의 첫 번째 미매칭 SELL을 찾음.
    # (이 기본 전략은 체결 로그 포맷에 의존. 필요시 partial/분할 체결 처리 추가 가능)
    for buy_idx, buy_row in buy_df.iterrows():
        matched = False
        # 우선 정확히 (code, name, qty)로 찾기 (이름도 동일할 때)
        candidates = sell_df[
            (sell_df["_matched"] == False)
            & (sell_df["code"] == buy_row["code"])
            & (sell_df["qty"] == buy_row["qty"])
            & (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))
        ]
        if candidates.shape[0] == 0:
            # 이름 불일치/누락 등 대비: code + qty + 시간 조건만으로 시도
            candidates = sell_df[
                (sell_df["_matched"] == False)
                & (sell_df["code"] == buy_row["code"])
                & (sell_df["qty"] == buy_row["qty"])
                & (sell_df["datetime"] >= (buy_row["datetime"] if not pd.isna(buy_row["datetime"]) else pd.Timestamp.min))
            ]

        if candidates.shape[0] > 0:
            # 가장 이른 sell 선택
            sidx = candidates.index[0]
            sell_row = sell_df.loc[sidx]
            # 매칭 표시
            sell_df.at[sidx, "_matched"] = True
            # 계산
            sell_price = float(sell_row["price"])
            buy_price = float(buy_row["price"])
            qty = int(buy_row["qty"])
            profit = int(round((sell_price - buy_price) * qty))
            # 수익률 안전 계산
            try:
                profit_pct = round((sell_price - buy_price) / buy_price * 100, 2) if buy_price != 0 else 0.0
            except Exception:
                profit_pct = 0.0
            # 반영 (원본 df에도 반영)
            df.loc[buy_idx, "매도단가"] = sell_price
            df.loc[buy_idx, "매도시간"] = sell_row["datetime"]
            df.loc[buy_idx, "수익"] = profit
            df.loc[buy_idx, "수익률"] = profit_pct
            matched = True

        if not matched:
            # 매칭 실패 — 매도 없음: 수익/수익률은 기본값(0/0.0) 유지
            continue

    # buy_trades 기준으로 요약 계산
    buy_trades = df[df["side"] == "BUY"].copy()

    total_invest = int(buy_trades["amount"].sum()) if not buy_trades.empty else 0
    total_pnl = int(buy_trades["수익"].sum()) if not buy_trades.empty else 0
    win_rate = (buy_trades["수익"] > 0).mean() * 100 if len(buy_trades) > 0 else 0.0
    mdd = "-"  # MDD 계산은 필요시 여기에 추가 가능
    total_trade_count = int(len(df))
    symbol_count = int(buy_trades["code"].nunique()) if not buy_trades.empty else 0

    top_win = buy_trades.sort_values("수익", ascending=False).head(5)
    top_lose = buy_trades.sort_values("수익", ascending=True).head(3)

    종목별상세 = []
    for idx, row in buy_trades.iterrows():
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
            "전략설명": _to_native(row.get("strategy", "N/A"))
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

    리포트 = {
        "title": title,
        "전략설정": 전략설명,
        "요약": {
            "총투자금액": f"{total_invest:,}원",
            "실현수익": f"{total_pnl:,}원 ({round(total_pnl / total_invest * 100, 2) if total_invest else 0}%)",
            "MDD": mdd,
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
