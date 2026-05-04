"""trader/position_age.py

포지션 보유 기간 (days_held, holding_bars) 계산 — KST 거래일 기준.

거래일(월~금) 기준 일수를 사용하므로, 주말/공휴일에 count 가 증가하지 않는다.
공휴일 캘린더는 외부 의존성(pykrx) 없이 요일 기반으로만 풀어서 항상 동작한다.
pykrx 기반 공휴일 보정이 필요하면 `_count_trading_days_weekday_only`를 오버라이드한다.

Public API
----------
- to_kst_date(value) -> date | None
- normalize_ohlcv_dates(df) -> pd.DataFrame          # date 컬럼을 KST date 로 정규화
- calc_position_age(entry_ts, trade_date_kst, ohlcv_df) -> PositionAge
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Optional, Union

import pandas as pd
import pytz

logger = logging.getLogger(__name__)

KST = pytz.timezone("Asia/Seoul")

# ---------------------------------------------------------------------------
# Date normalisation helpers
# ---------------------------------------------------------------------------


def to_kst_date(value: Any) -> Optional[date]:
    """다양한 타입의 날짜 값을 KST date 로 변환."""
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = KST.localize(value)
        return value.astimezone(KST).date()
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        # ISO format "YYYY-MM-DD" or "YYYY-MM-DDT..."
        try:
            dt = datetime.fromisoformat(s)
            # timezone이 있으면 반드시 KST로 변환 후 date 추출
            if dt.tzinfo is not None:
                dt = dt.astimezone(KST)
            else:
                # timezone 정보가 없으면 KST로 localize
                dt = KST.localize(dt)
            return dt.date()
        except ValueError:
            pass
        # "YYYYMMDD"
        if len(s) == 8 and s.isdigit():
            try:
                return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
            except ValueError:
                pass
        return None
    if isinstance(value, (int, float)):
        # Unix timestamp
        try:
            return datetime.fromtimestamp(value, tz=KST).date()
        except (OSError, ValueError, OverflowError):
            return None
    # pd.Timestamp
    try:
        ts = pd.Timestamp(value)
        if ts.tzinfo is None:
            ts = ts.tz_localize("Asia/Seoul")
        return ts.tz_convert("Asia/Seoul").date()
    except Exception:
        return None


def normalize_ohlcv_dates(df: pd.DataFrame) -> pd.DataFrame:
    """OHLCV DataFrame의 날짜 인덱스 또는 'date' 컬럼을 KST date 로 정규화.

    - DatetimeIndex 또는 'date' / 'Date' 컬럼을 KST date (python date) 로 변환.
    - 원본 DataFrame을 복사하지 않고 새 객체를 반환한다.
    """
    df = df.copy()
    # 인덱스 처리
    if isinstance(df.index, pd.DatetimeIndex):
        dates = [to_kst_date(v) for v in df.index]
        df.index = pd.Index(dates, name=df.index.name or "date")
        return df
    # 'date' 컬럼 처리
    for col in ("date", "Date", "DATE"):
        if col in df.columns:
            df[col] = df[col].apply(to_kst_date)
            return df
    return df


# ---------------------------------------------------------------------------
# Trading-day counting (weekday only — Mon–Fri)
# ---------------------------------------------------------------------------


def _count_trading_days_weekday_only(start: date, end: date) -> int:
    """start 다음 날부터 end(포함)까지 월~금 일수를 센다.

    공휴일은 제외하지 않는다 (단순 요일 기반). 보수적으로 더 크게 나온다.
    """
    if end <= start:
        return 0
    count = 0
    cur = start + timedelta(days=1)
    while cur <= end:
        if cur.weekday() < 5:  # Mon=0, Fri=4
            count += 1
        cur += timedelta(days=1)
    return count


# ---------------------------------------------------------------------------
# PositionAge dataclass
# ---------------------------------------------------------------------------


@dataclass
class PositionAge:
    """포지션 보유 기간 정보."""

    entry_date_kst: Optional[str]     # "YYYY-MM-DD" KST 진입일
    trade_date_kst: str               # "YYYY-MM-DD" KST 기준 오늘
    days_held: int                    # KST 거래일 기준 보유 일수 (0 = 당일 진입)
    holding_bars: int                 # OHLCV 기준 봉 수 (ohlcv_df가 없으면 days_held 와 동일)
    post_entry_rows: int              # entry 이후 존재하는 OHLCV row 수
    quality: str                      # "ohlcv" | "trading_days" | "calendar"

    def as_dict(self) -> dict:
        return {
            "entry_date_kst": self.entry_date_kst,
            "trade_date_kst": self.trade_date_kst,
            "days_held": self.days_held,
            "holding_bars": self.holding_bars,
            "post_entry_rows": self.post_entry_rows,
            "quality": self.quality,
        }


_POSITION_AGE_FALLBACK = PositionAge(
    entry_date_kst=None,
    trade_date_kst="",
    days_held=0,
    holding_bars=0,
    post_entry_rows=0,
    quality="fallback",
)


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------


def calc_position_age(
    entry_ts: Any,
    trade_date_kst: Union[str, date],
    ohlcv_df: Optional[pd.DataFrame] = None,
) -> PositionAge:
    """포지션 보유 기간을 KST 기준으로 계산.

    Parameters
    ----------
    entry_ts : Any
        진입 timestamp. datetime, pd.Timestamp, ISO str, "YYYYMMDD" str, date 모두 허용.
    trade_date_kst : str | date
        오늘의 KST 거래일 ("YYYY-MM-DD" 또는 date 객체).
    ohlcv_df : pd.DataFrame | None
        일봉 OHLCV DataFrame (선택). DatetimeIndex 또는 'date' 컬럼 필요.
        제공 시 OHLCV 행 기준 holding_bars를 계산한다.

    Returns
    -------
    PositionAge
        days_held=0 이면 당일 진입.
    """
    today: Optional[date]
    if isinstance(trade_date_kst, date):
        today = trade_date_kst
    else:
        today = to_kst_date(str(trade_date_kst).strip())
    if today is None:
        logger.warning("[POSITION_AGE][BAD_TRADE_DATE] trade_date_kst=%s", trade_date_kst)
        return _POSITION_AGE_FALLBACK

    entry_date = to_kst_date(entry_ts)
    if entry_date is None:
        logger.warning("[POSITION_AGE][MISSING_ENTRY_TS] entry_ts=%s", entry_ts)
        return PositionAge(
            entry_date_kst=None,
            trade_date_kst=today.isoformat(),
            days_held=0,
            holding_bars=0,
            post_entry_rows=0,
            quality="calendar",
        )

    # Calendar days (fallback)
    calendar_days = max(0, (today - entry_date).days)

    # Trading days (weekday only)
    trading_days = _count_trading_days_weekday_only(entry_date, today)

    quality = "trading_days"
    holding_bars = trading_days
    post_entry_rows = 0

    # OHLCV-based holding_bars
    if ohlcv_df is not None and not ohlcv_df.empty:
        try:
            df_norm = normalize_ohlcv_dates(ohlcv_df)
            if "date" in df_norm.columns or "Date" in df_norm.columns:
                col = "date" if "date" in df_norm.columns else "Date"
                col_dates = df_norm[col].dropna().tolist()
                post_rows = [d for d in col_dates if d > entry_date]
                post_entry_rows = len(post_rows)
                holding_bars = post_entry_rows
                quality = "ohlcv"
            elif isinstance(df_norm.index, pd.Index) and len(df_norm.index) > 0:
                # 날짜 인덱스인 경우
                idx_dates = [d for d in df_norm.index if d is not None]
                post_rows = [d for d in idx_dates if d > entry_date]
                post_entry_rows = len(post_rows)
                holding_bars = post_entry_rows
                quality = "ohlcv"
        except Exception as exc:
            logger.debug("[POSITION_AGE][OHLCV_FAIL] err=%s", exc)

    result = PositionAge(
        entry_date_kst=entry_date.isoformat(),
        trade_date_kst=today.isoformat(),
        days_held=trading_days,
        holding_bars=holding_bars,
        post_entry_rows=post_entry_rows,
        quality=quality,
    )
    logger.debug(
        "[POSITION_AGE] entry=%s today=%s days_held=%s holding_bars=%s quality=%s",
        result.entry_date_kst,
        result.trade_date_kst,
        result.days_held,
        result.holding_bars,
        result.quality,
    )
    return result
